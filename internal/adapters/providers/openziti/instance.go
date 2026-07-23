package openziti

import (
	"context"
	"fmt"
	"time"

	"github.com/rezarajan/platformctl/internal/adapters/providers/providerkit"
	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// instanceConfig is Provider(type: openziti).spec.configuration, decoded —
// see schemas/v1alpha1/fragments/provider/openziti.json (the shape-only
// mirror of this struct, docs/planning/08 E5) and
// docs/planning/03-resource-model-reference.md's openziti section.
type instanceConfig struct {
	// TargetNetworks are additional Docker networks the router container
	// joins so it can reach mediated Connections' targets while staying
	// off the shared/platform network those targets' consumers are on —
	// the explicit-declaration precedent docs/adr/023's peerNetwork sets
	// (this package's own doc comment: no generic runtime-port network
	// introspection exists to auto-discover a target's network,
	// docs/adr/023's own I1 closure note records the same limitation).
	TargetNetworks []string
	ControllerPort int
	RouterPort     int
}

func parseInstanceConfig(cfg provider.Provider) instanceConfig {
	ic := instanceConfig{ControllerPort: 1280, RouterPort: 3022}
	if v, ok := cfg.Configuration["controllerPort"]; ok {
		ic.ControllerPort = toInt(v, ic.ControllerPort)
	}
	if v, ok := cfg.Configuration["routerPort"]; ok {
		ic.RouterPort = toInt(v, ic.RouterPort)
	}
	if raw, ok := cfg.Configuration["targetNetworks"].([]any); ok {
		for _, n := range raw {
			if s, ok := n.(string); ok && s != "" {
				ic.TargetNetworks = append(ic.TargetNetworks, s)
			}
		}
	}
	return ic
}

func toInt(v any, def int) int {
	switch n := v.(type) {
	case int:
		return n
	case float64:
		return int(n)
	default:
		return def
	}
}

func controllerName(providerName string) string { return providerName + "-ctrl" }
func routerName(providerName string) string     { return providerName + "-router" }

// reconcileInstance bootstraps the controller and router containers and
// ensures the controller's default admin identity + router enrollment are
// in place. See this package's doc comment for the mechanism and the live
// verification it records.
func (p *Provider) reconcileInstance(ctx context.Context, req reconciler.Request) (status.Status, error) {
	res, rt := req.Resource, req.Runtime
	st := status.Status{}
	cfg, err := provider.FromEnvelope(res)
	if err != nil {
		return st, err
	}
	ic := parseInstanceConfig(cfg)
	creds, refName, err := providerkit.ResolveCredential(cfg, req.Secrets, "adminSecretRef", res.Metadata.Name)
	if err != nil {
		return st, err
	}
	username, password := creds["username"], creds["password"]
	if username == "" || password == "" {
		return st, fmt.Errorf("Provider %q (type: openziti): secretRef %q must carry keys \"username\" and \"password\"", res.Metadata.Name, refName)
	}

	providerName := naming.RuntimeObjectName(res)
	platformNetwork := providerkit.Network(cfg)
	labels := runtime.ManagedLabels(res.Metadata.Namespace, "Provider", providerName, providerName)
	if err := rt.EnsureNetwork(ctx, runtime.NetworkSpec{Name: platformNetwork, Labels: labels}); err != nil {
		return st, err
	}
	for _, n := range ic.TargetNetworks {
		if err := rt.EnsureNetwork(ctx, runtime.NetworkSpec{Name: n, Labels: labels}); err != nil {
			return st, err
		}
	}

	ctrlName := controllerName(providerName)
	if err := rt.EnsureVolume(ctx, runtime.VolumeSpec{Name: ctrlName + "-data", Labels: labels, Networks: []string{platformNetwork}}); err != nil {
		return st, err
	}
	ctrlState, err := rt.EnsureContainer(ctx, runtime.ContainerSpec{
		Name:     ctrlName,
		Image:    defaultControllerImage,
		Networks: []string{platformNetwork},
		Volumes:  []runtime.VolumeMount{{VolumeName: ctrlName + "-data", MountPath: "/ziti-controller"}},
		Env: map[string]string{
			"ZITI_BOOTSTRAP":               "true",
			"ZITI_BOOTSTRAP_CONFIG":        "true",
			"ZITI_BOOTSTRAP_DATABASE":      "true",
			"ZITI_BOOTSTRAP_PKI":           "true",
			"ZITI_CTRL_ADVERTISED_ADDRESS": ctrlName,
			"ZITI_CTRL_ADVERTISED_PORT":    fmt.Sprintf("%d", ic.ControllerPort),
			"ZITI_USER":                    username,
			"ZITI_PWD":                     password,
		},
		Ports:  []runtime.PortBinding{{ContainerPort: ic.ControllerPort, Audience: runtime.AudienceHost}},
		Labels: labels,
	})
	if err != nil {
		return st, err
	}

	// Reach the controller's Edge Management API through
	// runtime.EnsureReachable/WithReachable, never ctrlState.HostAddr:
	// HostAddr returns a real host address only where a port is published
	// to the host (Docker), and "" on a Kubernetes ClusterIP Service —
	// so a HostAddr-built client worked on Docker and silently produced
	// "https://" (empty host) on Kubernetes (found live against the shared
	// cluster). EnsureReachable is the substrate-neutral seam every other
	// provider dials its own admin API through (debezium's Connect
	// preflight, redpanda's admin): Docker resolves it to the published
	// port at ~zero cost, Kubernetes opens an ephemeral port-forward — the
	// adapter speaks only the ContainerRuntime port, no K8s-special path
	// (the archtest fence stays green).
	if err := waitControllerServing(ctx, rt, ctrlName, ic.ControllerPort); err != nil {
		return st, err
	}
	client, closeCtrl, err := dialController(ctx, rt, ctrlName, ic.ControllerPort)
	if err != nil {
		return st, err
	}
	defer func() { _ = closeCtrl() }()
	if err := client.Authenticate(ctx, username, password); err != nil {
		return st, fmt.Errorf("Provider %q (type: openziti): controller authentication: %w", res.Metadata.Name, err)
	}

	routerNm := routerName(providerName)
	routerID, enrollJWT, verified, err := client.upsertEdgeRouter(ctx, routerNm)
	if err != nil {
		return st, fmt.Errorf("Provider %q (type: openziti): ensure edge-router: %w", res.Metadata.Name, err)
	}

	if err := rt.EnsureVolume(ctx, runtime.VolumeSpec{Name: routerNm + "-data", Labels: labels, Networks: append([]string{platformNetwork}, ic.TargetNetworks...)}); err != nil {
		return st, err
	}
	routerSpec := runtime.ContainerSpec{
		Name:     routerNm,
		Image:    defaultRouterImage,
		Networks: append([]string{platformNetwork}, ic.TargetNetworks...),
		Volumes:  []runtime.VolumeMount{{VolumeName: routerNm + "-data", MountPath: "/ziti-router"}},
		Env: map[string]string{
			"ZITI_BOOTSTRAP":                 "true",
			"ZITI_CTRL_ADVERTISED_ADDRESS":   ctrlName,
			"ZITI_CTRL_ADVERTISED_PORT":      fmt.Sprintf("%d", ic.ControllerPort),
			"ZITI_ROUTER_ADVERTISED_ADDRESS": routerNm,
			"ZITI_ROUTER_PORT":               fmt.Sprintf("%d", ic.RouterPort),
			"ZITI_ROUTER_MODE":               "host",
		},
		// The router's own edge/link listener (both bound to ZITI_ROUTER_PORT,
		// router.yml's "edge" and "link.listeners[transport]" bindings) must be
		// declared, AudienceInternal — only other in-network containers/pods
		// (the per-Connection dial-side tunneler, other routers) ever dial it,
		// never this CLI process or an operator. Docker: a no-op on
		// reachability (ExposedPorts metadata only, docs/planning/08 F2 — the
		// Docker network already reaches every container port regardless of
		// publish status, so the controller's ZITI_ROUTER_ADVERTISED_ADDRESS ==
		// routerNm already resolved via Docker's embedded per-network DNS with
		// or without this). Kubernetes: without a declared port, EnsureContainer
		// skips creating this container's Service entirely
		// (kubernetes/container.go's ensureOneService: "len(desired.Spec.Ports)
		// == 0" -> no Service) — Kubernetes has no Docker-style "every
		// container name is DNS-resolvable regardless of exposed ports"
		// fallback, so ZITI_ROUTER_ADVERTISED_ADDRESS became a name nothing
		// could resolve, and the dial-side tunneler's SDK failed control-plane
		// connect with "no such host" before any circuit was ever attempted —
		// found live (K8s only) diagnosing
		// TestOpenZitiMediatedConnectionOnKubernetesEndToEnd's mid-handshake
		// EOF: the dial-side tunneler's own "no edge routers connected in
		// time" log, not a bind-side/target-reachability defect.
		Ports:  []runtime.PortBinding{{ContainerPort: ic.RouterPort, Audience: runtime.AudienceInternal}},
		Labels: labels,
	}
	if !verified {
		if enrollJWT == "" {
			return st, fmt.Errorf("Provider %q (type: openziti): edge-router %q has no enrollment JWT and is not yet verified", res.Metadata.Name, routerNm)
		}
		routerSpec.Env["ZITI_ENROLL_TOKEN"] = enrollJWT
	}
	if _, err := rt.EnsureContainer(ctx, routerSpec); err != nil {
		return st, err
	}

	// Settle to the STABLE, post-enrollment spec within this SAME reconcile
	// (docs/planning/02 §4.1's settle-before-Ready discipline — the same
	// pattern waitControllerServing/connection.go's waitMediatedServing
	// already apply in this package): ZITI_ENROLL_TOKEN above is a
	// one-time bootstrap fact, and upsertEdgeRouter's "already exists"
	// branch is a fresh REST read of the controller's live, async
	// isVerified flag every single time it's called. Leaving the
	// container's desired spec keyed to it would violate the CLAUDE.md
	// EnsureContainer idempotency bar ("a second call with the same spec
	// makes zero API calls") the moment the router finishes enrolling in
	// the background: a LATER, completely unrelated probe/drift/status
	// call recomputes routerSpec, now observes verified=true (no token),
	// and forces an unwanted Kubernetes Deployment rollout / Docker
	// container recreate — found live diagnosing
	// TestOpenZitiMediatedConnectionOnKubernetesEndToEnd: a `drift` call
	// moments after `apply` restarted the router (and, by the identical
	// mechanism, connection.go's dial-side tunneler), and the test's own
	// status assertion occasionally caught one mid-restart. Wait out the
	// router's real enrollment handshake here and re-converge to the
	// no-token spec before Reconcile returns Ready, so every later call
	// computes the exact spec this one already settled to.
	if !verified {
		if err := waitEdgeRouterVerified(ctx, client, routerNm); err != nil {
			return st, fmt.Errorf("Provider %q (type: openziti): edge-router %q did not verify: %w", res.Metadata.Name, routerNm, err)
		}
		stableSpec := routerSpec
		stableEnv := make(map[string]string, len(routerSpec.Env)-1)
		for k, v := range routerSpec.Env {
			if k != "ZITI_ENROLL_TOKEN" {
				stableEnv[k] = v
			}
		}
		stableSpec.Env = stableEnv
		if _, err := rt.EnsureContainer(ctx, stableSpec); err != nil {
			return st, err
		}
	}

	// The structural router-eligibility policies (client.go's
	// upsertCatchAllRouterPolicies doc comment) — without these, every
	// Dial through any service compiled below fails control-plane-side
	// with NO_EDGE_ROUTERS_AVAILABLE regardless of correct per-edge
	// authorization (found live at authorship time).
	if err := client.upsertCatchAllRouterPolicies(ctx, routerID); err != nil {
		return st, fmt.Errorf("Provider %q (type: openziti): %w", res.Metadata.Name, err)
	}

	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonMediationPlaneHealthy}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	// Never the admin password, never a private key — only host facts and
	// the (non-secret) router entity id (docs/adr/013 fingerprints-only
	// discipline, applied to the whole adapter, not only identity.go).
	st.ProviderState = map[string]any{
		"controllerContainerId": ctrlState.ID,
		// The in-cluster/in-network address the router dials — meaningful
		// on BOTH runtimes (Docker network DNS / Kubernetes Service DNS),
		// unlike a host address which is "" on a K8s ClusterIP. The
		// host-audience reachable address is ephemeral (a fresh
		// port-forward per call on K8s), so it is never persisted.
		"controllerInternal": fmt.Sprintf("%s:%d", ctrlName, ic.ControllerPort),
		"routerId":           routerID,
	}
	return st, nil
}

// dialController opens a substrate-neutral reachable tunnel to the
// controller's Edge Management API and returns a REST client bound to it
// plus the tunnel's close func (a no-op on Docker, a port-forward teardown
// on Kubernetes) — callers defer the close for the operation's duration.
func dialController(ctx context.Context, rt runtime.ContainerRuntime, ctrlName string, port int) (*edgeClient, func() error, error) {
	addr, closeAddr, err := rt.EnsureReachable(ctx, ctrlName, port)
	if err != nil {
		return nil, nil, fmt.Errorf("openziti: reach controller %q: %w", ctrlName, err)
	}
	return newEdgeClient("https://" + addr), closeAddr, nil
}

// waitControllerServing bounded-polls the controller's REST API until it
// answers, re-resolving reachability on every attempt via
// runtime.WithReachable — the "fresh tunnel per attempt" discipline
// (docs/planning/09 Class 2, runtime.WithReachable's own doc): a K8s
// port-forward opened while the controller pod is still starting can go
// permanently dead, so a held tunnel across the whole wait would hang; a
// re-resolved one recovers the moment the pod is ready.
func waitControllerServing(ctx context.Context, rt runtime.ContainerRuntime, ctrlName string, port int) error {
	opts := runtime.ReachableOptions{Timeout: 90 * time.Second, Interval: 2 * time.Second}
	return runtime.WithReachable(ctx, rt, ctrlName, port, opts, func(ctx context.Context, addr string) error {
		return newEdgeClient("https://" + addr).Version(ctx)
	})
}

// waitEdgeRouterVerified bounded-polls the controller (already-authenticated
// client, held across the whole wait — unlike waitControllerServing this
// isn't dialing through a per-attempt tunnel, just re-issuing the same REST
// call) until routerNm's edge-router entity reports isVerified: the router
// container's own self-enrollment handshake completing in the background
// after reconcileInstance's first EnsureContainer call created it carrying a
// one-time enrollment token. See reconcileInstance's settle comment for why
// this must complete, and the resulting spec re-converge to omit that token,
// before Reconcile returns Ready.
func waitEdgeRouterVerified(ctx context.Context, client *edgeClient, routerNm string) error {
	deadline := time.Now().Add(runtime.ScaledWait(90 * time.Second))
	for {
		_, _, verified, err := client.upsertEdgeRouter(ctx, routerNm)
		if err != nil {
			return err
		}
		if verified {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("edge-router %q did not verify within the settle window", routerNm)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func (p *Provider) probeInstance(ctx context.Context, req reconciler.Request) (status.Status, error) {
	st, err := p.reconcileInstance(ctx, req)
	if err != nil {
		st = status.Status{}
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonMediationPlaneUnhealthy, Message: err.Error()}, time.Now())
		return st, err
	}
	return st, nil
}

func (p *Provider) destroyInstance(ctx context.Context, req reconciler.Request) error {
	res, rt := req.Resource, req.Runtime
	providerName := naming.RuntimeObjectName(res)
	cfg, err := provider.FromEnvelope(res)
	if err != nil {
		return err
	}
	ic := parseInstanceConfig(cfg)
	ctrlName := controllerName(providerName)
	routerNm := routerName(providerName)

	// Remove the edge-router entity from the controller's own state before
	// tearing down the containers that back it — "destroy removes
	// identities/policies cleanly" (docs/planning/08 H6 accept) applies to
	// the router entity too, not only the per-Connection identities
	// destroyConnection revokes. Best-effort: a controller already gone
	// (partially-applied manifest, prior failed destroy) means nothing to
	// clean up REST-side — idempotent no-op, matching Remove's own
	// "already gone is success" contract.
	if sess, serr := newSession(ctx, req); serr == nil {
		if routerID, _, _, rerr := sess.client.upsertEdgeRouter(ctx, routerNm); rerr == nil {
			_ = sess.client.deleteEdgeRouter(ctx, routerID)
		}
	}

	if err := rt.Remove(ctx, routerNm); err != nil {
		return err
	}
	if err := rt.Remove(ctx, ctrlName); err != nil {
		return err
	}
	if err := rt.RemoveVolume(ctx, routerNm+"-data"); err != nil {
		return err
	}
	if err := rt.RemoveVolume(ctx, ctrlName+"-data"); err != nil {
		return err
	}
	_ = ic
	return nil
}
