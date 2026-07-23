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

	client := newEdgeClient(fmt.Sprintf("https://%s", ctrlState.HostAddr(ic.ControllerPort)))
	if err := waitControllerServing(ctx, client); err != nil {
		return st, err
	}
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
		"controllerAddr":        ctrlState.HostAddr(ic.ControllerPort),
		"routerId":              routerID,
	}
	return st, nil
}

func waitControllerServing(ctx context.Context, client *edgeClient) error {
	deadline := time.Now().Add(runtime.ScaledWait(60 * time.Second))
	var lastErr error
	for {
		if lastErr = client.Version(ctx); lastErr == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("openziti controller did not answer within timeout: %w", lastErr)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
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
