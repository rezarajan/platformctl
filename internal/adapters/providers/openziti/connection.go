package openziti

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/rezarajan/platformctl/internal/adapters/providers/providerkit"
	"github.com/rezarajan/platformctl/internal/application/graphaccess"
	"github.com/rezarajan/platformctl/internal/domain/connection"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/graph"
	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/mediation"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// mediationCapable reports whether provEnv is THIS provider type — the
// graphaccess.MediationCapable predicate this adapter supplies itself
// (internal/application/graphaccess's own doc comment: the predicate keeps
// that package layering-clean; here, self-recognition is trivially exact
// since only an "openziti" Provider realizes a MediatedConnection at all).
func mediationCapable(provEnv resource.Envelope) bool {
	t, _ := provEnv.Spec["type"].(string)
	return t == "openziti"
}

// compileMediatedConnection rebuilds the declared graph from req.Resources
// (already-validated, so Build cannot fail here in practice — validate/plan
// built the identical graph upstream) and returns the MediatedConnection
// entry for res, if any. Recomputing is cheap and keeps this adapter fully
// self-contained (docs/adr/027 requirement #3: "zero provider edits outside
// your new adapter" — no reconciler.Request field, no engine call site,
// needed for this).
func compileMediatedConnection(res resource.Envelope, resources map[resource.Key]resource.Envelope) (graphaccess.MediatedConnection, bool, error) {
	envs := make([]resource.Envelope, 0, len(resources))
	for _, e := range resources {
		envs = append(envs, e)
	}
	g, err := graph.Build(envs)
	if err != nil {
		return graphaccess.MediatedConnection{}, false, fmt.Errorf("openziti: rebuild graph: %w", err)
	}
	mcs := graphaccess.CompileMediatedConnections(g, resources, mediationCapable)
	for _, mc := range mcs {
		if mc.Connection == res.Key() {
			return mc, true, nil
		}
	}
	return graphaccess.MediatedConnection{}, false, nil
}

func tunnelContainerName(res resource.Envelope) string { return naming.RuntimeObjectName(res) }

func (p *Provider) reconcileConnection(ctx context.Context, req reconciler.Request) (status.Status, error) {
	res, rt := req.Resource, req.Runtime
	st := status.Status{}
	conn, err := connection.FromEnvelope(res)
	if err != nil {
		return st, err
	}
	if conn.External {
		return st, fmt.Errorf("Connection %q: openziti does not realize external connections", res.Metadata.Name)
	}
	provCfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return st, err
	}
	ic := parseInstanceConfig(provCfg)
	providerName := naming.RuntimeObjectName(req.Provider)
	platformNetwork := providerkit.Network(provCfg)

	sess, err := newSession(ctx, req)
	if err != nil {
		return st, err
	}
	defer func() { _ = sess.Close() }()

	mc, found, err := compileMediatedConnection(res, req.Resources)
	if err != nil {
		return st, err
	}

	// The "To" identity subject: the declared target resource's identity
	// when the graph resolves one in-set (the mediated Postgres source,
	// this task's accept scenario), else the Connection's own identity —
	// a genuinely external target still gets a service, just not a
	// second, distinct identity subject (openziti.go's doc comment).
	toEnv := res
	if found && len(mc.Targets) > 0 {
		if t, ok := req.Resources[mc.Targets[0]]; ok {
			toEnv = t
		}
	}
	toURI := naming.WorkloadIdentityURI(toEnv)
	svcID, err := sess.client.upsertService(ctx, identityRoleAttribute(toURI))
	if err != nil {
		return st, fmt.Errorf("Connection %q: ensure Ziti service: %w", res.Metadata.Name, err)
	}

	// Bind side: a router-hosted, raw-TCP terminator (this package's
	// "Mechanism summary" doc comment) — the router reaches conn.Target
	// only via configuration.targetNetworks, so the real backend stays
	// dark on the platform/shared network every consumer is on.
	routerNm := routerName(providerName)
	routerID, _, _, err := sess.client.upsertEdgeRouter(ctx, routerNm)
	if err != nil {
		return st, fmt.Errorf("Connection %q: resolve router: %w", res.Metadata.Name, err)
	}
	if err := sess.client.upsertTransportTerminator(ctx, svcID, routerID, conn.Target); err != nil {
		return st, fmt.Errorf("Connection %q: ensure terminator: %w", res.Metadata.Name, err)
	}

	// Dial side: ADR 026 per-edge authorization — exactly the consumers
	// the declared graph names, nothing wider.
	var consumerEnvs []resource.Envelope
	if found {
		for _, ck := range mc.Consumers {
			if e, ok := req.Resources[ck]; ok {
				consumerEnvs = append(consumerEnvs, e)
			}
		}
	}
	// mintIdentityWithToken (not the exported MintIdentity) is used for
	// EVERY consumer here, not just the one whose JWT the dial-side
	// container needs below: upsertIdentity's idempotency contract means a
	// SECOND mint call for the same identity returns an empty JWT
	// (client.go's own doc comment) — calling the exported, token-less
	// MintIdentity first and mintIdentityWithToken again right after would
	// always observe the token-less "already exists" branch on the second
	// call, silently starving the dial-side container of its one-time
	// enrollment JWT (found live: the container never enrolled). Minting
	// once, capturing the token when this is the container's own
	// identity (index 0), avoids the double-mint entirely.
	var dialJWT string
	for i, c := range consumerEnvs {
		fromIdentity, _, jwt, err := sess.mintIdentityWithToken(ctx, c)
		if err != nil {
			return st, fmt.Errorf("Connection %q: mint identity for consumer %s: %w", res.Metadata.Name, c.Key(), err)
		}
		if i == 0 {
			dialJWT = jwt
		}
		edge := mediation.Edge{
			From:       fromIdentity,
			To:         mediation.WorkloadIdentity{URI: toURI},
			Authorized: mediation.DialBind{Dial: true},
		}
		if err := sess.RealizeEdge(ctx, edge); err != nil {
			return st, fmt.Errorf("Connection %q: realize edge %s -> %s: %w", res.Metadata.Name, fromIdentity.URI, toURI, err)
		}
	}

	// The dial-side tunneler container: one per Connection (docs/adr/023's
	// precedent), enrolled under the FIRST consumer's identity when at
	// least one is declared. More than one consumer per mediated
	// Connection sharing one dial-side container is the same recorded
	// limitation docs/adr/023 Decision 4 accepted for wireguard's own
	// per-Provider (there, per-Connection) identity sharing — not
	// exercised by this task's accept scenario (one Binding per mediated
	// Connection), a follow-up otherwise.
	name := tunnelContainerName(res)
	labels := runtime.ManagedLabels(res.Metadata.Namespace, res.Kind, name, name)
	if err := rt.EnsureNetwork(ctx, runtime.NetworkSpec{Name: platformNetwork, Labels: labels}); err != nil {
		return st, err
	}
	if err := rt.EnsureVolume(ctx, runtime.VolumeSpec{Name: name + "-identity", Labels: labels, Networks: []string{platformNetwork}}); err != nil {
		return st, err
	}

	spec := runtime.ContainerSpec{
		Name:       name,
		Image:      defaultTunnelImage,
		Entrypoint: nil,
		Cmd:        []string{"proxy", fmt.Sprintf("%s:%d", identityRoleAttribute(toURI), conn.Port)},
		Networks:   []string{platformNetwork},
		Volumes:    []runtime.VolumeMount{{VolumeName: name + "-identity", MountPath: "/netfoundry"}},
		Env: map[string]string{
			"ZITI_IDENTITY_BASENAME": "ziti_id",
		},
		Ports:  []runtime.PortBinding{{ContainerPort: conn.Port, Audience: runtime.AudienceHost}},
		Labels: labels,
	}
	if dialJWT != "" {
		spec.Env["ZITI_ENROLL_TOKEN"] = dialJWT
	}

	ctrState, err := rt.EnsureContainer(ctx, spec)
	if err != nil {
		return st, err
	}

	// Settle to the STABLE, post-enrollment spec within this SAME
	// reconcile — the same discipline instance.go's reconcileInstance
	// applies to the router container, for the identical reason:
	// upsertIdentity's idempotency contract (client.go, mintIdentityWithToken's
	// own doc comment) means every LATER reconcile, by definition, finds
	// this consumer's identity already existing and gets back an empty
	// enrollment JWT — unlike the router's isVerified flag this needs no
	// bounded wait (identity existence, not a live async handshake, is
	// what upsertIdentity's "already exists" branch keys on: a second
	// mint call for the SAME identity returns empty immediately, whether
	// issued a microsecond or a day later). Leaving the container's
	// desired spec keyed to the one-time token would violate the
	// CLAUDE.md EnsureContainer idempotency bar the instant any later
	// probe/drift/status call recomputes it — found live coupled with the
	// router's own analogous churn (instance.go's settle comment has the
	// full account): TestOpenZitiMediatedConnectionOnKubernetesEndToEnd's
	// post-apply drift restarted this dial-side tunneler mid-test.
	if dialJWT != "" {
		stableSpec := spec
		stableEnv := make(map[string]string, len(spec.Env)-1)
		for k, v := range spec.Env {
			if k != "ZITI_ENROLL_TOKEN" {
				stableEnv[k] = v
			}
		}
		stableSpec.Env = stableEnv
		if ctrState, err = rt.EnsureContainer(ctx, stableSpec); err != nil {
			return st, err
		}
	}

	// Ready means serving NOW (docs/planning/02 §4.1 NFR-11), not merely
	// "the container started": the mediated path's first connection(s)
	// through a freshly-enrolled Ziti circuit can be flaky for a short
	// warm-up window (found live at authorship time — a raw TCP dial
	// through the dial-side tunneler succeeded once the circuit had
	// settled, failed moments earlier) — settle-poll a real dial through
	// the mediated entrypoint before declaring Ready, the same bar
	// docs/adr/023's waitTunnelServing holds for wireguard's own forwarder.
	// Dials through runtime.WithReachable so the settle-check works on both
	// runtimes: a published-port dial on Docker, an ephemeral port-forward
	// on Kubernetes (ctrState.HostAddr is "" for a K8s ClusterIP, so a
	// HostAddr-based dial would settle-fail every K8s apply).
	if err := waitMediatedServing(ctx, rt, name, conn.Port); err != nil {
		return st, fmt.Errorf("Connection %q: mediated entrypoint did not settle: %w", res.Metadata.Name, err)
	}

	hostAddr := ctrState.HostAddr(conn.Port)
	host, port := conn.Endpoint(name)
	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonMediatedEdgeReady}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	st.ProviderState = map[string]any{
		"containerId": ctrState.ID,
		"serviceId":   svcID,
		"target":      conn.Target,
		endpoint.Key: endpoint.List{
			{Name: "mediated", Scheme: conn.Scheme, Host: hostAddr, Internal: fmt.Sprintf("%s:%d", host, port), Insecure: false, RuntimeName: name, ContainerPort: conn.Port, Audience: runtime.AudienceHost, Network: platformNetwork},
		}.ToState(),
	}
	_ = ic
	return st, nil
}

func (p *Provider) probeConnection(ctx context.Context, req reconciler.Request) (status.Status, error) {
	st, err := p.reconcileConnection(ctx, req)
	if err != nil {
		st = status.Status{}
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonMediatedEdgeNotReady, Message: err.Error()}, time.Now())
		return st, err
	}
	return st, nil
}

func (p *Provider) destroyConnection(ctx context.Context, req reconciler.Request) error {
	res, rt := req.Resource, req.Runtime
	name := tunnelContainerName(res)
	if err := rt.Remove(ctx, name); err != nil {
		return err
	}
	if err := rt.RemoveVolume(ctx, name+"-identity"); err != nil {
		return err
	}

	// Revoke consumer identities/edges this Connection realized — teardown
	// must not leave dangling policy (mediation.MediationProvider's own
	// doc comment; docs/planning/08 H6 accept).
	sess, err := newSession(ctx, req)
	if err != nil {
		// The controller/provider may already be gone (Destroy ordering
		// removes the Provider after its Connections, docs/planning/08
		// §2.1, but a partially-applied manifest can still hit this) —
		// idempotent no-op rather than a hard failure, matching
		// ContainerRuntime.Remove's "already gone is success" contract.
		return nil
	}
	defer func() { _ = sess.Close() }()
	mc, found, err := compileMediatedConnection(res, req.Resources)
	if err != nil || !found {
		return nil
	}
	toEnv := res
	if len(mc.Targets) > 0 {
		if t, ok := req.Resources[mc.Targets[0]]; ok {
			toEnv = t
		}
	}
	toURI := naming.WorkloadIdentityURI(toEnv)
	for _, ck := range mc.Consumers {
		c, ok := req.Resources[ck]
		if !ok {
			continue
		}
		fromURI := naming.WorkloadIdentityURI(c)
		edge := mediation.Edge{From: mediation.WorkloadIdentity{URI: fromURI}, To: mediation.WorkloadIdentity{URI: toURI}, Authorized: mediation.DialBind{Dial: true}}
		if err := sess.RevokeEdge(ctx, edge); err != nil {
			return err
		}
		if err := sess.RevokeIdentity(ctx, mediation.WorkloadIdentity{URI: fromURI}); err != nil {
			return err
		}
	}
	svcID, ok, err := sess.client.findByName(ctx, "services", identityRoleAttribute(toURI))
	if err != nil {
		return err
	}
	if ok {
		if err := sess.client.deleteTerminatorsForService(ctx, svcID); err != nil {
			return err
		}
		if err := sess.client.deleteService(ctx, svcID); err != nil {
			return err
		}
	}
	return nil
}

// mediatedSettleTimeout/mediatedSettlePoll are vars, not consts, so tests
// can shrink them (docs/adr/023's shrinkTunnelSettle precedent) rather
// than waiting out a real bounded timeout to exercise the honest-failure
// path.
var (
	mediatedSettleTimeout = 30 * time.Second
	mediatedSettlePoll    = time.Second
)

// waitMediatedServing bounded-polls a raw TCP dial to the mediated
// Connection's dial-side entrypoint (name:port) until it succeeds or the
// deadline (docs/planning/02 §4.1's ScaledWait chokepoint) elapses — the
// NFR-11 "ready means serving now" bar, applied to the mediated path's own
// first-connection warm-up window. Dials through runtime.WithReachable so
// it resolves a real address on BOTH runtimes (Docker published port /
// Kubernetes ephemeral port-forward), re-resolving each attempt so a stale
// port-forward can't wedge the wait.
func waitMediatedServing(ctx context.Context, rt runtime.ContainerRuntime, name string, port int) error {
	opts := runtime.ReachableOptions{Timeout: mediatedSettleTimeout, Interval: mediatedSettlePoll}
	return runtime.WithReachable(ctx, rt, name, port, opts, func(ctx context.Context, addr string) error {
		conn, err := (&net.Dialer{Timeout: 3 * time.Second}).DialContext(ctx, "tcp", addr)
		if err != nil {
			return err
		}
		return conn.Close()
	})
}
