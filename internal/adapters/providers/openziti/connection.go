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

// dialEnrollTokenPath is where the dial-side tunneler's one-time enrollment
// JWT is FileMount-ed (docs/planning/08 H10) — one of the ziti-tunnel
// image's own documented JWT-discovery candidate directories (live-verified
// against entrypoint.sh), deliberately outside "/netfoundry" (the identity
// volume, see reconcileConnection's spec doc comment for why).
const dialEnrollTokenPath = "/enrollment-token/ziti_id.jwt"

// dialIdentityFilePath is where ziti-tunnel's entrypoint.sh writes the
// enrolled identity once ZITI_IDENTITY_BASENAME's enrollment (ziti edge
// enroll) succeeds — inside "/netfoundry", the persisted identity volume,
// so its existence is a durable, cross-recreate signal waitTunnelEnrolled
// polls for.
const dialIdentityFilePath = "/netfoundry/ziti_id.json"

// waitTunnelEnrolled bounded-polls until the dial-side tunneler container
// name has durably written its enrolled identity (reconcileConnection's
// settle-pass doc comment explains why this wait must happen BEFORE that
// pass recreates the container). Uses runtime.ContainerRuntime.ReadFile —
// implemented identically on Docker (CopyFromContainer, any live path) and
// Kubernetes (a live `cat` exec fallback for a path ReadFile didn't itself
// place, kubernetes/exec.go's own doc comment) — so this works unchanged on
// both runtimes, no capability assertion needed.
func waitTunnelEnrolled(ctx context.Context, rt runtime.ContainerRuntime, name string) error {
	deadline := time.Now().Add(runtime.ScaledWait(30 * time.Second))
	for {
		if b, err := rt.ReadFile(ctx, name, dialIdentityFilePath); err == nil && len(b) > 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("identity file %q did not appear within the settle window", dialIdentityFilePath)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

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
		// docs/planning/08 H10: FileMount, never Env — verified live
		// against the pinned ziti-tunnel image's own entrypoint.sh, which
		// (before ever consulting ZITI_ENROLL_TOKEN) already searches a
		// fixed list of candidate directories for
		// "<ZITI_IDENTITY_BASENAME>.jwt", "/enrollment-token" among them —
		// so no env var is needed at all here, unlike the router (see
		// instance.go's routerSpec doc comment for why THAT image has no
		// equivalent env-free path). Mode 0o600 (the wireguard precedent)
		// is fine here specifically because this image's entrypoint runs
		// as root (live-verified: `id` inside openziti/ziti-tunnel reports
		// uid=0) — root reads a root-owned 0o600 file regardless of which
		// process/namespace copied it in. Deliberately OUTSIDE "/netfoundry"
		// (the persisted identity volume mounted below) for the identical
		// reason instance.go's router token path is kept out of
		// "/ziti-router": an ephemeral, container-layer path the settle
		// recreate below discards, never left as on-disk residue.
		spec.Files = []runtime.FileMount{{Path: dialEnrollTokenPath, Content: []byte(dialJWT), Mode: 0o600}}
	}

	ctrState, err := rt.EnsureContainer(ctx, spec)
	if err != nil {
		return st, err
	}

	// Settle to the STABLE, post-enrollment spec within this SAME
	// reconcile — the same discipline instance.go's reconcileInstance
	// applies to the router container. upsertIdentity's idempotency
	// contract (client.go, mintIdentityWithToken's own doc comment) means
	// every LATER reconcile, by definition, finds this consumer's
	// CONTROLLER-side identity entity already existing and gets back an
	// empty enrollment JWT — that part needs no bounded wait, it's a
	// synchronous REST idempotency check. But (docs/planning/08 H10,
	// found live) the CONTAINER's own local enrollment — writing
	// dialIdentityFilePath into the persisted "…-identity" volume — is a
	// SEPARATE, genuinely async fact this settle-recreate must also wait
	// for: with the JWT delivered via an ephemeral FileMount (never
	// written into that volume itself, unlike the old Env-var path whose
	// entrypoint.sh side effect happened to copy the token INTO the
	// volume before any race could matter), recreating the container
	// before enrollment finishes destroys the only copy of the JWT along
	// with the container's writable layer, and the fresh replacement
	// starts with neither a JWT nor an identity file anywhere — reliably
	// reproduced live as an immediate "Exited (1)" (ziti-tunnel's own
	// entrypoint.sh: "zero identities found"). waitTunnelEnrolled closes
	// that window the same way waitEdgeRouterVerified does for the
	// router. Leaving the container's desired spec keyed to the one-time
	// token would ALSO violate the CLAUDE.md EnsureContainer idempotency
	// bar the instant any later probe/drift/status call recomputes it —
	// found live coupled with the router's own analogous churn
	// (instance.go's settle comment has the full account):
	// TestOpenZitiMediatedConnectionOnKubernetesEndToEnd's post-apply
	// drift restarted this dial-side tunneler mid-test. The FileMount
	// carrying the JWT is stripped in this same settle pass, exactly as
	// Env is — the steady-state spec must not carry either.
	if dialJWT != "" {
		if err := waitTunnelEnrolled(ctx, rt, name); err != nil {
			return st, fmt.Errorf("Connection %q: dial-side tunneler %q did not enroll: %w", res.Metadata.Name, name, err)
		}
		stableSpec := spec
		stableSpec.Files = nil
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
