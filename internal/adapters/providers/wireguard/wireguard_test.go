package wireguard

import (
	"context"
	"strings"
	"testing"
	"time"

	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	"github.com/rezarajan/platformctl/internal/domain/connection"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
)

// shrinkTunnelSettle lowers tunnelSettleTimeout/tunnelSettlePoll for the
// duration of a test, restoring them on cleanup — avoids waiting out a real
// 45s timeout to exercise the honest-failure path (docs/planning/11 B1
// finding 1).
//
// J1 fast-tier note: these are package-level vars (wireguard.go) mutated
// directly — every caller of shrinkTunnelSettle must stay serial (no
// t.Parallel()) with every other caller, or two goroutines race on the
// same package globals (the same class of hazard `go test -race` found
// live in the ingress package's shrinkRouteSettle).
func shrinkTunnelSettle(t *testing.T) {
	t.Helper()
	prevTimeout, prevPoll, prevReachable, prevInterval := tunnelSettleTimeout, tunnelSettlePoll, tunnelReachableTimeout, tunnelReachableInterval
	tunnelSettleTimeout = 150 * time.Millisecond
	tunnelSettlePoll = 20 * time.Millisecond
	tunnelReachableTimeout = 20 * time.Millisecond
	tunnelReachableInterval = 5 * time.Millisecond
	t.Cleanup(func() {
		tunnelSettleTimeout, tunnelSettlePoll, tunnelReachableTimeout, tunnelReachableInterval = prevTimeout, prevPoll, prevReachable, prevInterval
	})
}

func tunnelProviderEnvelope(name string, port int) resource.Envelope {
	return resource.Envelope{
		GroupVersionKind: resource.GroupVersionKind{Kind: "Provider"},
		Metadata:         resource.Metadata{Name: name},
		Spec: map[string]any{
			"type":    "wireguard",
			"runtime": map[string]any{"type": "docker"},
			"configuration": map[string]any{
				"peerNetwork":   "wg-net",
				"peerPublicKey": "cGVlcicgcHVibGljIGtleQ==",
				"peerEndpoint":  "203.0.113.1:51820",
				"address":       "10.10.0.2/24",
				"allowedIPs":    []any{"10.10.0.0/24"},
			},
			"secretRefs": []any{"wg-key"},
		},
	}
}

func tunnelConnectionEnvelope(name, providerRef string, port int, target string) resource.Envelope {
	return resource.Envelope{
		GroupVersionKind: resource.GroupVersionKind{Kind: "Connection"},
		Metadata:         resource.Metadata{Name: name},
		Spec: map[string]any{
			"providerRef": map[string]any{"name": providerRef},
			"port":        port,
			"target":      target,
		},
	}
}

// TestReconcileConnectionFailsHonestlyWhenUpstreamNeverAnswers is the I4
// acceptance bar (docs/planning/08 §7.8): reconcile must not set Ready from
// the tunnel container's own healthcheck alone (wg0 existing says nothing
// about whether the peer handshaked or the upstream answers) — it must
// dial through the forwarder to the upstream (dialUpstream, the same check
// Probe uses) and fail honestly, naming the last observed state, when
// nothing ever answers (docs/planning/11 B1 finding 1, CONFIRMED, the
// redpanda-93fbf14 signature).
func TestReconcileConnectionFailsHonestlyWhenUpstreamNeverAnswers(t *testing.T) {
	// serial: shrinkTunnelSettle mutates package-level tunnel settle vars
	// (see its doc comment).
	shrinkTunnelSettle(t)

	ctx := context.Background()
	rt := fakeruntime.New()
	p := New()

	provEnv := tunnelProviderEnvelope("vpc-tunnel", 0)
	// Nothing in this test environment listens on this port — every dial
	// attempt gets connection-refused, simulating an upstream that never
	// answers through the tunnel's forwarder rule.
	connEnv := tunnelConnectionEnvelope("db-tunnel", "vpc-tunnel", 58241, "10.10.0.5:5432")

	req := reconciler.Request{
		Resource: connEnv,
		Provider: provEnv,
		Runtime:  rt,
		Secrets:  map[string]map[string]string{"wg-key": {"privateKey": "cHJpdmF0ZScga2V5"}},
	}
	st, err := p.Reconcile(ctx, req)
	if err == nil {
		t.Fatal("expected Reconcile to fail honestly when the upstream never answers, got nil error")
	}
	if !strings.Contains(err.Error(), "did not settle") {
		t.Errorf("error = %q, want it to name the settle timeout (honest failure)", err.Error())
	}
	if !strings.Contains(err.Error(), "unreachable through tunnel") {
		t.Errorf("error = %q, want it to name the last observed state (upstream unreachable)", err.Error())
	}
	if ready, ok := st.Condition(status.Ready); ok && ready.Status == status.True {
		t.Error("status must not report Ready when the upstream never answered")
	}
}

// TestReconcileInstanceCreatesViaTunnelForChainedConnection is docs/planning/08
// I1's producer-side contract: reconciling the wireguard Provider itself
// (Kind == "Provider") ensures one via-tunnel container per managed
// Connection in req.Resources whose spec.via names this Provider —
// deterministically named (never naming.RuntimeObjectName(res), which
// would collide with the forwarder proxy realizes for the same
// Connection), attached ONLY to the transit network, and its dial address
// published as an endpoint fact named connection.ViaFactName(ns, name) —
// the exact fact the via'd Connection's own (proxy) reconcile reads back
// via Request.Facts.Endpoint (docs/planning/08 I9; originally
// Request.TunnelFacts, a bespoke field migrated and deleted).
func TestReconcileInstanceCreatesViaTunnelForChainedConnection(t *testing.T) {
	// serial: shrinkTunnelSettle mutates package-level tunnel settle vars
	// (see its doc comment).
	shrinkTunnelSettle(t)

	ctx := context.Background()
	rt := fakeruntime.New()
	p := New()

	provEnv := tunnelProviderEnvelope("vpc-tunnel", 0)
	viaConn := resource.Envelope{
		GroupVersionKind: resource.GroupVersionKind{Kind: "Connection"},
		Metadata:         resource.Metadata{Name: "private-db"},
		Spec: map[string]any{
			"providerRef": map[string]any{"name": "edge"},
			"scheme":      "tcp",
			"port":        15999,
			"target":      "10.10.0.5:5432",
			"via":         map[string]any{"name": "vpc-tunnel"},
		},
	}

	req := reconciler.Request{
		Resource: provEnv,
		Provider: provEnv,
		Runtime:  rt,
		Secrets:  map[string]map[string]string{"wg-key": {"privateKey": "cHJpdmF0ZScga2V5"}},
		Resources: map[resource.Key]resource.Envelope{
			provEnv.Key(): provEnv,
			viaConn.Key(): viaConn,
		},
	}

	st, err := p.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !st.IsReady() {
		t.Error("expected the Provider to be Ready once its via tunnel settles")
	}

	tunnelName := "private-db-via-tunnel"
	if _, found, err := rt.Inspect(ctx, tunnelName); err != nil || !found {
		t.Fatalf("via tunnel container %q not created: found=%v err=%v", tunnelName, found, err)
	}
	wantTarget := tunnelName + ":15999"
	if err := rt.ProbeReachable(ctx, "wg-net", wantTarget); err != nil {
		t.Errorf("via tunnel not reachable from its own transit network: %v", err)
	}

	facts := endpoint.FromState(st.ProviderState[endpoint.Key])
	wantName := connection.ViaFactName("", "private-db")
	var got *endpoint.Endpoint
	for i := range facts {
		if facts[i].Name == wantName {
			got = &facts[i]
		}
	}
	if got == nil {
		t.Fatalf("no published endpoint fact named %q; facts=%+v", wantName, facts)
	}
	if got.Internal != wantTarget {
		t.Errorf("published fact Internal = %q, want %q", got.Internal, wantTarget)
	}
}

// TestReconcileInstanceFailsHonestlyWhenViaTunnelUpstreamUnreachable mirrors
// TestReconcileConnectionFailsHonestlyWhenUpstreamNeverAnswers above for the
// via path: a via tunnel that never becomes reachable from the transit
// network must fail the Provider's own reconcile honestly, not report
// Ready from the container healthcheck alone.
func TestReconcileInstanceFailsHonestlyWhenViaTunnelUpstreamUnreachable(t *testing.T) {
	// serial: shrinkTunnelSettle mutates package-level tunnel settle vars
	// (see its doc comment).
	shrinkTunnelSettle(t)

	ctx := context.Background()
	// A runtime whose ProbeReachable always refuses — simulating a via
	// tunnel container that never becomes dialable from the transit
	// network (e.g. the DNAT rule never took effect).
	rt := &alwaysUnreachableProbeRuntime{Runtime: fakeruntime.New()}
	p := New()

	provEnv := tunnelProviderEnvelope("vpc-tunnel", 0)
	viaConn := resource.Envelope{
		GroupVersionKind: resource.GroupVersionKind{Kind: "Connection"},
		Metadata:         resource.Metadata{Name: "private-db"},
		Spec: map[string]any{
			"providerRef": map[string]any{"name": "edge"},
			"scheme":      "tcp",
			"port":        15999,
			"target":      "10.10.0.5:5432",
			"via":         map[string]any{"name": "vpc-tunnel"},
		},
	}
	req := reconciler.Request{
		Resource: provEnv,
		Provider: provEnv,
		Runtime:  rt,
		Secrets:  map[string]map[string]string{"wg-key": {"privateKey": "cHJpdmF0ZScga2V5"}},
		Resources: map[resource.Key]resource.Envelope{
			provEnv.Key(): provEnv,
			viaConn.Key(): viaConn,
		},
	}

	_, err := p.Reconcile(ctx, req)
	if err == nil {
		t.Fatal("expected Reconcile to fail honestly when the via tunnel never becomes reachable")
	}
	if !strings.Contains(err.Error(), "did not settle") {
		t.Errorf("error = %q, want it to name the settle timeout (honest failure)", err.Error())
	}
}

type alwaysUnreachableProbeRuntime struct {
	*fakeruntime.Runtime
}

func (r *alwaysUnreachableProbeRuntime) ProbeReachable(ctx context.Context, network, target string) error {
	return errString("simulated: never reachable")
}

type errString string

func (e errString) Error() string { return string(e) }
