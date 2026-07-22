package proxy

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// shrinkForwarderSettle lowers forwarderSettleTimeout/forwarderSettlePoll
// for the duration of a test, restoring them on cleanup — avoids waiting
// out a real 45s timeout to exercise the honest-failure path
// (docs/planning/11 B1 finding 3).
func shrinkForwarderSettle(t *testing.T) {
	t.Helper()
	prevTimeout, prevPoll := forwarderSettleTimeout, forwarderSettlePoll
	forwarderSettleTimeout = 150 * time.Millisecond
	forwarderSettlePoll = 20 * time.Millisecond
	t.Cleanup(func() {
		forwarderSettleTimeout, forwarderSettlePoll = prevTimeout, prevPoll
	})
}

func providerEnvelope(name string) resource.Envelope {
	return resource.Envelope{
		GroupVersionKind: resource.GroupVersionKind{Kind: "Provider"},
		Metadata:         resource.Metadata{Name: name},
		Spec: map[string]any{
			"type":    "proxy",
			"runtime": map[string]any{"type": "docker"},
		},
	}
}

func connectionEnvelope(name, providerRef string, port int, target string) resource.Envelope {
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
// the forwarder container's own health alone — it must dial through to the
// upstream (probeThroughForwarder, the same check Probe uses) and fail
// honestly, naming the last observed state, when nothing ever answers.
func TestReconcileConnectionFailsHonestlyWhenUpstreamNeverAnswers(t *testing.T) {
	shrinkForwarderSettle(t)

	ctx := context.Background()
	rt := fakeruntime.New()
	p := New()

	provEnv := providerEnvelope("edge")
	// Nothing in this test environment listens on this port — every dial
	// attempt gets connection-refused, simulating an upstream that never
	// answers.
	connEnv := connectionEnvelope("upstream-db", "edge", 58231, "127.0.0.1:1")

	req := reconciler.Request{Resource: connEnv, Provider: provEnv, Runtime: rt}
	st, err := p.Reconcile(ctx, req)
	if err == nil {
		t.Fatal("expected Reconcile to fail honestly when the upstream never answers, got nil error")
	}
	if !strings.Contains(err.Error(), "did not settle") {
		t.Errorf("error = %q, want it to name the settle timeout (honest failure)", err.Error())
	}
	if ready, ok := st.Condition(status.Ready); ok && ready.Status == status.True {
		t.Error("status must not report Ready when the upstream never answered")
	}
}

// noHostAddrRuntime wraps the fake runtime but reports no host-side port
// binding from Inspect — the shape the Kubernetes adapter presents under
// its default ClusterIP/port-forward access mode, where only NodePort/
// LoadBalancer Services ever get a HostIP/HostPort.
type noHostAddrRuntime struct {
	*fakeruntime.Runtime
}

func (r *noHostAddrRuntime) Inspect(ctx context.Context, name string) (runtime.ContainerState, bool, error) {
	st, found, err := r.Runtime.Inspect(ctx, name)
	for i := range st.Ports {
		st.Ports[i].HostIP = ""
		st.Ports[i].HostPort = 0
	}
	return st, found, err
}

// TestReconcileConnectionReadyWhenRuntimePublishesNoHostAddress pins the
// I4 follow-up found live in TestLakehouseExampleOnKubernetes: on a
// runtime that publishes no host-side binding (Kubernetes, default
// ClusterIP/port-forward access mode), Probe's own dial-through is guarded
// by `if addr != ""` and skipped — so reconcile's settle bar there is
// container health, the same as Probe's, and it must NOT wait out the
// settle timeout for an address that can never appear (nor demand the
// upstream answer: the target may be a genuinely external host
// unresolvable from the cluster).
func TestReconcileConnectionReadyWhenRuntimePublishesNoHostAddress(t *testing.T) {
	shrinkForwarderSettle(t)

	ctx := context.Background()
	rt := &noHostAddrRuntime{Runtime: fakeruntime.New()}
	p := New()

	provEnv := providerEnvelope("edge")
	// Nothing listens anywhere; the target is an external placeholder host.
	connEnv := connectionEnvelope("orders-db", "edge", 58233, "external-orders-db:5432")

	req := reconciler.Request{Resource: connEnv, Provider: provEnv, Runtime: rt}
	start := time.Now()
	st, err := p.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !st.IsReady() {
		t.Error("expected Ready on a runtime with no published host address (Probe's bar there is container health)")
	}
	if elapsed := time.Since(start); elapsed >= forwarderSettleTimeout {
		t.Errorf("Reconcile took %s — it waited out the settle timeout instead of matching Probe's addr==\"\" skip", elapsed)
	}
}

// TestReconcileConnectionSucceedsWhenUpstreamAnswers is the mirror positive
// case: once something is actually listening on the forwarder's published
// port, reconcile settles and reports Ready — the settle poll must not
// regress the healthy path.
func TestReconcileConnectionSucceedsWhenUpstreamAnswers(t *testing.T) {
	shrinkForwarderSettle(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	port := ln.Addr().(*net.TCPAddr).Port
	holdOpen := make(chan struct{})
	t.Cleanup(func() { close(holdOpen) })
	go func() {
		c, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer c.Close()
		// Hold the session open until the test ends — a live upstream's
		// accepted session, with no wall-clock assumption about how long
		// the probe under test takes (doc 11 timed-poll census).
		<-holdOpen
	}()

	ctx := context.Background()
	rt := fakeruntime.New()
	p := New()

	provEnv := providerEnvelope("edge")
	connEnv := connectionEnvelope("upstream-db", "edge", port, "127.0.0.1:1")
	req := reconciler.Request{Resource: connEnv, Provider: provEnv, Runtime: rt}

	st, err := p.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !st.IsReady() {
		t.Error("expected Ready once the upstream answers through the forwarder")
	}
}

// TestReconcileConnectionViaFailsHonestlyWithoutTunnelFacts is docs/planning/08
// I1's honest-failure bar: a Connection declaring spec.via must never
// silently realize as a plain, untunneled forwarder just because the
// engine hasn't published TunnelFacts yet (e.g. the tunnel Provider hasn't
// reconciled this apply) — graph.Build's via -> Provider edge means this
// should not arise in practice, but reconcile must still refuse rather than
// guess if it somehow does.
func TestReconcileConnectionViaFailsHonestlyWithoutTunnelFacts(t *testing.T) {
	ctx := context.Background()
	rt := fakeruntime.New()
	p := New()

	provEnv := providerEnvelope("edge")
	connEnv := connectionEnvelope("private-db", "edge", 58234, "10.8.0.10:5432")
	connEnv.Spec["via"] = map[string]any{"name": "vpc-tunnel"}

	req := reconciler.Request{Resource: connEnv, Provider: provEnv, Runtime: rt}
	_, err := p.Reconcile(ctx, req)
	if err == nil {
		t.Fatal("expected Reconcile to fail honestly when spec.via is set but TunnelFacts is nil")
	}
	if !strings.Contains(err.Error(), "vpc-tunnel") || !strings.Contains(err.Error(), "not yet published") {
		t.Errorf("error = %q, want it to name the via Provider and say facts are not yet published", err.Error())
	}
}

// TestReconcileConnectionViaJoinsTransitNetworkAndDialsTunnel is docs/planning/08
// I1's core realization behavior: a via'd Connection's forwarder joins ONLY
// the shared platform network plus the tunnel's own transit network (never
// a third network — blast-minimized), and settles Ready using the exact
// same dial-through-forwarder check as an untunneled Connection (Probe
// symmetry, I4) — via-awareness lives entirely in which address the
// forwarder was built to dial, not in a separate settledness path.
func TestReconcileConnectionViaJoinsTransitNetworkAndDialsTunnel(t *testing.T) {
	shrinkForwarderSettle(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	port := ln.Addr().(*net.TCPAddr).Port
	holdOpen := make(chan struct{})
	t.Cleanup(func() { close(holdOpen) })
	go func() {
		c, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer c.Close()
		<-holdOpen
	}()

	ctx := context.Background()
	rt := fakeruntime.New()
	p := New()

	provEnv := providerEnvelope("edge")
	connEnv := connectionEnvelope("private-db", "edge", port, "10.8.0.10:5432")
	connEnv.Spec["via"] = map[string]any{"name": "vpc-tunnel"}

	req := reconciler.Request{
		Resource: connEnv,
		Provider: provEnv,
		Runtime:  rt,
		TunnelFacts: &reconciler.TunnelFacts{
			TransitNetwork: "datascape-vpc-transit",
			// The fake runtime never actually dials a socat Cmd — the
			// settle check dials ctr.HostAddr(conn.Port) directly (the
			// same trick TestReconcileConnectionSucceedsWhenUpstreamAnswers
			// above uses), so this value only needs to be a plausible
			// "host:port" string, never actually dialed by this test.
			Internal: "wg-private-db-via-tunnel:5432",
		},
	}
	st, err := p.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !st.IsReady() {
		t.Error("expected Ready for a via'd Connection whose TunnelFacts are populated and upstream answers")
	}

	name := "private-db"
	dial := fmt.Sprintf("%s:%d", name, port)
	if err := rt.ProbeReachable(ctx, "datascape", dial); err != nil {
		t.Errorf("forwarder not attached to the shared platform network: %v", err)
	}
	if err := rt.ProbeReachable(ctx, "datascape-vpc-transit", dial); err != nil {
		t.Errorf("forwarder not attached to the tunnel's transit network: %v", err)
	}
	if err := rt.ProbeReachable(ctx, "some-other-network", dial); err == nil {
		t.Error("forwarder must not be attached to any network beyond [shared, transit] (blast radius)")
	}

	via, _ := st.ProviderState["via"].(string)
	if via != "vpc-tunnel" {
		t.Errorf("ProviderState[via] = %q, want %q", via, "vpc-tunnel")
	}
	transit, _ := st.ProviderState["transit"].(string)
	if transit != "datascape-vpc-transit" {
		t.Errorf("ProviderState[transit] = %q, want %q", transit, "datascape-vpc-transit")
	}
}
