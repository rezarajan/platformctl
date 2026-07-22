package providerkit

import (
	"context"
	"strings"
	"testing"
	"time"

	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// replicaSpec builds a fake-runtime ContainerSpec exposing port 8083 with an
// explicit host-port pin — real providers always resolve a concrete
// HostPort via hostport.Resolve before calling EnsureContainer (HostPort: 0
// means "no host binding" on both the real Docker adapter and the fake, not
// "auto-assign"), so tests must do the same to get an observable host
// address back.
func replicaSpec(name string, n int) runtime.ContainerSpec {
	return runtime.ContainerSpec{
		Name:     name,
		Image:    "test-image",
		Replicas: n,
		Ports:    []runtime.PortBinding{{HostPort: 18083, ContainerPort: 8083, Audience: runtime.AudienceHost}},
	}
}

// TestReachableURLsSingleMember covers docs/planning/08 C3's zero-behavior-
// change bar: members <= 1 is byte-for-byte the single-address ReachableURL
// path, wrapped in a one-element slice.
func TestReachableURLsSingleMember(t *testing.T) {
	rt := fakeruntime.New()
	if _, err := rt.EnsureContainer(context.Background(), runtime.ContainerSpec{
		Name: "connect", Image: "test-image",
		Ports: []runtime.PortBinding{{HostPort: 18083, ContainerPort: 8083, Audience: runtime.AudienceHost}},
	}); err != nil {
		t.Fatalf("EnsureContainer: %v", err)
	}
	urls, closeURLs, err := ReachableURLs(context.Background(), rt, "connect", 8083, 0)
	if err != nil {
		t.Fatalf("ReachableURLs: %v", err)
	}
	defer closeURLs()
	if len(urls) != 1 || !strings.HasPrefix(urls[0], "http://") {
		t.Fatalf("urls = %v, want exactly one http:// address", urls)
	}
}

// TestReachableURLsSkipsDeadOrdinal covers the C3 kill-test contract: one
// unreachable ordinal among several must not fail resolution — the survivors
// are returned as failover candidates.
func TestReachableURLsSkipsDeadOrdinal(t *testing.T) {
	rt := fakeruntime.New()
	if _, err := rt.EnsureContainer(context.Background(), replicaSpec("connect", 2)); err != nil {
		t.Fatalf("EnsureContainer: %v", err)
	}
	if err := rt.Remove(context.Background(), runtime.OrdinalName("connect", 1)); err != nil {
		t.Fatalf("remove ordinal 1: %v", err)
	}
	urls, closeURLs, err := ReachableURLs(context.Background(), rt, "connect", 8083, 2)
	if err != nil {
		t.Fatalf("ReachableURLs with one dead ordinal: %v", err)
	}
	defer closeURLs()
	if len(urls) != 1 {
		t.Fatalf("urls = %v, want exactly the one surviving ordinal", urls)
	}
}

// TestReachableURLsAllDeadErrors covers the "error only when zero members
// are reachable" rule.
func TestReachableURLsAllDeadErrors(t *testing.T) {
	rt := fakeruntime.New()
	if _, err := rt.EnsureContainer(context.Background(), replicaSpec("connect", 2)); err != nil {
		t.Fatalf("EnsureContainer: %v", err)
	}
	if err := rt.Remove(context.Background(), runtime.OrdinalName("connect", 0)); err != nil {
		t.Fatal(err)
	}
	if err := rt.Remove(context.Background(), runtime.OrdinalName("connect", 1)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReachableURLs(context.Background(), rt, "connect", 8083, 2); err == nil {
		t.Fatal("want an error when every ordinal is unreachable")
	}
}

// TestProbeConnectWorkerSetAllPresent covers the healthy path: every
// ordinal running yields Ready/no-drift.
func TestProbeConnectWorkerSetAllPresent(t *testing.T) {
	rt := fakeruntime.New()
	if _, err := rt.EnsureContainer(context.Background(), replicaSpec("connect", 2)); err != nil {
		t.Fatalf("EnsureContainer: %v", err)
	}
	st, err := ProbeConnectWorkerSet(context.Background(), rt, "connect", 2, time.Now())
	if err != nil {
		t.Fatalf("ProbeConnectWorkerSet: %v", err)
	}
	if c, ok := st.Condition(status.Ready); !ok || c.Status != status.True {
		t.Errorf("Ready condition = %+v (ok=%v), want True", c, ok)
	}
	if c, ok := st.Condition(status.DriftDetected); !ok || c.Status != status.False {
		t.Errorf("DriftDetected condition = %+v (ok=%v), want False", c, ok)
	}
}

// TestProbeConnectWorkerSetMissingOrdinal is the docs/planning/08 C3
// "worker-count drift detected" Accept item: a missing ordinal is reported
// as drift naming it, via the shared ConnectWorkerMissing prefix.
func TestProbeConnectWorkerSetMissingOrdinal(t *testing.T) {
	rt := fakeruntime.New()
	if _, err := rt.EnsureContainer(context.Background(), replicaSpec("connect", 2)); err != nil {
		t.Fatalf("EnsureContainer: %v", err)
	}
	if err := rt.Remove(context.Background(), runtime.OrdinalName("connect", 1)); err != nil {
		t.Fatal(err)
	}
	st, err := ProbeConnectWorkerSet(context.Background(), rt, "connect", 2, time.Now())
	if err != nil {
		t.Fatalf("ProbeConnectWorkerSet: %v", err)
	}
	c, ok := st.Condition(status.Ready)
	if !ok || c.Status != status.False || !strings.Contains(c.Reason, status.ReasonConnectWorkerMissing) || !strings.Contains(c.Reason, "connect-1") {
		t.Errorf("Ready condition = %+v (ok=%v), want False naming ConnectWorkerMissing(connect-1)", c, ok)
	}
	if c, ok := st.Condition(status.DriftDetected); !ok || c.Status != status.True {
		t.Errorf("DriftDetected condition = %+v (ok=%v), want True", c, ok)
	}
}

// collectiveRuntime wraps the fake runtime and adds a bare-bones
// runtime.MemberSetRuntime implementation, exercising
// ReachableURLs/ProbeConnectWorkerSet's Kubernetes-shaped branch
// (docs/adr/004's I7 addendum, docs/planning/08 §7.8) without importing
// internal/adapters/runtime/kubernetes (providerkit_test.go is not an
// application test, but follows the identical "stub the port, don't add a
// second technology-adapter import" discipline CLAUDE.md's Layering
// section requires of internal/application's tests). Only the marker
// method is added — Inspect's own bare-name aggregation already reports
// the live ReadyReplicas count for a replica-set base name on the fake
// (see aggregateStateLocked), which is exactly what a real Kubernetes
// Inspect(name) reports for a Deployment, so ProbeConnectWorkerSet's
// collective branch is exercised faithfully with no further overrides.
// ReachableURLs' collective branch only needs proof that it resolves the
// set's bare Name exactly once rather than ever looping OrdinalName — the
// fixtures below use a plain, non-replicated fake container for that
// reason (a real ordinal loop attempt would find nothing and fail).
type collectiveRuntime struct {
	*fakeruntime.Runtime
}

func (collectiveRuntime) AddressesMembersCollectively() bool { return true }

// TestReachableURLsCollectiveRuntimeAddressesByBareName proves the I7 fix:
// on a runtime.MemberSetRuntime, members > 1 resolves the set's own bare
// Name once rather than iterating OrdinalName ordinals (which do not exist
// here at all — a regression back to the ordinal loop would fail this test
// outright with "container not found").
func TestReachableURLsCollectiveRuntimeAddressesByBareName(t *testing.T) {
	rt := collectiveRuntime{fakeruntime.New()}
	if _, err := rt.EnsureContainer(context.Background(), runtime.ContainerSpec{
		Name: "connect", Image: "test-image",
		Ports: []runtime.PortBinding{{HostPort: 18083, ContainerPort: 8083, Audience: runtime.AudienceHost}},
	}); err != nil {
		t.Fatalf("EnsureContainer: %v", err)
	}
	urls, closeURLs, err := ReachableURLs(context.Background(), rt, "connect", 8083, 2)
	if err != nil {
		t.Fatalf("ReachableURLs (collective): %v", err)
	}
	defer closeURLs()
	if len(urls) != 1 || !strings.HasPrefix(urls[0], "http://") {
		t.Fatalf("urls = %v, want exactly one http:// address resolved by the set's bare name", urls)
	}
}

// TestReachableURLsCollectiveRuntimeErrorsWhenUnreachable covers the "error
// only when the set is genuinely unreachable" rule on the collective path.
func TestReachableURLsCollectiveRuntimeErrorsWhenUnreachable(t *testing.T) {
	rt := collectiveRuntime{fakeruntime.New()}
	if _, _, err := ReachableURLs(context.Background(), rt, "connect", 8083, 2); err == nil {
		t.Fatal("want an error when the set's bare name resolves to nothing")
	}
}

// TestProbeConnectWorkerSetCollectiveAllReady covers the collective branch's
// healthy path: the aggregate ReadyReplicas matches the declared count.
func TestProbeConnectWorkerSetCollectiveAllReady(t *testing.T) {
	rt := collectiveRuntime{fakeruntime.New()}
	if _, err := rt.EnsureContainer(context.Background(), replicaSpec("connect", 2)); err != nil {
		t.Fatalf("EnsureContainer: %v", err)
	}
	st, err := ProbeConnectWorkerSet(context.Background(), rt, "connect", 2, time.Now())
	if err != nil {
		t.Fatalf("ProbeConnectWorkerSet: %v", err)
	}
	if c, ok := st.Condition(status.Ready); !ok || c.Status != status.True {
		t.Errorf("Ready condition = %+v (ok=%v), want True", c, ok)
	}
	if c, ok := st.Condition(status.DriftDetected); !ok || c.Status != status.False {
		t.Errorf("DriftDetected condition = %+v (ok=%v), want False", c, ok)
	}
}

// TestProbeConnectWorkerSetCollectiveDegraded covers the collective branch's
// degraded path: one of two members gone reports drift naming a ready/
// expected count (there is no ordinal name to name on this runtime).
func TestProbeConnectWorkerSetCollectiveDegraded(t *testing.T) {
	rt := collectiveRuntime{fakeruntime.New()}
	if _, err := rt.EnsureContainer(context.Background(), replicaSpec("connect", 2)); err != nil {
		t.Fatalf("EnsureContainer: %v", err)
	}
	if err := rt.Remove(context.Background(), runtime.OrdinalName("connect", 1)); err != nil {
		t.Fatal(err)
	}
	st, err := ProbeConnectWorkerSet(context.Background(), rt, "connect", 2, time.Now())
	if err != nil {
		t.Fatalf("ProbeConnectWorkerSet: %v", err)
	}
	c, ok := st.Condition(status.Ready)
	if !ok || c.Status != status.False || !strings.Contains(c.Reason, status.ReasonConnectWorkerMissing) || !strings.Contains(c.Reason, "1/2") {
		t.Errorf("Ready condition = %+v (ok=%v), want False naming ConnectWorkerMissing(1/2 ready)", c, ok)
	}
	if c, ok := st.Condition(status.DriftDetected); !ok || c.Status != status.True {
		t.Errorf("DriftDetected condition = %+v (ok=%v), want True", c, ok)
	}
}
