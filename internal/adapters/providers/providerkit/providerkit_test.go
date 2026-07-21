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
