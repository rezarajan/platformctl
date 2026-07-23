//go:build integration

package kubernetes

import (
	"context"
	"os"
	"testing"

	runtimeport "github.com/rezarajan/platformctl/internal/ports/runtime"
)

// TestObserveIsolationEnforcement is docs/planning/08 H8's accept leg for
// the runtime capability itself (as distinct from
// TestNetworkPolicyEnforcementIsLive above, which it productizes): on a
// non-enforcing CNI (the default local-cluster shape) it must report
// IsolationNotEnforced; on the CI Calico cluster
// (PLATFORMCTL_REQUIRE_NETPOL_ENFORCEMENT set) non-enforcement is a hard
// failure, the same "a skip can never masquerade as coverage" bar
// TestNetworkPolicyEnforcementIsLive already enforces — both tests share
// that env var and the same CI k8s "adapter" shard picks this one up for
// free (ci.yml already runs the whole package).
func TestObserveIsolationEnforcement(t *testing.T) {
	requireEnforce := os.Getenv("PLATFORMCTL_REQUIRE_NETPOL_ENFORCEMENT") != ""
	rt, err := New(nil)
	if err != nil {
		if requireEnforce {
			t.Fatalf("connect to kubernetes (required by PLATFORMCTL_REQUIRE_NETPOL_ENFORCEMENT): %v", err)
		}
		t.Skipf("no kubernetes configuration; skipping: %v", err)
	}
	if _, err := rt.clientset.Discovery().ServerVersion(); err != nil {
		if requireEnforce {
			t.Fatalf("kubernetes cluster unreachable (required): %v", err)
		}
		t.Skipf("kubernetes cluster unreachable; skipping: %v", err)
	}

	ctx := context.Background()
	labels := map[string]string{runtimeport.LabelManagedBy: runtimeport.ManagedByValue}
	const nsA = "datascape-isolation-observe-a"
	const nsB = "datascape-isolation-observe-b"
	t.Cleanup(func() {
		_ = rt.RemoveNetwork(ctx, nsA)
		_ = rt.RemoveNetwork(ctx, nsB)
	})
	for _, ns := range []string{nsA, nsB} {
		if err := rt.EnsureNetwork(ctx, runtimeport.NetworkSpec{Name: ns, Labels: labels}); err != nil {
			t.Fatalf("EnsureNetwork %s: %v", ns, err)
		}
	}

	status, err := rt.ObserveIsolationEnforcement(ctx)
	if err != nil {
		t.Fatalf("ObserveIsolationEnforcement: %v", err)
	}
	switch status.State {
	case runtimeport.IsolationEnforced:
		// The CI Calico-cluster accept leg.
	case runtimeport.IsolationNotEnforced:
		if requireEnforce {
			t.Fatalf("ObserveIsolationEnforcement reported not-enforced on a cluster required to enforce NetworkPolicy: %s", status.Reason)
		}
		t.Logf("not-enforced (expected on a non-policy-enforcing local cluster): %s", status.Reason)
	case runtimeport.IsolationUnknown:
		t.Fatalf("ObserveIsolationEnforcement returned Unknown even though two walled managed namespaces exist: %s", status.Reason)
	default:
		t.Fatalf("unexpected IsolationStatus.State %q (reason: %s)", status.State, status.Reason)
	}
}
