//go:build integration

package kubernetes

import (
	"context"
	"os"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	runtimeport "github.com/rezarajan/platformctl/internal/ports/runtime"
)

// TestEnsureNetworkProvisionsIsolationBoundary covers docs/planning/08 B7
// against a real cluster: EnsureNetwork's default path creates the
// default-deny + allow-same-namespace NetworkPolicy pair, idempotently
// (a second call updates rather than erroring), and the "none" opt-out
// creates neither.
//
// This asserts the *objects* are correct — the actual isolation behavior
// ("in-namespace pod reaches, out-of-namespace pod cannot") additionally
// requires a CNI that enforces NetworkPolicy at all (Calico, Cilium, ...);
// minikube's default driver doesn't ship one, and standing up a
// policy-enforcing cluster is a separate, heavier environment decision
// (docs/planning/08 B7's own "kind + Calico or minikube CNI" wording
// anticipates this) — not something this test silently assumes.
func TestEnsureNetworkProvisionsIsolationBoundary(t *testing.T) {
	require := os.Getenv("PLATFORMCTL_REQUIRE_K8S") != ""
	rt, err := New(nil)
	if err != nil {
		if require {
			t.Fatalf("connect to kubernetes (required by PLATFORMCTL_REQUIRE_K8S): %v", err)
		}
		t.Skipf("no kubernetes configuration; skipping: %v", err)
	}
	if _, err := rt.clientset.Discovery().ServerVersion(); err != nil {
		if require {
			t.Fatalf("kubernetes cluster unreachable (required by PLATFORMCTL_REQUIRE_K8S): %v", err)
		}
		t.Skipf("kubernetes cluster unreachable; skipping: %v", err)
	}

	ctx := context.Background()
	labels := map[string]string{runtimeport.LabelManagedBy: runtimeport.ManagedByValue}

	t.Run("default_provisions_both_policies", func(t *testing.T) {
		const ns = "datascape-netpol-default-test"
		t.Cleanup(func() { _ = rt.RemoveNetwork(ctx, ns) })

		if err := rt.EnsureNetwork(ctx, runtimeport.NetworkSpec{Name: ns, Labels: labels}); err != nil {
			t.Fatalf("EnsureNetwork: %v", err)
		}
		for _, name := range []string{denyAllIngressPolicyName, allowSameNamespacePolicyName} {
			if _, err := rt.clientset.NetworkingV1().NetworkPolicies(ns).Get(ctx, name, metav1.GetOptions{}); err != nil {
				t.Errorf("networkpolicy %q missing: %v", name, err)
			}
		}

		// Idempotent: a second call updates cleanly, doesn't error or
		// duplicate.
		if err := rt.EnsureNetwork(ctx, runtimeport.NetworkSpec{Name: ns, Labels: labels}); err != nil {
			t.Fatalf("second EnsureNetwork: %v", err)
		}
		list, err := rt.clientset.NetworkingV1().NetworkPolicies(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			t.Fatalf("list networkpolicies: %v", err)
		}
		if len(list.Items) != 2 {
			t.Errorf("networkpolicy count = %d, want exactly 2 (no duplicates)", len(list.Items))
		}
	})

	t.Run("none_opts_out", func(t *testing.T) {
		const ns = "datascape-netpol-none-test"
		t.Cleanup(func() { _ = rt.RemoveNetwork(ctx, ns) })

		if err := rt.EnsureNetwork(ctx, runtimeport.NetworkSpec{Name: ns, Labels: labels, IsolationPolicy: runtimeport.IsolationNone}); err != nil {
			t.Fatalf("EnsureNetwork with IsolationNone: %v", err)
		}
		list, err := rt.clientset.NetworkingV1().NetworkPolicies(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			t.Fatalf("list networkpolicies: %v", err)
		}
		if len(list.Items) != 0 {
			t.Errorf("networkpolicy count = %d with IsolationNone, want 0", len(list.Items))
		}
	})
}
