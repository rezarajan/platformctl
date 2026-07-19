//go:build integration

package kubernetes

import (
	"context"
	"os"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

		// Idempotent: a second call updates cleanly, doesn't error. Both
		// fixed, hardcoded names must still resolve — Kubernetes' own
		// per-namespace name uniqueness rules out a "duplicate" existing
		// under either name, so re-confirming both Gets succeed (rather
		// than enumerating via List, a permission the adapter itself never
		// needs — see deploy/kubernetes/rbac/role.yaml) is a complete check.
		if err := rt.EnsureNetwork(ctx, runtimeport.NetworkSpec{Name: ns, Labels: labels}); err != nil {
			t.Fatalf("second EnsureNetwork: %v", err)
		}
		for _, name := range []string{denyAllIngressPolicyName, allowSameNamespacePolicyName} {
			if _, err := rt.clientset.NetworkingV1().NetworkPolicies(ns).Get(ctx, name, metav1.GetOptions{}); err != nil {
				t.Errorf("networkpolicy %q missing after second EnsureNetwork: %v", name, err)
			}
		}
	})

	t.Run("none_opts_out", func(t *testing.T) {
		const ns = "datascape-netpol-none-test"
		t.Cleanup(func() { _ = rt.RemoveNetwork(ctx, ns) })

		if err := rt.EnsureNetwork(ctx, runtimeport.NetworkSpec{Name: ns, Labels: labels, IsolationPolicy: runtimeport.IsolationNone}); err != nil {
			t.Fatalf("EnsureNetwork with IsolationNone: %v", err)
		}
		for _, name := range []string{denyAllIngressPolicyName, allowSameNamespacePolicyName} {
			if _, err := rt.clientset.NetworkingV1().NetworkPolicies(ns).Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				t.Errorf("networkpolicy %q: err = %v, want IsNotFound (IsolationNone must provision neither policy)", name, err)
			}
		}
	})
}
