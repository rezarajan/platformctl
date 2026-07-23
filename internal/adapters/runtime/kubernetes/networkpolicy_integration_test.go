//go:build integration

package kubernetes

import (
	"context"
	"os"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	runtimeport "github.com/rezarajan/platformctl/internal/ports/runtime"
	"github.com/rezarajan/platformctl/internal/testkit"
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

// TestNetworkPolicyEnforcementIsLive is the B7 caveat closed (doc 11,
// 2026-07-22 "no GA without conclusive evidence"): the objects being
// correct is not proof the cluster ENFORCES them — default minikube/kind
// CNIs silently don't. This test proves enforcement with a live negative
// probe: a pod OUTSIDE a default-deny namespace must FAIL to dial a
// listener inside it, and a same-namespace pod must SUCCEED. Behavior:
//   - PLATFORMCTL_REQUIRE_NETPOL_ENFORCEMENT set (CI, Calico-backed
//     cluster): non-enforcement is a hard FAILURE — a skip can never
//     masquerade as coverage again.
//   - unset (local convenience clusters): the documented skip remains,
//     with the exact command to reproduce a policy-enforcing cluster.
func TestNetworkPolicyEnforcementIsLive(t *testing.T) {
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
	const nsIn = "datascape-netpol-enforce-in"
	const nsOut = "datascape-netpol-enforce-out"
	// docs/adr/029: the janitor owns removal order (workloads before
	// namespaces — RemoveNetwork refuses while occupied) and loudness
	// (silent pre-clean, t.Errorf post-clean). This test was the audit's
	// exemplar stray: its old namespace-only cleanup swallowed the refusal
	// and stranded the listener on every skip-path run.
	jan := testkit.Janitor{RT: rt, Workloads: []string{"npl-listener"}, Networks: []string{nsIn, nsOut}}
	jan.CleanSilent(ctx)
	jan.Register(ctx, t)
	for _, ns := range []string{nsIn, nsOut} {
		if err := rt.EnsureNetwork(ctx, runtimeport.NetworkSpec{Name: ns, Labels: labels}); err != nil {
			t.Fatalf("EnsureNetwork %s: %v", ns, err)
		}
	}

	// A listener inside the default-deny namespace.
	listener := runtimeport.ContainerSpec{
		Name:     "npl-listener",
		Image:    "alpine/socat:1.8.0.3@sha256:beb4a68d9e4fe6b0f21ea774a0fde6c31f580dde6368939ed70100c5385b015e",
		Cmd:      []string{"tcp-listen:9999,fork,reuseaddr", "exec:'echo ok'"},
		Networks: []string{nsIn},
		Labels:   labels,
		Ports:    []runtimeport.PortBinding{{ContainerPort: 9999, Audience: runtimeport.AudienceInternal}},
	}
	if _, err := rt.EnsureContainer(ctx, listener); err != nil {
		t.Fatalf("EnsureContainer listener: %v", err)
	}
	if err := rt.WaitHealthy(ctx, "npl-listener", 120*time.Second); err != nil {
		t.Fatalf("listener never Running: %v", err)
	}

	target := "npl-listener." + nsIn + ".svc.cluster.local:9999"

	// Same-namespace dial must SUCCEED (allow-same-namespace policy).
	if err := rt.ProbeReachable(ctx, nsIn, target); err != nil {
		t.Fatalf("same-namespace dial failed — allow-same-namespace not working: %v", err)
	}

	// Cross-namespace dial must FAIL (default-deny). If it SUCCEEDS the
	// CNI is not enforcing NetworkPolicy.
	err = rt.ProbeReachable(ctx, nsOut, target)
	if err == nil {
		msg := "cross-namespace dial SUCCEEDED through a default-deny wall — this cluster's CNI does not enforce NetworkPolicy"
		if requireEnforce {
			t.Fatal(msg + " (required by PLATFORMCTL_REQUIRE_NETPOL_ENFORCEMENT: use a policy-enforcing CNI, e.g. kind with Calico or `minikube start --cni=calico`)")
		}
		t.Skip(msg + "; skipping (set up Calico/Cilium to prove enforcement locally)")
	}
}
