package kubernetes

import (
	"context"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	runtimeport "github.com/rezarajan/platformctl/internal/ports/runtime"
)

// TestEnsureNetworkGraphScopedOmitsAllowSameNamespace pins docs/adr/026 H7's
// core Kubernetes claim end to end against the adapter's own EnsureNetwork
// (not just the pure builder in convert_test.go): a namespace ensured with
// IsolationPolicy: IsolationGraphScoped gets ONLY the default-deny policy —
// no allow-same-namespace rule at all.
func TestEnsureNetworkGraphScopedOmitsAllowSameNamespace(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	r := &Runtime{clientset: clientset}
	ctx := context.Background()

	if err := r.EnsureNetwork(ctx, runtimeport.NetworkSpec{Name: "ns", IsolationPolicy: runtimeport.IsolationGraphScoped}); err != nil {
		t.Fatalf("EnsureNetwork: %v", err)
	}

	if _, err := clientset.NetworkingV1().NetworkPolicies("ns").Get(ctx, denyAllIngressPolicyName, metav1.GetOptions{}); err != nil {
		t.Errorf("expected the default-deny policy to exist: %v", err)
	}
	if _, err := clientset.NetworkingV1().NetworkPolicies("ns").Get(ctx, allowSameNamespacePolicyName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("expected NO allow-same-namespace policy under graph-scoped access, got err=%v", err)
	}
}

// TestEnsureNetworkGraphScopedHealsExistingAllowSameNamespace proves the
// drift-heal path: a namespace that ALREADY carries the (pre-gate) pair —
// e.g. GraphScopedAccess flipped on for a namespace previously reconciled
// without it — converges to default-deny-only on the very next apply,
// rather than silently keeping the stale, gate-defeating hole forever.
func TestEnsureNetworkGraphScopedHealsExistingAllowSameNamespace(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	r := &Runtime{clientset: clientset}
	ctx := context.Background()

	if err := r.EnsureNetwork(ctx, runtimeport.NetworkSpec{Name: "ns"}); err != nil {
		t.Fatalf("EnsureNetwork (pre-gate): %v", err)
	}
	if _, err := clientset.NetworkingV1().NetworkPolicies("ns").Get(ctx, allowSameNamespacePolicyName, metav1.GetOptions{}); err != nil {
		t.Fatalf("precondition: expected allow-same-namespace to exist pre-gate: %v", err)
	}

	if err := r.EnsureNetwork(ctx, runtimeport.NetworkSpec{Name: "ns", IsolationPolicy: runtimeport.IsolationGraphScoped}); err != nil {
		t.Fatalf("EnsureNetwork (gate on): %v", err)
	}
	if _, err := clientset.NetworkingV1().NetworkPolicies("ns").Get(ctx, allowSameNamespacePolicyName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("expected the stale allow-same-namespace policy to be deleted once the gate is on, got err=%v", err)
	}
}

// TestEnsureNetworkGraphScopedRespectsIsolationNone proves the gate never
// silently overrides an operator's stronger, explicit opt-out: a namespace
// declared IsolationNone stays exactly that (no NetworkPolicy provisioning
// at all), even under GraphScopedAccess.
func TestEnsureNetworkGraphScopedRespectsIsolationNone(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	r := &Runtime{clientset: clientset}
	ctx := context.Background()

	if err := r.EnsureNetwork(ctx, runtimeport.NetworkSpec{Name: "ns", IsolationPolicy: runtimeport.IsolationNone}); err != nil {
		t.Fatalf("EnsureNetwork: %v", err)
	}
	if _, err := clientset.NetworkingV1().NetworkPolicies("ns").Get(ctx, denyAllIngressPolicyName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("IsolationNone must provision no NetworkPolicy at all, got err=%v", err)
	}
}

// TestEnsureContainerGraphScopedIngressPolicyLifecycle proves the
// per-container policy's full idempotent lifecycle through the real
// EnsureContainer path: created when AllowFromPeers is declared, removed
// again once a later apply drops it (the same converge-to-declared shape
// ensureExternalIngressPolicy already holds for its own hole).
func TestEnsureContainerGraphScopedIngressPolicyLifecycle(t *testing.T) {
	clientset := fake.NewSimpleClientset(managedNamespace("b"))
	r := &Runtime{clientset: clientset}
	ctx := context.Background()

	spec := runtimeport.ContainerSpec{
		Name:     "x",
		Image:    "img",
		Networks: []string{"b"},
		Labels:   runtimeport.ManagedLabels("b", "Provider", "x", "x"),
		Ports:    []runtimeport.PortBinding{{ContainerPort: 1, Audience: runtimeport.AudienceInternal}},
		AllowFromPeers: []runtimeport.NetworkPeer{
			{Network: "a", Name: "r1"},
		},
	}
	if _, err := r.EnsureContainer(ctx, spec); err != nil {
		t.Fatalf("EnsureContainer: %v", err)
	}
	if _, err := clientset.NetworkingV1().NetworkPolicies("b").Get(ctx, graphScopedIngressPolicyName("x"), metav1.GetOptions{}); err != nil {
		t.Fatalf("expected the graph-scoped ingress policy to exist: %v", err)
	}

	spec.AllowFromPeers = nil
	if _, err := r.EnsureContainer(ctx, spec); err != nil {
		t.Fatalf("EnsureContainer (peers dropped): %v", err)
	}
	if _, err := clientset.NetworkingV1().NetworkPolicies("b").Get(ctx, graphScopedIngressPolicyName("x"), metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("expected the graph-scoped ingress policy to be removed once no peer is declared, got err=%v", err)
	}
}
