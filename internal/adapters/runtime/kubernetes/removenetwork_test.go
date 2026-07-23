package kubernetes

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	runtimeport "github.com/rezarajan/platformctl/internal/ports/runtime"
)

func managedNamespace(name string) *corev1.Namespace {
	return &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{runtimeport.LabelManagedBy: runtimeport.ManagedByValue},
		},
	}
}

func deploymentIn(ns, name string) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      name,
			Labels:    map[string]string{runtimeport.LabelManagedBy: runtimeport.ManagedByValue},
		},
	}
}

// TestRemoveNetworkRefusesWhileWorkloadsRemain pins the shared-namespace
// safety guard: providers that share one network each best-effort-call
// RemoveNetwork on Destroy, so deleting the namespace out from under a still-
// running sibling (or any unmanaged workload placed alongside it) must not
// happen. This mirrors Docker's refusal to remove a network that still has
// attached containers. Previously this was only observable through the live
// lakehouse integration test (external-orders-db surviving destroy); here it
// is pinned in `go test ./...`.
func TestRemoveNetworkRefusesWhileWorkloadsRemain(t *testing.T) {
	const ns = "shared"
	clientset := fake.NewSimpleClientset(
		managedNamespace(ns),
		deploymentIn(ns, "still-running"),
	)
	r := &Runtime{clientset: clientset}

	if err := r.RemoveNetwork(context.Background(), ns); err == nil {
		t.Fatal("RemoveNetwork deleted a namespace that still holds a workload")
	}
	if _, err := clientset.CoreV1().Namespaces().Get(context.Background(), ns, metav1.GetOptions{}); apierrors.IsNotFound(err) {
		t.Fatal("namespace was deleted despite a remaining workload")
	} else if err != nil {
		t.Fatalf("get namespace: %v", err)
	}
}

// TestRemoveNetworkDeletesEmptyNamespace is the other half: once the namespace
// has been emptied of workloads (the last member's Remove blocks until its
// Deployment is gone), RemoveNetwork actually deletes it and reports success —
// exactly what the conformance suite's Remove_then_absent step relies on.
func TestRemoveNetworkDeletesEmptyNamespace(t *testing.T) {
	const ns = "empty"
	clientset := fake.NewSimpleClientset(managedNamespace(ns))
	r := &Runtime{clientset: clientset}

	if err := r.RemoveNetwork(context.Background(), ns); err != nil {
		t.Fatalf("RemoveNetwork on an empty managed namespace: %v", err)
	}
	if _, err := clientset.CoreV1().Namespaces().Get(context.Background(), ns, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("namespace was not deleted (err=%v)", err)
	}
}

// TestEnsureNetworkCrossDomainIngressConverges is docs/adr/022 Ring 1's
// Kubernetes end-to-end (docs/planning/08 H5): EnsureNetwork with
// AllowFromNetworks creates the cross-domain NetworkPolicy alongside the
// ordinary B7 pair, updates it when the allow-list changes, and removes it
// again once no domain is allowed in — the same create/converge/delete
// lifecycle every other managed object in this adapter gets.
func TestEnsureNetworkCrossDomainIngressConverges(t *testing.T) {
	const ns = "datascape-analytics"
	clientset := fake.NewSimpleClientset()
	r := &Runtime{clientset: clientset}
	ctx := context.Background()
	labels := map[string]string{runtimeport.LabelManagedBy: runtimeport.ManagedByValue}

	if err := r.EnsureNetwork(ctx, runtimeport.NetworkSpec{Name: ns, Labels: labels, AllowFromNetworks: []string{"datascape-payments"}}); err != nil {
		t.Fatalf("EnsureNetwork (create): %v", err)
	}
	policy, err := clientset.NetworkingV1().NetworkPolicies(ns).Get(ctx, crossDomainIngressPolicyName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("cross-domain ingress policy was not created: %v", err)
	}
	if got := len(policy.Spec.Ingress[0].From); got != 1 {
		t.Fatalf("peers = %d, want 1", got)
	}

	if err := r.EnsureNetwork(ctx, runtimeport.NetworkSpec{Name: ns, Labels: labels, AllowFromNetworks: []string{"datascape-payments", "datascape-billing"}}); err != nil {
		t.Fatalf("EnsureNetwork (update): %v", err)
	}
	policy, err = clientset.NetworkingV1().NetworkPolicies(ns).Get(ctx, crossDomainIngressPolicyName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("cross-domain ingress policy missing after update: %v", err)
	}
	if got := len(policy.Spec.Ingress[0].From); got != 2 {
		t.Fatalf("peers after update = %d, want 2", got)
	}

	if err := r.EnsureNetwork(ctx, runtimeport.NetworkSpec{Name: ns, Labels: labels}); err != nil {
		t.Fatalf("EnsureNetwork (no allow-list): %v", err)
	}
	if _, err := clientset.NetworkingV1().NetworkPolicies(ns).Get(ctx, crossDomainIngressPolicyName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("cross-domain ingress policy should be removed once no domain is allowed in (err=%v)", err)
	}
	// The ordinary B7 pair must still be present regardless.
	for _, name := range []string{denyAllIngressPolicyName, allowSameNamespacePolicyName} {
		if _, err := clientset.NetworkingV1().NetworkPolicies(ns).Get(ctx, name, metav1.GetOptions{}); err != nil {
			t.Errorf("expected %q to still exist: %v", name, err)
		}
	}
}

// TestRemoveNetworkRefusesUnmanagedNamespace keeps the pre-existing ownership
// guard intact: a namespace lacking the managed-by label is never touched,
// regardless of whether it is empty.
func TestRemoveNetworkRefusesUnmanagedNamespace(t *testing.T) {
	const ns = "not-ours"
	clientset := fake.NewSimpleClientset(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: ns},
	})
	r := &Runtime{clientset: clientset}

	if err := r.RemoveNetwork(context.Background(), ns); err == nil {
		t.Fatal("RemoveNetwork removed a namespace not managed by platformctl")
	}
	if _, err := clientset.CoreV1().Namespaces().Get(context.Background(), ns, metav1.GetOptions{}); err != nil {
		t.Fatalf("unmanaged namespace was disturbed: %v", err)
	}
}
