package kubernetes

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	runtimeport "github.com/rezarajan/platformctl/internal/ports/runtime"
)

func statefulSetIn(ns, name string) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      name,
			Labels:    map[string]string{runtimeport.LabelManagedBy: runtimeport.ManagedByValue},
		},
	}
}

// TestEnsureContainerRefusesStatefulSetToDeployment pins the shape-transition
// guard (docs/adr/004): a container last applied as a StableIdentity
// replica set (a StatefulSet) cannot be converted to the Deployment shape in
// place — StableIdentity: false or Replicas <= 1 against the same name must
// refuse with the destroy-and-recreate remedy, not leave the old StatefulSet
// serving the same app=<name> selector.
func TestEnsureContainerRefusesStatefulSetToDeployment(t *testing.T) {
	t.Parallel()
	const ns = "shape-net"
	clientset := fake.NewSimpleClientset(
		managedNamespace(ns),
		statefulSetIn(ns, "broker"),
	)
	r := &Runtime{clientset: clientset}

	_, err := r.EnsureContainer(context.Background(), runtimeport.ContainerSpec{
		Name:     "broker",
		Image:    "alpine:3.20",
		Networks: []string{ns},
		Labels:   map[string]string{runtimeport.LabelManagedBy: runtimeport.ManagedByValue},
		Replicas: 1,
	})
	if err == nil {
		t.Fatal("EnsureContainer converted a StatefulSet-backed container to a Deployment in place; the transition must be refused")
	}
	if !strings.Contains(err.Error(), "refusing to convert") {
		t.Errorf("refusal error should name the remedy; got: %v", err)
	}
	if _, gerr := clientset.AppsV1().StatefulSets(ns).Get(context.Background(), "broker", metav1.GetOptions{}); gerr != nil {
		t.Fatalf("statefulset was disturbed by the refused transition: %v", gerr)
	}
}

// TestEnsureContainerRefusesDeploymentToStatefulSet is the inverse guard: a
// container last applied as a Deployment cannot be converted to the
// StatefulSet shape in place. ensureHeadlessService refuses the ClusterIP
// Service half of this transition, but a portless Deployment has no Service —
// this pins the Deployment-level refusal that covers that case too.
func TestEnsureContainerRefusesDeploymentToStatefulSet(t *testing.T) {
	t.Parallel()
	const ns = "shape-net"
	clientset := fake.NewSimpleClientset(
		managedNamespace(ns),
		deploymentIn(ns, "worker"),
	)
	r := &Runtime{clientset: clientset}

	_, err := r.EnsureContainer(context.Background(), runtimeport.ContainerSpec{
		Name:           "worker",
		Image:          "alpine:3.20",
		Networks:       []string{ns},
		Labels:         map[string]string{runtimeport.LabelManagedBy: runtimeport.ManagedByValue},
		Replicas:       2,
		StableIdentity: true,
	})
	if err == nil {
		t.Fatal("EnsureContainer converted a Deployment-backed container to a StatefulSet in place; the transition must be refused")
	}
	if !strings.Contains(err.Error(), "refusing to convert") {
		t.Errorf("refusal error should name the remedy; got: %v", err)
	}
	if _, gerr := clientset.AppsV1().Deployments(ns).Get(context.Background(), "worker", metav1.GetOptions{}); gerr != nil {
		t.Fatalf("deployment was disturbed by the refused transition: %v", gerr)
	}
}
