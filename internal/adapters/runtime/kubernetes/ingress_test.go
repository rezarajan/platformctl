package kubernetes

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	runtimeport "github.com/rezarajan/platformctl/internal/ports/runtime"
)

func TestEnsureIngressCreatesAndUpdates(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	r := &Runtime{clientset: clientset}
	ctx := context.Background()

	spec := runtimeport.IngressSpec{
		Name: "route-nessie", Namespace: "datascape", Host: "nessie.localhost",
		TargetName: "nessie", TargetPort: 19120,
		Labels: runtimeport.ManagedLabels("datascape", "Connection", "nessie", "nessie"),
	}
	if _, err := r.EnsureIngress(ctx, spec); err != nil {
		t.Fatalf("create: %v", err)
	}
	ing, err := clientset.NetworkingV1().Ingresses("datascape").Get(ctx, "route-nessie", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if ing.Spec.Rules[0].Host != "nessie.localhost" {
		t.Errorf("host = %q, want nessie.localhost", ing.Spec.Rules[0].Host)
	}
	backend := ing.Spec.Rules[0].HTTP.Paths[0].Backend.Service
	if backend.Name != "nessie" || backend.Port.Number != 19120 {
		t.Errorf("backend = %s:%d, want nessie:19120", backend.Name, backend.Port.Number)
	}

	// Idempotent re-ensure with an identical spec: no error, same content.
	if _, err := r.EnsureIngress(ctx, spec); err != nil {
		t.Fatalf("re-ensure: %v", err)
	}

	// Drift heal: spec changed (different target) converges the live object.
	spec.TargetPort = 8080
	if _, err := r.EnsureIngress(ctx, spec); err != nil {
		t.Fatalf("update: %v", err)
	}
	ing, err = clientset.NetworkingV1().Ingresses("datascape").Get(ctx, "route-nessie", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get after update: %v", err)
	}
	if ing.Spec.Rules[0].HTTP.Paths[0].Backend.Service.Port.Number != 8080 {
		t.Errorf("port not updated: got %d, want 8080", ing.Spec.Rules[0].HTTP.Paths[0].Backend.Service.Port.Number)
	}
}

func TestGetIngressNotFound(t *testing.T) {
	r := &Runtime{clientset: fake.NewSimpleClientset()}
	_, found, err := r.GetIngress(context.Background(), "datascape", "route-missing")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Error("found = true, want false for a nonexistent Ingress")
	}
}

func TestRemoveIngressIdempotent(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	r := &Runtime{clientset: clientset}
	ctx := context.Background()
	spec := runtimeport.IngressSpec{Name: "route-nessie", Namespace: "datascape", Host: "nessie.localhost", TargetName: "nessie", TargetPort: 19120}
	if _, err := r.EnsureIngress(ctx, spec); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := r.RemoveIngress(ctx, "datascape", "route-nessie"); err != nil {
		t.Fatalf("remove: %v", err)
	}
	// Second remove: no-op, not an error (idempotent, matching every other
	// Remove* on this port).
	if err := r.RemoveIngress(ctx, "datascape", "route-nessie"); err != nil {
		t.Fatalf("second remove: %v", err)
	}
	if _, found, _ := r.GetIngress(ctx, "datascape", "route-nessie"); found {
		t.Error("ingress still present after RemoveIngress")
	}
}

func TestEnsureIngressSetsTLSBlock(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	r := &Runtime{clientset: clientset}
	ctx := context.Background()
	spec := runtimeport.IngressSpec{
		Name: "route-nessie", Namespace: "datascape", Host: "nessie.localhost",
		TargetName: "nessie", TargetPort: 19120, TLSSecretName: "tls-nessie",
	}
	state, err := r.EnsureIngress(ctx, spec)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if state.TLSSecretName != "tls-nessie" {
		t.Errorf("returned state TLSSecretName = %q, want tls-nessie", state.TLSSecretName)
	}
	ing, err := clientset.NetworkingV1().Ingresses("datascape").Get(ctx, "route-nessie", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(ing.Spec.TLS) != 1 || ing.Spec.TLS[0].SecretName != "tls-nessie" || ing.Spec.TLS[0].Hosts[0] != "nessie.localhost" {
		t.Errorf("Spec.TLS = %+v, want one entry for nessie.localhost -> tls-nessie", ing.Spec.TLS)
	}

	got, found, err := r.GetIngress(ctx, "datascape", "route-nessie")
	if err != nil || !found {
		t.Fatalf("GetIngress: found=%v err=%v", found, err)
	}
	if got.TLSSecretName != "tls-nessie" {
		t.Errorf("GetIngress TLSSecretName = %q, want tls-nessie", got.TLSSecretName)
	}
}

func TestBuildIngressNoTLSBlockWhenSecretNameEmpty(t *testing.T) {
	ing := buildIngress(runtimeport.IngressSpec{Name: "route-x", Namespace: "datascape", Host: "x.localhost", TargetName: "x", TargetPort: 1})
	if len(ing.Spec.TLS) != 0 {
		t.Errorf("Spec.TLS = %+v, want empty for a plaintext (no TLSSecretName) spec", ing.Spec.TLS)
	}
}

func TestEnsureIngressRefusesUnmanaged(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	r := &Runtime{clientset: clientset}
	ctx := context.Background()
	unmanaged := buildIngress(runtimeport.IngressSpec{Name: "route-nessie", Namespace: "datascape", Host: "x", TargetName: "y", TargetPort: 1})
	unmanaged.Labels = map[string]string{} // no ownership label
	if _, err := clientset.NetworkingV1().Ingresses("datascape").Create(ctx, unmanaged, metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed unmanaged ingress: %v", err)
	}
	if _, err := r.EnsureIngress(ctx, runtimeport.IngressSpec{Name: "route-nessie", Namespace: "datascape", Host: "nessie.localhost", TargetName: "nessie", TargetPort: 19120}); err == nil {
		t.Error("expected refusal to replace an unmanaged ingress, got nil error")
	}
}
