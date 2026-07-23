package kubernetes

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	runtimeport "github.com/rezarajan/platformctl/internal/ports/runtime"
)

func TestEnsureTLSSecretCreatesAndUpdates(t *testing.T) {
	t.Parallel()
	clientset := fake.NewSimpleClientset()
	r := &Runtime{clientset: clientset}
	ctx := context.Background()
	labels := runtimeport.ManagedLabels("datascape", "Connection", "nessie", "nessie")

	if err := r.EnsureTLSSecret(ctx, "datascape", "tls-nessie", []byte("cert-v1"), []byte("key-v1"), labels); err != nil {
		t.Fatalf("create: %v", err)
	}
	secret, err := clientset.CoreV1().Secrets("datascape").Get(ctx, "tls-nessie", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if secret.Type != corev1.SecretTypeTLS {
		t.Errorf("Type = %v, want kubernetes.io/tls", secret.Type)
	}
	if string(secret.Data["tls.crt"]) != "cert-v1" || string(secret.Data["tls.key"]) != "key-v1" {
		t.Errorf("Data = %+v, want cert-v1/key-v1", secret.Data)
	}

	// Idempotent re-ensure with identical content.
	if err := r.EnsureTLSSecret(ctx, "datascape", "tls-nessie", []byte("cert-v1"), []byte("key-v1"), labels); err != nil {
		t.Fatalf("re-ensure: %v", err)
	}

	// Rotation: content changes converge the live Secret (no restart concept
	// on Kubernetes — Update just replaces Data).
	if err := r.EnsureTLSSecret(ctx, "datascape", "tls-nessie", []byte("cert-v2"), []byte("key-v2"), labels); err != nil {
		t.Fatalf("update: %v", err)
	}
	secret, err = clientset.CoreV1().Secrets("datascape").Get(ctx, "tls-nessie", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get after update: %v", err)
	}
	if string(secret.Data["tls.crt"]) != "cert-v2" {
		t.Errorf("cert not rotated: got %q, want cert-v2", secret.Data["tls.crt"])
	}
}

func TestGetTLSSecretNotFound(t *testing.T) {
	t.Parallel()
	r := &Runtime{clientset: fake.NewSimpleClientset()}
	cert, key, found, err := r.GetTLSSecret(context.Background(), "datascape", "missing")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found || cert != nil || key != nil {
		t.Errorf("found=%v cert=%v key=%v, want (nil, nil, false)", found, cert, key)
	}
}

func TestGetTLSSecretRoundTrips(t *testing.T) {
	t.Parallel()
	clientset := fake.NewSimpleClientset()
	r := &Runtime{clientset: clientset}
	ctx := context.Background()
	if err := r.EnsureTLSSecret(ctx, "datascape", "ingress-ca", []byte("ca-cert"), []byte("ca-key"), nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	cert, key, found, err := r.GetTLSSecret(ctx, "datascape", "ingress-ca")
	if err != nil || !found {
		t.Fatalf("get: found=%v err=%v", found, err)
	}
	if string(cert) != "ca-cert" || string(key) != "ca-key" {
		t.Errorf("got cert=%q key=%q, want ca-cert/ca-key", cert, key)
	}
}

func TestRemoveTLSSecretIdempotent(t *testing.T) {
	t.Parallel()
	clientset := fake.NewSimpleClientset()
	r := &Runtime{clientset: clientset}
	ctx := context.Background()
	if err := r.EnsureTLSSecret(ctx, "datascape", "tls-nessie", []byte("c"), []byte("k"), nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := r.RemoveTLSSecret(ctx, "datascape", "tls-nessie"); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if err := r.RemoveTLSSecret(ctx, "datascape", "tls-nessie"); err != nil {
		t.Fatalf("second remove: %v", err)
	}
	if _, _, found, _ := r.GetTLSSecret(ctx, "datascape", "tls-nessie"); found {
		t.Error("secret still present after RemoveTLSSecret")
	}
}

func TestEnsureTLSSecretRefusesUnmanaged(t *testing.T) {
	t.Parallel()
	clientset := fake.NewSimpleClientset()
	r := &Runtime{clientset: clientset}
	ctx := context.Background()
	unmanaged := buildTLSSecret("datascape", "tls-nessie", []byte("c"), []byte("k"), nil)
	unmanaged.Labels = map[string]string{} // no ownership label
	if _, err := clientset.CoreV1().Secrets("datascape").Create(ctx, unmanaged, metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed unmanaged secret: %v", err)
	}
	if err := r.EnsureTLSSecret(ctx, "datascape", "tls-nessie", []byte("c2"), []byte("k2"), nil); err == nil {
		t.Error("expected refusal to replace an unmanaged secret, got nil error")
	}
}

// TestRuntimeImplementsIngressCapableRuntimeTLSMethods is a compile-time-ish
// guard: a real *Runtime value must satisfy the full extended
// IngressCapableRuntime interface (docs/adr/018 addendum's own lesson —
// found only via a real end-to-end apply through the registry wrapper, not
// a fake-clientset unit test, so this at least pins the adapter side).
func TestRuntimeImplementsIngressCapableRuntimeTLSMethods(t *testing.T) {
	t.Parallel()
	var _ runtimeport.IngressCapableRuntime = (*Runtime)(nil)
}
