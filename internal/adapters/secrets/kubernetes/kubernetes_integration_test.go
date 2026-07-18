//go:build integration

package kubernetes

import (
	"context"
	"os"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/rezarajan/platformctl/internal/domain/secret"
)

// TestResolveLiveCluster covers docs/planning/08 B4 against a real cluster:
// Resolve/Preflight against a real Secret object, and — the "rotation
// fingerprinting works" accept criterion — that changing the Secret's data
// out-of-band produces a different resolved value on the next Resolve
// (the engine's existing SecretHashes mechanism fingerprints whatever
// Resolve returns, so proving Resolve reflects live Secret content is what
// makes rotation detection work for this backend, same as every other one).
func TestResolveLiveCluster(t *testing.T) {
	require := os.Getenv("PLATFORMCTL_REQUIRE_K8S") != ""
	s := New()
	clientset, err := s.clientsetFor()
	if err != nil {
		if require {
			t.Fatalf("connect to kubernetes (required by PLATFORMCTL_REQUIRE_K8S): %v", err)
		}
		t.Skipf("no kubernetes configuration; skipping: %v", err)
	}
	if _, err := clientset.Discovery().ServerVersion(); err != nil {
		if require {
			t.Fatalf("kubernetes cluster unreachable (required by PLATFORMCTL_REQUIRE_K8S): %v", err)
		}
		t.Skipf("kubernetes cluster unreachable; skipping: %v", err)
	}

	ctx := context.Background()
	const ns = "default"
	const name = "datascape-secretstore-test"
	t.Cleanup(func() { _ = clientset.CoreV1().Secrets(ns).Delete(ctx, name, metav1.DeleteOptions{}) })

	if _, err := clientset.CoreV1().Secrets(ns).Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Data:       map[string][]byte{"password": []byte("initial-pw")},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create test secret: %v", err)
	}

	ref := secret.SecretReference{Name: name, Namespace: ns, Backend: secret.BackendKubernetes, Keys: []string{"password"}}
	if err := s.Preflight(ctx, ref); err != nil {
		t.Fatalf("Preflight: %v", err)
	}
	got, err := s.Resolve(ctx, ref)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got["password"] != "initial-pw" {
		t.Fatalf("Resolve = %+v, want password=initial-pw", got)
	}

	// Rotate out-of-band; the next Resolve must reflect the new value —
	// this is what makes the engine's SecretHashes fingerprinting detect
	// SecretChanged for the kubernetes backend, the same as every other one.
	sec, err := clientset.CoreV1().Secrets(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get secret for rotation: %v", err)
	}
	sec.Data["password"] = []byte("rotated-pw")
	if _, err := clientset.CoreV1().Secrets(ns).Update(ctx, sec, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("rotate secret: %v", err)
	}
	got, err = s.Resolve(ctx, ref)
	if err != nil {
		t.Fatalf("Resolve after rotation: %v", err)
	}
	if got["password"] != "rotated-pw" {
		t.Fatalf("Resolve after rotation = %+v, want password=rotated-pw (fingerprinting depends on this reflecting live content)", got)
	}

	// A missing key after rotation drops it entirely: aggregated error.
	delete(sec.Data, "password")
	sec.ResourceVersion = "" // re-fetch to avoid a stale conflict
	fresh, err := clientset.CoreV1().Secrets(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	fresh.Data = map[string][]byte{}
	if _, err := clientset.CoreV1().Secrets(ns).Update(ctx, fresh, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("clear secret data: %v", err)
	}
	if err := s.Preflight(ctx, ref); err == nil || !strings.Contains(err.Error(), "password") {
		t.Errorf("Preflight after clearing the key = %v, want an error naming password", err)
	}
}
