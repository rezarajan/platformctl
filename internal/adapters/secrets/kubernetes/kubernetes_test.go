package kubernetes

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/rezarajan/platformctl/internal/domain/secret"
)

func TestResolve(t *testing.T) {
	t.Parallel()
	secretObj := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "db-creds", Namespace: "default"},
		Data:       map[string][]byte{"username": []byte("admin"), "password": []byte("hunter2")},
	}
	clientset := fake.NewSimpleClientset(secretObj)
	s := &Store{clientsetFor: func() (kubernetes.Interface, error) { return clientset, nil }}

	ref := secret.SecretReference{Name: "db-creds", Namespace: "default", Backend: secret.BackendKubernetes, Keys: []string{"username", "password"}}
	got, err := s.Resolve(context.Background(), ref)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got["username"] != "admin" || got["password"] != "hunter2" {
		t.Errorf("Resolve = %+v, want username=admin password=hunter2", got)
	}
}

func TestResolveWithKubernetesOverride(t *testing.T) {
	t.Parallel()
	secretObj := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "actual-secret-name", Namespace: "prod"},
		Data:       map[string][]byte{"token": []byte("abc123")},
	}
	clientset := fake.NewSimpleClientset(secretObj)
	s := &Store{clientsetFor: func() (kubernetes.Interface, error) { return clientset, nil }}

	ref := secret.SecretReference{
		Name: "logical-ref-name", Namespace: "default", Backend: secret.BackendKubernetes, Keys: []string{"token"},
		Kubernetes: secret.KubernetesRef{Name: "actual-secret-name", Namespace: "prod"},
	}
	got, err := s.Resolve(context.Background(), ref)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got["token"] != "abc123" {
		t.Errorf("Resolve = %+v, want token=abc123", got)
	}
}

func TestResolveMissingSecretIsClear(t *testing.T) {
	t.Parallel()
	s := &Store{clientsetFor: func() (kubernetes.Interface, error) { return fake.NewSimpleClientset(), nil }}
	ref := secret.SecretReference{Name: "nope", Namespace: "default", Backend: secret.BackendKubernetes, Keys: []string{"x"}}
	_, err := s.Resolve(context.Background(), ref)
	if err == nil {
		t.Fatal("Resolve accepted a nonexistent secret")
	}
	if !strings.Contains(err.Error(), "default/nope") {
		t.Errorf("error does not name the missing secret: %v", err)
	}
}

// TestPreflightAggregatesMissingKeys covers docs/planning/08 B4's own
// accept criterion: every missing key is named in one error, not just the
// first.
func TestPreflightAggregatesMissingKeys(t *testing.T) {
	t.Parallel()
	secretObj := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "partial", Namespace: "default"},
		Data:       map[string][]byte{"username": []byte("admin")},
	}
	clientset := fake.NewSimpleClientset(secretObj)
	s := &Store{clientsetFor: func() (kubernetes.Interface, error) { return clientset, nil }}

	ref := secret.SecretReference{Name: "partial", Namespace: "default", Backend: secret.BackendKubernetes, Keys: []string{"username", "password", "apiKey"}}
	err := s.Preflight(context.Background(), ref)
	if err == nil {
		t.Fatal("Preflight accepted a secret missing keys")
	}
	if !strings.Contains(err.Error(), "password") || !strings.Contains(err.Error(), "apiKey") {
		t.Errorf("Preflight error does not name every missing key: %v", err)
	}
}

func TestNamespaceDefaultsToSecretReferenceNamespace(t *testing.T) {
	t.Parallel()
	secretObj := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "team-secret", Namespace: "team-a"},
		Data:       map[string][]byte{"key": []byte("value")},
	}
	clientset := fake.NewSimpleClientset(secretObj)
	s := &Store{clientsetFor: func() (kubernetes.Interface, error) { return clientset, nil }}

	// No spec.kubernetes override: the Datascape namespace ("team-a") is
	// used directly as the Kubernetes namespace.
	ref := secret.SecretReference{Name: "team-secret", Namespace: "team-a", Backend: secret.BackendKubernetes, Keys: []string{"key"}}
	if _, err := s.Resolve(context.Background(), ref); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
}
