package kubernetes

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	authorizationv1 "k8s.io/api/authorization/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// TestPreflightFailsFastOnUnreachableCluster covers docs/planning/08 B6's
// "unit tests with a stub rest.Config" criterion: a kubeconfig pointing at a
// host nothing listens on must fail Preflight naming the kubeconfig path,
// within a bounded time — not hang until some long client-go default
// timeout, and not panic.
func TestPreflightFailsFastOnUnreachableCluster(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kubeconfig")
	// 127.0.0.1:1 is a reserved port nothing binds — the connection refusal
	// is immediate, unlike a routing black hole that would need the timeout
	// below to actually fire.
	const unreachable = `apiVersion: v1
kind: Config
clusters:
- name: nowhere
  cluster:
    server: https://127.0.0.1:1
contexts:
- name: nowhere
  context:
    cluster: nowhere
current-context: nowhere
`
	if err := os.WriteFile(path, []byte(unreachable), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := Preflight(ctx, map[string]any{"kubeconfig": path, "context": "nowhere"})
	if err == nil {
		t.Fatal("Preflight succeeded against an unreachable cluster")
	}
	if !strings.Contains(err.Error(), path) || !strings.Contains(err.Error(), "nowhere") {
		t.Errorf("Preflight error does not name the kubeconfig path and context: %v", err)
	}
}

// TestPreflightWithClientReportsMissingPermissions exercises the
// permission-check half directly against a fake clientset (the seam
// preflightWithClient provides), independent of any real cluster: a denied
// SelfSubjectAccessReview for one verb/resource must be named in the error;
// an all-allowed clientset must pass clean.
func TestPreflightWithClientReportsMissingPermissions(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	clientset.Fake.PrependReactor("create", "selfsubjectaccessreviews", func(action k8stesting.Action) (bool, apiruntime.Object, error) {
		review := action.(k8stesting.CreateAction).GetObject().(*authorizationv1.SelfSubjectAccessReview)
		attr := review.Spec.ResourceAttributes
		review.Status.Allowed = !(attr.Verb == "delete" && attr.Resource == "secrets")
		return true, review, nil
	})

	err := preflightWithClient(context.Background(), clientset, "kubeconfig=test, context=test")
	if err == nil {
		t.Fatal("preflightWithClient accepted a clientset missing delete secrets permission")
	}
	if !strings.Contains(err.Error(), "delete secrets") {
		t.Errorf("preflight error does not name the missing permission: %v", err)
	}
}

func TestPreflightWithClientPassesWhenFullyAllowed(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	clientset.Fake.PrependReactor("create", "selfsubjectaccessreviews", func(action k8stesting.Action) (bool, apiruntime.Object, error) {
		review := action.(k8stesting.CreateAction).GetObject().(*authorizationv1.SelfSubjectAccessReview)
		review.Status.Allowed = true
		return true, review, nil
	})

	if err := preflightWithClient(context.Background(), clientset, "kubeconfig=test, context=test"); err != nil {
		t.Fatalf("preflightWithClient rejected a fully-allowed clientset: %v", err)
	}
}
