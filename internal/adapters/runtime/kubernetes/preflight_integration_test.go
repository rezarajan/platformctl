//go:build integration

package kubernetes

import (
	"context"
	"os"
	"strings"
	"testing"
)

// TestPreflightLiveCluster covers docs/planning/08 B6 against a real
// cluster: the ambient kubeconfig (whatever `New(nil)` would also use)
// passes Preflight cleanly, and a deliberately wrong context name fails
// fast naming both the kubeconfig and the bad context — not a raw,
// unhelpful client-go error.
func TestPreflightLiveCluster(t *testing.T) {
	require := os.Getenv("PLATFORMCTL_REQUIRE_K8S") != ""
	if _, err := New(nil); err != nil {
		if require {
			t.Fatalf("connect to kubernetes (required by PLATFORMCTL_REQUIRE_K8S): %v", err)
		}
		t.Skipf("no kubernetes configuration; skipping: %v", err)
	}

	if err := Preflight(context.Background(), map[string]any{}); err != nil {
		t.Fatalf("Preflight against the live cluster (ambient config): %v", err)
	}

	err := Preflight(context.Background(), map[string]any{"context": "this-context-does-not-exist"})
	if err == nil {
		t.Fatal("Preflight accepted a nonexistent context")
	}
	if !strings.Contains(err.Error(), "this-context-does-not-exist") {
		t.Errorf("Preflight error does not name the bad context: %v", err)
	}
}
