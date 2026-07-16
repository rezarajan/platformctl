//go:build integration

package kubernetes

import (
	"os"
	"testing"

	"github.com/rezarajan/platformctl/internal/ports/runtime/conformance"
)

// TestConformance runs the shared runtime contract suite against a real
// cluster. It skips self-descriptively when no reachable cluster is
// configured (e.g. the Docker-only CI runner), because absence of a cluster
// is an environment limitation, not a code failure. Set
// PLATFORMCTL_REQUIRE_K8S=1 to turn the skip into a failure on runners that
// are supposed to provide a cluster.
func TestConformance(t *testing.T) {
	require := os.Getenv("PLATFORMCTL_REQUIRE_K8S") != ""
	rt, err := New(nil)
	if err != nil {
		if require {
			t.Fatalf("connect to kubernetes (required by PLATFORMCTL_REQUIRE_K8S): %v", err)
		}
		t.Skipf("no kubernetes configuration; skipping (set KUBECONFIG to run, PLATFORMCTL_REQUIRE_K8S=1 to enforce): %v", err)
	}
	if _, err := rt.clientset.Discovery().ServerVersion(); err != nil {
		if require {
			t.Fatalf("kubernetes cluster unreachable (required by PLATFORMCTL_REQUIRE_K8S): %v", err)
		}
		t.Skipf("kubernetes cluster unreachable; skipping (PLATFORMCTL_REQUIRE_K8S=1 to enforce): %v", err)
	}
	conformance.Run(t, rt, "datascape-k8s-conf")
}
