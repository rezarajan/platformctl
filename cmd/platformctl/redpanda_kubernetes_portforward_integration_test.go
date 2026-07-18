//go:build integration

package main

import (
	"context"
	"path/filepath"
	"testing"

	k8sruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/kubernetes"
)

// TestRedpandaKubernetesPortForwardEndToEnd covers docs/planning/08 B1's
// other explicit accept criterion: the default access mode (port-forward)
// works for a real admin operation (topic creation) against a real cluster,
// with no runtime.access set in the manifest at all.
func TestRedpandaKubernetesPortForwardEndToEnd(t *testing.T) {
	requireK8s(t)
	rt, err := k8sruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to kubernetes: %v", err)
	}
	ctx := context.Background()
	const ns = "datascape-rpk8s-pf-test-ns"
	cleanup := func() { _ = rt.RemoveNetwork(ctx, ns) }
	cleanup()
	t.Cleanup(cleanup)

	stateFile := filepath.Join(t.TempDir(), "state.json")
	manifests := "testdata/redpanda-k8s-pf-scenario"
	const gateVal = "KubernetesRuntime=true"

	out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", gateVal)
	if err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}

	// Independently verify via a fresh port-forward tunnel (not reusing
	// anything the CLI process itself opened) that the topic genuinely
	// exists — proof reconcileTopic's admin call actually reached the
	// broker and created it, not just that apply exited 0.
	addr, closeFn, err := rt.EnsureReachable(ctx, "datascape-rpk8s-pf-test", 9092)
	if err != nil {
		t.Fatalf("EnsureReachable: %v", err)
	}
	defer closeFn()
	partitions, _ := describeTopicAt(t, addr, "datascape-rpk8s-pf-test-events")
	if partitions != 2 {
		t.Errorf("topic partitions = %d, want 2", partitions)
	}

	out, err, code = run(t, "destroy", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", gateVal)
	if err != nil || code != 0 {
		t.Fatalf("destroy failed (code %d): %v\n%s", code, err, out)
	}
}
