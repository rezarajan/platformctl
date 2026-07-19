//go:build integration

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	k8sruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/kubernetes"
)

// requireK8s skips (or fails, under PLATFORMCTL_REQUIRE_K8S=1) when no
// reachable cluster is configured — the same self-descriptive pattern
// internal/adapters/runtime/kubernetes's own conformance test uses, so a
// Docker-only CI runner or laptop never fails these for an environment
// reason rather than a code one.
func requireK8s(t *testing.T) {
	t.Helper()
	require := os.Getenv("PLATFORMCTL_REQUIRE_K8S") != ""
	if _, err := k8sruntime.New(nil); err != nil {
		if require {
			t.Fatalf("connect to kubernetes (required by PLATFORMCTL_REQUIRE_K8S): %v", err)
		}
		t.Skipf("no kubernetes configuration; skipping (set KUBECONFIG to run, PLATFORMCTL_REQUIRE_K8S=1 to enforce): %v", err)
	}
}

func writeManifest(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "manifests.yaml"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

// TestValidateRefusesKubernetesRuntimeWhenGateExplicitlyDisabled covers
// docs/planning/08 B6/B9: KubernetesRuntime graduated to Beta (enabled by
// default) at Stage B close, but the gate mechanism itself must still work
// as an off-switch — a manifest naming a kubernetes runtime Provider fails
// validate with the standard gate error when the gate is explicitly turned
// off, never a network dial, and never silently accepted.
func TestValidateRefusesKubernetesRuntimeWhenGateExplicitlyDisabled(t *testing.T) {
	dir := writeManifest(t, `
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: k8s-gate-test
spec:
  type: redpanda
  runtime:
    type: kubernetes
    network: datascape-gate-test
`)
	_, err, code := run(t, "validate", dir, "--feature-gates", "KubernetesRuntime=false")
	if err == nil {
		t.Fatal("validate accepted a kubernetes runtime Provider with the KubernetesRuntime gate explicitly disabled")
	}
	if code != 3 { // cliutil.ExitValidation
		t.Errorf("exit code = %d, want 3 (ExitValidation)", code)
	}
	if !strings.Contains(err.Error(), "KubernetesRuntime") {
		t.Errorf("error does not name the KubernetesRuntime gate: %v", err)
	}
}

// TestValidateFailsFastOnBadKubernetesContext covers docs/planning/08 B6's
// literal accept criterion: a manifest naming a nonexistent kubeconfig
// context fails `validate` itself, naming both the kubeconfig and the bad
// context — not a raw client-go error surfacing mid-apply.
func TestValidateFailsFastOnBadKubernetesContext(t *testing.T) {
	requireK8s(t)
	dir := writeManifest(t, `
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: k8s-context-test
spec:
  type: redpanda
  runtime:
    type: kubernetes
    network: datascape-context-test
    context: this-context-does-not-exist
`)
	_, err, code := run(t, "validate", dir)
	if err == nil {
		t.Fatal("validate accepted a nonexistent kubernetes context")
	}
	if code != 3 { // cliutil.ExitValidation
		t.Errorf("exit code = %d, want 3 (ExitValidation)", code)
	}
	if !strings.Contains(err.Error(), "this-context-does-not-exist") {
		t.Errorf("error does not name the bad context: %v", err)
	}
}

// TestValidatePassesWithReachableKubernetesCluster is the positive-path
// counterpart: a manifest naming the ambient (working) cluster passes
// validate cleanly with no --feature-gates flag at all — KubernetesRuntime
// is Beta (enabled by default) as of Stage B close (docs/planning/08 B9).
func TestValidatePassesWithReachableKubernetesCluster(t *testing.T) {
	requireK8s(t)
	dir := writeManifest(t, `
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: k8s-reachable-test
spec:
  type: redpanda
  runtime:
    type: kubernetes
    network: datascape-reachable-test
`)
	out, err, code := run(t, "validate", dir)
	if err != nil || code != 0 {
		t.Fatalf("validate against a reachable cluster failed (code %d): %v\n%s", code, err, out)
	}
}
