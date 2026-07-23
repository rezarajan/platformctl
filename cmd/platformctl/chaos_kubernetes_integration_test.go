//go:build integration

package main

import (
	"bufio"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	k8sruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/kubernetes"
	"github.com/rezarajan/platformctl/internal/adapters/state/localfile"
	"github.com/rezarajan/platformctl/internal/testkit"
)

const chaosK8sGates = "KubernetesRuntime=true"

// TestKubernetesChaosApplyKilledMidRun is I6's (docs/planning/08 §7.8)
// Kubernetes leg of TestChaosApplyKilledMidRun: the same NFR-9
// (recoverability) proof — SIGKILL the CLI partway through an apply, require
// the state file to remain valid JSON reflecting exactly the completed
// resources, and require a re-run to finish the job — run against a real
// cluster via testdata/chaos-k8s-scenario (the runtime: kubernetes mirror of
// testdata/cdc-scenario, the Docker test's own fixture) instead of Docker.
func TestKubernetesChaosApplyKilledMidRun(t *testing.T) {
	requireK8s(t)
	t.Setenv("DATASCAPE_SECRET_CHAOSK8S_PG_ADMIN_USERNAME", "datascape_admin")
	t.Setenv("DATASCAPE_SECRET_CHAOSK8S_PG_ADMIN_PASSWORD", "admin-secret-pw")
	t.Setenv("DATASCAPE_SECRET_CHAOSK8S_PG_REPL_USERNAME", "datascape_repl")
	t.Setenv("DATASCAPE_SECRET_CHAOSK8S_PG_REPL_PASSWORD", "repl-secret-pw")

	rt, err := k8sruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to kubernetes: %v", err)
	}
	ctx := context.Background()
	const ns = "datascape-chaosk8s-test-ns"
	workloads := []string{"datascape-chaosk8s-dbz", "datascape-chaosk8s-pg", "datascape-chaosk8s-rp"}
	// Remove the workloads first: RemoveNetwork refuses (by contract) while
	// the namespace still holds one, so a network-only cleanup would leak
	// the whole deployment into the next run (the same rule
	// TestRedpandaHAKubernetesEndToEnd and the lakehouse K8s example lean
	// on).
	// docs/adr/029: janitor-owned cleanup (J2 sweep) — declared
	// objects, canonical order, silent pre-clean, loud post-clean.
	jan := testkit.Janitor{
		RT:        rt,
		Workloads: workloads,
		Networks:  []string{ns},
	}
	jan.CleanSilent(ctx)
	jan.Register(ctx, t)

	bin := filepath.Join(t.TempDir(), "platformctl")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build binary: %v\n%s", err, out)
	}

	stateFile := filepath.Join(t.TempDir(), "state.json")
	manifests := "testdata/chaos-k8s-scenario"

	cmd := exec.Command(bin, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", chaosK8sGates)
	// Inherit the whole environment — KUBECONFIG (the minted minimal-RBAC
	// kubeconfig, docs/planning/06 §8 rule 4) included — exactly as the
	// Docker chaos test inherits DATASCAPE_SECRET_*.
	cmd.Env = os.Environ()
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start apply: %v", err)
	}

	// Kill hard after the first resource completes (the progress reporter
	// prints a "✓" done line per finished step) — the same signal the
	// Docker chaos test uses, just against a slower (real pod
	// scheduling/image pull) apply sequence.
	scanner := bufio.NewScanner(stderr)
	killed := false
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), "✓") {
			if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
				t.Fatalf("kill apply: %v", err)
			}
			killed = true
			break
		}
	}
	_ = cmd.Wait()
	if !killed {
		t.Fatal("apply finished before it could be killed; scenario too small for this test")
	}

	// State must be valid and reflect at least the completed resource.
	st, err := localfile.New(stateFile).Load(ctx)
	if err != nil {
		t.Fatalf("state file invalid after SIGKILL: %v", err)
	}
	if len(st.Resources) == 0 {
		t.Fatal("state file empty after a resource reported ok")
	}
	t.Logf("state after kill: %d resource(s) recorded", len(st.Resources))

	// A re-run completes the remainder. Kubernetes scheduling/image pulls
	// are slower and share the cluster with other agents' suites
	// (docs/planning/06 §10 rule 3/4) — this deadline is generous relative
	// to the Docker test's 60s.
	deadline := time.Now().Add(10 * time.Minute)
	for {
		out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", chaosK8sGates)
		if err == nil && code == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("re-apply after kill did not converge (code %d): %v\n%s", code, err, out)
		}
		time.Sleep(5 * time.Second)
	}

	out, err, code := run(t, "status", manifests, "--state-file", stateFile, "--feature-gates", chaosK8sGates)
	if err != nil || code != 0 {
		t.Fatalf("status failed (code %d): %v\n%s", code, err, out)
	}
	assertAllStatusReady(t, out, "recovery")

	// Clean drift: no changes recorded once converged.
	if report, code := runDrift(t, manifests, stateFile, "--feature-gates", chaosK8sGates); code != 0 {
		t.Errorf("drift after recovery exited %d:\n%+v", code, report)
	}

	out, err, code = run(t, "destroy", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", chaosK8sGates)
	if err != nil || code != 0 {
		t.Fatalf("destroy failed (code %d): %v\n%s", code, err, out)
	}
	for _, name := range workloads {
		if _, found, _ := rt.Inspect(ctx, name); found {
			t.Errorf("deployment %q still present after destroy", name)
		}
	}
}
