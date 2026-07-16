//go:build integration

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	dockerruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/docker"
	"github.com/rezarajan/platformctl/internal/adapters/state/localfile"
	"github.com/rezarajan/platformctl/internal/cliutil"
)

func setSinkSecrets(t *testing.T) {
	t.Setenv("DATASCAPE_SECRET_SINK_PG_ADMIN_USERNAME", "datascape_admin")
	t.Setenv("DATASCAPE_SECRET_SINK_PG_ADMIN_PASSWORD", "admin-secret-pw")
	t.Setenv("DATASCAPE_SECRET_SINK_PG_REPL_USERNAME", "datascape_repl")
	t.Setenv("DATASCAPE_SECRET_SINK_PG_REPL_PASSWORD", "repl-secret-pw")
	t.Setenv("DATASCAPE_SECRET_SINK_MINIO_ROOT_USERNAME", "datascape_minio")
	t.Setenv("DATASCAPE_SECRET_SINK_MINIO_ROOT_PASSWORD", "minio-secret-pw")
}

type driftReport struct {
	Resource string `json:"resource"`
	Ready    string `json:"ready"`
	Drift    string `json:"drift"`
	Reason   string `json:"reason"`
}

func runDrift(t *testing.T, manifests, stateFile string) (map[string]driftReport, int) {
	t.Helper()
	out, _, code := run(t, "drift", manifests, "--state-file", stateFile, "-o", "json")
	var payload struct {
		Resources []driftReport `json:"resources"`
	}
	if err := json.NewDecoder(strings.NewReader(out)).Decode(&payload); err != nil {
		t.Fatalf("decode drift output: %v\n%s", err, out)
	}
	byResource := make(map[string]driftReport, len(payload.Resources))
	for _, r := range payload.Resources {
		byResource[r.Resource] = r
		if trimmed := strings.TrimPrefix(r.Resource, "default/"); trimmed != r.Resource {
			byResource[trimmed] = r
		}
	}
	return byResource, code
}

// TestChaosExternalFailures is the chaos-monkey scenario: kill and stop
// managed containers out-of-band, then require the tooling to observe it
// (drift), refuse to panic-mutate (plan), heal it (apply), and still tear
// everything down cleanly when parts of the platform are already dead
// (destroy). This is the errors.md "external failures are not observed and
// thus impossible to reconcile" regression test.
func TestChaosExternalFailures(t *testing.T) {
	setSinkSecrets(t)
	buildSinkConnectImage(t)

	rt, err := dockerruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to Docker: %v", err)
	}
	ctx := context.Background()

	containers := []string{"datascape-sink-s3", "datascape-sink-minio", "datascape-sink-dbz", "datascape-sink-pg", "datascape-sink-rp"}
	cleanup := func() {
		for _, c := range containers {
			_ = rt.Remove(ctx, c)
		}
		for _, v := range []string{"datascape-sink-pg-data", "datascape-sink-rp-data", "datascape-sink-minio-data"} {
			_ = rt.RemoveVolume(ctx, v)
		}
		_ = rt.RemoveNetwork(ctx, "datascape-sink-net")
	}
	cleanup()
	t.Cleanup(cleanup)

	stateFile := filepath.Join(t.TempDir(), "state.json")
	manifests := "testdata/sink-scenario"

	out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}

	// Baseline: a healthy platform reports no drift.
	report, code := runDrift(t, manifests, stateFile)
	if code != 0 {
		t.Fatalf("drift on a healthy platform exited %d:\n%+v", code, report)
	}

	// CHAOS: remove the object store and the database; stop the CDC worker.
	if err := rt.Remove(ctx, "datascape-sink-minio"); err != nil {
		t.Fatalf("chaos remove minio: %v", err)
	}
	if err := rt.Remove(ctx, "datascape-sink-pg"); err != nil {
		t.Fatalf("chaos remove postgres: %v", err)
	}
	if out, err := exec.Command("docker", "stop", "datascape-sink-dbz").CombinedOutput(); err != nil {
		t.Fatalf("chaos stop debezium: %v\n%s", err, out)
	}

	// drift observes every casualty and exits 1.
	report, code = runDrift(t, manifests, stateFile)
	if code != cliutil.ExitPlanChanges {
		t.Errorf("drift exit code = %d, want %d", code, cliutil.ExitPlanChanges)
	}
	for _, victim := range []string{
		"Provider/datascape-sink-minio",
		"Provider/datascape-sink-pg",
		"Provider/datascape-sink-dbz",
		"Dataset/attendance-raw",
		"Source/sink-students",
	} {
		if r := report[victim]; r.Drift != "True" {
			t.Errorf("%s drift = %q (reason %q), want True", victim, r.Drift, r.Reason)
		}
	}

	// status reflects the recorded observation without re-probing.
	out, err, code = run(t, "status", manifests, "--state-file", stateFile, "-o", "json")
	if err != nil || code != 0 {
		t.Fatalf("status failed (code %d): %v\n%s", code, err, out)
	}
	if !strings.Contains(out, `"drift": "True"`) {
		t.Errorf("status does not reflect recorded drift:\n%s", out)
	}

	// plan stays deterministic: no spec changes, exit 0, nothing restarted.
	out, err, code = run(t, "plan", manifests, "--state-file", stateFile)
	if err != nil || code != 0 {
		t.Fatalf("plan after chaos: code %d, err %v\n%s", code, err, out)
	}
	if _, found, _ := rt.Inspect(ctx, "datascape-sink-minio"); found {
		t.Error("plan restarted the object store; plan must never mutate")
	}

	// apply heals: containers recreated/restarted, connectors RUNNING again.
	out, err, code = run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("healing apply failed (code %d): %v\n%s", code, err, out)
	}
	for _, c := range containers {
		st, found, err := rt.Inspect(ctx, c)
		if err != nil || !found || !st.Running {
			t.Errorf("%s not running after healing apply (found=%v err=%v)", c, found, err)
		}
	}
	report, code = runDrift(t, manifests, stateFile)
	if code != 0 {
		t.Errorf("drift after healing apply exited %d:\n%+v", code, report)
	}

	// CHAOS on teardown: kill the object store again, then destroy — the
	// Dataset (whose data died with its store) must not strand the teardown.
	if err := rt.Remove(ctx, "datascape-sink-minio"); err != nil {
		t.Fatalf("chaos remove minio pre-destroy: %v", err)
	}
	out, err, code = run(t, "destroy", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("destroy with dead object store failed (code %d): %v\n%s", code, err, out)
	}
	for _, c := range containers {
		if _, found, _ := rt.Inspect(ctx, c); found {
			t.Errorf("container %s still present after destroy", c)
		}
	}
	st, err := localfile.New(stateFile).Load(ctx)
	if err != nil {
		t.Fatalf("reload state: %v", err)
	}
	if len(st.Resources) != 0 {
		t.Errorf("state still holds %d resource(s) after destroy: %v", len(st.Resources), st.Resources)
	}
}

// TestChaosApplyKilledMidRun covers NFR-9 (recoverability): SIGKILL the CLI
// partway through an apply, require the state file to remain valid JSON
// reflecting exactly the completed resources, and require a re-run to finish
// the job.
func TestChaosApplyKilledMidRun(t *testing.T) {
	t.Setenv("DATASCAPE_SECRET_CDC_PG_ADMIN_USERNAME", "datascape_admin")
	t.Setenv("DATASCAPE_SECRET_CDC_PG_ADMIN_PASSWORD", "admin-secret-pw")
	t.Setenv("DATASCAPE_SECRET_CDC_PG_REPL_USERNAME", "datascape_repl")
	t.Setenv("DATASCAPE_SECRET_CDC_PG_REPL_PASSWORD", "repl-secret-pw")

	rt, err := dockerruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to Docker: %v", err)
	}
	ctx := context.Background()

	cleanup := func() {
		for _, c := range []string{"datascape-cdc-dbz", "datascape-cdc-pg", "datascape-cdc-rp"} {
			_ = rt.Remove(ctx, c)
		}
		for _, v := range []string{"datascape-cdc-pg-data", "datascape-cdc-rp-data"} {
			_ = rt.RemoveVolume(ctx, v)
		}
		_ = rt.RemoveNetwork(ctx, "datascape-cdc-net")
	}
	cleanup()
	t.Cleanup(cleanup)

	bin := filepath.Join(t.TempDir(), "platformctl")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build binary: %v\n%s", err, out)
	}

	stateFile := filepath.Join(t.TempDir(), "state.json")
	manifests := "testdata/cdc-scenario"

	cmd := exec.Command(bin, "apply", manifests, "--state-file", stateFile, "--auto-approve")
	cmd.Env = os.Environ()
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start apply: %v", err)
	}

	// Kill hard after the first resource completes (the progress reporter
	// prints a "✓" done line per finished step).
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

	// A re-run completes the remainder. Allow time for the killed run's
	// half-started containers to be adopted or replaced.
	deadline := time.Now().Add(60 * time.Second)
	for {
		out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve")
		if err == nil && code == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("re-apply after kill did not converge (code %d): %v\n%s", code, err, out)
		}
		time.Sleep(3 * time.Second)
	}

	out, err, code := run(t, "status", manifests, "--state-file", stateFile)
	if err != nil || code != 0 {
		t.Fatalf("status failed (code %d): %v\n%s", code, err, out)
	}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n")[1:] {
		if !strings.Contains(line, "True") {
			t.Errorf("resource not Ready after recovery: %s", line)
		}
	}

	out, err, code = run(t, "destroy", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("destroy failed (code %d): %v\n%s", code, err, out)
	}
}
