package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rezarajan/platformctl/internal/adapters/state/localfile"
	"github.com/rezarajan/platformctl/internal/cliutil"
)

// run executes the CLI with args and returns stdout, the error, and the exit
// code that main() would produce.
func run(t *testing.T, args ...string) (string, error, int) { //nolint:staticcheck // ST1008: established (stdout, err, exitCode) shape used at 128+ call sites; reordering is a repo-wide churn out of scope here
	t.Helper()
	root := newRootCmd(defaultWiring)
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs(args)
	err := root.Execute()
	code := 0
	if err != nil {
		code = cliutil.ExitExecution
		var exitErr cliutil.ExitError
		if errors.As(err, &exitErr) {
			code = exitErr.Code
		}
	}
	return out.String(), err, code
}

// TestNoopEndToEnd covers the Phase 0 exit criterion: a manifest set using
// only noop-typed Providers can be validated, planned, applied, and shows
// Ready in status, with state persisted and reloadable.
func TestNoopEndToEnd(t *testing.T) {
	t.Parallel()
	stateFile := filepath.Join(t.TempDir(), "state.json")
	manifests := "testdata/noop-scenario"

	out, err, code := run(t, "validate", manifests)
	if err != nil || code != 0 {
		t.Fatalf("validate failed (code %d): %v\n%s", code, err, out)
	}

	// plan before apply: exits 1 because changes are pending.
	out, _, code = run(t, "plan", manifests, "--state-file", stateFile)
	if code != cliutil.ExitPlanChanges {
		t.Fatalf("plan exit code = %d, want %d (changes pending)\n%s", code, cliutil.ExitPlanChanges, out)
	}
	if !strings.Contains(out, "create") {
		t.Fatalf("plan output missing create actions:\n%s", out)
	}

	out, err, code = run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}

	out, err, code = run(t, "status", manifests, "--state-file", stateFile)
	if err != nil || code != 0 {
		t.Fatalf("status failed (code %d): %v\n%s", code, err, out)
	}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n")[1:] {
		if !strings.Contains(line, "True") {
			t.Errorf("resource not Ready after apply: %s", line)
		}
	}

	// State is persisted and reloadable.
	st, err := localfile.New(stateFile).Load(context.Background())
	if err != nil {
		t.Fatalf("reload state: %v", err)
	}
	if len(st.Resources) != 2 {
		t.Fatalf("state has %d resources, want 2", len(st.Resources))
	}
}

// TestApplyIdempotent covers the Phase 0 exit criterion: re-running apply
// performs zero state mutations (NFR-2), asserted via no state-file diff.
func TestApplyIdempotent(t *testing.T) {
	t.Parallel()
	stateFile := filepath.Join(t.TempDir(), "state.json")
	manifests := "testdata/noop-scenario"

	if out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve"); err != nil {
		t.Fatalf("first apply failed (code %d): %v\n%s", code, err, out)
	}
	before, err := os.ReadFile(stateFile)
	if err != nil {
		t.Fatalf("read state after first apply: %v", err)
	}

	out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil {
		t.Fatalf("second apply failed (code %d): %v\n%s", code, err, out)
	}
	if !strings.Contains(out, "no changes") {
		t.Errorf("second apply did not report 'no changes':\n%s", out)
	}
	after, err := os.ReadFile(stateFile)
	if err != nil {
		t.Fatalf("read state after second apply: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Errorf("second apply mutated state (NFR-2 violation)")
	}

	// A clean plan exits 0 with no changes.
	if _, _, code := run(t, "plan", manifests, "--state-file", stateFile); code != 0 {
		t.Errorf("plan after apply exit code = %d, want 0", code)
	}
}

// TestCycleRejected covers the Phase 0 exit criterion: a cyclic
// providerRef/sourceRef graph is rejected by validate with a clear error.
func TestCycleRejected(t *testing.T) {
	t.Parallel()
	_, err, code := run(t, "validate", "testdata/cycle")
	if err == nil {
		t.Fatal("validate accepted a cyclic graph")
	}
	if code != cliutil.ExitValidation {
		t.Errorf("exit code = %d, want %d", code, cliutil.ExitValidation)
	}
	if !strings.Contains(err.Error(), "cycle") {
		t.Errorf("error does not mention the cycle: %v", err)
	}
}

// TestPlanGoldenFile covers the Phase 0 exit criterion: plan output for a
// fixed manifest set + fixed prior state is compared byte-for-byte against a
// committed golden file (NFR-1 determinism baseline).
func TestPlanGoldenFile(t *testing.T) {
	t.Parallel()
	stateFile := filepath.Join(t.TempDir(), "state.json")

	out, _, _ := run(t, "plan", "testdata/noop-scenario", "--state-file", stateFile, "-o", "json")

	// The golden file must live outside the manifest directory — manifest.Load
	// treats every *.json under the path as a manifest.
	golden := filepath.Join("testdata", "plan.golden.json")
	if os.Getenv("UPDATE_GOLDEN") == "1" {
		if err := os.WriteFile(golden, []byte(out), 0o644); err != nil {
			t.Fatalf("update golden: %v", err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("read golden file (run with UPDATE_GOLDEN=1 to create): %v", err)
	}
	if !bytes.Equal([]byte(out), want) {
		t.Errorf("plan output differs from golden file\ngot:\n%s\nwant:\n%s", out, want)
	}
}
