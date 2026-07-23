package main

import (
	"strings"
	"testing"

	"github.com/rezarajan/platformctl/internal/cliutil"
)

// TestRestoreRefusesWithoutOverwriteFlag pins docs/planning/08 C6's accept
// criterion "restore onto live data without flags refuses": restore always
// overwrites, so it refuses outright without
// --yes-i-understand-this-overwrites-existing-data — and does so before the
// BackupRestore gate check, manifest loading, or any state/secret access, so
// this needs no manifest file, no state file, and no real infrastructure to
// demonstrate (mirroring apply/destroy's --include-external +
// --yes-i-understand-this-is-destructive client-side pre-check).
func TestRestoreRefusesWithoutOverwriteFlag(t *testing.T) {
	t.Parallel()
	out, err, code := run(t, "restore", "Source/orders", "nonexistent-path-never-read", "--from", "Dataset/wherever")
	if err == nil {
		t.Fatal("restore without the overwrite flag: expected a refusal, got success")
	}
	if code != cliutil.ExitValidation {
		t.Fatalf("exit code = %d, want %d (ExitValidation); output:\n%s", code, cliutil.ExitValidation, out)
	}
	if !strings.Contains(err.Error(), "yes-i-understand-this-overwrites-existing-data") {
		t.Fatalf("error does not name the required flag: %v", err)
	}
}

// TestBackupRestoreRequireGate proves BackupRestore is Alpha/disabled by
// default: both commands refuse, naming the gate, even with an otherwise
// well-formed invocation.
func TestBackupRestoreRequireGate(t *testing.T) {
	t.Parallel()
	_, err, code := run(t, "backup", "Source/orders", "testdata/noop-scenario", "--to", "Dataset/wherever")
	if err == nil || code != cliutil.ExitValidation {
		t.Fatalf("backup with BackupRestore disabled: got code %d, err %v; want ExitValidation naming the gate", code, err)
	}
	if !strings.Contains(err.Error(), "BackupRestore") {
		t.Fatalf("error does not name the BackupRestore gate: %v", err)
	}

	_, err, code = run(t, "restore", "Source/orders", "testdata/noop-scenario", "--from", "Dataset/wherever",
		"--yes-i-understand-this-overwrites-existing-data")
	if err == nil || code != cliutil.ExitValidation {
		t.Fatalf("restore with BackupRestore disabled: got code %d, err %v; want ExitValidation naming the gate", code, err)
	}
	if !strings.Contains(err.Error(), "BackupRestore") {
		t.Fatalf("error does not name the BackupRestore gate: %v", err)
	}
}

// TestBackupRestoreHelp is a smoke check that both commands are wired into
// the CLI with their documented flags.
func TestBackupRestoreHelp(t *testing.T) {
	t.Parallel()
	out, err, code := run(t, "backup", "--help")
	if err != nil || code != 0 {
		t.Fatalf("backup --help failed (code %d): %v", code, err)
	}
	for _, want := range []string{"--to", "--credentials-secret-ref"} {
		if !strings.Contains(out, want) {
			t.Errorf("backup --help missing %q:\n%s", want, out)
		}
	}

	out, err, code = run(t, "restore", "--help")
	if err != nil || code != 0 {
		t.Fatalf("restore --help failed (code %d): %v", code, err)
	}
	for _, want := range []string{"--from", "--object", "--credentials-secret-ref", "--yes-i-understand-this-overwrites-existing-data"} {
		if !strings.Contains(out, want) {
			t.Errorf("restore --help missing %q:\n%s", want, out)
		}
	}
}
