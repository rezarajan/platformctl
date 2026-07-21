package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rezarajan/platformctl/internal/cliutil"
)

// TestApplyRefusesOnMissingSecrets: apply must fail before touching any
// infrastructure when a declared secret is unresolvable, listing every
// missing variable — the "cannot half-apply" guard from docs/history/errors.md.
func TestApplyRefusesOnMissingSecrets(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "state.json")
	// import-scenario declares SecretReference cdc? no — use import-scenario
	// which has imp-pg-admin; ensure its env vars are unset.
	os.Unsetenv("DATASCAPE_SECRET_IMP_PG_ADMIN_USERNAME")
	os.Unsetenv("DATASCAPE_SECRET_IMP_PG_ADMIN_PASSWORD")

	out, err, code := run(t, "apply", "testdata/import-scenario", "--state-file", stateFile, "--auto-approve")
	if code != cliutil.ExitValidation {
		t.Fatalf("apply exit = %d, want %d (validation)\n%s", code, cliutil.ExitValidation, out)
	}
	if err == nil || !strings.Contains(err.Error(), "DATASCAPE_SECRET_IMP_PG_ADMIN_USERNAME") {
		t.Errorf("refusal does not name the missing variable: %v\n%s", err, out)
	}
	// Nothing should have been written to state.
	if _, err := os.Stat(stateFile); err == nil {
		t.Errorf("state file created despite preflight failure")
	}
}

// TestEnvFileLoads: --env-file supplies the missing variables, and an
// exported shell value wins over the file.
func TestEnvFileLoads(t *testing.T) {
	dir := t.TempDir()
	envPath := filepath.Join(dir, ".env")
	content := "# comment\n\nexport DATASCAPE_SECRET_IMP_PG_ADMIN_USERNAME=fileuser\n" +
		"DATASCAPE_SECRET_IMP_PG_ADMIN_PASSWORD='filepass'\n"
	if err := os.WriteFile(envPath, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := loadEnvFile(envPath); err != nil {
		t.Fatalf("loadEnvFile: %v", err)
	}
	t.Cleanup(func() {
		os.Unsetenv("DATASCAPE_SECRET_IMP_PG_ADMIN_USERNAME")
		os.Unsetenv("DATASCAPE_SECRET_IMP_PG_ADMIN_PASSWORD")
	})
	if got := os.Getenv("DATASCAPE_SECRET_IMP_PG_ADMIN_USERNAME"); got != "fileuser" {
		t.Errorf("username = %q, want fileuser", got)
	}
	if got := os.Getenv("DATASCAPE_SECRET_IMP_PG_ADMIN_PASSWORD"); got != "filepass" {
		t.Errorf("password = %q, want filepass (quotes stripped)", got)
	}

	// Shell env wins: a pre-set value is not overwritten by the file.
	t.Setenv("DATASCAPE_SECRET_IMP_PG_ADMIN_USERNAME", "shelluser")
	if err := loadEnvFile(envPath); err != nil {
		t.Fatal(err)
	}
	if got := os.Getenv("DATASCAPE_SECRET_IMP_PG_ADMIN_USERNAME"); got != "shelluser" {
		t.Errorf("username = %q, want shelluser (shell wins over file)", got)
	}
}
