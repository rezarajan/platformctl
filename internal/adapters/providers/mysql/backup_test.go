package mysql

import (
	"strings"
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/provider"
)

// TestDBSideDoesNotInterpolateDatabaseName pins doc 11 B4 finding 1's fix:
// a manifest-declared database name must never appear in the sh -c text
// (it would execute inside a job container holding root DB and
// object-store credentials) — it rides an env var the shell expands
// quoted, postgres's PGDATABASE pattern.
func TestDBSideDoesNotInterpolateDatabaseName(t *testing.T) {
	t.Parallel()
	hostile := `appdb; touch /tmp/pwned $(cat /run/datascape/client.cnf)`
	cfg := provider.Provider{Type: "mysql", Configuration: map[string]any{}}
	for _, tool := range []string{dumpTool(cfg), restoreTool(cfg)} {
		side := dbSide(tool, cfg, "img", "db-host", "rootpw", hostile)
		if strings.Contains(side.ShellCmd, hostile) || strings.Contains(side.ShellCmd, "touch") {
			t.Fatalf("database name interpolated into ShellCmd: %q", side.ShellCmd)
		}
		if !strings.Contains(side.ShellCmd, `"$DATASCAPE_BACKUP_DATABASE"`) {
			t.Fatalf("ShellCmd does not expand the env var quoted: %q", side.ShellCmd)
		}
		if side.Env["DATASCAPE_BACKUP_DATABASE"] != hostile {
			t.Fatalf("env var does not carry the database name verbatim: %q", side.Env)
		}
	}
}
