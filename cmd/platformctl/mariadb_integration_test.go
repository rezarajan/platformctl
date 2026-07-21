//go:build integration

package main

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const mariaConnectURL = "http://localhost:18184"

// TestMariaDBCDCEndToEnd covers docs/planning/08 A9: `mariadb` shares the
// mysql adapter (same protocol; per-type image and binlog flags) but no test
// had ever applied a `type: mariadb` Provider. Clones TestCDCEndToEnd's
// postgres CDC shape onto MariaDB: the version-profile image selection
// (mariadb:11), the row-format binlog flag MariaDB needs explicitly (unlike
// MySQL 8.x's default), and MariaDB's admin-tool binary name
// (mariadb-admin vs mysqladmin) all get exercised for real.
func TestMariaDBCDCEndToEnd(t *testing.T) {
	t.Setenv("DATASCAPE_SECRET_MARIA_ROOT_USERNAME", "root")
	t.Setenv("DATASCAPE_SECRET_MARIA_ROOT_PASSWORD", "maria-root-pw")
	t.Setenv("DATASCAPE_SECRET_MARIA_REPL_USERNAME", "datascape_repl")
	t.Setenv("DATASCAPE_SECRET_MARIA_REPL_PASSWORD", "repl-secret-pw")

	rt := requireDocker(t)
	ctx := context.Background()

	containers := []string{"datascape-maria-dbz", "datascape-maria-db", "datascape-maria-rp"}
	volumes := []string{"datascape-maria-db-data", "datascape-maria-rp-data"}
	cleanup := registerDockerCleanup(t, rt, containers, volumes, "datascape-maria-net")
	cleanup()

	stateFile := filepath.Join(t.TempDir(), "state.json")
	manifests := "testdata/mariadb-cdc-scenario"

	start := time.Now()
	out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}
	t.Logf("mariadb CDC stack applied in %s", time.Since(start).Round(time.Second))

	out, err, code = run(t, "status", manifests, "--state-file", stateFile)
	if err != nil || code != 0 {
		t.Fatalf("status failed (code %d): %v\n%s", code, err, out)
	}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n")[1:] {
		if !strings.Contains(line, "True") {
			t.Errorf("resource not Ready after apply: %s", line)
		}
	}
	if state := mariaConnectorStatus(t, "maria-students-to-events"); state != "RUNNING" {
		t.Errorf("connector state = %q, want RUNNING", state)
	}

	// The Source database is provisioned and reachable — the version-profile
	// image selection and root-password bootstrap actually worked, not just
	// the container starting.
	db, err := sql.Open("mysql", "root:maria-root-pw@tcp(127.0.0.1:13307)/attendance?timeout=10s")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		t.Errorf("mariadb source not reachable: %v", err)
	}
	db.Close()

	// Exit criterion (mirrors TestCDCEndToEnd): idempotent re-apply — zero
	// mutating calls.
	out, err, code = run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("re-apply failed (code %d): %v\n%s", code, err, out)
	}
	if !strings.Contains(out, "no changes") {
		t.Errorf("re-apply did not report 'no changes':\n%s", out)
	}

	cfg := mariaConnectorConfig(t, "maria-students-to-events")
	if got, want := cfg["table.include.list"], "attendance.students"; got != want {
		t.Errorf("table.include.list = %q, want %q", got, want)
	}

	// Clean destroy; no orphaned managed objects.
	out, err, code = run(t, "destroy", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("destroy failed (code %d): %v\n%s", code, err, out)
	}
	for _, c := range containers {
		if _, found, _ := rt.Inspect(ctx, c); found {
			t.Errorf("container %s still present after destroy", c)
		}
	}
	managed, err := rt.ListManaged(ctx)
	if err != nil {
		t.Fatalf("list managed: %v", err)
	}
	for _, m := range managed {
		if strings.HasPrefix(m.Name, "datascape-maria-") {
			t.Errorf("orphaned managed container after destroy: %s", m.Name)
		}
	}
}

func mariaConnectorStatus(t *testing.T, name string) string {
	t.Helper()
	var body struct {
		Connector struct {
			State string `json:"state"`
		} `json:"connector"`
	}
	getJSON(t, fmt.Sprintf("%s/connectors/%s/status", mariaConnectURL, name), &body)
	return body.Connector.State
}

func mariaConnectorConfig(t *testing.T, name string) map[string]string {
	t.Helper()
	var cfg map[string]string
	getJSON(t, fmt.Sprintf("%s/connectors/%s/config", mariaConnectURL, name), &cfg)
	return cfg
}
