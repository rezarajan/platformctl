//go:build integration

package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	k8sruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/kubernetes"
	"github.com/rezarajan/platformctl/internal/domain/backup"
	"github.com/rezarajan/platformctl/internal/testkit"
)

// TestBackupRestoreKubernetesPostgresRoundTrip is I15's (docs/planning/08
// §7.8; docs/adr/007-backup-restore.md addendum 3) live proof: dbjob's
// producer/consumer/cleanup pipeline, realized as a Kubernetes Job
// (runtime.JobCapableRuntime) instead of Docker's two-EnsureContainer-calls
// path, actually round-trips a real backup and a real verify-then-promote
// restore (I13) against a live cluster — the same accept shape as
// TestBackupRestorePostgresRoundTrip, minus the destroy/apply-fresh cycle
// (a direct-SQL "simulate data loss" step stands in for it here, since this
// test's job is proving the K8s Job realization works end to end, not
// re-proving the Docker round trip's own destroy/apply semantics — those
// are runtime-independent and already covered).
func TestBackupRestoreKubernetesPostgresRoundTrip(t *testing.T) {
	requireK8s(t)
	t.Setenv("DATASCAPE_SECRET_BKPK8S_PG_ADMIN_USERNAME", "bkpk8sadmin")
	t.Setenv("DATASCAPE_SECRET_BKPK8S_PG_ADMIN_PASSWORD", "bkpk8s-admin-pw")
	t.Setenv("DATASCAPE_SECRET_BKPK8S_MINIO_ROOT_USERNAME", "bkpk8sminio")
	t.Setenv("DATASCAPE_SECRET_BKPK8S_MINIO_ROOT_PASSWORD", "bkpk8s-minio-pw")

	rt, err := k8sruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to kubernetes: %v", err)
	}
	ctx := context.Background()
	const ns = "datascape-bkpk8s-test-ns"
	workloads := []string{"bkpk8s-postgres", "bkpk8s-minio"}
	// docs/adr/029: janitor-owned cleanup (J2 sweep) — declared
	// objects, canonical order, silent pre-clean, loud post-clean.
	jan := testkit.Janitor{
		RT:        rt,
		Workloads: workloads,
		Networks:  []string{ns},
	}
	jan.CleanSilent(ctx)
	jan.Register(ctx, t)

	stateFile := t.TempDir() + "/state.json"
	manifests := "testdata/backup-restore-k8s-scenario"
	gates := "BackupRestore=true"

	if out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", gates); err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}

	addr, closeAddr, err := rt.EnsureReachable(ctx, "bkpk8s-postgres", 5432)
	if err != nil {
		t.Fatalf("EnsureReachable(bkpk8s-postgres): %v", err)
	}
	// Each EnsureReachable mints a FRESH local forward (a new port) and its
	// predecessor is closed by then — the DSN must be rebuilt per call, never
	// reused across closeAddr boundaries.
	dsnFor := func(addr string) string {
		return "postgres://bkpk8sadmin:bkpk8s-admin-pw@" + addr + "/bkpk8sdb?sslmode=disable"
	}
	dsn := dsnFor(addr)

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		closeAddr()
		t.Fatalf("connect to postgres: %v", err)
	}
	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS widgets (id serial PRIMARY KEY, name text NOT NULL)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO widgets (name) VALUES ('sprocket'), ('gizmo')`); err != nil {
		t.Fatalf("seed rows: %v", err)
	}
	_ = conn.Close(ctx)
	closeAddr()

	out, err, code := run(t, "backup", "Source/bkpk8s-pg-src", manifests, "--to", "Dataset/bkpk8s-store",
		"--state-file", stateFile, "--feature-gates", gates, "-o", "json")
	if err != nil || code != 0 {
		t.Fatalf("backup failed (code %d): %v\n%s", code, err, out)
	}
	var manifest backup.Manifest
	if err := json.Unmarshal([]byte(out), &manifest); err != nil {
		t.Fatalf("parse backup manifest: %v\n%s", err, out)
	}
	if manifest.Destination.Key == "" {
		t.Fatalf("manifest has no destination key:\n%s", out)
	}
	if strings.Contains(out, "bkpk8s-admin-pw") || strings.Contains(out, "bkpk8s-minio-pw") {
		t.Fatalf("backup -o json output embeds a plaintext credential:\n%s", out)
	}

	// Simulate data loss: drop the table entirely (a stand-in for the
	// Docker test's destroy/apply-fresh cycle — this test's job is proving
	// the Kubernetes Job realization round-trips a real restore, not
	// re-proving the runtime-independent destroy/apply semantics).
	addr, closeAddr, err = rt.EnsureReachable(ctx, "bkpk8s-postgres", 5432)
	if err != nil {
		t.Fatalf("EnsureReachable(bkpk8s-postgres) before drop: %v", err)
	}
	conn, err = pgx.Connect(ctx, dsnFor(addr))
	if err != nil {
		closeAddr()
		t.Fatalf("connect to postgres before drop: %v", err)
	}
	if _, err := conn.Exec(ctx, `DROP TABLE widgets`); err != nil {
		t.Fatalf("simulate data loss: %v", err)
	}
	_ = conn.Close(ctx)
	closeAddr()

	out, err, code = run(t, "restore", "Source/bkpk8s-pg-src", manifests, "--from", "Dataset/bkpk8s-store",
		"--object", strings.TrimPrefix(manifest.Destination.Key, "dumps/"),
		"--state-file", stateFile, "--feature-gates", gates,
		"--yes-i-understand-this-overwrites-existing-data", "-o", "json")
	if err != nil || code != 0 {
		t.Fatalf("restore failed (code %d): %v\n%s", code, err, out)
	}

	addr, closeAddr, err = rt.EnsureReachable(ctx, "bkpk8s-postgres", 5432)
	if err != nil {
		t.Fatalf("EnsureReachable(bkpk8s-postgres) after restore: %v", err)
	}
	defer closeAddr()
	conn, err = pgx.Connect(ctx, dsnFor(addr))
	if err != nil {
		t.Fatalf("connect to restored postgres: %v", err)
	}
	defer conn.Close(ctx)
	var names []string
	rows, err := conn.Query(ctx, `SELECT name FROM widgets ORDER BY name`)
	if err != nil {
		t.Fatalf("query restored rows: %v", err)
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		names = append(names, name)
	}
	if got := strings.Join(names, ","); got != "gizmo,sprocket" {
		t.Fatalf("restored rows = %v, want [gizmo sprocket]", names)
	}
}
