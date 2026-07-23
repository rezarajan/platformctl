//go:build integration

package main

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	dockerruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/docker"
	"github.com/rezarajan/platformctl/internal/domain/hostport"
)

// TestBlueprintCDCToLakeAppliesToReady is the docs/planning/08 §E1 accept
// criterion that needs real Docker: `platformctl init cdc-to-lake` writes a
// manifest set that reaches Ready under `apply`, with zero manifest edits
// beyond filling in the secrets init's own README instructs the user to
// set. It mirrors TestAcceptanceCDCAttendance's shape but drives the
// generated blueprint (auto-assigned host ports, generic resource names)
// rather than the hand-written examples/cdc-attendance/ manifests.
func TestBlueprintCDCToLakeAppliesToReady(t *testing.T) {
	dir := t.TempDir()
	manifests := filepath.Join(dir, "cdc-to-lake")

	out, err, code := run(t, "init", "cdc-to-lake", "--dir", manifests)
	if err != nil || code != 0 {
		t.Fatalf("init cdc-to-lake failed (code %d): %v\n%s", code, err, out)
	}

	// The blueprint's own placeholder credentials (from .env) — set
	// directly via the process environment rather than --env-file, so this
	// test does not depend on the .env parser (covered elsewhere).
	t.Setenv("DATASCAPE_SECRET_DB_ADMIN_CREDS_USERNAME", "admin")
	t.Setenv("DATASCAPE_SECRET_DB_ADMIN_CREDS_PASSWORD", "admin-pw")
	t.Setenv("DATASCAPE_SECRET_DB_REPLICATION_CREDS_USERNAME", "repl")
	t.Setenv("DATASCAPE_SECRET_DB_REPLICATION_CREDS_PASSWORD", "repl-pw")
	t.Setenv("DATASCAPE_SECRET_LAKE_ROOT_CREDS_USERNAME", "minioadmin")
	t.Setenv("DATASCAPE_SECRET_LAKE_ROOT_CREDS_PASSWORD", "minioadmin-pw")

	build := exec.Command("docker", "build", "-t", "datascape-s3sink-connect:local", filepath.Join(manifests, "s3sink-image"))
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build sink connect image: %v\n%s", out, err)
	}

	rt, err := dockerruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to Docker: %v", err)
	}
	ctx := context.Background()

	// Resource names are the blueprint's own (naming.RuntimeObjectName is
	// the identity function): db, broker, cdc, lake, sink; volumes carry
	// each data-bearing provider's "<name>-data" convention.
	containers := []string{"sink", "lake", "cdc", "db", "broker"}
	cleanup := func() {
		for _, c := range containers {
			_ = rt.Remove(ctx, c)
		}
		for _, v := range []string{"db-data", "broker-data", "lake-data"} {
			_ = rt.RemoveVolume(ctx, v)
		}
		_ = rt.RemoveNetwork(ctx, "datascape")
		_ = exec.Command("docker", "network", "rm", "datascape").Run()
	}
	cleanup()
	t.Cleanup(cleanup)

	stateFile := filepath.Join(t.TempDir(), "state.json")

	// validate: green with zero manifest edits (the e2e half of this
	// accept criterion is also covered, Docker-free, in init_test.go).
	out, err, code = run(t, "validate", manifests)
	if err != nil || code != 0 {
		t.Fatalf("validate failed (code %d): %v\n%s", code, err, out)
	}

	// apply reconciles in dependency order to Ready.
	out, err, code = run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}

	out, err, code = run(t, "status", manifests, "--state-file", stateFile)
	if err != nil || code != 0 {
		t.Fatalf("status failed (code %d): %v\n%s", code, err, out)
	}
	assertAllStatusReady(t, out, "after apply")

	// Idempotent re-apply: zero mutating calls, "no changes".
	out, err, code = run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("re-apply failed (code %d): %v\n%s", code, err, out)
	}
	if !strings.Contains(out, "no changes") {
		t.Errorf("re-apply did not report 'no changes':\n%s", out)
	}

	// Data actually flows: insert rows into the captured table, assert the
	// sink lands an object under the Dataset's bucket/prefix.
	dbPort := hostport.For("db")
	lakePort := hostport.For("lake")

	insertBlueprintRows(t, ctx, dbPort)
	obj := waitForObjectAt(t, ctx, fmt.Sprintf("127.0.0.1:%d", lakePort), "minioadmin", "minioadmin-pw", "raw-events", "events/", 180*time.Second)
	if !strings.Contains(obj, "blueprint-alice") {
		t.Errorf("landed object missing inserted row:\n%.500s", obj)
	}

	// destroy tears down every managed resource, no labeled leftovers.
	out, err, code = run(t, "destroy", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("destroy failed (code %d): %v\n%s", code, err, out)
	}
	managed, err := rt.ListManaged(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range managed {
		for _, c := range containers {
			if m.Name == c {
				t.Errorf("labeled leftover after destroy: %s", m.Name)
			}
		}
	}
}

func insertBlueprintRows(t *testing.T, ctx context.Context, dbPort int) {
	t.Helper()
	conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://admin:admin-pw@127.0.0.1:%d/appdb?sslmode=disable", dbPort))
	if err != nil {
		t.Fatalf("connect to postgres: %v", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS records (id serial PRIMARY KEY, payload text NOT NULL)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO records (payload) VALUES ('blueprint-alice')`); err != nil {
		t.Fatalf("insert: %v", err)
	}
}
