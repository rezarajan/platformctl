//go:build integration

package main

import (
	"context"
	"database/sql"
	"net/http"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	dockerruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/docker"
	"github.com/rezarajan/platformctl/internal/cliutil"
)

// TestLakehouse covers the Phase 6.5 exit criteria against the literal
// examples/lakehouse/ manifests: the orchestrator-ready stack reaches Ready
// with the Catalog and Connection kinds realized; Nessie and Marquez answer
// on their published endpoints; the external database is reachable — and
// CDC flows — through the managed Connection's entrypoint; drift healing
// covers the new kinds; destroy is clean and never touches the external
// database.
func TestLakehouse(t *testing.T) {
	t.Setenv("DATASCAPE_SECRET_LAKE_MINIO_ROOT_USERNAME", "minioadmin")
	t.Setenv("DATASCAPE_SECRET_LAKE_MINIO_ROOT_PASSWORD", "minioadmin-pw")
	t.Setenv("DATASCAPE_SECRET_LAKE_PG_ADMIN_USERNAME", "admin")
	t.Setenv("DATASCAPE_SECRET_LAKE_PG_ADMIN_PASSWORD", "admin-pw")
	t.Setenv("DATASCAPE_SECRET_LAKE_MYSQL_ROOT_USERNAME", "root")
	t.Setenv("DATASCAPE_SECRET_LAKE_MYSQL_ROOT_PASSWORD", "mysql-root-pw")
	t.Setenv("DATASCAPE_SECRET_EXT_ORDERS_CREDS_USERNAME", "orders_ro")
	t.Setenv("DATASCAPE_SECRET_EXT_ORDERS_CREDS_PASSWORD", "orders-pw")

	rt, err := dockerruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to Docker: %v", err)
	}
	ctx := context.Background()

	containers := []string{"orders-cdc", "lake-redpanda", "orders-db", "lake-lineage", "lake-lineage-db", "catalog-svc", "lake-mysql", "lake-postgres", "lake-minio"}
	cleanup := func() {
		for _, c := range containers {
			_ = rt.Remove(ctx, c)
		}
		_ = exec.Command("docker", "rm", "-f", "external-orders-db").Run()
		for _, v := range []string{"lake-minio-data", "lake-postgres-data", "lake-mysql-data", "lake-lineage-db-data", "lake-redpanda-data"} {
			_ = rt.RemoveVolume(ctx, v)
		}
		_ = rt.RemoveNetwork(ctx, "datascape")
		_ = exec.Command("docker", "network", "rm", "datascape").Run()
	}
	cleanup()
	t.Cleanup(cleanup)

	// The external database the Connection targets — out-of-band, on the
	// shared network, never managed. wal_level=logical so the CDC Binding
	// can stream from it.
	if out, err := exec.Command("docker", "network", "create",
		"--label", "io.datascape.managed-by=platformctl",
		"datascape").CombinedOutput(); err != nil &&
		!strings.Contains(string(out), "already exists") {
		t.Fatalf("create network: %v\n%s", err, out)
	}
	if out, err := exec.Command("docker", "run", "-d", "--name", "external-orders-db",
		"--network", "datascape",
		"-e", "POSTGRES_USER=orders_ro", "-e", "POSTGRES_PASSWORD=orders-pw", "-e", "POSTGRES_DB=orders",
		"postgres:16@sha256:33f923b05f64ca54ac4401c01126a6b92afe839a0aa0a52bc5aeb5cc958e5f20", "postgres", "-c", "wal_level=logical").CombinedOutput(); err != nil {
		t.Fatalf("out-of-band docker run: %v\n%s", err, out)
	}

	stateFile := filepath.Join(t.TempDir(), "state.json")
	manifests := "../../examples/lakehouse"

	start := time.Now()
	out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}
	t.Logf("lakehouse stack applied in %s", time.Since(start).Round(time.Second))

	out, err, code = run(t, "status", manifests, "--state-file", stateFile)
	if err != nil || code != 0 {
		t.Fatalf("status failed (code %d): %v\n%s", code, err, out)
	}
	assertAllStatusReady(t, out, "after apply")

	// Orchestrator endpoints answer; the Catalog's default branch exists.
	for _, url := range []string{
		"http://127.0.0.1:19121/api/v2/config",     // Nessie
		"http://127.0.0.1:19121/api/v2/trees/main", // the Catalog's declared branch
		"http://127.0.0.1:15100/api/v1/namespaces", // Marquez
	} {
		resp, err := http.Get(url)
		if err != nil {
			t.Errorf("GET %s: %v", url, err)
			continue
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("GET %s: HTTP %d", url, resp.StatusCode)
		}
	}

	// MySQL Source provisioned.
	mysqlDB, err := sql.Open("mysql", "root:mysql-root-pw@tcp(127.0.0.1:13306)/eventsdb?timeout=10s")
	if err != nil {
		t.Fatal(err)
	}
	if err := mysqlDB.Ping(); err != nil {
		t.Errorf("mysql source not reachable: %v", err)
	}
	mysqlDB.Close()

	// Secret rotation: changing the root SecretReference must rotate the
	// backing MySQL account and reconcile dependents, not leave the old DB
	// password in place while probes start failing.
	t.Setenv("DATASCAPE_SECRET_LAKE_MYSQL_ROOT_PASSWORD", "mysql-root-pw-rotated")
	out, err, code = run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("mysql secret rotation apply failed (code %d): %v\n%s", code, err, out)
	}
	if err := pingMySQL("mysql-root-pw-rotated"); err != nil {
		t.Fatalf("rotated mysql password not accepted: %v", err)
	}
	if err := pingMySQL("mysql-root-pw"); err == nil {
		t.Fatal("old mysql password still accepted after rotation")
	}

	t.Setenv("DATASCAPE_SECRET_LAKE_PG_ADMIN_PASSWORD", "admin-pw-rotated")
	out, err, code = run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("postgres secret rotation apply failed (code %d): %v\n%s", code, err, out)
	}
	if err := pingPostgres("admin-pw-rotated"); err != nil {
		t.Fatalf("rotated postgres password not accepted: %v", err)
	}
	if err := pingPostgres("admin-pw"); err == nil {
		t.Fatal("old postgres password still accepted after rotation")
	}

	// The Connection is a working entrypoint to the external database.
	conn, err := pgx.Connect(ctx, "postgres://orders_ro:orders-pw@127.0.0.1:15999/orders?sslmode=disable")
	if err != nil {
		t.Fatalf("connect through the Connection entrypoint: %v", err)
	}
	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS orders (id serial PRIMARY KEY, sku text);
		INSERT INTO orders (sku) VALUES ('sku-1')`); err != nil {
		t.Fatalf("write through the Connection entrypoint: %v", err)
	}
	conn.Close(ctx)

	// The CDC connector was registered at the Connection's in-network
	// endpoint with the Connection's credentials, and observers wired the
	// lineage backend's endpoint into it.
	cfg := acceptanceConnectorConfig(t, "http://127.0.0.1:18086", "orders-to-events")
	if got, want := cfg["database.hostname"], "orders-db"; got != want {
		t.Errorf("connector database.hostname = %q, want %q (the Connection's endpoint)", got, want)
	}
	if got := cfg["openlineage.integration.config.transport.url"]; !strings.Contains(got, "lake-lineage") {
		t.Errorf("connector lineage endpoint = %q, want the lake-lineage URL", got)
	}

	// Idempotent re-apply.
	out, err, code = run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("re-apply failed (code %d): %v\n%s", code, err, out)
	}
	if !strings.Contains(out, "no changes") {
		t.Errorf("re-apply did not report 'no changes':\n%s", out)
	}

	// Drift healing covers the new kinds: kill the Connection's forwarder,
	// drift observes it, apply heals it, traffic flows again.
	if out, err := exec.Command("docker", "rm", "-f", "orders-db").CombinedOutput(); err != nil {
		t.Fatalf("chaos remove forwarder: %v\n%s", err, out)
	}
	report, code := runDrift(t, manifests, stateFile)
	if code != cliutil.ExitPlanChanges {
		t.Errorf("drift exit = %d, want %d", code, cliutil.ExitPlanChanges)
	}
	if r := report["Connection/orders-db"]; r.Drift != "True" {
		t.Errorf("Connection drift = %q (reason %q), want True", r.Drift, r.Reason)
	}
	out, err, code = run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("healing apply failed (code %d): %v\n%s", code, err, out)
	}
	conn, err = pgx.Connect(ctx, "postgres://orders_ro:orders-pw@127.0.0.1:15999/orders?sslmode=disable")
	if err != nil {
		t.Fatalf("Connection entrypoint dead after heal: %v", err)
	}
	conn.Close(ctx)

	// Clean destroy; the external database is untouched.
	out, err, code = run(t, "destroy", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("destroy failed (code %d): %v\n%s", code, err, out)
	}
	for _, c := range containers {
		if _, found, _ := rt.Inspect(ctx, c); found {
			t.Errorf("container %s still present after destroy", c)
		}
	}
	if st, found, _ := rt.Inspect(ctx, "external-orders-db"); !found || !st.Running {
		t.Error("destroy touched the external database container")
	}
}

func pingMySQL(password string) error {
	db, err := sql.Open("mysql", "root:"+password+"@tcp(127.0.0.1:13306)/eventsdb?timeout=10s")
	if err != nil {
		return err
	}
	defer db.Close()
	return db.Ping()
}

func pingPostgres(password string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, "postgres://admin:"+password+"@127.0.0.1:15434/appdb?sslmode=disable")
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	return nil
}
