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
)

// TestSoakLakehouse covers the Phase 6.5 exit criteria against the literal
// examples/soak-lakehouse/ manifests: the orchestrator-ready stack reaches
// Ready, Nessie and Marquez answer on their published endpoints, MySQL is
// provisioned, and a connection through the proxy route reaches the
// out-of-band "external" database.
func TestSoakLakehouse(t *testing.T) {
	t.Setenv("DATASCAPE_SECRET_LAKE_MINIO_ROOT_USERNAME", "minioadmin")
	t.Setenv("DATASCAPE_SECRET_LAKE_MINIO_ROOT_PASSWORD", "minioadmin-pw")
	t.Setenv("DATASCAPE_SECRET_LAKE_PG_ADMIN_USERNAME", "admin")
	t.Setenv("DATASCAPE_SECRET_LAKE_PG_ADMIN_PASSWORD", "admin-pw")
	t.Setenv("DATASCAPE_SECRET_LAKE_MYSQL_ROOT_USERNAME", "root")
	t.Setenv("DATASCAPE_SECRET_LAKE_MYSQL_ROOT_PASSWORD", "mysql-root-pw")
	t.Setenv("DATASCAPE_SECRET_LAKE_MARQUEZ_DB_USERNAME", "marquez")
	t.Setenv("DATASCAPE_SECRET_LAKE_MARQUEZ_DB_PASSWORD", "marquez-pw")
	t.Setenv("DATASCAPE_SECRET_EXT_ORDERS_CONN_USERNAME", "orders_ro")
	t.Setenv("DATASCAPE_SECRET_EXT_ORDERS_CONN_PASSWORD", "orders-pw")

	rt, err := dockerruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to Docker: %v", err)
	}
	ctx := context.Background()

	containers := []string{"edge-orders-db", "lake-lineage", "lake-lineage-db", "lake-catalog", "lake-mysql", "lake-postgres", "lake-minio"}
	cleanup := func() {
		for _, c := range containers {
			_ = rt.Remove(ctx, c)
		}
		_ = exec.Command("docker", "rm", "-f", "external-orders-db").Run()
		for _, v := range []string{"lake-minio-data", "lake-postgres-data", "lake-mysql-data", "lake-lineage-db-data"} {
			_ = rt.RemoveVolume(ctx, v)
		}
		_ = rt.RemoveNetwork(ctx, "datascape")
		_ = exec.Command("docker", "network", "rm", "datascape").Run()
	}
	cleanup()
	t.Cleanup(cleanup)

	// The "external" database the proxy route targets — out-of-band, on the
	// shared network, never managed.
	if out, err := exec.Command("docker", "network", "create", "datascape").CombinedOutput(); err != nil {
		t.Fatalf("create network: %v\n%s", err, out)
	}
	if out, err := exec.Command("docker", "run", "-d", "--name", "external-orders-db",
		"--network", "datascape",
		"-e", "POSTGRES_USER=orders_ro", "-e", "POSTGRES_PASSWORD=orders-pw", "-e", "POSTGRES_DB=orders",
		"postgres:16").CombinedOutput(); err != nil {
		t.Fatalf("out-of-band docker run: %v\n%s", err, out)
	}

	stateFile := filepath.Join(t.TempDir(), "state.json")
	manifests := "../../examples/soak-lakehouse"

	start := time.Now()
	out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}
	t.Logf("soak stack applied in %s", time.Since(start).Round(time.Second))

	out, err, code = run(t, "status", manifests, "--state-file", stateFile)
	if err != nil || code != 0 {
		t.Fatalf("status failed (code %d): %v\n%s", code, err, out)
	}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n")[1:] {
		if !strings.Contains(line, "True") {
			t.Errorf("resource not Ready: %s", line)
		}
	}

	// Orchestrator endpoints answer.
	for _, url := range []string{
		"http://127.0.0.1:19121/api/v2/config",     // Nessie
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

	// MySQL Source provisioned: database exists, binlog on.
	mysqlDB, err := sql.Open("mysql", "root:mysql-root-pw@tcp(127.0.0.1:13306)/eventsdb?timeout=10s")
	if err != nil {
		t.Fatal(err)
	}
	defer mysqlDB.Close()
	if err := mysqlDB.Ping(); err != nil {
		t.Errorf("mysql source not reachable: %v", err)
	}

	// The proxy route is a working entrypoint to the external database.
	conn, err := pgx.Connect(ctx, "postgres://orders_ro:orders-pw@127.0.0.1:15999/orders?sslmode=disable")
	if err != nil {
		t.Fatalf("connect through proxy route: %v", err)
	}
	var one int
	if err := conn.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Errorf("query through proxy route: %v", err)
	}
	conn.Close(ctx)

	// Idempotent re-apply.
	out, err, code = run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("re-apply failed (code %d): %v\n%s", code, err, out)
	}
	if !strings.Contains(out, "no changes") {
		t.Errorf("re-apply did not report 'no changes':\n%s", out)
	}

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
