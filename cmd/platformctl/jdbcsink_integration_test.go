//go:build integration

package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	dockerruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/docker"
	"github.com/rezarajan/platformctl/internal/testkit"
)

const (
	jsinkConnectURL = "http://localhost:18288"
	jsinkSrcPGConn  = "postgres://datascape_admin:admin-secret-pw@localhost:15648/school?sslmode=disable"
	jsinkDstPGConn  = "postgres://datascape_admin:admin-secret-pw@localhost:15649/warehouse?sslmode=disable"
	jsinkGates      = "JDBCSinkProvider=true"
)

// TestJDBCSinkEndToEnd covers docs/planning/08 D3's accept criteria: a CDC
// topic (Avro-serialized — required, see jdbcsink.ValidateBindingOptions)
// lands as rows in a SECOND managed postgres Source's table via the
// jdbcsink provider, both insert and upsert modes asserted, plus the
// standing per-task bars: idempotent re-apply, config-change-in-place
// (mode switch), and clean destroy.
func TestJDBCSinkEndToEnd(t *testing.T) {
	t.Setenv("DATASCAPE_SECRET_JDBCSINK_PG_SRC_ADMIN_USERNAME", "datascape_admin")
	t.Setenv("DATASCAPE_SECRET_JDBCSINK_PG_SRC_ADMIN_PASSWORD", "admin-secret-pw")
	t.Setenv("DATASCAPE_SECRET_JDBCSINK_PG_SRC_REPL_USERNAME", "datascape_repl")
	t.Setenv("DATASCAPE_SECRET_JDBCSINK_PG_SRC_REPL_PASSWORD", "repl-secret-pw")
	t.Setenv("DATASCAPE_SECRET_JDBCSINK_PG_DST_ADMIN_USERNAME", "datascape_admin")
	t.Setenv("DATASCAPE_SECRET_JDBCSINK_PG_DST_ADMIN_PASSWORD", "admin-secret-pw")

	// Two worker images: the D1 avro-connect-image (Debezium's CDC leg,
	// reused verbatim — read-only reference) and this task's own
	// jdbcsink-image (the JDBC sink plugin + the same Avro converter jar).
	buildImage(t, "datascape-avro-connect:test", "testdata/avro-connect-image")
	buildImage(t, "datascape-jdbcsink-connect:test", "testdata/jdbcsink-image")

	rt, err := dockerruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to Docker: %v", err)
	}
	ctx := context.Background()

	containers := []string{
		"datascape-jdbcsink-connect", "datascape-jdbcsink-dbz",
		"datascape-jdbcsink-pg-dst", "datascape-jdbcsink-pg-src", "datascape-jdbcsink-rp",
	}
	// docs/adr/029: janitor-owned cleanup (J2 sweep) — declared
	// objects, canonical order, silent pre-clean, loud post-clean.
	jan := testkit.Janitor{
		RT:        rt,
		Workloads: containers,
		Volumes:   []string{"datascape-jdbcsink-pg-src-data", "datascape-jdbcsink-pg-dst-data", "datascape-jdbcsink-rp-data"},
		Networks:  []string{"datascape-jdbcsink-net"},
	}
	jan.CleanSilent(ctx)
	jan.Register(ctx, t)

	stateFile := filepath.Join(t.TempDir(), "state.json")
	manifests := "testdata/jdbcsink-scenario"

	start := time.Now()
	out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", jsinkGates)
	if err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}
	t.Logf("apply from empty state took %s", time.Since(start).Round(time.Second))

	out, err, code = run(t, "status", manifests, "--state-file", stateFile, "--feature-gates", jsinkGates)
	if err != nil || code != 0 {
		t.Fatalf("status failed (code %d): %v\n%s", code, err, out)
	}
	assertAllStatusReady(t, out, "apply")

	// Pre-create the target table (auto.create is off by default —
	// docs/planning/03's jdbcsink note: schemaless records can't drive DDL
	// anyway, and this provider requires schema-carrying avro regardless).
	createTargetTable(t, ctx)

	// Real CDC traffic, insert mode: rows appear in the target table
	// exactly as inserted.
	insertSourceRows(t, ctx, "INSERT INTO students (id, name) VALUES (1, 'alice'), (2, 'bob')")
	waitForRow(t, ctx, jsinkDstPGConn, 1, "alice", 180*time.Second)
	waitForRow(t, ctx, jsinkDstPGConn, 2, "bob", 180*time.Second)

	cfg := jsinkConnectorConfig(t, "jdbcsink-events-to-warehouse")
	if got, want := cfg["insert.mode"], "insert"; got != want {
		t.Errorf("insert.mode = %q, want %q", got, want)
	}

	pgSrcBefore, _, _ := rt.Inspect(ctx, "datascape-jdbcsink-pg-src")
	pgDstBefore, _, _ := rt.Inspect(ctx, "datascape-jdbcsink-pg-dst")
	rpBefore, _, _ := rt.Inspect(ctx, "datascape-jdbcsink-rp")

	// Idempotent re-apply.
	out, err, code = run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", jsinkGates)
	if err != nil || code != 0 {
		t.Fatalf("re-apply failed (code %d): %v\n%s", code, err, out)
	}
	if !strings.Contains(out, "no changes") {
		t.Errorf("re-apply did not report 'no changes':\n%s", out)
	}

	// Switch to upsert mode: a manifest-only change updates the connector
	// config in place (mirrors sink_integration_test.go's format-change
	// assertion), without recreating any infrastructure container.
	changed := filepath.Join(t.TempDir(), "changed")
	if err := os.MkdirAll(changed, 0o755); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(manifests, "manifests.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	bumped := strings.Replace(string(data), "mode: insert", "mode: upsert", 1)
	if bumped == string(data) {
		t.Fatal("mode replacement did not change the manifest")
	}
	if err := os.WriteFile(filepath.Join(changed, "manifests.yaml"), []byte(bumped), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err, code = run(t, "apply", changed, "--state-file", stateFile, "--auto-approve", "--feature-gates", jsinkGates)
	if err != nil || code != 0 {
		t.Fatalf("mode-change apply failed (code %d): %v\n%s", code, err, out)
	}
	cfg = jsinkConnectorConfig(t, "jdbcsink-events-to-warehouse")
	if got, want := cfg["insert.mode"], "upsert"; got != want {
		t.Errorf("insert.mode = %q, want %q after mode switch", got, want)
	}
	if got, want := cfg["pk.mode"], "record_key"; got != want {
		t.Errorf("pk.mode = %q, want %q (no pkFields override — the full CDC key, the row's PK)", got, want)
	}
	for name, before := range map[string]string{
		"datascape-jdbcsink-pg-src": pgSrcBefore.ID,
		"datascape-jdbcsink-pg-dst": pgDstBefore.ID,
		"datascape-jdbcsink-rp":     rpBefore.ID,
	} {
		after, found, err := rt.Inspect(ctx, name)
		if err != nil || !found {
			t.Fatalf("%s missing after mode-change: %v", name, err)
		}
		if after.ID != before {
			t.Errorf("%s was recreated (ID %s -> %s); a Binding options change must not touch it", name, before, after.ID)
		}
	}

	// Real CDC traffic, upsert mode: an UPDATE lands as an in-place row
	// update, a fresh INSERT lands as a new row — both via the identical
	// pk.mode=record_key path (no pkFields configured).
	insertSourceRows(t, ctx, "UPDATE students SET name = 'alice2' WHERE id = 1")
	insertSourceRows(t, ctx, "INSERT INTO students (id, name) VALUES (3, 'carol')")
	waitForRow(t, ctx, jsinkDstPGConn, 1, "alice2", 180*time.Second)
	waitForRow(t, ctx, jsinkDstPGConn, 3, "carol", 180*time.Second)
	// bob (id 2) was never touched again — still present, unmodified.
	waitForRow(t, ctx, jsinkDstPGConn, 2, "bob", 30*time.Second)
	assertRowCount(t, ctx, jsinkDstPGConn, 3)

	// Clean destroy.
	out, err, code = run(t, "destroy", changed, "--state-file", stateFile, "--auto-approve", "--feature-gates", jsinkGates)
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
		if strings.HasPrefix(m.Name, "datascape-jdbcsink-") {
			t.Errorf("orphaned managed container after destroy: %s", m.Name)
		}
	}
}

// TestJDBCSinkValidateCapabilityErrorExact covers this task's negative-path
// requirement: a sink-mode Binding targeting a Source against a provider
// that does NOT implement DatabaseSinkCapableProvider fails at validate
// with the exact ADR 009 error shape — no image build, no Docker
// containers, pure CLI validate.
func TestJDBCSinkValidateCapabilityErrorExact(t *testing.T) {
	dir := t.TempDir()
	manifest := `
apiVersion: datascape.io/v1alpha1
kind: SecretReference
metadata:
  name: creds
spec:
  backend: env
  keys: [username, password]
---
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: not-capable
spec:
  type: debezium
  runtime: {type: docker, network: n}
  configuration: {bootstrapServers: "broker:29092"}
---
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: broker
spec:
  type: redpanda
  runtime: {type: docker, network: n}
---
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: db
spec:
  type: postgres
  runtime: {type: docker, network: n}
  configuration: {superuserSecretRef: creds}
  secretRefs: [creds]
---
apiVersion: datascape.io/v1alpha1
kind: Source
metadata:
  name: warehouse
spec:
  engine: postgres
  providerRef: {name: db}
  postgres: {database: w}
---
apiVersion: datascape.io/v1alpha1
kind: EventStream
metadata:
  name: events
spec:
  providerRef: {name: broker}
  partitions: 1
---
apiVersion: datascape.io/v1alpha1
kind: Binding
metadata:
  name: events-to-warehouse
spec:
  mode: sink
  sourceRef: {name: events}
  targetRef: {name: warehouse}
  providerRef: {name: not-capable}
`
	if err := os.WriteFile(filepath.Join(dir, "manifests.yaml"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err, code := run(t, "validate", dir)
	if err == nil || code == 0 {
		t.Fatalf("want validate to fail against a non-database-sink-capable provider, got code %d\n%s", code, out)
	}
	const want = `Binding "events-to-warehouse": Provider "not-capable" (type: debezium)
does not support mode "sink" into a Source (provider implements no database-sink capability)`
	if !strings.Contains(out, want) && !strings.Contains(err.Error(), want) {
		t.Errorf("error does not match the exact ADR 009 shape.\nwant substring:\n%s\ngot stdout:\n%s\ngot err: %v", want, out, err)
	}
}

func buildImage(t *testing.T, tag, dir string) {
	t.Helper()
	build := exec.Command("docker", "build", "-t", tag, dir)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build image %s from %s: %v\n%s", tag, dir, err, out)
	}
}

func insertSourceRows(t *testing.T, ctx context.Context, sql string) {
	t.Helper()
	conn, err := pgx.Connect(ctx, jsinkSrcPGConn)
	if err != nil {
		t.Fatalf("connect to source postgres: %v", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS students (id integer PRIMARY KEY, name text NOT NULL)`); err != nil {
		t.Fatalf("create source table: %v", err)
	}
	if _, err := conn.Exec(ctx, sql); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

func createTargetTable(t *testing.T, ctx context.Context) {
	t.Helper()
	conn, err := pgx.Connect(ctx, jsinkDstPGConn)
	if err != nil {
		t.Fatalf("connect to target postgres: %v", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS attendance (id integer PRIMARY KEY, name text)`); err != nil {
		t.Fatalf("create target table: %v", err)
	}
}

// waitForRow polls the target table until id's name column equals want, or
// timeout elapses (reporting the connector's live state on failure, mirrors
// sink_integration_test.go's waitForObjectAt failure message pattern).
func waitForRow(t *testing.T, ctx context.Context, connStr string, id int, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		conn, err := pgx.Connect(ctx, connStr)
		if err == nil {
			var got string
			scanErr := conn.QueryRow(ctx, `SELECT name FROM attendance WHERE id = $1`, id).Scan(&got)
			conn.Close(ctx)
			if scanErr == nil && got == want {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("row id=%d name=%q did not appear within %s (connector state: %s)", id, want, timeout, jsinkConnectorState(t))
		}
		time.Sleep(3 * time.Second)
	}
}

func assertRowCount(t *testing.T, ctx context.Context, connStr string, want int) {
	t.Helper()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	var got int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM attendance`).Scan(&got); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if got != want {
		t.Errorf("row count = %d, want %d (upsert must not create duplicate rows)", got, want)
	}
}

func jsinkConnectorConfig(t *testing.T, name string) map[string]string {
	t.Helper()
	var cfg map[string]string
	getJSON(t, fmt.Sprintf("%s/connectors/%s/config", jsinkConnectURL, name), &cfg)
	return cfg
}

func jsinkConnectorState(t *testing.T) string {
	var body struct {
		Connector struct {
			State string `json:"state"`
		} `json:"connector"`
		Tasks []struct {
			State string `json:"state"`
			Trace string `json:"trace"`
		} `json:"tasks"`
	}
	getJSON(t, fmt.Sprintf("%s/connectors/%s/status", jsinkConnectURL, "jdbcsink-events-to-warehouse"), &body)
	states := body.Connector.State
	for _, task := range body.Tasks {
		states += " task:" + task.State
		if task.Trace != "" {
			states += " trace: " + task.Trace
		}
	}
	return states
}
