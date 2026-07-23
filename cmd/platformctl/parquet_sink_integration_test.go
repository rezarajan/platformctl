//go:build integration

package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/parquet-go/parquet-go"

	dockerruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/docker"
	"github.com/rezarajan/platformctl/internal/testkit"
)

const (
	pqtSinkConnectURL = "http://localhost:18188"
	pqtMinioAddr      = "localhost:19102"
	pqtPGConn         = "postgres://datascape_admin:admin-secret-pw@localhost:15645/attendance?sslmode=disable"
)

// TestParquetSinkEndToEnd covers docs/planning/08 D2's accept criteria on
// real Docker:
//
//   - phase 1 (testdata/parquet-sink-scenario/json): the schemaless json
//     path applies to Ready and lands objects — the kept json-variant
//     fixture;
//   - phase 2 (…/parquet): flipping Dataset.spec.format json→parquet (+ CDC
//     options.format: avro) updates the two connectors WITHOUT recreating
//     the broker, database, or object store (container IDs asserted — the
//     Phase 4 exit-criterion bar);
//   - parquet objects land and are READABLE: rows are parsed in-test with a
//     Go parquet reader (github.com/parquet-go/parquet-go, a test-only
//     dependency) and the inserted row's content is asserted;
//   - the sink connector's converters are registry-driven Avro, auto-wired
//     to the EventStream Provider's internal registry address;
//   - a parquet Dataset behind a chain without a registry endpoint fails at
//     validate with the standard capability-error shape (ADR 009);
//   - re-apply is idempotent; destroy leaves no managed leftovers.
func TestParquetSinkEndToEnd(t *testing.T) {
	t.Setenv("DATASCAPE_SECRET_PQT_PG_ADMIN_USERNAME", "datascape_admin")
	t.Setenv("DATASCAPE_SECRET_PQT_PG_ADMIN_PASSWORD", "admin-secret-pw")
	t.Setenv("DATASCAPE_SECRET_PQT_PG_REPL_USERNAME", "datascape_repl")
	t.Setenv("DATASCAPE_SECRET_PQT_PG_REPL_PASSWORD", "repl-secret-pw")
	t.Setenv("DATASCAPE_SECRET_PQT_MINIO_ROOT_USERNAME", "datascape_minio")
	t.Setenv("DATASCAPE_SECRET_PQT_MINIO_ROOT_PASSWORD", "minio-secret-pw")

	buildSinkConnectImage(t)

	rt, err := dockerruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to Docker: %v", err)
	}
	ctx := context.Background()

	containers := []string{"datascape-pqt-s3", "datascape-pqt-minio", "datascape-pqt-dbz", "datascape-pqt-pg", "datascape-pqt-rp"}
	// docs/adr/029: janitor-owned cleanup (J2 sweep) — declared
	// objects, canonical order, silent pre-clean, loud post-clean.
	jan := testkit.Janitor{
		RT:        rt,
		Workloads: containers,
		Volumes:   []string{"datascape-pqt-pg-data", "datascape-pqt-rp-data", "datascape-pqt-minio-data"},
		Networks:  []string{"datascape-pqt-net"},
	}
	jan.CleanSilent(ctx)
	jan.Register(ctx, t)

	stateFile := filepath.Join(t.TempDir(), "state.json")
	jsonManifests := "testdata/parquet-sink-scenario/json"
	parquetManifests := "testdata/parquet-sink-scenario/parquet"
	gate := "SchemaRegistrySupport=true"

	// Phase 1: the schemaless json path reaches Ready and lands objects.
	start := time.Now()
	out, err, code := run(t, "apply", jsonManifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", gate)
	if err != nil || code != 0 {
		t.Fatalf("json-phase apply failed (code %d): %v\n%s", code, err, out)
	}
	t.Logf("json-phase apply from empty state took %s", time.Since(start).Round(time.Second))

	out, err, code = run(t, "status", jsonManifests, "--state-file", stateFile, "--feature-gates", gate)
	if err != nil || code != 0 {
		t.Fatalf("status failed (code %d): %v\n%s", code, err, out)
	}
	assertAllStatusReady(t, out, "json-phase apply")

	pqtSeedRow(t, ctx, "pqt-alice")
	obj := waitForObjectAt(t, ctx, pqtMinioAddr, "datascape_minio", "minio-secret-pw", "raw-events", "attendance/", 180*time.Second)
	if !strings.Contains(obj, "pqt-alice") {
		t.Errorf("json-phase object does not contain inserted row 'pqt-alice':\n%.300s", obj)
	}

	rpBefore, found, err := rt.Inspect(ctx, "datascape-pqt-rp")
	if err != nil || !found {
		t.Fatalf("broker container not found after apply: %v", err)
	}
	pgBefore, _, _ := rt.Inspect(ctx, "datascape-pqt-pg")
	minioBefore, _, _ := rt.Inspect(ctx, "datascape-pqt-minio")

	// Phase 2: json→parquet (+ CDC avro) is a connector-config-only change.
	out, err, code = run(t, "apply", parquetManifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", gate)
	if err != nil || code != 0 {
		t.Fatalf("parquet-phase apply failed (code %d): %v\n%s", code, err, out)
	}

	// The sink connector now writes parquet from registry-driven Avro
	// records, the registry address auto-wired from the EventStream's
	// Provider (its in-network name + fixed registry port) — never typed in
	// any manifest.
	cfg := pqtConnectorConfig(t, "pqt-events-to-lake")
	const wantConverter = "io.confluent.connect.avro.AvroConverter"
	const wantRegistry = "http://datascape-pqt-rp:8081"
	if got, want := cfg["format.output.type"], "parquet"; got != want {
		t.Errorf("format.output.type = %q, want %q", got, want)
	}
	for _, key := range []string{"key.converter", "value.converter"} {
		if cfg[key] != wantConverter {
			t.Errorf("%s = %q, want %q", key, cfg[key], wantConverter)
		}
	}
	for _, key := range []string{"key.converter.schema.registry.url", "value.converter.schema.registry.url"} {
		if cfg[key] != wantRegistry {
			t.Errorf("%s = %q, want %q (auto-wired internal registry address)", key, cfg[key], wantRegistry)
		}
	}

	// The format change must not have recreated the broker, database, or
	// object store (Phase 4 exit-criterion bar, docs/planning/08 D2 accept).
	for name, before := range map[string]string{
		"datascape-pqt-rp":    rpBefore.ID,
		"datascape-pqt-pg":    pgBefore.ID,
		"datascape-pqt-minio": minioBefore.ID,
	} {
		after, found, err := rt.Inspect(ctx, name)
		if err != nil || !found {
			t.Fatalf("%s missing after format update: %v", name, err)
		}
		if after.ID != before {
			t.Errorf("%s was recreated (ID %s -> %s); a Dataset format change must not touch it", name, before, after.ID)
		}
	}

	// Idempotent re-apply of the parquet manifests: zero mutating calls.
	out, err, code = run(t, "apply", parquetManifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", gate)
	if err != nil || code != 0 {
		t.Fatalf("parquet re-apply failed (code %d): %v\n%s", code, err, out)
	}
	if !strings.Contains(out, "no changes") {
		t.Errorf("parquet re-apply did not report 'no changes':\n%s", out)
	}

	// Parquet objects land and are READABLE: a new row streams through the
	// Avro chain and is asserted from a parsed parquet file, not a string
	// grep over bytes.
	pqtSeedRow(t, ctx, "pqt-parquet-charlie")
	waitForParquetRow(t, ctx, pqtMinioAddr, "datascape_minio", "minio-secret-pw", "raw-events", "attendance/", "name", "pqt-parquet-charlie", 180*time.Second)

	// Negative validate (ADR 009): the same parquet manifests minus the
	// registry (and minus the CDC avro option, so the parquet check — not
	// D1's avro check — is what fires) fail at validate with the standard
	// capability-error shape naming the EventStream's Provider.
	noRegistry := filepath.Join(t.TempDir(), "no-registry")
	if err := os.MkdirAll(noRegistry, 0o755); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(parquetManifests, "manifests.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	stripped := string(data)
	for _, line := range []string{"    schemaRegistry: enabled\n", "    schemaRegistryPort: 18582\n", "    format: avro\n"} {
		if !strings.Contains(stripped, line) {
			t.Fatalf("negative fixture: line %q not found in parquet manifests", strings.TrimSpace(line))
		}
		stripped = strings.Replace(stripped, line, "", 1)
	}
	if err := os.WriteFile(filepath.Join(noRegistry, "manifests.yaml"), []byte(stripped), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err, code = run(t, "validate", noRegistry, "--feature-gates", gate)
	if code == 0 || err == nil {
		t.Fatalf("validate accepted a parquet Dataset against a registry-less chain:\n%s", out)
	}
	want := `Binding "pqt-events-to-lake": Provider "datascape-pqt-rp" (type: redpanda)
does not support format "parquet" (supported: json)`
	if !strings.Contains(err.Error(), want) {
		t.Errorf("validate error lacks the standard capability shape\ngot: %v\nwant substring:\n%s", err, want)
	}

	// Destroy tears everything down cleanly.
	out, err, code = run(t, "destroy", parquetManifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", gate)
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
		if strings.HasPrefix(m.Name, "datascape-pqt-") {
			t.Errorf("orphaned managed container after destroy: %s", m.Name)
		}
	}
}

func pqtSeedRow(t *testing.T, ctx context.Context, name string) {
	t.Helper()
	conn, err := pgx.Connect(ctx, pqtPGConn)
	if err != nil {
		t.Fatalf("connect to postgres: %v", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS students (id serial PRIMARY KEY, name text NOT NULL)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO students (name) VALUES ($1)`, name); err != nil {
		t.Fatalf("insert row: %v", err)
	}
}

func pqtConnectorConfig(t *testing.T, name string) map[string]string {
	t.Helper()
	var cfg map[string]string
	getJSON(t, fmt.Sprintf("%s/connectors/%s/config", pqtSinkConnectURL, name), &cfg)
	return cfg
}

// waitForParquetRow polls bucket/prefix until an object parses as a parquet
// file containing want in a leaf column whose dotted path ends in colSuffix
// — the "parquet objects land and are readable" assertion (docs/planning/08
// D2): row content is read back with a real Go parquet reader, so a file
// that merely embeds the bytes without valid parquet structure can never
// pass.
func waitForParquetRow(t *testing.T, ctx context.Context, addr, user, pass, bucket, prefix, colSuffix, want string, timeout time.Duration) {
	t.Helper()
	cl, err := minio.New(addr, &minio.Options{
		Creds:  credentials.NewStaticV4(user, pass, ""),
		Secure: false,
	})
	if err != nil {
		t.Fatalf("minio client: %v", err)
	}
	deadline := time.Now().Add(timeout)
	var lastParquet string
	for {
		for obj := range cl.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
			if obj.Err != nil {
				break // bucket may not exist yet
			}
			r, err := cl.GetObject(ctx, bucket, obj.Key, minio.GetObjectOptions{})
			if err != nil {
				t.Fatalf("get object %s: %v", obj.Key, err)
			}
			body, err := io.ReadAll(r)
			r.Close()
			if err != nil {
				t.Fatalf("read object %s: %v", obj.Key, err)
			}
			// Phase-1 json objects share the prefix; only parquet files
			// (PAR1 magic) are candidates.
			if !bytes.HasPrefix(body, []byte("PAR1")) {
				continue
			}
			lastParquet = obj.Key
			if parquetContains(t, obj.Key, body, colSuffix, want) {
				t.Logf("parquet row found in %s/%s (%d bytes): %s=%s", bucket, obj.Key, len(body), colSuffix, want)
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no readable parquet object under %s/%s carried %s=%q within %s (last parquet candidate: %q)",
				bucket, prefix, colSuffix, want, timeout, lastParquet)
		}
		time.Sleep(3 * time.Second)
	}
}

// parquetContains parses data as a parquet file and reports whether any row
// holds want in a leaf column whose path ends with colSuffix. A file with
// the PAR1 magic that fails to parse is a hard test failure — landing
// unreadable "parquet" is exactly what this suite exists to catch.
func parquetContains(t *testing.T, key string, data []byte, colSuffix, want string) bool {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("object %s is not readable parquet: %v", key, err)
	}
	cols := f.Schema().Columns()
	for _, rg := range f.RowGroups() {
		rows := rg.Rows()
		buf := make([]parquet.Row, 64)
		for {
			n, readErr := rows.ReadRows(buf)
			for _, row := range buf[:n] {
				for _, v := range row {
					if v.Kind() != parquet.ByteArray {
						continue
					}
					path := cols[v.Column()]
					if len(path) == 0 || path[len(path)-1] != colSuffix {
						continue
					}
					if string(v.ByteArray()) == want {
						rows.Close()
						return true
					}
				}
			}
			if readErr == io.EOF {
				break
			} else if readErr != nil {
				t.Fatalf("reading parquet rows from %s: %v", key, readErr)
			}
		}
		rows.Close()
	}
	return false
}
