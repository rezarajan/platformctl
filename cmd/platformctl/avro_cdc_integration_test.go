//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	dockerruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/docker"
)

const (
	avroConnectURL  = "http://localhost:18283"
	avroRegistryURL = "http://localhost:18581"
	avroPGConn      = "postgres://datascape_admin:admin-secret-pw@localhost:15644/attendance?sslmode=disable"
)

// TestAvroCDCEndToEnd covers docs/planning/08 D1's accept criteria: a CDC
// pipeline serialized as Avro against Redpanda's built-in schema registry —
// Debezium Avro converters register subjects in the registry, and the
// registered value schema is retrievable and decodes as valid Avro.
//
// The reviewer runs this (`just test-integration` / `-tags integration`); it
// is skipped by default and never runs on the shared Docker daemon in CI
// alongside the other integration suites without the usual isolation.
//
// What it asserts (for the reviewer):
//   - apply with --feature-gates=SchemaRegistrySupport=true reaches Ready for
//     every resource, connector RUNNING;
//   - the live connector config carries the Confluent Avro converter for both
//     key and value, each pointed at the registry's *internal* address
//     (http://datascape-avro-rp:8081) — auto-wired from the EventStream's
//     Provider, never typed in the manifest;
//   - after a row is inserted, subjects appear in the registry
//     (`/subjects` on the host-published registry port) — the converter
//     registered schemas;
//   - the value subject's latest schema is retrievable and parses as an Avro
//     record schema (the "consumer decodes" leg: a schema-registry-aware
//     consumer resolves the writer schema by id from exactly this endpoint);
//   - `inventory` surfaces the schema-registry endpoint;
//   - destroy leaves no orphans.
func TestAvroCDCEndToEnd(t *testing.T) {
	t.Setenv("DATASCAPE_SECRET_AVRO_PG_ADMIN_USERNAME", "datascape_admin")
	t.Setenv("DATASCAPE_SECRET_AVRO_PG_ADMIN_PASSWORD", "admin-secret-pw")
	t.Setenv("DATASCAPE_SECRET_AVRO_PG_REPL_USERNAME", "datascape_repl")
	t.Setenv("DATASCAPE_SECRET_AVRO_PG_REPL_PASSWORD", "repl-secret-pw")

	// The Connect worker image needs Confluent's Avro converter jars, which
	// the stock Debezium image does not ship (Apicurio only) — build the
	// testdata image first, the same pattern as sink_integration_test.go.
	build := exec.Command("docker", "build", "-t", "datascape-avro-connect:test", "testdata/avro-connect-image")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build avro connect image: %v\n%s", err, out)
	}

	rt, err := dockerruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to Docker: %v", err)
	}
	ctx := context.Background()

	cleanup := func() {
		for _, c := range []string{"datascape-avro-dbz", "datascape-avro-pg", "datascape-avro-rp"} {
			_ = rt.Remove(ctx, c)
		}
		for _, v := range []string{"datascape-avro-pg-data", "datascape-avro-rp-data"} {
			_ = rt.RemoveVolume(ctx, v)
		}
		_ = rt.RemoveNetwork(ctx, "datascape-avro-net")
	}
	cleanup()
	t.Cleanup(cleanup)

	stateFile := filepath.Join(t.TempDir(), "state.json")
	manifests := "testdata/avro-cdc-scenario"
	gate := "SchemaRegistrySupport=true"

	// The whole set applies cleanly from empty state (gate enabled).
	out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", gate)
	if err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}

	// Every resource, including the Binding, reports Ready=True.
	out, err, code = run(t, "status", manifests, "--state-file", stateFile, "--feature-gates", gate)
	if err != nil || code != 0 {
		t.Fatalf("status failed (code %d): %v\n%s", code, err, out)
	}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n")[1:] {
		if !strings.Contains(line, "True") {
			t.Errorf("resource not Ready after apply: %s", line)
		}
	}

	// The connector is RUNNING and configured with the Avro converters, each
	// wired to the registry's INTERNAL address (auto-resolved, not typed).
	if state := avroConnectorStatus(t, "avro-students-to-events"); state != "RUNNING" {
		t.Errorf("connector state = %q, want RUNNING", state)
	}
	cfg := avroConnectorConfig(t, "avro-students-to-events")
	const wantConverter = "io.confluent.connect.avro.AvroConverter"
	const wantRegistry = "http://datascape-avro-rp:8081"
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

	// Seed a table + row so the connector emits a record and the Avro
	// converter registers its schemas.
	seedAvroRow(t, ctx)

	// Subjects become visible in the registry — the converter registered
	// them. Debezium's default subject naming is <topic>-key / <topic>-value;
	// the value subject for the students table is what a downstream consumer
	// resolves to decode.
	valueSubject := "avro-attendance-events.public.students-value"
	waitForSubject(t, valueSubject, 90*time.Second)

	// The "consumer decodes" leg: the registered writer schema is retrievable
	// by exactly the endpoint a schema-registry-aware consumer would use, and
	// parses as an Avro record schema.
	schema := latestSchema(t, valueSubject)
	if schema["type"] != "record" {
		t.Errorf("registered value schema is not an Avro record: %v", schema)
	}

	// inventory surfaces the schema-registry endpoint (both host and
	// in-network address).
	out, err, code = run(t, "inventory", manifests, "--state-file", stateFile, "--feature-gates", gate)
	if err != nil || code != 0 {
		t.Fatalf("inventory failed (code %d): %v\n%s", code, err, out)
	}
	if !strings.Contains(out, "schema-registry") {
		t.Errorf("inventory does not surface the schema-registry endpoint:\n%s", out)
	}

	// Teardown leaves no orphans.
	out, err, code = run(t, "destroy", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", gate)
	if err != nil || code != 0 {
		t.Fatalf("destroy failed (code %d): %v\n%s", code, err, out)
	}
	managed, err := rt.ListManaged(ctx)
	if err != nil {
		t.Fatalf("list managed: %v", err)
	}
	for _, m := range managed {
		if strings.HasPrefix(m.Name, "datascape-avro-") {
			t.Errorf("orphaned managed container after destroy: %s", m.Name)
		}
	}
}

func seedAvroRow(t *testing.T, ctx context.Context) {
	t.Helper()
	conn, err := pgx.Connect(ctx, avroPGConn)
	if err != nil {
		t.Fatalf("connect to postgres: %v", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS students (id serial PRIMARY KEY, name text NOT NULL)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO students (name) VALUES ('alice'), ('bob')`); err != nil {
		t.Fatalf("insert rows: %v", err)
	}
}

func avroConnectorStatus(t *testing.T, name string) string {
	t.Helper()
	var body struct {
		Connector struct {
			State string `json:"state"`
		} `json:"connector"`
	}
	getJSON(t, fmt.Sprintf("%s/connectors/%s/status", avroConnectURL, name), &body)
	return body.Connector.State
}

func avroConnectorConfig(t *testing.T, name string) map[string]string {
	t.Helper()
	var cfg map[string]string
	getJSON(t, fmt.Sprintf("%s/connectors/%s/config", avroConnectURL, name), &cfg)
	return cfg
}

// waitForSubject polls the registry's /subjects until subject appears.
func waitForSubject(t *testing.T, subject string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		resp, err := http.Get(avroRegistryURL + "/subjects")
		if err == nil && resp.StatusCode == http.StatusOK {
			var subjects []string
			_ = json.NewDecoder(resp.Body).Decode(&subjects)
			resp.Body.Close()
			for _, s := range subjects {
				if s == subject {
					return
				}
			}
		} else if resp != nil {
			resp.Body.Close()
		}
		if time.Now().After(deadline) {
			t.Fatalf("subject %q did not appear in the registry within %s", subject, timeout)
		}
		time.Sleep(2 * time.Second)
	}
}

// latestSchema fetches and parses the latest registered schema for subject.
func latestSchema(t *testing.T, subject string) map[string]any {
	t.Helper()
	var body struct {
		Schema string `json:"schema"`
	}
	getJSON(t, fmt.Sprintf("%s/subjects/%s/versions/latest", avroRegistryURL, subject), &body)
	var schema map[string]any
	if err := json.Unmarshal([]byte(body.Schema), &schema); err != nil {
		t.Fatalf("registered schema for %q is not valid Avro JSON: %v\n%s", subject, err, body.Schema)
	}
	return schema
}
