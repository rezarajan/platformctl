//go:build integration

package main

import (
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	dockerruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/docker"
	"github.com/rezarajan/platformctl/internal/cliutil"
)

// TestAcceptanceCDCAttendance is the v1.0.0 acceptance scenario
// (docs/planning/05-v1-first-version-spec.md §6) run against the literal
// examples/cdc-attendance/ manifests. Steps 8 (external-source destroy
// refusal) and 9 (mid-apply kill recovery) are covered by
// TestExternalSourceEndToEnd and TestChaosApplyKilledMidRun on equivalent
// resource sets; step 5's fake-provider contract is covered in
// application/engine — here the forwarded endpoint is asserted on the real
// connector.
//
// Since docs/planning/08 D2 the example lands PARQUET (closing the
// checkpoint's open item 2, the deliberate json deviation from the §6
// sketch): the CDC leg serializes Avro against Redpanda's built-in schema
// registry, so every CLI call passes the SchemaRegistrySupport gate and the
// landed object is asserted with a real parquet reader, not a string grep.
func TestAcceptanceCDCAttendance(t *testing.T) {
	t.Setenv("DATASCAPE_SECRET_POSTGRES_ADMIN_CREDS_USERNAME", "admin")
	t.Setenv("DATASCAPE_SECRET_POSTGRES_ADMIN_CREDS_PASSWORD", "admin-pw")
	t.Setenv("DATASCAPE_SECRET_POSTGRES_REPLICATION_CREDS_USERNAME", "repl")
	t.Setenv("DATASCAPE_SECRET_POSTGRES_REPLICATION_CREDS_PASSWORD", "repl-pw")
	t.Setenv("DATASCAPE_SECRET_MINIO_ROOT_CREDS_USERNAME", "minioadmin")
	t.Setenv("DATASCAPE_SECRET_MINIO_ROOT_CREDS_PASSWORD", "minioadmin-pw")

	build := exec.Command("docker", "build", "-t", "datascape-s3sink-connect:local", "../../examples/cdc-attendance/s3sink-image")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build sink connect image: %v\n%s", err, out)
	}

	rt, err := dockerruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to Docker: %v", err)
	}
	ctx := context.Background()

	containers := []string{"s3-sink", "local-minio", "postgres-cdc", "local-postgres", "local-redpanda"}
	cleanup := func() {
		for _, c := range containers {
			_ = rt.Remove(ctx, c)
		}
		for _, v := range []string{"local-postgres-data", "local-redpanda-data", "local-minio-data"} {
			_ = rt.RemoveVolume(ctx, v)
		}
		_ = rt.RemoveNetwork(ctx, "datascape")
		_ = exec.Command("docker", "network", "rm", "datascape").Run()
	}
	cleanup()
	t.Cleanup(cleanup)

	stateFile := filepath.Join(t.TempDir(), "state.json")
	manifests := "../../examples/cdc-attendance"
	// The example uses the schema-registry chain (parquet ⇒ avro ⇒
	// registry), which sits behind the Alpha SchemaRegistrySupport gate —
	// exactly what README.md#run-it tells a user to pass.
	const gate = "SchemaRegistrySupport=true"
	runG := func(args ...string) (string, error, int) {
		return run(t, append(args, "--feature-gates", gate)...)
	}
	start := time.Now()

	// Step 1: validate exits 0.
	out, err, code := runG("validate", manifests)
	if err != nil || code != 0 {
		t.Fatalf("validate failed (code %d): %v\n%s", code, err, out)
	}

	// Step 2: plan against empty state — everything a create.
	out, _, code = runG("plan", manifests, "--state-file", stateFile)
	if code != cliutil.ExitPlanChanges {
		t.Fatalf("plan exit = %d, want %d\n%s", code, cliutil.ExitPlanChanges, out)
	}
	if strings.Contains(out, "update") || strings.Contains(out, "no-op") {
		t.Errorf("plan against empty state is not all-create:\n%s", out)
	}

	// Step 3: apply reconciles in dependency order.
	out, err, code = runG("apply", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}

	// Step 4: all resources Ready.
	out, err, code = runG("status", manifests, "--state-file", stateFile)
	if err != nil || code != 0 {
		t.Fatalf("status failed (code %d): %v\n%s", code, err, out)
	}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n")[1:] {
		if !strings.Contains(line, "True") {
			t.Errorf("resource not Ready: %s", line)
		}
	}

	// NFR-8: steps 1–4 within the four-minute budget (images pre-pulled).
	if elapsed := time.Since(start); elapsed > 4*time.Minute {
		t.Errorf("steps 1-4 took %s, exceeding the 4-minute NFR-8 budget", elapsed.Round(time.Second))
	} else {
		t.Logf("steps 1-4 completed in %s", elapsed.Round(time.Second))
	}

	// inventory surfaces the service endpoints an operator configures tools
	// against, with the credential SecretReference.
	out, err, code = runG("inventory", manifests, "--state-file", stateFile)
	if err != nil || code != 0 {
		t.Fatalf("inventory failed (code %d): %v\n%s", code, err, out)
	}
	for _, want := range []string{"Provider/local-redpanda", "kafka", "127.0.0.1:19093", "Provider/local-postgres", "postgres-admin-creds"} {
		if !strings.Contains(out, want) {
			t.Errorf("inventory missing %q:\n%s", want, out)
		}
	}

	// graph renders the architecture pipeline (Source→EventStream→Dataset).
	out, _, code = runG("graph", manifests)
	if code != 0 || !strings.Contains(out, "DATA FLOW") || !strings.Contains(out, "──[cdc") {
		t.Errorf("graph did not render the data-flow architecture:\n%s", out)
	}

	// Step 5 (real half): the observers entry on the CDC Binding resolved
	// test-lineage-fake's URL and Debezium (LineageAware) received it.
	cfg := acceptanceConnectorConfig(t, "http://127.0.0.1:18083", "student-db-to-events")
	if got, want := cfg["openlineage.integration.config.transport.url"], "http://localhost:8080/api/v1/lineage"; got != want {
		t.Errorf("lineage endpoint on connector = %q, want %q", got, want)
	}

	// Sink correctness (§7, parquet since docs/planning/08 D2): CDC traffic
	// lands as PARQUET objects under the Dataset's bucket/prefix, readable
	// by a real Go parquet reader with the inserted row's content intact.
	insertAcceptanceRows(t, ctx)
	waitForParquetRow(t, ctx, "127.0.0.1:19000", "minioadmin", "minioadmin-pw", "raw-events", "attendance/", "name", "acceptance-alice", 180*time.Second)

	// Step 6: idempotent re-apply — zero mutating calls.
	out, err, code = runG("apply", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("re-apply failed (code %d): %v\n%s", code, err, out)
	}
	if !strings.Contains(out, "no changes") {
		t.Errorf("re-apply did not report 'no changes':\n%s", out)
	}

	// Step 7: stop MinIO out-of-band → drift reports it; plan does not
	// restart it; apply does.
	if out, err := exec.Command("docker", "stop", "local-minio").CombinedOutput(); err != nil {
		t.Fatalf("stop minio: %v\n%s", err, out)
	}
	report, code := runDrift(t, manifests, stateFile, "--feature-gates", gate)
	if code != cliutil.ExitPlanChanges {
		t.Errorf("drift exit = %d, want %d", code, cliutil.ExitPlanChanges)
	}
	for _, victim := range []string{"Provider/local-minio", "Dataset/attendance-raw"} {
		if r := report[victim]; r.Drift != "True" {
			t.Errorf("%s drift = %q, want True", victim, r.Drift)
		}
	}
	out, err, code = runG("plan", manifests, "--state-file", stateFile)
	if err != nil || code != 0 {
		t.Fatalf("plan during drift: code %d err %v\n%s", code, err, out)
	}
	if st, _, _ := rt.Inspect(ctx, "local-minio"); st.Running {
		t.Error("plan restarted the stopped container; plan must never mutate")
	}
	out, err, code = runG("apply", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("healing apply failed (code %d): %v\n%s", code, err, out)
	}
	if st, found, _ := rt.Inspect(ctx, "local-minio"); !found || !st.Running {
		t.Error("apply did not restart the stopped object store")
	}

	// Step 10: destroy removes every managed resource, no labeled leftovers.
	out, err, code = runG("destroy", manifests, "--state-file", stateFile, "--auto-approve")
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

func insertAcceptanceRows(t *testing.T, ctx context.Context) {
	t.Helper()
	conn, err := pgx.Connect(ctx, "postgres://admin:admin-pw@127.0.0.1:15432/studentdb?sslmode=disable")
	if err != nil {
		t.Fatalf("connect to postgres: %v", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS students (id serial PRIMARY KEY, name text NOT NULL)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO students (name) VALUES ('acceptance-alice')`); err != nil {
		t.Fatalf("insert: %v", err)
	}
}

func acceptanceConnectorConfig(t *testing.T, baseURL, name string) map[string]string {
	t.Helper()
	var cfg map[string]string
	getJSON(t, baseURL+"/connectors/"+name+"/config", &cfg)
	return cfg
}
