//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	dockerruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/docker"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

const (
	trnPGConn      = "postgres://datascape_admin:admin-secret-pw@localhost:15647/attendance?sslmode=disable"
	trnMinioAddr   = "localhost:19104"
	trnCoordinator = "localhost:18091"
	// HighAvailability=true is required throughout: the base scenario
	// already declares workers: 2 (see manifests.yaml's comment on why 1
	// is not a valid starting point for a later in-place scale-up).
	trnGates = "SchemaRegistrySupport=true,TrinoProvider=true,HighAvailability=true"
)

// TestTrinoComputeEngineEndToEnd covers docs/planning/08 D10's accept list
// against real Docker, literally, and now also D8's "trino e2e still green
// (its resolution-order change)" accept item: testdata/trino-scenario's
// Catalog carries spec.warehouseRef (a dedicated trn-warehouse Dataset) with
// no Provider-level configuration.defaultWarehouseLocation at all — nessie's
// warehouse config is derived entirely through the D8 path — while the
// trino Provider's own warehouseProviderRef stays set to the identical
// target, proving the two mechanisms coexist without conflict (see docs/
// adr/006's "Implementation notes (D8, added post-implementation)" section
// for the recorded resolution order and reconciliation design).
//
//   - a trino Provider + catalogRef to the lakehouse Catalog reaches Ready;
//   - a query through the coordinator returns rows tied to the D2 CDC->
//     parquet path (see the deviation note below — D1/D2's raw parquet
//     output carries no Iceberg metadata, so this proves the wiring by
//     having Trino itself write/read an Iceberg table over the same row
//     content, rather than reading the raw parquet file directly);
//   - worker scale-up is in-place (coordinator container ID unchanged;
//     2->3, not 1->3 — see deviation 3 below);
//   - an out-of-band catalog config edit is reported as drift and healed
//     by the next apply;
//   - idempotent re-apply reports "no changes";
//   - catalogRef naming a non-Catalog resource is rejected at validate.
//
// Deviations (recorded per docs/planning/08 §2.1's protocol — findings, not
// silent workarounds; see docs/adr/006's Implementation notes for the full
// writeup):
//
//  1. No component in this codebase (s3sink's Aiven connector, specifically)
//     generates genuine Iceberg table metadata (manifests/snapshots) for the
//     CDC->parquet path — it writes plain partitioned Parquet files. Trino's
//     `iceberg` connector cannot query that data directly (it would need the
//     `hive` connector + a metastore, which this stack does not provision).
//  2. Genuine Iceberg table *writes* through Nessie's REST Catalog
//     personality with `STATIC` S3 credentials hit a further, separate gap:
//     `CREATE SCHEMA` (a namespace-level operation) succeeds — proving the
//     full chain (coordinator/worker provisioning, catalog auto-
//     configuration from Nessie+MinIO facts, credential wiring, query
//     execution) genuinely works end-to-end — but `CREATE TABLE` fails
//     server-side with `"...cannot be associated with any configured object
//     storage location: Missing access key and secret for STATIC
//     authentication mode"` regardless of location, and Trino's alternative
//     (`iceberg.rest-catalog.vended-credentials-enabled=true`) fails
//     differently (`"Failed to initialize the vended credentials from the
//     provided fileIoProperties"`). Both were reproduced live against a
//     bare REST call (bypassing Trino entirely) — this is Nessie 0.108.1's
//     Iceberg-Catalog write path, not a trino-provider defect. Resolving it
//     is out of D10's scope (provisioning the compute engine and wiring the
//     catalog config it already achieves) and is left to a follow-up.
//  3. "1->3 worker scale-up" as literally written is not achievable: C1/C2's
//     replica primitive defines `Replicas <= 1` with `StableIdentity: false`
//     (workers' shape — pure compute, no per-replica storage, per ADR 006)
//     as byte-for-byte the single-container shape, not an ordinal set of
//     one; scaling from it to any `Replicas > 1` is a shape transition,
//     refused in place by the runtime port's own conformance-pinned
//     contract — the identical rule redpanda's `brokers` field already
//     obeys for its legacy-to-declared transition (docs/adr/017 §a.1). The
//     scenario below starts at `workers: 2` (the smallest count already in
//     the ordinal-set shape) and scales 2->3, proving a genuine in-place
//     scale-up within that shape — the coordinator, entirely unaffected by
//     worker-set shape either way, never restarts in any of these cases.
//
// The test below proves everything D10 actually owns end-to-end (Ready,
// scale-up, drift-heal, idempotent re-apply, validate rejection) and proves
// "a query through the coordinator returns rows" two ways given the above:
// a metadata-level query against the wired Iceberg REST catalog
// (CREATE/SHOW SCHEMA, exercising the full Nessie+MinIO+credential chain),
// and a row-returning SELECT against Trino's built-in system catalog
// (proving query submission/scheduling/execution across the worker set).
func TestTrinoComputeEngineEndToEnd(t *testing.T) {
	t.Setenv("DATASCAPE_SECRET_TRN_PG_ADMIN_USERNAME", "datascape_admin")
	t.Setenv("DATASCAPE_SECRET_TRN_PG_ADMIN_PASSWORD", "admin-secret-pw")
	t.Setenv("DATASCAPE_SECRET_TRN_PG_REPL_USERNAME", "datascape_repl")
	t.Setenv("DATASCAPE_SECRET_TRN_PG_REPL_PASSWORD", "repl-secret-pw")
	t.Setenv("DATASCAPE_SECRET_TRN_MINIO_ROOT_USERNAME", "datascape_minio")
	t.Setenv("DATASCAPE_SECRET_TRN_MINIO_ROOT_PASSWORD", "minio-secret-pw")

	buildSinkConnectImage(t)

	rt, err := dockerruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to Docker: %v", err)
	}
	ctx := context.Background()

	containers := []string{
		"datascape-trn-trino-coordinator", "datascape-trn-trino-worker",
		"datascape-trn-nessie", "datascape-trn-s3", "datascape-trn-minio",
		"datascape-trn-dbz", "datascape-trn-pg", "datascape-trn-rp",
	}
	cleanup := func() {
		for _, c := range containers {
			_ = rt.Remove(ctx, c)
		}
		for i := 0; i < 3; i++ {
			_ = rt.Remove(ctx, fmt.Sprintf("datascape-trn-trino-worker-%d", i))
		}
		for _, v := range []string{"datascape-trn-pg-data", "datascape-trn-rp-data", "datascape-trn-minio-data"} {
			_ = rt.RemoveVolume(ctx, v)
		}
		_ = rt.RemoveNetwork(ctx, "datascape-trn-net")
	}
	cleanup()
	t.Cleanup(cleanup)

	stateFile := filepath.Join(t.TempDir(), "state.json")
	manifests := "testdata/trino-scenario"

	// --- Reaches Ready ------------------------------------------------
	start := time.Now()
	out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", trnGates)
	if err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}
	t.Logf("trino scenario applied in %s", time.Since(start).Round(time.Second))

	out, err, code = run(t, "status", manifests, "--state-file", stateFile, "--feature-gates", trnGates)
	if err != nil || code != 0 {
		t.Fatalf("status failed (code %d): %v\n%s", code, err, out)
	}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n")[1:] {
		if !strings.Contains(line, "True") {
			t.Errorf("resource not Ready after apply: %s", line)
		}
	}

	coordBefore, found, err := rt.Inspect(ctx, "datascape-trn-trino-coordinator")
	if err != nil || !found {
		t.Fatalf("coordinator not found after apply: %v", err)
	}

	// --- CDC row lands as parquet (D1/D2 spine, unchanged) -------------
	trnSeedRow(t, ctx, "trino-alice")
	obj := waitForObjectAt(t, ctx, trnMinioAddr, "datascape_minio", "minio-secret-pw", "raw-events", "attendance/", 180*time.Second)
	if !strings.Contains(obj, "trino-alice") {
		t.Fatalf("CDC parquet object does not contain seeded row 'trino-alice':\n%.300s", obj)
	}

	// --- Query through the coordinator (deviations noted above) ---------
	// Metadata-level: exercises the full Iceberg REST catalog chain this
	// provider wires (Nessie's REST endpoint, the resolved MinIO endpoint +
	// credentials) — proves the catalog is genuinely reachable and
	// writable at the namespace level, not just configured.
	trinoExec(t, trnCoordinator, `CREATE SCHEMA IF NOT EXISTS lakehouse.default`)
	schemas := trinoQuery(t, trnCoordinator, `SHOW SCHEMAS FROM lakehouse`)
	foundDefault := false
	for _, row := range schemas {
		if len(row) > 0 && fmt.Sprint(row[0]) == "default" {
			foundDefault = true
		}
	}
	if !foundDefault {
		t.Fatalf("SHOW SCHEMAS FROM lakehouse = %v, want to include 'default'", schemas)
	}
	// Row-returning: proves query submission/scheduling/execution actually
	// works across the worker set (Trino's built-in system catalog, no
	// external storage needed — the row-count-returning half of "a query
	// through the coordinator returns rows", given deviation 2 above).
	rows := trinoQuery(t, trnCoordinator, `SELECT count(*) FROM system.runtime.nodes WHERE coordinator = false`)
	if len(rows) != 1 {
		t.Fatalf("SELECT through the coordinator = %v, want exactly one row", rows)
	}
	if n := fmt.Sprint(rows[0][0]); n != "2" {
		t.Fatalf("worker node count reported by the coordinator = %v, want 2 (configuration.workers)", n)
	}

	// --- Idempotent re-apply: zero mutating calls (surfaced as "no
	// changes" — the same convention redpanda's HA e2e test uses) --------
	out, err, code = run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", trnGates)
	if err != nil || code != 0 {
		t.Fatalf("idempotent re-apply failed (code %d): %v\n%s", code, err, out)
	}
	if !strings.Contains(out, "no changes") {
		t.Errorf("idempotent re-apply did not report 'no changes':\n%s", out)
	}
	if after, found, err := rt.Inspect(ctx, "datascape-trn-trino-coordinator"); err != nil || !found || after.ID != coordBefore.ID {
		t.Errorf("idempotent re-apply touched the coordinator container (before %s, after %+v, err %v)", coordBefore.ID, after, err)
	}

	// --- Out-of-band catalog config edit -> drift -> heal ---------------
	corrupted := "connector.name=iceberg\niceberg.catalog.type=rest\niceberg.rest-catalog.uri=http://corrupted:0\n"
	// -u root: the file is intentionally read-only to the container's own
	// (non-root, uid 1000) trino process (ContainerSpec.FileMount Mode
	// 0o444) — an out-of-band edit needs admin-equivalent access, the same
	// way it would on a real host, not the app's own runtime identity.
	corruptCmd := exec.Command("docker", "exec", "-u", "root", "-i", "datascape-trn-trino-coordinator", "sh", "-c", "cat > /etc/trino/catalog/lakehouse.properties")
	corruptCmd.Stdin = strings.NewReader(corrupted)
	if out, err := corruptCmd.CombinedOutput(); err != nil {
		t.Fatalf("corrupt catalog config out-of-band: %v\n%s", err, out)
	}

	// `status` alone only replays the last-recorded condition (it does not
	// re-probe) — `drift` is the subcommand that actually re-probes every
	// resource, the same distinction chaos_integration_test.go's runDrift
	// and redpanda_ha_integration_test.go's drift check rely on.
	// drift's own exit code is non-zero when drift is found (by design,
	// like a diff tool) — that is the expected outcome here, not a
	// failure; only a decode error on stdout is fatal.
	out, _, _ = run(t, "drift", manifests, "--state-file", stateFile, "-o", "json", "--feature-gates", trnGates)
	if !strings.Contains(out, `"resource": "default/Provider/datascape-trn-trino"`) && !strings.Contains(out, `"resource":"default/Provider/datascape-trn-trino"`) {
		t.Fatalf("drift output does not mention the trino Provider:\n%s", out)
	}
	var driftPayload struct {
		Resources []struct {
			Resource string `json:"resource"`
			Drift    string `json:"drift"`
		} `json:"resources"`
	}
	if err := json.Unmarshal([]byte(out), &driftPayload); err != nil {
		t.Fatalf("decode drift output: %v\n%s", err, out)
	}
	trinoDrifted := false
	for _, r := range driftPayload.Resources {
		if r.Resource == "default/Provider/datascape-trn-trino" && r.Drift == "True" {
			trinoDrifted = true
		}
	}
	if !trinoDrifted {
		t.Errorf("drift did not report the trino Provider as drifted after the out-of-band edit:\n%s", out)
	}

	out, err, code = run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", trnGates)
	if err != nil || code != 0 {
		t.Fatalf("heal apply failed (code %d): %v\n%s", code, err, out)
	}
	healed, healErr := exec.Command("docker", "exec", "datascape-trn-trino-coordinator", "cat", "/etc/trino/catalog/lakehouse.properties").CombinedOutput()
	if healErr != nil {
		t.Fatalf("read healed catalog config: %v\n%s", healErr, healed)
	}
	if strings.Contains(string(healed), "corrupted") {
		t.Errorf("catalog config was not healed:\n%s", healed)
	}
	if !strings.Contains(string(healed), "iceberg.rest-catalog.uri=http://datascape-trn-nessie:19120/iceberg") {
		t.Errorf("healed catalog config missing the correct catalog URI:\n%s", healed)
	}

	// The heal apply just above legitimately recreated the coordinator
	// (that's what healing a file-mounted config means, per docs/adr/006's
	// Implementation notes) — coordBefore (captured before any of that)
	// would wrongly read as "scale-up recreated it" below. Re-baseline.
	coordBeforeScale, found, err := rt.Inspect(ctx, "datascape-trn-trino-coordinator")
	if err != nil || !found {
		t.Fatalf("coordinator missing after heal: %v", err)
	}

	// --- 2->3 worker scale-up in place, no coordinator restart -----------
	// (not 1->3: StableIdentity: false + Replicas <= 1 is always the
	// single-container shape — see manifests.yaml's comment on why the
	// base scenario already starts at workers: 2, the smallest count that
	// is genuinely in the ordinal-set shape a further scale-up can extend
	// in place.)
	data, err := os.ReadFile(filepath.Join(manifests, "manifests.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	scaled := strings.Replace(string(data), "workers: 2", "workers: 3", 1)
	if scaled == string(data) {
		t.Fatal("scale fixture: 'workers: 2' not found in base manifest")
	}
	scaledDir := filepath.Join(t.TempDir(), "scaled")
	if err := os.MkdirAll(scaledDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(scaledDir, "manifests.yaml"), []byte(scaled), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err, code = run(t, "apply", scaledDir, "--state-file", stateFile, "--auto-approve", "--feature-gates", trnGates)
	if err != nil || code != 0 {
		t.Fatalf("scale-up apply failed (code %d): %v\n%s", code, err, out)
	}
	coordAfterScale, found, err := rt.Inspect(ctx, "datascape-trn-trino-coordinator")
	if err != nil || !found {
		t.Fatalf("coordinator missing after scale-up: %v", err)
	}
	if coordAfterScale.ID != coordBeforeScale.ID {
		t.Errorf("scale-up recreated the coordinator (before %s, after %s)", coordBeforeScale.ID, coordAfterScale.ID)
	}
	// Reconcile's own WaitHealthy returns at one-member-healthy (ADR 004's
	// deliberate at-least-one rule, docs/adr/017 §a.6's precedent) — apply
	// can legitimately return success while the newest ordinal is still a
	// few seconds from passing its own healthcheck. Poll briefly rather
	// than asserting immediately.
	deadline := time.Now().Add(30 * time.Second)
	var workerState runtime.ContainerState
	for {
		workerState, found, err = rt.Inspect(ctx, "datascape-trn-trino-worker")
		if err != nil || !found {
			t.Fatalf("worker set missing after scale-up: %v", err)
		}
		if workerState.ReadyReplicas == 3 || time.Now().After(deadline) {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if workerState.ReadyReplicas != 3 {
		t.Errorf("ReadyReplicas after scale-up = %d, want 3", workerState.ReadyReplicas)
	}

	// --- Negative validate: catalogRef to a non-Catalog kind ------------
	badRef := strings.Replace(string(data), "catalogRef:\n      name: trn-catalog", "catalogRef:\n      name: datascape-trn-minio", 1)
	if badRef == string(data) {
		t.Fatal("negative fixture: catalogRef target not found in base manifest")
	}
	badDir := filepath.Join(t.TempDir(), "bad-catalogref")
	if err := os.MkdirAll(badDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(badDir, "manifests.yaml"), []byte(badRef), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err, code = run(t, "validate", badDir, "--feature-gates", trnGates)
	if code == 0 || err == nil {
		t.Fatalf("validate accepted catalogRef naming a non-Catalog resource:\n%s", out)
	}
	if !strings.Contains(err.Error(), "does not resolve to any resource") || !strings.Contains(err.Error(), "configuration.catalogRef") {
		t.Errorf("validate error lacks the expected kind-check shape: %v", err)
	}

	// --- Destroy tears everything down cleanly ---------------------------
	out, err, code = run(t, "destroy", scaledDir, "--state-file", stateFile, "--auto-approve", "--feature-gates", trnGates)
	if err != nil || code != 0 {
		t.Fatalf("destroy failed (code %d): %v\n%s", code, err, out)
	}
	for _, c := range containers {
		if _, found, _ := rt.Inspect(ctx, c); found {
			t.Errorf("container %s still present after destroy", c)
		}
	}
	for i := 0; i < 3; i++ {
		if _, found, _ := rt.Inspect(ctx, fmt.Sprintf("datascape-trn-trino-worker-%d", i)); found {
			t.Errorf("worker ordinal %d still present after destroy", i)
		}
	}
	managed, err := rt.ListManaged(ctx)
	if err != nil {
		t.Fatalf("list managed: %v", err)
	}
	for _, m := range managed {
		if strings.HasPrefix(m.Name, "datascape-trn-") {
			t.Errorf("orphaned managed container after destroy: %s", m.Name)
		}
	}
}

func trnSeedRow(t *testing.T, ctx context.Context, name string) {
	t.Helper()
	conn, err := pgx.Connect(ctx, trnPGConn)
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

// trinoStatementPage is the subset of Trino's /v1/statement (and its
// nextUri follow-on pages') response shape this package reads.
type trinoStatementPage struct {
	NextURI string          `json:"nextUri"`
	Data    [][]interface{} `json:"data"`
	Error   *struct {
		Message string `json:"message"`
	} `json:"error"`
}

// trinoQuery drives Trino's async /v1/statement HTTP query protocol
// (docs/planning/08 D10's accept item: "use the Trino HTTP query API from
// the test") to completion and returns every row across every page. addr is
// a bare "host:port".
func trinoQuery(t *testing.T, addr, sql string) [][]interface{} {
	t.Helper()
	client := &http.Client{Timeout: 30 * time.Second}
	req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/v1/statement", bytes.NewReader([]byte(sql)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Trino-User", "platformctl")
	req.Header.Set("Content-Type", "text/plain")

	var all [][]interface{}
	nextURL := ""
	deadline := time.Now().Add(90 * time.Second)
	for {
		var resp *http.Response
		if nextURL == "" {
			resp, err = client.Do(req)
		} else {
			resp, err = client.Get(nextURL)
		}
		if err != nil {
			t.Fatalf("trino query request: %v", err)
		}
		var page trinoStatementPage
		decErr := json.NewDecoder(resp.Body).Decode(&page)
		resp.Body.Close()
		if decErr != nil {
			t.Fatalf("decode trino statement response: %v", decErr)
		}
		if page.Error != nil {
			t.Fatalf("trino query %q failed: %s", sql, page.Error.Message)
		}
		all = append(all, page.Data...)
		if page.NextURI == "" {
			return all
		}
		if time.Now().After(deadline) {
			t.Fatalf("trino query %q did not complete within the deadline", sql)
		}
		nextURL = page.NextURI
		time.Sleep(200 * time.Millisecond)
	}
}

// trinoExec runs a statement for its side effect (DDL/DML), ignoring any
// returned rows.
func trinoExec(t *testing.T, addr, sql string) {
	t.Helper()
	trinoQuery(t, addr, sql)
}
