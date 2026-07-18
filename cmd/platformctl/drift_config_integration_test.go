//go:build integration

package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	dockerruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/docker"
)

// This file covers docs/planning/08 A8: doc 07 §2.1's per-provider drift
// equivalence table and connector-config-diff mechanism were unit-covered,
// and the chaos suite covers out-of-band *liveness* changes (kill/stop a
// container), but nothing mutated real infrastructure *configuration*
// out-of-band and asserted drift catches it, then apply heals it. Each test
// here: apply to Ready, mutate one declared-config fact directly against the
// real technology (bypassing platformctl), assert `drift` reports the
// specific mismatch, assert `apply` heals it, assert `drift` goes clean
// again.

// TestDriftDetectsRedpandaRetentionMismatch: ALTER a topic's retention.ms
// out-of-band via the Kafka admin API (the same one ensureTopic/probeTopic
// use internally) — drift must report RetentionMismatch, never just
// liveness, and apply must heal it back to the manifest's declared value.
func TestDriftDetectsRedpandaRetentionMismatch(t *testing.T) {
	rt, err := dockerruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to Docker: %v", err)
	}
	ctx := context.Background()
	cleanup := func() {
		_ = rt.Remove(ctx, "datascape-rp-test")
		_ = rt.RemoveVolume(ctx, "datascape-rp-test-data")
		_ = rt.RemoveNetwork(ctx, "datascape-rp-test-net")
	}
	cleanup()
	t.Cleanup(cleanup)

	stateFile := filepath.Join(t.TempDir(), "state.json")
	manifests := "testdata/redpanda-scenario"

	if out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve"); err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}

	// The manifest declares retention.duration: 7d (604800000ms). Alter it
	// out-of-band to something else, exactly as an operator running rpk or a
	// Kafka admin client directly against the broker would.
	cl, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:19192"))
	if err != nil {
		t.Fatalf("connect to broker: %v", err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	tamperedMS := "3600000" // 1h, not the declared 7d
	if _, err := adm.AlterTopicConfigs(ctx,
		[]kadm.AlterConfig{{Op: kadm.SetConfig, Name: "retention.ms", Value: &tamperedMS}},
		"datascape-test-events"); err != nil {
		t.Fatalf("out-of-band retention.ms alter: %v", err)
	}

	report, code := runDrift(t, manifests, stateFile)
	if code == 0 {
		t.Fatal("drift reported clean after an out-of-band retention.ms change")
	}
	r := report["EventStream/datascape-test-events"]
	if r.Drift != "True" || !strings.Contains(r.Reason, "RetentionMismatch") {
		t.Fatalf("EventStream drift = %+v, want RetentionMismatch", r)
	}

	if out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve"); err != nil || code != 0 {
		t.Fatalf("healing apply failed (code %d): %v\n%s", code, err, out)
	}
	report, code = runDrift(t, manifests, stateFile)
	if code != 0 {
		t.Fatalf("drift still reports changes after healing apply: %+v", report)
	}
}

// TestDriftDetectsDebeziumConnectorConfigMismatch: PATCH a running Debezium
// connector's config directly via the Kafka Connect REST API — drift must
// report the drifted key *name* (table.include.list), never the value
// (values may carry credentials for other connector types), and apply must
// heal the connector back to the manifest-derived config.
func TestDriftDetectsDebeziumConnectorConfigMismatch(t *testing.T) {
	t.Setenv("DATASCAPE_SECRET_CDC_PG_ADMIN_USERNAME", "datascape_admin")
	t.Setenv("DATASCAPE_SECRET_CDC_PG_ADMIN_PASSWORD", "admin-secret-pw")
	t.Setenv("DATASCAPE_SECRET_CDC_PG_REPL_USERNAME", "datascape_repl")
	t.Setenv("DATASCAPE_SECRET_CDC_PG_REPL_PASSWORD", "repl-secret-pw")

	rt, err := dockerruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to Docker: %v", err)
	}
	ctx := context.Background()
	cleanup := func() {
		for _, c := range []string{"datascape-cdc-dbz", "datascape-cdc-pg", "datascape-cdc-rp"} {
			_ = rt.Remove(ctx, c)
		}
		for _, v := range []string{"datascape-cdc-pg-data", "datascape-cdc-rp-data"} {
			_ = rt.RemoveVolume(ctx, v)
		}
		_ = rt.RemoveNetwork(ctx, "datascape-cdc-net")
	}
	cleanup()
	t.Cleanup(cleanup)

	stateFile := filepath.Join(t.TempDir(), "state.json")
	manifests := "testdata/cdc-scenario"

	if out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve"); err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}

	const connector = "cdc-students-to-events"
	cfg := connectorConfig(t, connector)
	desired := cfg["table.include.list"]
	if desired == "" {
		t.Fatal("connector config missing table.include.list before tampering")
	}
	cfg["table.include.list"] = "public.some_other_table"
	putConnectorConfig(t, cdcConnectURL, connector, cfg)

	report, code := runDrift(t, manifests, stateFile)
	if code == 0 {
		t.Fatal("drift reported clean after an out-of-band connector config PATCH")
	}
	r := report["Binding/cdc-students-to-events"]
	if r.Drift != "True" || r.Reason != "ConnectorConfigDrift" || !strings.Contains(r.Message, "table.include.list") {
		t.Fatalf("Binding drift = %+v, want ConnectorConfigDrift naming table.include.list in its message", r)
	}
	if strings.Contains(r.Message, "some_other_table") || strings.Contains(r.Message, desired) {
		t.Errorf("Binding drift message leaked a config value, not just the key name: %q", r.Message)
	}

	if out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve"); err != nil || code != 0 {
		t.Fatalf("healing apply failed (code %d): %v\n%s", code, err, out)
	}
	healedCfg := connectorConfig(t, connector)
	if healedCfg["table.include.list"] != desired {
		t.Errorf("table.include.list after healing = %q, want %q", healedCfg["table.include.list"], desired)
	}
	report, code = runDrift(t, manifests, stateFile)
	if code != 0 {
		t.Fatalf("drift still reports changes after healing apply: %+v", report)
	}
}

// TestDriftDetectsMariaDBReplicationCredentialMismatch: flip the CDC
// replication user's password directly against the database (out-of-band,
// bypassing the SecretReference) — the "credentials still authenticate"
// half of the Source CDC-readiness contract (docs/planning/07 §2.1's
// equivalence table). wal_level/binlog_format flips need a server restart
// to take effect on postgres/mysql, so aren't reproducible cheaply in a
// test; the replication-credential fact is the CDC-readiness check that's
// both live-mutable and provably healed by a plain re-apply (reconcileSource
// unconditionally re-asserts the SecretReference's password on every
// reconcile).
func TestDriftDetectsMariaDBReplicationCredentialMismatch(t *testing.T) {
	t.Setenv("DATASCAPE_SECRET_MARIA_ROOT_USERNAME", "root")
	t.Setenv("DATASCAPE_SECRET_MARIA_ROOT_PASSWORD", "maria-root-pw")
	t.Setenv("DATASCAPE_SECRET_MARIA_REPL_USERNAME", "datascape_repl")
	t.Setenv("DATASCAPE_SECRET_MARIA_REPL_PASSWORD", "repl-secret-pw")

	rt, err := dockerruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to Docker: %v", err)
	}
	ctx := context.Background()
	cleanup := func() {
		for _, c := range []string{"datascape-maria-dbz", "datascape-maria-db", "datascape-maria-rp"} {
			_ = rt.Remove(ctx, c)
		}
		for _, v := range []string{"datascape-maria-db-data", "datascape-maria-rp-data"} {
			_ = rt.RemoveVolume(ctx, v)
		}
		_ = rt.RemoveNetwork(ctx, "datascape-maria-net")
	}
	cleanup()
	t.Cleanup(cleanup)

	stateFile := filepath.Join(t.TempDir(), "state.json")
	manifests := "testdata/mariadb-cdc-scenario"

	if out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve"); err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}

	admin, err := sql.Open("mysql", "root:maria-root-pw@tcp(127.0.0.1:13307)/attendance?timeout=10s")
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if _, err := admin.ExecContext(ctx, "ALTER USER 'datascape_repl'@'%' IDENTIFIED BY 'tampered-out-of-band-pw'"); err != nil {
		t.Fatalf("out-of-band replication user password change: %v", err)
	}

	report, code := runDrift(t, manifests, stateFile)
	if code == 0 {
		t.Fatal("drift reported clean after tampering with the replication user's password")
	}
	r := report["Source/maria-students"]
	if r.Drift != "True" || !strings.Contains(r.Reason, "ReplicationCredentialsInvalid") {
		t.Fatalf("Source drift = %+v, want ReplicationCredentialsInvalid", r)
	}

	if out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve"); err != nil || code != 0 {
		t.Fatalf("healing apply failed (code %d): %v\n%s", code, err, out)
	}
	// The declared credentials work again; the tampered password does not.
	replDB, err := sql.Open("mysql", "datascape_repl:repl-secret-pw@tcp(127.0.0.1:13307)/attendance?timeout=10s")
	if err != nil {
		t.Fatal(err)
	}
	defer replDB.Close()
	if err := replDB.Ping(); err != nil {
		t.Errorf("declared replication credentials do not authenticate after healing: %v", err)
	}
	report, code = runDrift(t, manifests, stateFile)
	if code != 0 {
		t.Fatalf("drift still reports changes after healing apply: %+v", report)
	}
}

// putConnectorConfig mirrors kafkaconnect.PutConnectorConfig without
// importing the internal adapter package from a cmd-level test — it issues
// the identical PUT /connectors/<name>/config Kafka Connect REST call an
// operator would make directly.
func putConnectorConfig(t *testing.T, baseURL, name string, config map[string]string) {
	t.Helper()
	body, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("marshal connector config: %v", err)
	}
	req, err := http.NewRequest(http.MethodPut, baseURL+"/connectors/"+name+"/config", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build PUT request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT %s: %v", req.URL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		t.Fatalf("PUT %s: HTTP %d", req.URL, resp.StatusCode)
	}
	// Kafka Connect's config PUT applies asynchronously; give the worker a
	// moment to pick it up before the next GET/probe reads it back.
	time.Sleep(500 * time.Millisecond)
}
