//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	dockerruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/docker"
	"github.com/rezarajan/platformctl/internal/testkit"
)

const cdcConnectURL = "http://localhost:18183"

// TestCDCEndToEnd covers the Phase 3 exit criteria: the full Provider×3 +
// Source + EventStream + Binding manifest set against real Postgres, Debezium
// (Kafka Connect), and Redpanda containers. The lineage-forwarding and
// compatibility-error-shape criteria are covered at the unit level in
// application/engine and application/compatibility respectively.
func TestCDCEndToEnd(t *testing.T) {
	t.Setenv("DATASCAPE_SECRET_CDC_PG_ADMIN_USERNAME", "datascape_admin")
	t.Setenv("DATASCAPE_SECRET_CDC_PG_ADMIN_PASSWORD", "admin-secret-pw")
	t.Setenv("DATASCAPE_SECRET_CDC_PG_REPL_USERNAME", "datascape_repl")
	t.Setenv("DATASCAPE_SECRET_CDC_PG_REPL_PASSWORD", "repl-secret-pw")

	rt, err := dockerruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to Docker: %v", err)
	}
	ctx := context.Background()

	// docs/adr/029: janitor-owned cleanup (J2 sweep) — declared
	// objects, canonical order, silent pre-clean, loud post-clean.
	jan := testkit.Janitor{
		RT:        rt,
		Workloads: []string{"datascape-cdc-dbz", "datascape-cdc-pg", "datascape-cdc-rp"},
		Volumes:   []string{"datascape-cdc-pg-data", "datascape-cdc-rp-data"},
		Networks:  []string{"datascape-cdc-net"},
	}
	jan.CleanSilent(ctx)
	jan.Register(ctx, t)

	stateFile := filepath.Join(t.TempDir(), "state.json")
	manifests := "testdata/cdc-scenario"

	// Exit criterion: the full manifest set applies cleanly from empty state.
	start := time.Now()
	out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}
	t.Logf("apply from empty state took %s", time.Since(start).Round(time.Second))

	// Exit criterion: status shows Ready=True for every resource, including
	// the Binding (connector verified RUNNING, not just container started).
	out, err, code = run(t, "status", manifests, "--state-file", stateFile)
	if err != nil || code != 0 {
		t.Fatalf("status failed (code %d): %v\n%s", code, err, out)
	}
	assertAllStatusReady(t, out, "apply")
	if state := connectorStatus(t, "cdc-students-to-events"); state != "RUNNING" {
		t.Errorf("connector state = %q, want RUNNING", state)
	}

	pgBefore, found, err := rt.Inspect(ctx, "datascape-cdc-pg")
	if err != nil || !found {
		t.Fatalf("postgres container not found after apply: %v", err)
	}
	rpBefore, found, err := rt.Inspect(ctx, "datascape-cdc-rp")
	if err != nil || !found {
		t.Fatalf("redpanda container not found after apply: %v", err)
	}

	// Exit criterion: idempotent re-apply — zero mutating calls.
	out, err, code = run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("re-apply failed (code %d): %v\n%s", code, err, out)
	}
	if !strings.Contains(out, "no changes") {
		t.Errorf("re-apply did not report 'no changes':\n%s", out)
	}

	// Exit criterion: changing Binding.spec.options.tables updates the running
	// connector without recreating the Postgres or Redpanda containers.
	changed := filepath.Join(t.TempDir(), "changed")
	if err := os.MkdirAll(changed, 0o755); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(manifests, "manifests.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	bumped := strings.Replace(string(data), "tables: [students]", "tables: [students, attendance]", 1)
	if bumped == string(data) {
		t.Fatal("tables replacement did not change the manifest")
	}
	if err := os.WriteFile(filepath.Join(changed, "manifests.yaml"), []byte(bumped), 0o644); err != nil {
		t.Fatal(err)
	}

	out, err, code = run(t, "apply", changed, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("tables-change apply failed (code %d): %v\n%s", code, err, out)
	}
	cfg := connectorConfig(t, "cdc-students-to-events")
	if got, want := cfg["table.include.list"], "public.students,public.attendance"; got != want {
		t.Errorf("table.include.list = %q, want %q", got, want)
	}
	pgAfter, found, err := rt.Inspect(ctx, "datascape-cdc-pg")
	if err != nil || !found {
		t.Fatalf("postgres container missing after tables update: %v", err)
	}
	if pgAfter.ID != pgBefore.ID {
		t.Errorf("postgres container was recreated (ID %s -> %s)", pgBefore.ID, pgAfter.ID)
	}
	rpAfter, found, err := rt.Inspect(ctx, "datascape-cdc-rp")
	if err != nil || !found {
		t.Fatalf("redpanda container missing after tables update: %v", err)
	}
	if rpAfter.ID != rpBefore.ID {
		t.Errorf("redpanda container was recreated (ID %s -> %s)", rpBefore.ID, rpAfter.ID)
	}

	// Exit criterion: destroy tears everything down in reverse dependency
	// order with no orphaned containers, networks, or volumes.
	out, err, code = run(t, "destroy", changed, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("destroy failed (code %d): %v\n%s", code, err, out)
	}
	for _, c := range []string{"datascape-cdc-dbz", "datascape-cdc-pg", "datascape-cdc-rp"} {
		if _, found, _ := rt.Inspect(ctx, c); found {
			t.Errorf("container %s still present after destroy", c)
		}
	}
	managed, err := rt.ListManaged(ctx)
	if err != nil {
		t.Fatalf("list managed: %v", err)
	}
	for _, m := range managed {
		if strings.HasPrefix(m.Name, "datascape-cdc-") {
			t.Errorf("orphaned managed container after destroy: %s", m.Name)
		}
	}
}

func connectorStatus(t *testing.T, name string) string {
	t.Helper()
	var body struct {
		Connector struct {
			State string `json:"state"`
		} `json:"connector"`
	}
	getJSON(t, fmt.Sprintf("%s/connectors/%s/status", cdcConnectURL, name), &body)
	return body.Connector.State
}

func connectorConfig(t *testing.T, name string) map[string]string {
	t.Helper()
	var cfg map[string]string
	getJSON(t, fmt.Sprintf("%s/connectors/%s/config", cdcConnectURL, name), &cfg)
	return cfg
}

func getJSON(t *testing.T, url string, into any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: HTTP %d", url, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(into); err != nil {
		t.Fatalf("decode %s: %v", url, err)
	}
}
