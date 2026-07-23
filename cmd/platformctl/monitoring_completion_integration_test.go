//go:build integration

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	monPromAPIBase = "http://127.0.0.1:19434"
	monGrafanaBase = "http://127.0.0.1:19435"
)

// TestMonitoringStackCompletionEndToEnd covers docs/planning/08 C9's
// completion Accept criteria: postgres+mysql Providers with
// configuration.metrics: enabled (the postgres_exporter/mysqld_exporter
// sidecars), plus a prometheus + grafana Provider —
//   - /api/v1/targets shows broker, minio, postgres-exporter, mysql-exporter
//     all up == 1 within a deadline;
//   - Grafana reaches Prometheus (datasource health via Grafana's own API)
//     and the starter dashboard exists;
//   - `inventory --for prometheus` includes the exporter targets;
//   - idempotent re-apply (every managed container's ID unchanged,
//     including both exporter sidecars and grafana);
//   - grafana admin credential rotation (docs/planning/08 I14): rotating
//     the SecretReference and re-applying makes the new password log in
//     and refuses the old one;
//   - clean destroy.
//
// Three apply calls against the same state file, not one — mirroring
// testdata/prometheus-scenario's own infra/combined split and the C9
// status note's recorded convergence caveat (no graph edge orders
// prometheus after the providers it scrapes, or grafana after prometheus):
// infra/ (redpanda+EventStream+minio+postgres+mysql, publishing their
// "metrics"/"postgres"/"mysql" endpoint facts) first, then
// plus-prometheus/ (adds prometheus, which resolves those already-published
// facts), then full/ (adds grafana, which resolves prometheus's
// already-published "prometheus" fact) — each a real Create action against
// already-Ready infra, not a race.
func TestMonitoringStackCompletionEndToEnd(t *testing.T) {
	t.Setenv("DATASCAPE_SECRET_MON_MINIO_ROOT_USERNAME", "datascape_mon_minio")
	t.Setenv("DATASCAPE_SECRET_MON_MINIO_ROOT_PASSWORD", "mon-minio-secret-pw")
	t.Setenv("DATASCAPE_SECRET_MON_PG_ADMIN_USERNAME", "datascape_mon_pg")
	t.Setenv("DATASCAPE_SECRET_MON_PG_ADMIN_PASSWORD", "mon-pg-secret-pw")
	t.Setenv("DATASCAPE_SECRET_MON_MYSQL_ROOT_PASSWORD", "mon-mysql-secret-pw")
	t.Setenv("DATASCAPE_SECRET_MON_GRAFANA_ADMIN_USERNAME", "admin")
	t.Setenv("DATASCAPE_SECRET_MON_GRAFANA_ADMIN_PASSWORD", "mon-grafana-secret-pw")

	rt := requireDocker(t)
	containers := []string{
		"mon-redpanda", "mon-minio", "mon-postgres", "mon-postgres-exporter",
		"mon-mysql", "mon-mysql-exporter", "mon-prometheus", "mon-grafana",
	}
	volumes := []string{"mon-redpanda-data", "mon-minio-data", "mon-postgres-data", "mon-mysql-data"}
	cleanup := registerDockerCleanup(t, rt, containers, volumes, "datascape-mon-net")
	cleanup()

	stateFile := filepath.Join(t.TempDir(), "state.json")
	infra := "testdata/monitoring-completion-scenario/infra"
	plusPrometheus := "testdata/monitoring-completion-scenario/plus-prometheus"
	full := "testdata/monitoring-completion-scenario/full"
	gateFlag := "--feature-gates=MonitoringStackProvider=true"

	// Tier 1: infra — redpanda+EventStream+minio+postgres(metrics)+mysql(metrics).
	// No gate needed for postgres/mysql/redpanda/minio themselves, but
	// configuration.metrics: enabled on postgres/mysql DOES require the
	// gate (checkMonitoringMetricsGate) — pass it here too.
	out, err, code := run(t, "apply", infra, "--state-file", stateFile, "--auto-approve", gateFlag)
	if err != nil || code != 0 {
		t.Fatalf("infra apply failed (code %d): %v\n%s", code, err, out)
	}

	// Tier 2: + prometheus.
	out, err, code = run(t, "apply", plusPrometheus, "--state-file", stateFile, "--auto-approve", gateFlag)
	if err != nil || code != 0 {
		t.Fatalf("plus-prometheus apply failed (code %d): %v\n%s", code, err, out)
	}

	// Exit criterion: activeTargets already carries all four jobs the
	// moment apply returns (Reconcile's own convergence wait blocks until
	// /api/v1/targets' activeTargets count matches the configured target
	// count, not just /-/ready 200ing).
	targets := fetchMonTargets(t, monPromAPIBase)
	wantJobs := []string{"mon-redpanda", "mon-minio", "mon-postgres", "mon-mysql"}
	if len(targets) != len(wantJobs) {
		t.Fatalf("activeTargets = %+v, want exactly %d entries (%v)", targets, len(wantJobs), wantJobs)
	}
	for _, job := range wantJobs {
		if _, ok := targets[job]; !ok {
			t.Errorf("activeTargets missing job %q: %+v", job, targets)
		}
	}

	// Exit criterion: up == 1 for every job within a deadline — broker,
	// minio, the postgres exporter, the mysql exporter (job names are the
	// realizing Provider's own resource name — mon-postgres/mon-mysql, not
	// the exporter container's own "-exporter" name; see engine.go's
	// resolveMetricsTargets: JobName is always the Provider's own name).
	waitForMonTargetsUp(t, monPromAPIBase, wantJobs, 30*time.Second)

	// Tier 3: + grafana.
	out, err, code = run(t, "apply", full, "--state-file", stateFile, "--auto-approve", gateFlag)
	if err != nil || code != 0 {
		t.Fatalf("full apply failed (code %d): %v\n%s", code, err, out)
	}

	grafUser, grafPass := "admin", "mon-grafana-secret-pw"

	// Exit criterion: Grafana reaches Prometheus (datasource health via
	// Grafana's own API), and the starter dashboard exists.
	if !monDatasourceHealthy(t, monGrafanaBase, grafUser, grafPass) {
		t.Error("grafana datasource health check did not report OK")
	}
	if !monHTTPOK(t, monGrafanaBase+"/api/dashboards/uid/datascape-overview", grafUser, grafPass) {
		t.Error("starter dashboard (uid datascape-overview) not found via Grafana's API")
	}

	// Exit criterion: `inventory --for prometheus` includes the exporter
	// targets (docs/planning/08 C9 completion — the tension between
	// Audience: internal-only exporter facts and this bring-your-own
	// rendering path, resolved in cmd/platformctl/toolconfig.go's
	// gatherToolFacts by falling back to the in-network address).
	out, err, code = run(t, "inventory", full, "--state-file", stateFile, "--for", "prometheus", gateFlag)
	if err != nil || code != 0 {
		t.Fatalf("inventory --for prometheus failed (code %d): %v\n%s", code, err, out)
	}
	if !strings.Contains(out, "mon-postgres") {
		t.Errorf("inventory --for prometheus missing the postgres exporter job:\n%s", out)
	}
	if !strings.Contains(out, "mon-mysql") {
		t.Errorf("inventory --for prometheus missing the mysql exporter job:\n%s", out)
	}
	if !strings.Contains(out, "mon-postgres-exporter") {
		t.Errorf("inventory --for prometheus missing the postgres exporter's in-network target:\n%s", out)
	}
	if !strings.Contains(out, "mon-mysql-exporter") {
		t.Errorf("inventory --for prometheus missing the mysql exporter's in-network target:\n%s", out)
	}

	// Exit criterion: idempotent re-apply — every managed container's ID
	// unchanged, including both exporter sidecars and grafana.
	before := map[string]string{}
	for _, c := range containers {
		ctrState, found, err := rt.Inspect(context.Background(), c)
		if err != nil || !found {
			t.Fatalf("Inspect(%q) before re-apply: found=%v err=%v", c, found, err)
		}
		before[c] = ctrState.ID
	}

	out, err, code = run(t, "apply", full, "--state-file", stateFile, "--auto-approve", gateFlag)
	if err != nil || code != 0 {
		t.Fatalf("re-apply failed (code %d): %v\n%s", code, err, out)
	}
	if !strings.Contains(out, "no changes") {
		t.Errorf("re-apply did not report 'no changes':\n%s", out)
	}
	for _, c := range containers {
		ctrState, found, err := rt.Inspect(context.Background(), c)
		if err != nil || !found {
			t.Fatalf("Inspect(%q) after re-apply: found=%v err=%v", c, found, err)
		}
		if ctrState.ID != before[c] {
			t.Errorf("container %q was recreated on a no-op re-apply (ID %s -> %s)", c, before[c], ctrState.ID)
		}
	}

	// Secret rotation (docs/planning/08 I14): changing the grafana admin
	// SecretReference must rotate the *live* admin credential, not just the
	// container's bootstrap password file — Grafana only consumes
	// GF_SECURITY_ADMIN_PASSWORD__FILE at first boot (initdb-shaped, like
	// postgres/mysql), so without I14's grafana-cli-exec-and-reprobe path
	// the live account would silently keep the old password while the
	// container quietly recreated with a new (unused) file. This is C9's
	// recorded "rotation after first apply is a documented Grafana
	// limitation" note, solved: mirrors lakehouse_integration_test.go's own
	// mysql/postgres rotation proof (rotated accepted, old refused).
	t.Setenv("DATASCAPE_SECRET_MON_GRAFANA_ADMIN_PASSWORD", "mon-grafana-secret-pw-rotated")
	out, err, code = run(t, "apply", full, "--state-file", stateFile, "--auto-approve", gateFlag)
	if err != nil || code != 0 {
		t.Fatalf("grafana secret rotation apply failed (code %d): %v\n%s", code, err, out)
	}
	if !monHTTPOK(t, monGrafanaBase+"/api/org", grafUser, "mon-grafana-secret-pw-rotated") {
		t.Fatal("rotated grafana admin password not accepted")
	}
	if monHTTPOK(t, monGrafanaBase+"/api/org", grafUser, grafPass) {
		t.Fatal("old grafana admin password still accepted after rotation")
	}

	// Exit criterion: destroy tears down cleanly.
	out, err, code = run(t, "destroy", full, "--state-file", stateFile, "--auto-approve", gateFlag)
	if err != nil || code != 0 {
		t.Fatalf("destroy failed (code %d): %v\n%s", code, err, out)
	}
	for _, c := range containers {
		if _, found, _ := rt.Inspect(context.Background(), c); found {
			t.Errorf("container %q still present after destroy", c)
		}
	}
}

type monTargetsAPIResponse struct {
	Data struct {
		ActiveTargets []struct {
			ScrapePool string `json:"scrapePool"`
			Health     string `json:"health"`
		} `json:"activeTargets"`
	} `json:"data"`
}

func fetchMonTargets(t *testing.T, baseURL string) map[string]string {
	t.Helper()
	resp, err := http.Get(baseURL + "/api/v1/targets") //nolint:noctx
	if err != nil {
		t.Fatalf("GET /api/v1/targets: %v", err)
	}
	defer resp.Body.Close()
	var tr monTargetsAPIResponse
	if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
		t.Fatalf("decode /api/v1/targets: %v", err)
	}
	out := make(map[string]string, len(tr.Data.ActiveTargets))
	for _, at := range tr.Data.ActiveTargets {
		out[at.ScrapePool] = at.Health
	}
	return out
}

func waitForMonTargetsUp(t *testing.T, baseURL string, jobs []string, deadline time.Duration) {
	t.Helper()
	end := time.Now().Add(deadline)
	for {
		health := fetchMonTargets(t, baseURL)
		allUp := true
		for _, j := range jobs {
			if health[j] != "up" {
				allUp = false
			}
		}
		if allUp {
			return
		}
		if time.Now().After(end) {
			t.Fatalf("targets not all up within %s: %+v", deadline, health)
		}
		time.Sleep(1 * time.Second)
	}
}

func monHTTPOK(t *testing.T, url, user, pass string) bool {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil) //nolint:noctx
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth(user, pass)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

type monDatasourceHealthResponse struct {
	Status string `json:"status"`
}

func monDatasourceHealthy(t *testing.T, baseURL, user, pass string) bool {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, baseURL+"/api/datasources/uid/prometheus/health", nil) //nolint:noctx
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth(user, pass)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	var hr monDatasourceHealthResponse
	if err := json.NewDecoder(resp.Body).Decode(&hr); err != nil {
		t.Fatal(err)
	}
	return hr.Status == "OK"
}
