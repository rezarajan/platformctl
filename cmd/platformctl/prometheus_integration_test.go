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

const promAPIBase = "http://127.0.0.1:19398"

// TestPrometheusMonitoringStackEndToEnd covers docs/planning/08 C9's
// integration accept criterion: a managed Prometheus, applied alongside a
// redpanda broker (+ EventStream) and a minio store, scrapes both via their
// published "metrics" endpoint facts — never a constructed address — to
// up == 1 within a deadline; idempotent re-apply; clean destroy.
//
// Two apply calls against the same state file, not one: infra/manifests.yaml
// (redpanda+EventStream+minio) first, so their "metrics" endpoint facts are
// already published in state by the time combined/manifests.yaml's
// prometheus Provider reconciles — deliberately sidesteps relying on
// same-level topological ordering between three Providers with no
// dependency edges between them (see the prometheus package's engine
// wiring, internal/application/engine's resolveMetricsTargets).
func TestPrometheusMonitoringStackEndToEnd(t *testing.T) {
	t.Setenv("DATASCAPE_SECRET_PROM_MINIO_ROOT_USERNAME", "datascape_prom_minio")
	t.Setenv("DATASCAPE_SECRET_PROM_MINIO_ROOT_PASSWORD", "prom-minio-secret-pw")

	rt := requireDocker(t)
	containers := []string{"prom-test-redpanda", "prom-test-minio", "prom-test-prometheus"}
	volumes := []string{"prom-test-redpanda-data", "prom-test-minio-data"}
	cleanup := registerDockerCleanup(t, rt, containers, volumes, "datascape-prom-net")
	cleanup()

	stateFile := filepath.Join(t.TempDir(), "state.json")
	infra := "testdata/prometheus-scenario/infra"
	combined := "testdata/prometheus-scenario/combined"

	// Infra tier first: redpanda + EventStream + minio, no gate needed.
	out, err, code := run(t, "apply", infra, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("infra apply failed (code %d): %v\n%s", code, err, out)
	}

	// Combined tier: adds the prometheus Provider (gate MonitoringStackProvider).
	out, err, code = run(t, "apply", combined, "--state-file", stateFile, "--auto-approve", "--feature-gates=MonitoringStackProvider=true")
	if err != nil || code != 0 {
		t.Fatalf("combined apply failed (code %d): %v\n%s", code, err, out)
	}

	// Exit criterion: activeTargets already carries both jobs the moment
	// apply returns — Reconcile's own convergence wait (waitReady) blocks
	// apply until /api/v1/targets' activeTargets count matches the
	// configured target count, not just /-/ready 200ing (Prometheus's
	// target-discovery sync lags /-/ready by a few seconds at startup even
	// for a purely static config — found live, not by reasoning).
	targets := fetchPromTargets(t, promAPIBase)
	if len(targets) != 2 {
		t.Fatalf("activeTargets = %+v, want exactly 2 (prom-test-redpanda, prom-test-minio)", targets)
	}
	if _, ok := targets["prom-test-redpanda"]; !ok {
		t.Errorf("activeTargets missing job %q: %+v", "prom-test-redpanda", targets)
	}
	if _, ok := targets["prom-test-minio"]; !ok {
		t.Errorf("activeTargets missing job %q: %+v", "prom-test-minio", targets)
	}

	// Exit criterion: up == 1 for both within a deadline — Prometheus's own
	// concern once scrape_interval (2s in the scenario manifest) elapses;
	// never part of the platform's own Ready gate (see the prometheus
	// package's Probe doc comment), but this is exactly the fact the C9
	// accept criterion wants verified live.
	waitForTargetsUp(t, promAPIBase, []string{"prom-test-redpanda", "prom-test-minio"}, 30*time.Second)

	ctrBefore, found, err := rt.Inspect(context.Background(), "prom-test-prometheus")
	if err != nil || !found {
		t.Fatalf("Inspect before re-apply: found=%v err=%v", found, err)
	}

	// Exit criterion: idempotent re-apply.
	out, err, code = run(t, "apply", combined, "--state-file", stateFile, "--auto-approve", "--feature-gates=MonitoringStackProvider=true")
	if err != nil || code != 0 {
		t.Fatalf("re-apply failed (code %d): %v\n%s", code, err, out)
	}
	if !strings.Contains(out, "no changes") {
		t.Errorf("re-apply did not report 'no changes':\n%s", out)
	}
	ctrAfter, found, err := rt.Inspect(context.Background(), "prom-test-prometheus")
	if err != nil || !found {
		t.Fatalf("Inspect after re-apply: found=%v err=%v", found, err)
	}
	if ctrAfter.ID != ctrBefore.ID {
		t.Errorf("prometheus container was recreated on a no-op re-apply (ID %s -> %s)", ctrBefore.ID, ctrAfter.ID)
	}

	// Exit criterion: destroy tears down cleanly.
	out, err, code = run(t, "destroy", combined, "--state-file", stateFile, "--auto-approve", "--feature-gates=MonitoringStackProvider=true")
	if err != nil || code != 0 {
		t.Fatalf("destroy failed (code %d): %v\n%s", code, err, out)
	}
	for _, c := range containers {
		if _, found, _ := rt.Inspect(context.Background(), c); found {
			t.Errorf("container %q still present after destroy", c)
		}
	}
}

// promTargetsAPIResponse is the subset of Prometheus's /api/v1/targets
// response this test reads.
type promTargetsAPIResponse struct {
	Data struct {
		ActiveTargets []struct {
			ScrapePool string `json:"scrapePool"`
			Health     string `json:"health"`
		} `json:"activeTargets"`
	} `json:"data"`
}

// fetchPromTargets returns the scrape pool (job) -> health map of every
// entry in Prometheus's /api/v1/targets activeTargets list.
func fetchPromTargets(t *testing.T, baseURL string) map[string]string {
	t.Helper()
	resp, err := http.Get(baseURL + "/api/v1/targets") //nolint:noctx
	if err != nil {
		t.Fatalf("GET /api/v1/targets: %v", err)
	}
	defer resp.Body.Close()
	var tr promTargetsAPIResponse
	if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
		t.Fatalf("decode /api/v1/targets: %v", err)
	}
	out := make(map[string]string, len(tr.Data.ActiveTargets))
	for _, at := range tr.Data.ActiveTargets {
		out[at.ScrapePool] = at.Health
	}
	return out
}

// waitForTargetsUp polls /api/v1/targets until every named job reports
// health "up", or fails the test once deadline elapses.
func waitForTargetsUp(t *testing.T, baseURL string, jobs []string, deadline time.Duration) {
	t.Helper()
	end := time.Now().Add(deadline)
	for {
		health := fetchPromTargets(t, baseURL)
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
