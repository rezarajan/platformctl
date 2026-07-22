//go:build integration

package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
)

const (
	ingressEdgeHTTPAddr  = "127.0.0.1:19622"
	ingressEdgeAdminAddr = "127.0.0.1:19623"
)

// getThroughEdge dials the shared ingress proxy's published HTTP port with
// the given Host header — the literal mechanism the C7 accept criterion
// describes ("nessie REST reachable via http://nessie.localhost:<port>"):
// *.localhost resolves to loopback (docs/adr/018 Decision 4), so a real
// client would just use the URL directly; the test sets Host explicitly to
// avoid depending on the runner's own resolver configuration.
func getThroughEdge(t *testing.T, host, path string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "http://"+ingressEdgeHTTPAddr+path, nil) //nolint:noctx
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Host = host
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s (Host: %s): %v", path, host, err)
	}
	return resp
}

// mangleRouteOutOfBand simulates an operator editing Caddy's live config
// directly (bypassing platformctl) — the C7 accept criterion's "drift
// detects a mangled route and heals" scenario.
func mangleRouteOutOfBand(t *testing.T, routeID, wrongHost string) {
	t.Helper()
	body := []byte(`{"@id":"` + routeID + `","match":[{"host":["` + wrongHost + `"]}],"handle":[{"handler":"reverse_proxy","upstreams":[{"dial":"nowhere:1"}]}]}`)
	req, err := http.NewRequest(http.MethodPatch, "http://"+ingressEdgeAdminAddr+"/id/"+routeID, bytes.NewReader(body)) //nolint:noctx
	if err != nil {
		t.Fatalf("build mangle request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("mangle route %q: %v", routeID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("mangle route %q: HTTP %d: %s", routeID, resp.StatusCode, string(b))
	}
}

// TestIngressRoutingEndToEnd covers docs/planning/08 C7's accept criterion
// live: nessie reachable via http://nessie.localhost:<port> through the
// Docker shared proxy, two Connections (nessie, minio) routing
// independently, inventory showing the routed URL, drift detecting +
// healing a mangled route, idempotent re-apply, clean destroy.
func TestIngressRoutingEndToEnd(t *testing.T) {
	t.Setenv("DATASCAPE_SECRET_ING_TEST_MINIO_ROOT_USERNAME", "datascape_ing_minio")
	t.Setenv("DATASCAPE_SECRET_ING_TEST_MINIO_ROOT_PASSWORD", "ing-minio-secret-pw")

	rt := requireDocker(t)
	containers := []string{"ing-test-nessie", "ing-test-minio", "ing-test-edge"}
	volumes := []string{"ing-test-minio-data"}
	cleanup := registerDockerCleanup(t, rt, containers, volumes, "datascape-ingress-net")
	cleanup()

	stateFile := filepath.Join(t.TempDir(), "state.json")
	manifests := "testdata/ingress-scenario"
	gate := "--feature-gates=IngressProvider=true"

	out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", gate)
	if err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}

	// NFR-11 acceptance bar (docs/planning/01, I4 doc 08 §7.8): Ready means
	// serving NOW — an immediate drift probe right after apply must report
	// clean. Before I4, a Connection's route could reach Ready as soon as
	// the admin API accepted the route write, without ever having dialed
	// through the route the way Probe does — this would have shown up here
	// as drift immediately after a "clean" apply (docs/planning/11 B1
	// finding 2).
	if report, driftCode := runDrift(t, manifests, stateFile, gate); driftCode != 0 {
		t.Fatalf("drift immediately after apply reports changes (NFR-11 violation): %+v", report)
	}

	// Accept: nessie REST reachable via http://nessie.localhost:<port>
	// through the Docker proxy — the routed Host, not nessie's own
	// container port, which is never published to the host at all.
	resp := getThroughEdge(t, "nessie.localhost", "/api/v2/config")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET /api/v2/config via nessie.localhost: HTTP %d: %s", resp.StatusCode, string(b))
	}

	// Accept: two Connections route independently — minio.localhost reaches
	// the minio container, not nessie's.
	respMinio := getThroughEdge(t, "minio.localhost", "/minio/health/live")
	defer respMinio.Body.Close()
	if respMinio.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(respMinio.Body)
		t.Fatalf("GET /minio/health/live via minio.localhost: HTTP %d: %s", respMinio.StatusCode, string(b))
	}

	// An unrecognized Host must not fall through to either upstream: Caddy's
	// own default for no matching route is HTTP 200 with an empty body (not
	// a 404 — found live), so the signal is body content, not status code.
	respUnknown := getThroughEdge(t, "nobody.localhost", "/api/v2/config")
	unknownBody, _ := io.ReadAll(respUnknown.Body)
	respUnknown.Body.Close()
	if len(unknownBody) > 0 {
		t.Errorf("unrecognized Host nobody.localhost got a non-empty response (routed to something): %q", string(unknownBody))
	}

	// Accept: inventory shows the routed URL.
	invOut, err, code := run(t, "inventory", manifests, "--state-file", stateFile, gate)
	if err != nil || code != 0 {
		t.Fatalf("inventory failed (code %d): %v\n%s", code, err, invOut)
	}
	for _, want := range []string{"nessie.localhost", "minio.localhost"} {
		if !strings.Contains(invOut, want) {
			t.Errorf("inventory missing routed URL containing %q:\n%s", want, invOut)
		}
	}

	// Accept: idempotent re-apply.
	ctrBefore, found, err := rt.Inspect(context.Background(), "ing-test-edge")
	if err != nil || !found {
		t.Fatalf("Inspect edge before re-apply: found=%v err=%v", found, err)
	}
	out, err, code = run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", gate)
	if err != nil || code != 0 {
		t.Fatalf("re-apply failed (code %d): %v\n%s", code, err, out)
	}
	if !strings.Contains(out, "no changes") {
		t.Errorf("re-apply did not report 'no changes':\n%s", out)
	}
	ctrAfter, found, err := rt.Inspect(context.Background(), "ing-test-edge")
	if err != nil || !found {
		t.Fatalf("Inspect edge after re-apply: found=%v err=%v", found, err)
	}
	if ctrAfter.ID != ctrBefore.ID {
		t.Errorf("shared proxy container was recreated on a no-op re-apply (ID %s -> %s) — a per-Connection route change must never restart it (docs/adr/018 Decision 3)", ctrBefore.ID, ctrAfter.ID)
	}

	// Accept: drift detects a mangled route and heals. Mangle nessie's
	// route directly against Caddy's admin API (bypassing platformctl
	// entirely), confirm drift names it, apply heals it, and the route
	// answers correctly again.
	mangleRouteOutOfBand(t, "route-nessie", "mangled.localhost")
	drift, code := runDrift(t, manifests, stateFile, gate)
	if code == 0 {
		t.Fatalf("drift exit code = 0, want nonzero (drift present):\n%+v", drift)
	}
	nessieDrift, ok := drift["Connection/nessie"]
	if !ok {
		t.Fatalf("drift report missing Connection/nessie: %+v", drift)
	}
	if nessieDrift.Drift != "True" {
		t.Errorf("Connection/nessie drift = %+v, want Drift=\"True\"", nessieDrift)
	}
	if !strings.Contains(nessieDrift.Reason, "RouteConfigDrift") {
		t.Errorf("Connection/nessie drift reason = %q, want RouteConfigDrift", nessieDrift.Reason)
	}

	out, err, code = run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", gate)
	if err != nil || code != 0 {
		t.Fatalf("heal apply failed (code %d): %v\n%s", code, err, out)
	}
	healed := getThroughEdge(t, "nessie.localhost", "/api/v2/config")
	defer healed.Body.Close()
	if healed.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(healed.Body)
		t.Fatalf("nessie.localhost still not routed correctly after heal: HTTP %d: %s", healed.StatusCode, string(b))
	}
	stillMangled := getThroughEdge(t, "mangled.localhost", "/api/v2/config")
	mangledBody, _ := io.ReadAll(stillMangled.Body)
	stillMangled.Body.Close()
	if len(mangledBody) > 0 {
		t.Errorf("mangled.localhost still routes to something after heal: %q", string(mangledBody))
	}

	driftAfterHeal, code := runDrift(t, manifests, stateFile, gate)
	if code != 0 {
		t.Errorf("drift after heal: exit code %d, want 0 (clean): %+v", code, driftAfterHeal)
	}

	// Accept: clean destroy.
	out, err, code = run(t, "destroy", manifests, "--state-file", stateFile, "--auto-approve", gate)
	if err != nil || code != 0 {
		t.Fatalf("destroy failed (code %d): %v\n%s", code, err, out)
	}
	for _, c := range containers {
		if _, found, _ := rt.Inspect(context.Background(), c); found {
			t.Errorf("container %q still present after destroy", c)
		}
	}
}

// TestIngressProviderGateGuardsApply proves the standard gate-disabled
// refusal: with IngressProvider left at its Alpha/disabled default, apply
// against a manifest declaring an ingress Provider fails fast naming the
// gate, never half-applies.
func TestIngressProviderGateGuardsApply(t *testing.T) {
	t.Setenv("DATASCAPE_SECRET_ING_TEST_MINIO_ROOT_USERNAME", "datascape_ing_minio")
	t.Setenv("DATASCAPE_SECRET_ING_TEST_MINIO_ROOT_PASSWORD", "ing-minio-secret-pw")
	rt := requireDocker(t)
	registerDockerCleanup(t, rt, []string{"ing-test-nessie", "ing-test-minio", "ing-test-edge"}, []string{"ing-test-minio-data"}, "datascape-ingress-net")()

	stateFile := filepath.Join(t.TempDir(), "state.json")
	out, err, code := run(t, "apply", "testdata/ingress-scenario", "--state-file", stateFile, "--auto-approve")
	if code == 0 {
		t.Fatalf("apply succeeded with IngressProvider gate left disabled (code %d): %v\n%s", code, err, out)
	}
	errText := out
	if err != nil {
		errText += err.Error()
	}
	if !strings.Contains(errText, "IngressProvider") {
		t.Errorf("gate-refusal error does not name IngressProvider:\nstdout: %s\nerr: %v", out, err)
	}
}
