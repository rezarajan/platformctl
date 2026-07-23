//go:build integration

package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	k8sruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/kubernetes"
	"github.com/rezarajan/platformctl/internal/cliutil"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
	"github.com/rezarajan/platformctl/internal/testkit"
)

// TestOpenZitiCrossDomainPolicyOnKubernetesEndToEnd is docs/planning/08 H9's
// Kubernetes leg, mirroring TestCrossDomainMediatedPolicyEndToEnd's Docker
// scenario exactly (testdata/crossdomain-mediated-k8s-scenario/
// manifests.yaml — same topology, runtime: kubernetes) — the
// substrate-parity bar H6/H9 both hold: identical governance/mediation
// guarantees on Docker AND Kubernetes (docs/adr/027's central claim).
//
// The name matches the CI scenarios-apps shard pattern
// (`TestOpenZiti.*Kubernetes`, .github/workflows/ci.yml,
// internal/archtest/ci_shard_partition_test.go's guard) by construction.
//
// This leg is also where docs/planning/08's H6 Kubernetes addendum
// recorded gap gets its first live exercise: xd-mesh (domain "analytics")
// and xd-pg (domain "payments") — the real, mediated-dark backend — live
// in DIFFERENT domain-scoped namespaces here (unlike every prior openziti
// Kubernetes scenario, which was single-namespace). Without this task's
// runtime.AddressQualifier fix (internal/application/engine/
// domainruntime.go), the router's terminator would target a bare
// "xd-pg:5432" that only resolves inside xd-pg's own namespace — see
// testdata/crossdomain-mediated-k8s-scenario/manifests.yaml's header
// comment for the full mechanism.
func TestOpenZitiCrossDomainPolicyOnKubernetesEndToEnd(t *testing.T) {
	requireK8s(t)
	rt, err := k8sruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to kubernetes: %v", err)
	}
	ctx := context.Background()

	t.Setenv("DATASCAPE_SECRET_XD_PG_SUPER_USERNAME", "xd_super")
	t.Setenv("DATASCAPE_SECRET_XD_PG_SUPER_PASSWORD", "xd-super-pw")
	t.Setenv("DATASCAPE_SECRET_XD_MESH_ADMIN_USERNAME", "admin")
	t.Setenv("DATASCAPE_SECRET_XD_MESH_ADMIN_PASSWORD", "xd-mesh-admin-pw")

	const manifestsWith = "testdata/crossdomain-mediated-k8s-scenario"
	const policies = "testdata/crossdomain-mediated-k8s-scenario/policies"
	const gates = "PolicyEngine=true,MediatedConnections=true,KubernetesRuntime=true"
	stateFile := t.TempDir() + "/state.json"

	// docs/adr/029 (J2 sweep): destroy is this scenario's workhorse
	// cleanup (removes every state-managed workload across BOTH
	// domain-scoped namespaces); the janitor owns what destroy cannot —
	// the namespaces themselves, loud, destroy-then-janitor so the
	// namespace removal (which refuses while occupied) runs after
	// destroy has emptied both.
	jan := testkit.Janitor{RT: rt, Networks: []string{"datascape-payments", "datascape-analytics"}}
	destroy := func() {
		_, _, _ = run(t, "destroy", manifestsWith, "--state-file", stateFile, "--auto-approve", "--policies", policies, "--feature-gates", gates)
	}
	jan.CleanSilent(ctx)
	destroy()
	jan.Register(ctx, t)
	t.Cleanup(destroy)

	manifestBytes, rerr := readFileT(t, manifestsWith+"/manifests.yaml")
	if rerr != nil {
		t.Fatal(rerr)
	}
	withoutExemption := stripCrossDomainExemptions(manifestBytes)
	withoutMediation := removeYAMLDocsContaining(manifestBytes, "kind: Connection", "kind: Source", "kind: Binding")

	// --- Leg 1: validate WITHOUT the exemption refuses -------------------
	noExemptDir := writeManifest(t, withoutExemption)
	out, verr, code := run(t, "validate", noExemptDir, "--policies", policies, "--feature-gates", gates)
	if code != cliutil.ExitValidation {
		t.Fatalf("leg1: validate exit code = %d, want %d (ExitValidation); err=%v\n%s", code, cliutil.ExitValidation, verr, out)
	}
	if verr == nil {
		t.Fatal("leg1: expected a denial error")
	}
	for _, want := range []string{"deny-payments-to-analytics", "payments", "analytics", "xd-src", "xd-cdc"} {
		if !strings.Contains(verr.Error(), want) {
			t.Errorf("leg1: validate error %q missing %q (rule id, both domains, both denied edges)", verr.Error(), want)
		}
	}

	// --- Leg 2: WITH the exemption, apply reaches Ready; the CDC
	// connector runs RUNNING through the mediated path. -------------------
	start := time.Now()
	out, err, code = run(t, "apply", manifestsWith, "--state-file", stateFile, "--auto-approve", "--policies", policies, "--feature-gates", gates)
	if err != nil || code != 0 {
		t.Fatalf("leg2: apply failed (code %d): %v\n%s", code, err, out)
	}
	t.Logf("leg2: k8s apply took %s", time.Since(start).Round(time.Second))

	out, err, code = run(t, "status", manifestsWith, "--state-file", stateFile, "--policies", policies, "--feature-gates", gates)
	if err != nil || code != 0 {
		t.Fatalf("leg2: status failed (code %d): %v\n%s", code, err, out)
	}
	assertAllStatusReady(t, out, "leg2 apply")

	if state := xdK8sConnectorState(t, rt, ctx, "xd-cdc"); state != "RUNNING" {
		t.Errorf("leg2: connector state = %q, want RUNNING", state)
	}

	// --- Leg 3: POSITIVE mediator evidence — the Ziti management API's
	// own services/service-policies/identities are EXACTLY the expected
	// set for this one edge. -----------------------------------------------
	withXdMeshCtrlReachable(t, rt, ctx, func(client *http.Client, addr, token string) {
		assertMediatorStateExactly(t, client, addr, token)
	})

	// --- Leg 4: remove the exemption, re-apply is REFUSED fail-closed,
	// naming the edge; validate/plan report the denial while the path
	// (leg 2's infra) keeps standing. ---------------------------------------
	noExemptDir2 := writeManifest(t, withoutExemption)
	out, verr, code = run(t, "apply", noExemptDir2, "--state-file", stateFile, "--auto-approve", "--policies", policies, "--feature-gates", gates)
	if code != cliutil.ExitValidation {
		t.Fatalf("leg4: re-apply exit code = %d, want %d (ExitValidation); err=%v\n%s", code, cliutil.ExitValidation, verr, out)
	}
	for _, want := range []string{"deny-payments-to-analytics", "payments", "analytics"} {
		if verr == nil || !strings.Contains(verr.Error(), want) {
			t.Errorf("leg4: re-apply error missing %q: %v", want, verr)
		}
	}
	out, verr, code = run(t, "plan", noExemptDir2, "--state-file", stateFile, "--policies", policies, "--feature-gates", gates)
	if code != cliutil.ExitValidation {
		t.Fatalf("leg4: plan exit code = %d, want %d (ExitValidation); err=%v\n%s", code, cliutil.ExitValidation, verr, out)
	}

	out, err, code = run(t, "status", manifestsWith, "--state-file", stateFile, "--policies", policies, "--feature-gates", gates)
	if err != nil || code != 0 {
		t.Fatalf("leg4: status (unaffected by policy, per ADR 021 wiring) failed (code %d): %v\n%s", code, err, out)
	}
	assertAllStatusReady(t, out, "leg4: path stands after the withdrawn exemption")
	if state := xdK8sConnectorState(t, rt, ctx, "xd-cdc"); state != "RUNNING" {
		t.Errorf("leg4: connector state = %q after withdrawal, want RUNNING (severing refuses NEW authorization, it does not tear down the standing path)", state)
	}

	// --- Leg 5: remove the Binding+Connection+Source, apply — manifest-
	// driven teardown: the Ziti service/policies/identities for the edge
	// are GONE. The External Source's removal needs the NFR-3 double flags
	// (see the Docker leg's comment — the amendment's own "approved like
	// any other destructive change"). -----------------------------------------
	teardownDir := writeManifest(t, withoutMediation)
	out, err, code = run(t, "apply", teardownDir, "--state-file", stateFile, "--auto-approve", "--policies", policies, "--feature-gates", gates,
		"--include-external", "--yes-i-understand-this-is-destructive")
	if err != nil || code != 0 {
		t.Fatalf("leg5: apply (teardown) failed (code %d): %v\n%s", code, err, out)
	}
	withXdMeshCtrlReachable(t, rt, ctx, func(client *http.Client, addr, token string) {
		assertMediatorStateEmpty(t, client, addr, token)
	})
}

// xdK8sConnectorState reaches xd-dbz's Debezium Connect REST API through an
// ephemeral port-forward (runtime.EnsureReachable — the substrate-neutral
// seam) and returns the named connector's state. Mirrors
// zitiK8sConnectorState (openziti_kubernetes_integration_test.go),
// parameterized by this scenario's own Debezium Provider name (xd-dbz, not
// H6's zk8s-dbz).
func xdK8sConnectorState(t *testing.T, rt *k8sruntime.Runtime, ctx context.Context, connector string) string {
	t.Helper()
	var state string
	err := runtime.WithReachable(ctx, rt, "xd-dbz", 8083, runtime.ReachableOptions{Timeout: 60 * time.Second, Interval: 3 * time.Second}, func(ctx context.Context, addr string) error {
		resp, err := http.Get("http://" + addr + "/connectors/" + connector + "/status")
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		var body struct {
			Connector struct {
				State string `json:"state"`
			} `json:"connector"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			return err
		}
		if body.Connector.State == "" {
			return fmt.Errorf("connector %q not present yet", connector)
		}
		state = body.Connector.State
		return nil
	})
	if err != nil {
		t.Fatalf("reach Debezium Connect REST for connector %q: %v", connector, err)
	}
	return state
}

// withXdMeshCtrlReachable opens a bounded, reachable tunnel to xd-mesh's
// controller (a published host port on Docker; an ephemeral port-forward
// on Kubernetes — runtime.WithReachable is the substrate-neutral seam both
// legs' own reconcile paths already use), authenticates, and invokes fn
// with a client bound to the tunnel's lifetime — the shape
// assertMediatorStateExactly/assertMediatorStateEmpty need, since a K8s
// port-forward is only valid for the duration of the callback that opened
// it.
func withXdMeshCtrlReachable(t *testing.T, rt *k8sruntime.Runtime, ctx context.Context, fn func(client *http.Client, addr, token string)) {
	t.Helper()
	client := &http.Client{Timeout: 15 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}} //nolint:gosec // test-only, mirrors the adapter's own documented trust posture
	err := runtime.WithReachable(ctx, rt, "xd-mesh-ctrl", 12895, runtime.ReachableOptions{Timeout: 60 * time.Second, Interval: 3 * time.Second}, func(ctx context.Context, addr string) error {
		token := zitiAuthenticate(t, client, addr, "admin", "xd-mesh-admin-pw")
		fn(client, addr, token)
		return nil
	})
	if err != nil {
		t.Fatalf("reach xd-mesh-ctrl: %v", err)
	}
}
