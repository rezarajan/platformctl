//go:build integration

package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	k8sruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/kubernetes"
)

// TestGraphScopedAccessOnKubernetesEndToEnd is docs/planning/08 H7's accept
// bar on the Kubernetes runtime: the same edge shape as the Docker leg
// (TestGraphScopedAccessEndToEnd) — R1 -> {X, Y}, R2 -> {X}, other-b
// unreferenced — realized as per-container NetworkPolicy
// (buildGraphScopedIngressPolicy) rather than per-edge Docker networks. See
// testdata/graphscoped-k8s-scenario/manifests.yaml's own doc comment for
// why R1/R2 each get a dedicated domain/namespace (an unambiguous live
// vantage point — Kubernetes' ProbeReachable has no per-pod-identity
// selector, only per-namespace) while X and other-b deliberately SHARE one
// namespace — the single most important proof here: two Kubernetes
// NAMESPACE-MATES, differentiated ONLY by H7's per-container policy (H5's
// domain walls alone cannot make this distinction; they operate one level
// coarser).
//
// Enforcement honesty (docs/adr/027's claims table, productized by H8):
// this repo's local/shared minikube CNI does not enforce NetworkPolicy at
// all (the SAME caveat domains_kubernetes_integration_test.go's own
// TestDomainSegmentationOnKubernetesEndToEnd documents), so the negative
// proof (the ONLY assertion enforcement actually changes the outcome of —
// an allowed pair is reachable either way) is structured exactly like
// TestNetworkPolicyEnforcementIsLive: PLATFORMCTL_REQUIRE_NETPOL_ENFORCEMENT
// set (CI, Calico-backed cluster) makes non-enforcement a hard FAILURE;
// unset (local convenience clusters) skips with the exact reason and
// reproduction command, never silently passing a check that proved
// nothing.
func TestGraphScopedAccessOnKubernetesEndToEnd(t *testing.T) {
	requireK8s(t)
	rt, err := k8sruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to kubernetes: %v", err)
	}
	requireEnforce := os.Getenv("PLATFORMCTL_REQUIRE_NETPOL_ENFORCEMENT") != ""

	ctx := context.Background()
	const (
		r1NS = "datascape-r1dom"
		r2NS = "datascape-r2dom"
		bNS  = "datascape-b"
		cNS  = "datascape-c"
	)
	cleanup := func() {
		for _, ns := range []string{r1NS, r2NS, bNS, cNS} {
			_ = rt.RemoveNetwork(context.Background(), ns)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	stateFile := filepath.Join(t.TempDir(), "state.json")
	manifests := "testdata/graphscoped-k8s-scenario"
	const gateVal = "KubernetesRuntime=true,GraphScopedAccess=true"

	out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", gateVal)
	if err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}
	t.Cleanup(func() {
		_, _, _ = run(t, "destroy", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", gateVal)
	})

	// Positive proofs: allowed pairs are reachable REGARDLESS of whether
	// this cluster's CNI enforces NetworkPolicy (the underlying pod network
	// permits it either way; only a DENY needs real enforcement to prove).
	if err := probeReachableEventually(t, rt, r1NS, "gsa-it-x."+bNS+":8080", 30*time.Second); err != nil {
		t.Errorf("R1 must reach X: %v", err)
	}
	if err := probeReachableEventually(t, rt, r1NS, "gsa-it-y."+cNS+":8080", 30*time.Second); err != nil {
		t.Errorf("R1 must reach Y: %v", err)
	}
	if err := probeReachableEventually(t, rt, r2NS, "gsa-it-x."+bNS+":8080", 30*time.Second); err != nil {
		t.Errorf("R2 must reach X: %v", err)
	}

	// Negative proofs, from each consumer's own (unambiguous, single-pod)
	// namespace vantage — the assertions that genuinely require an
	// enforcing CNI (see the function doc comment).
	type negative struct {
		fromNS, label, target string
	}
	for _, n := range []negative{
		{r2NS, "R2->Y (undeclared edge)", "gsa-it-y." + cNS + ":8080"},
		{r1NS, "R1->other-b (namespace-mate of X, but never referenced)", "gsa-it-other-b." + bNS + ":8080"},
	} {
		negErr := rt.ProbeReachable(ctx, n.fromNS, n.target)
		if negErr == nil {
			msg := n.label + " was reachable — this cluster's CNI does not appear to enforce NetworkPolicy; H7's per-container policy compilation is unit-tested directly (internal/adapters/runtime/kubernetes: TestBuildGraphScopedIngressPolicy, TestEnsureNetworkGraphScopedOmitsAllowSameNamespace) and the Docker leg (TestGraphScopedAccessEndToEnd) proves the SAME graph compilation with real enforcement (Docker network membership is enforced by construction); Kubernetes enforcement itself requires a policy-enforcing CNI (kind+Calico or equivalent), a separate environment decision docs/planning/08 B7 already documents as not always available"
			if requireEnforce {
				t.Fatal(msg + " (required by PLATFORMCTL_REQUIRE_NETPOL_ENFORCEMENT: use a policy-enforcing CNI, e.g. kind with Calico or `minikube start --cni=calico`)")
			}
			t.Skip(msg + "; skipping (set up Calico/Cilium to prove enforcement locally)")
		}
	}
}
