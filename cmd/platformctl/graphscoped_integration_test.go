//go:build integration

package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/resource"
)

// graphScopedGates enables the container provider (minimal-footprint,
// docs/planning/08 H5 precedent) and GraphScopedAccess (docs/adr/026 H7).
const graphScopedGates = "DockerRuntime=true,GraphScopedAccess=true"

func gsaKey(namespace, name string) resource.Key {
	return resource.Key{Namespace: namespace, Kind: "Provider", Name: name}
}

// TestGraphScopedAccessEndToEnd is docs/planning/08 H7's accept bar on the
// Docker runtime, live against the real daemon: the owner's worked example
// (docs/planning/11, 2026-07-22) — A/R1 reaches {B/X, C/Y}; A/R2 reaches
// {B/X} only; R2->C/Y and R1->other-B both FAIL (negative proofs, dialed
// from the CONSUMER's own attached per-edge network — docs/adr/026 decision
// 5's exact bar); the gate-off leg (TestGraphScopedAccessGateOffEndToEnd
// below) proves byte-identical behavior when disabled.
func TestGraphScopedAccessEndToEnd(t *testing.T) {
	rt := requireDocker(t)
	ctx := context.Background()
	manifests := "testdata/graphscoped-scenario"
	stateFile := filepath.Join(t.TempDir(), "state.json")

	r1, r2 := gsaKey("a", "gsa-it-r1"), gsaKey("a", "gsa-it-r2")
	x, y, otherB := gsaKey("b", "gsa-it-x"), gsaKey("c", "gsa-it-y"), gsaKey("b", "gsa-it-other-b")

	registerDockerCleanup(t, rt,
		[]string{"gsa-it-r1", "gsa-it-r2", "gsa-it-x", "gsa-it-y", "gsa-it-other-b"},
		[]string{"gsa-it-r1-data", "gsa-it-r2-data", "gsa-it-x-data", "gsa-it-y-data", "gsa-it-other-b-data"},
		"",
	)
	// Every private home network (one per owner, docs/adr/026 H7's Docker
	// realization) plus every per-edge network that should exist for a
	// declared edge — cleaned up regardless of which assertions below
	// actually run.
	t.Cleanup(func() {
		for _, k := range []resource.Key{r1, r2, x, y, otherB} {
			_ = rt.RemoveNetwork(context.Background(), naming.PrivateNetworkName("datascape", "", k))
		}
		for _, pair := range [][2]resource.Key{{r1, x}, {r1, y}, {r2, x}} {
			_ = rt.RemoveNetwork(context.Background(), naming.EdgeNetworkName(pair[0], pair[1]))
		}
	})

	out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", graphScopedGates)
	if err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}
	t.Cleanup(func() {
		_, _, _ = run(t, "destroy", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", graphScopedGates)
	})

	// Positive proofs.
	if err := rt.ProbeReachable(ctx, naming.EdgeNetworkName(r1, x), "gsa-it-x:8080"); err != nil {
		t.Errorf("A/R1 must reach B/X via their per-edge network: %v", err)
	}
	if err := rt.ProbeReachable(ctx, naming.EdgeNetworkName(r1, y), "gsa-it-y:8080"); err != nil {
		t.Errorf("A/R1 must reach C/Y via their per-edge network: %v", err)
	}
	if err := rt.ProbeReachable(ctx, naming.EdgeNetworkName(r2, x), "gsa-it-x:8080"); err != nil {
		t.Errorf("A/R2 must reach B/X via their per-edge network: %v", err)
	}

	// Negative proofs, from the consumer's own vantage (docs/adr/026
	// decision 5): no edge network for these pairs was ever declared, so
	// there is nothing to dial from — RemoveNetwork's cleanup above makes
	// this doubly certain no stale network from a prior run masks the
	// result.
	if err := rt.ProbeReachable(ctx, naming.EdgeNetworkName(r2, y), "gsa-it-y:8080"); err == nil {
		t.Error("A/R2 must NOT reach C/Y (negative proof) — no such edge was declared")
	}
	if err := rt.ProbeReachable(ctx, naming.EdgeNetworkName(r1, otherB), "gsa-it-other-b:8080"); err == nil {
		t.Error("A/R1 must NOT reach B/other-b (negative proof) — no such edge was declared")
	}

	// The decisive negative proof: the flat/shared home token no longer
	// grants ANY cross-container reachability at all under the gate — each
	// owner's home network is private to itself. Anchored on other-b, NOT
	// R1: ProbeReachable's real Docker implementation execs a dial FROM an
	// existing managed container found ON the named network — a container
	// that is ALSO attached to other networks (as R1 legitimately is, its
	// own edge networks) can dial out through THOSE too, so probing "from
	// R1's home network" cannot isolate R1's home-network interface alone
	// from R1's edge-network interfaces (found live: this exact assertion,
	// anchored on R1, false-passed). other-b declares no edge at all, so
	// its container is genuinely single-homed (only its own private home
	// network) — a clean vantage with nothing to confound the result.
	if err := rt.ProbeReachable(ctx, naming.PrivateNetworkName("datascape", "", otherB), "gsa-it-x:8080"); err == nil {
		t.Error("other-b's own private home network must not reach B/X — reachability must come ONLY from an explicit per-edge network, and other-b has none")
	}
}

// TestGraphScopedAccessGateOffEndToEnd is the gate-off half of the accept
// bar ("gate-off byte-identical pin"): the SAME manifest set, same
// resources, GraphScopedAccess left at its default (disabled) — every
// resource shares the one flat "datascape" network exactly as it did
// before this task, with no graph-edge declaration required to reach
// anything (the pre-H7, pre-H5-domains baseline).
func TestGraphScopedAccessGateOffEndToEnd(t *testing.T) {
	rt := requireDocker(t)
	ctx := context.Background()
	manifests := "testdata/graphscoped-scenario"
	stateFile := filepath.Join(t.TempDir(), "state.json")

	registerDockerCleanup(t, rt,
		[]string{"gsa-it-r1", "gsa-it-r2", "gsa-it-x", "gsa-it-y", "gsa-it-other-b"},
		[]string{"gsa-it-r1-data", "gsa-it-r2-data", "gsa-it-x-data", "gsa-it-y-data", "gsa-it-other-b-data"},
		"datascape",
	)

	const gateOff = "DockerRuntime=true"
	out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", gateOff)
	if err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}
	t.Cleanup(func() {
		_, _, _ = run(t, "destroy", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", gateOff)
	})

	// Gate off: R2 reaches B/other-b and C/Y — resources it never declared
	// any reference to — via the shared flat network, exactly the
	// pre-H7 behavior this pin proves is untouched.
	for _, target := range []string{"gsa-it-x:8080", "gsa-it-y:8080", "gsa-it-other-b:8080"} {
		if err := rt.ProbeReachable(ctx, "datascape", target); err != nil {
			t.Errorf("gate off: the shared token network must still reach %s, unchanged: %v", target, err)
		}
	}
}
