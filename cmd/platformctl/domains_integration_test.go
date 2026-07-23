//go:build integration

package main

import (
	"context"
	"path/filepath"
	"testing"
)

// domainsGates enables the Docker runtime (the "container" placeholder
// provider used throughout this scenario for its minimal footprint is
// registered ungated — docs/planning/08 E7 retired the ContainerProvider
// gate) and the proxy Connection provider realizing the mediated entrypoint
// — PolicyEngine is deliberately NOT enabled here: docs/planning/08 H5's
// accept criterion (b) is Ring 1 segmentation alone, proven with no
// crossDomain policy declared at all (see the Done-note under H5 for the
// full activation-semantics writeup).
const domainsGates = "DockerRuntime=true"

// TestDomainSegmentationEndToEnd is docs/planning/08 H5's accept criterion
// (b): segmentation on the Docker runtime — two domains, no allowed path
// means an in-network dial FAILS across domains (negative proof) while
// same-domain traffic works, and an allowed path via a Connection works
// across (docs/adr/022 Ring 1). testdata/domains-scenario declares domain
// "alpha" (alpha-app, alpha-app2) and domain "beta" (beta-app, no path to
// alpha at all; beta-consumer, which reaches alpha ONLY through the
// "domains-it-bridge" Connection via connectionRef).
func TestDomainSegmentationEndToEnd(t *testing.T) {
	rt := requireDocker(t)
	ctx := context.Background()
	manifests := "testdata/domains-scenario"
	stateFile := filepath.Join(t.TempDir(), "state.json")

	registerDockerCleanup(t,
		rt,
		[]string{"domains-it-alpha-app", "domains-it-alpha-app2", "domains-it-beta-app", "domains-it-beta-consumer", "domains-it-bridge"},
		nil,
		"datascape",
	)
	// Domain-scoped networks (docs/adr/022 Ring 1, docs/planning/08 H5) —
	// naming.NetworkName("datascape", <domain>).
	for _, net := range []string{"datascape-alpha", "datascape-beta"} {
		net := net
		t.Cleanup(func() { _ = rt.RemoveNetwork(context.Background(), net) })
	}

	out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", domainsGates)
	if err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}
	t.Cleanup(func() {
		_, _, _ = run(t, "destroy", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", domainsGates)
	})

	// Same-domain traffic works: two "alpha" resources share
	// datascape-alpha.
	if err := rt.ProbeReachable(ctx, "datascape-alpha", "domains-it-alpha-app2:8080"); err != nil {
		t.Errorf("same-domain dial (alpha -> alpha) should succeed: %v", err)
	}

	// Negative proof: "beta" has no declared path to "alpha" at all
	// (beta-app never references domains-it-bridge) — an in-network dial
	// across the domain boundary must fail, not silently succeed.
	if err := rt.ProbeReachable(ctx, "datascape-beta", "domains-it-alpha-app:8080"); err == nil {
		t.Error("cross-domain dial with no allowed path must FAIL (undeclared cross-domain path must physically fail — docs/adr/022 Ring 1), but it succeeded")
	}
	if err := rt.ProbeReachable(ctx, "datascape-alpha", "domains-it-beta-app:8080"); err == nil {
		t.Error("cross-domain dial with no allowed path must FAIL in the other direction too, but it succeeded")
	}

	// Positive proof: the allowed path — beta-consumer's connectionRef to
	// domains-it-bridge — compiles to the forwarder joining BOTH domains'
	// networks ("exactly the holes the mediated entrypoint needs"), so the
	// bridge is reachable from both, even though beta-app right next to it
	// (same domain, no connectionRef) has no path to alpha at all.
	if err := rt.ProbeReachable(ctx, "datascape-beta", "domains-it-bridge:25990"); err != nil {
		t.Errorf("the mediated entrypoint must be reachable from the consumer's own domain (beta): %v", err)
	}
	if err := rt.ProbeReachable(ctx, "datascape-alpha", "domains-it-bridge:25990"); err != nil {
		t.Errorf("the mediated entrypoint must remain reachable from its home domain (alpha): %v", err)
	}
}
