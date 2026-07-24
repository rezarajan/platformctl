package main

import (
	"strings"
	"testing"
)

// TestZeroTrustDefaultOnForProject pins M4 (docs/adr/035 decision 3): a
// project (datascape.yaml present) is zero-trust ON by default — the four
// subsumed gates (here MediatedConnections, guarding the openziti mesh
// provider) are active with NO feature-gate flags — and --no-zero-trust
// turns it all off. The developer never reasons about the individual
// mechanisms.
func TestZeroTrustDefaultOnForProject(t *testing.T) {
	t.Parallel()
	dir := "testdata/zerotrust-project"

	// Project present, no flags: ZeroTrust default-on -> openziti available.
	out, _, err := runSplit(t, "validate", dir)
	if err != nil {
		t.Fatalf("validate a zero-trust project should pass with no flags (ZeroTrust default-on), got: %v\n%s", err, out)
	}
	if !strings.Contains(out, "valid") {
		t.Errorf("expected valid, got:\n%s", out)
	}

	// --no-zero-trust: the subsumed MediatedConnections gate is off -> refused.
	out, _, err = runSplit(t, "validate", dir, "--no-zero-trust")
	if err == nil {
		t.Fatalf("validate --no-zero-trust should refuse the openziti mesh (MediatedConnections off), but passed:\n%s", out)
	}
	if !strings.Contains(err.Error(), "MediatedConnections") {
		t.Errorf("expected the refusal to name the subsumed MediatedConnections gate, got: %v", err)
	}
}
