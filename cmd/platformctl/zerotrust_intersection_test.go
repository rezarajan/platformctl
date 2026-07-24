package main

import (
	"strings"
	"testing"
)

// TestZeroTrustRefusesWideGrants pins M6 (docs/adr/035 decision 3): under
// zero-trust the declared graph is the complete allow-set, so a spec.access
// namespace grant — the one mechanism that widens reachability beyond
// declared edges — is refused; the developer declares a Connection/Binding
// instead. --no-zero-trust (legacy) keeps wide grants working.
func TestZeroTrustRefusesWideGrants(t *testing.T) {
	t.Parallel()
	dir := "testdata/zerotrust-wide-grant"

	_, _, err := runSplit(t, "validate", dir)
	if err == nil {
		t.Fatal("a spec.access wide grant under zero-trust should be refused (it widens beyond the declared graph)")
	}
	if !strings.Contains(err.Error(), "widen reachability beyond the declared graph") {
		t.Errorf("expected the zero-trust intersection refusal, got: %v", err)
	}

	// Legacy: --no-zero-trust keeps wide grants working.
	out, _, err := runSplit(t, "validate", dir, "--no-zero-trust")
	if err != nil {
		t.Fatalf("--no-zero-trust should allow wide grants (legacy), got: %v\n%s", err, out)
	}
}
