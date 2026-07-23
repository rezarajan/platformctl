package trino

import (
	"strings"
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/provider"
)

// Conformance scoping decision (docs/planning/08 E6 done-note's recorded
// follow-up; ADR 028's fake-honesty rule, docs/contributing/
// provider-authoring.md §6): trino's ONLY Kind ("Provider") cannot reach
// Ready against the fake runtime. reconcileInstance's waitCoordinatorReady
// dials the coordinator's real /v1/info endpoint before Reconcile ever
// returns, and the Provider realizes TWO containers (coordinator +
// StableIdentity worker set) coordinated through Trino's own real
// discovery protocol — no dialer seam to intercept short of faking Trino's
// HTTP API surface, which ADR 028 §2's fake-honesty rule would require
// pinning against a real Trino coordinator's observed behavior before
// trusting a green result — out of this retrofit's scope. Covered instead
// by the Docker integration suite (cmd/platformctl's lakehouse scenarios).
//
// conformance.Run is therefore never called here — it would require
// Reconcile to reach Ready, which is unreachable without a real dial, and
// this task's own brief is explicit: never a fabricated pass.
//
// What IS fast-tier-provable without touching Reconcile/Probe/Destroy at
// all: ValidateSpec, a pure function with no runtime dependency — exercised
// directly below with the same exact-WantSubstrings discipline
// conformance.CapabilityCheck applies in every other exemplar in this
// retrofit.
func TestValidateSpecCapabilityChecks(t *testing.T) {
	t.Parallel()
	p := New()

	t.Run("workers-below-one", func(t *testing.T) {
		t.Parallel()
		err := p.ValidateSpec(provider.Provider{
			Type:          "trino",
			Configuration: map[string]any{"workers": float64(0)},
		})
		requireErrorContains(t, err, "spec.configuration.workers must be >= 1, got 0")
	})

	t.Run("image-wrong-type", func(t *testing.T) {
		t.Parallel()
		err := p.ValidateSpec(provider.Provider{
			Type:          "trino",
			Configuration: map[string]any{"image": 7},
		})
		requireErrorContains(t, err, "spec.configuration.image must be a non-empty string")
	})
}

// requireErrorContains is this file's own tiny stand-in for
// conformance.CapabilityCheck's assertion shape (Name/Invoke/
// WantSubstrings) — see grafana/conformance_test.go's identical helper for
// why this stays local per-package rather than a shared test helper.
func requireErrorContains(t *testing.T, err error, wantSubstrings ...string) {
	t.Helper()
	if err == nil {
		t.Fatal("want an error, got nil")
	}
	for _, want := range wantSubstrings {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %q, want it to contain %q", err.Error(), want)
		}
	}
}
