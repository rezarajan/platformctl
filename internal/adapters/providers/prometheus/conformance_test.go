package prometheus

import (
	"strings"
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/provider"
)

// Conformance scoping decision (docs/planning/08 E6 done-note's recorded
// follow-up; ADR 028's fake-honesty rule, docs/contributing/
// provider-authoring.md §6): prometheus's ONLY Kind ("Provider") cannot
// reach Ready against the fake runtime. reconcileInstance's waitReady polls
// two real HTTP endpoints (/-/ready, then /api/v1/targets — the latter
// JSON-decoded and compared by count against the generated scrape config,
// per this package's own doc comment on why /-/ready alone is insufficient)
// before Reconcile ever returns — this package's own
// TestReconcileInstanceGeneratesConfigAndPublishesPort already documents
// the identical constraint verbatim ("independent of readiness, which the
// fake runtime cannot serve real HTTP for"). Building a fake that answers
// /api/v1/targets with a JSON body matching Prometheus's own schema would
// be exactly the "fake HTTP admin API" ADR 028 §2 requires pinning against
// a real Prometheus's observed behavior for — out of this retrofit's scope.
// Covered instead by the Docker integration suite (cmd/platformctl's
// monitoring-stack scenarios).
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

	t.Run("scrapeInterval-wrong-type", func(t *testing.T) {
		t.Parallel()
		err := p.ValidateSpec(provider.Provider{
			Type:          "prometheus",
			Configuration: map[string]any{"scrapeInterval": 15},
		})
		requireErrorContains(t, err, "spec.configuration.scrapeInterval must be a non-empty duration string")
	})

	t.Run("image-wrong-type", func(t *testing.T) {
		t.Parallel()
		err := p.ValidateSpec(provider.Provider{
			Type:          "prometheus",
			Configuration: map[string]any{"image": false},
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
