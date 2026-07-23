package ingress

import (
	"strings"
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/provider"
)

// Conformance scoping decision (docs/planning/08 E6 done-note's recorded
// follow-up; ADR 028's fake-honesty rule, docs/contributing/
// provider-authoring.md §6): ingress cannot reach a fast-tier-provable
// Ready determination for either of its Kinds. reconcileInstanceDocker
// itself (Provider kind) is pure container-lifecycle — no real dial in
// Reconcile — but conformance.Run's Settledness/Probe-honesty subtests
// call Probe immediately after, and probeInstanceDocker's own Ready
// determination REQUIRES caddyReady, a real GET /config/ against Caddy's
// admin API (caddy.go), with no dialer/transport seam the fake can serve
// honestly without impersonating Caddy's actual admin API surface — the
// same class of dial ADR 028 §2's fake-honesty rule and the
// provider-authoring guide's §6 keep out of the fast tier. Since
// conformance.Run's harness always exercises Probe as part of Settledness,
// there is no way to drive the Provider kind through the full suite
// without that real dial succeeding. The Connection kind (route
// reconciliation through the same admin API, docker.go/caddy.go) needs
// the identical real dial, and on Kubernetes the Provider kind has no
// central object of its own at all (kubernetes.go) — no fallback shape
// exists. Covered instead by the Docker and Kubernetes integration suites
// (cmd/platformctl's ingress/TLS scenarios).
//
// conformance.Run is therefore never called here — it would require Probe
// to reach Ready, which is unreachable without a real dial, and this
// task's own brief is explicit: never a fabricated pass.
//
// What IS fast-tier-provable without touching Reconcile/Probe/Destroy at
// all: ValidateSpec, a pure function with no runtime dependency — exercised
// directly below with the same exact-WantSubstrings discipline
// conformance.CapabilityCheck applies in every other exemplar in this
// retrofit.
func TestValidateSpecCapabilityChecks(t *testing.T) {
	t.Parallel()
	p := New()

	t.Run("image-wrong-type", func(t *testing.T) {
		t.Parallel()
		err := p.ValidateSpec(provider.Provider{
			Type:          "ingress",
			Configuration: map[string]any{"image": 1},
		})
		requireErrorContains(t, err, "spec.configuration.image must be a non-empty string")
	})

	t.Run("domain-wrong-type", func(t *testing.T) {
		t.Parallel()
		err := p.ValidateSpec(provider.Provider{
			Type:          "ingress",
			Configuration: map[string]any{"domain": false},
		})
		requireErrorContains(t, err, "spec.configuration.domain must be a non-empty string")
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
