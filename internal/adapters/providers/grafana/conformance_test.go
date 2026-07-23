package grafana

import (
	"strings"
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/provider"
)

// Conformance scoping decision (docs/planning/08 E6 done-note's recorded
// follow-up; ADR 028's fake-honesty rule, docs/contributing/
// provider-authoring.md §6): grafana's ONLY Kind ("Provider") cannot reach
// Ready against the fake runtime by any path. Unlike every provider this
// retrofit DID find a fast-tier-provable Kind for, there is no container-
// lifecycle-only sub-path to fall back to here: reconcileInstance's
// ensureAdminCredential unconditionally dials a real HTTP login check
// (pingLogin -> loginOK -> httpOK against /api/org) through
// providerkit.CredentialRotation.Run — even on a fresh instance's very
// first Reconcile, CredentialRotation.NoPreviousOrUnchanged still calls
// WaitReachable(PingDesired) (rotation.go's Run, first branch). Reconcile
// also calls waitAPIReady (a real GET /api/health check) before that. Both
// are real application-layer HTTP dials with no dialer/transport seam the
// fake can serve honestly without impersonating Grafana's actual login/
// health API surface — precisely the class of dial ADR 028 §2's
// fake-honesty rule and the provider-authoring guide's §6 keep out of the
// fast tier (a hand-built HTTP mock would need pinning against a real
// Grafana's observed behavior before its "green" meant anything — this
// repo has no such pinned harness, and building one is out of this
// retrofit's scope). Covered instead by the Docker integration suite
// (cmd/platformctl's monitoring-stack scenarios).
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

	t.Run("adminSecretRef-not-wired", func(t *testing.T) {
		t.Parallel()
		err := p.ValidateSpec(provider.Provider{
			Type:          "grafana",
			Configuration: map[string]any{"adminSecretRef": "admin-creds"},
			SecretRefs:    nil, // deliberately not listed
		})
		requireErrorContains(t, err, "adminSecretRef", "must also be listed in spec.secretRefs")
	})

	t.Run("no-secretRefs-at-all", func(t *testing.T) {
		t.Parallel()
		err := p.ValidateSpec(provider.Provider{Type: "grafana", Configuration: map[string]any{}})
		requireErrorContains(t, err, "spec.secretRefs must name at least one SecretReference")
	})

	t.Run("image-wrong-type", func(t *testing.T) {
		t.Parallel()
		err := p.ValidateSpec(provider.Provider{
			Type:          "grafana",
			Configuration: map[string]any{"image": 42},
			SecretRefs:    []string{"admin"},
		})
		requireErrorContains(t, err, "spec.configuration.image must be a non-empty string")
	})
}

// requireErrorContains is this file's own tiny stand-in for
// conformance.CapabilityCheck's assertion shape (Name/Invoke/
// WantSubstrings) — reused across this retrofit's scoped-out providers'
// direct capability tests, kept local rather than promoted to a shared
// helper since each occurrence is a handful of lines and the providers
// needing it (grafana/prometheus/trino/ingress) don't otherwise share a
// test-only package.
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
