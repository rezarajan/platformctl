// Package lint is the design-lint engine (docs/adr/020-design-lints.md):
// pure functions over the resolved manifest set (envelopes + graph +
// provider capability declarations), reusing
// internal/application/compatibility's resolved name index rather than
// re-resolving references. It never touches live infrastructure and never
// blocks — enforcement is the policy layer's job (ADR 021); this package
// only detects and reports.
package lint

import (
	"sort"

	"github.com/rezarajan/platformctl/internal/application/compatibility"
	"github.com/rezarajan/platformctl/internal/domain/graph"
	"github.com/rezarajan/platformctl/internal/domain/lint"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/state"
)

// Finding re-exports domain/lint.Finding so callers of this package need
// only one import for the common case.
type Finding = lint.Finding

// Options carries the inputs Run needs beyond envelopes/graph/resolver that
// aren't themselves graph-derivable: gate state and recorded state are both
// facts the caller (cmd/platformctl) already has and lint has no business
// resolving itself (mirrors how compatibility.Check never reads feature
// gates either — cmd/platformctl's own checkHighAvailabilityGate does that
// separately).
type Options struct {
	// HighAvailabilityEnabled gates DL014 (docs/adr/004 — the same gate
	// checkHighAvailabilityGate enforces at validate time for the >1 case;
	// DL014 is the softer, informational ==1 case).
	HighAvailabilityEnabled bool
	// State is recorded state, when available, for DL021 (plan-aware: "a
	// set that also uses authoritative deletes"). nil skips DL021 entirely
	// — never an error, since a lint run against a manifest set alone (no
	// state file yet, e.g. a blueprint fixture) is a perfectly normal case.
	State *state.State
}

// Run computes every built-in (DL001-DL022, DL000) and provider-contributed
// (DL-<type>-NNN) finding for envelopes, applies waivers declared via
// metadata.annotations, and returns the result sorted per
// domain/lint.Less — deterministic, byte-identical output for identical
// input (ADR 020's determinism bar).
func Run(envelopes []resource.Envelope, g *graph.Graph, resolve compatibility.ProviderResolver, opts Options) ([]Finding, error) {
	idx := compatibility.NewIndex(envelopes)

	var findings []Finding
	findings = append(findings, lintDuplicateCapture(envelopes, idx)...)
	findings = append(findings, lintSinkCollision(envelopes, idx)...)
	findings = append(findings, lintObserverNotConsumed(envelopes, idx, resolve)...)
	findings = append(findings, lintPlaintextBoundary(envelopes, idx, resolve)...)
	findings = append(findings, lintOrphanedEventStream(envelopes, g)...)
	findings = append(findings, lintUnreferencedResources(envelopes, g)...)
	findings = append(findings, lintDeadEndPipeline(envelopes, g)...)
	findings = append(findings, lintSingleReplicaWithHAGate(envelopes, opts)...)
	findings = append(findings, lintDeletionPolicyUnset(envelopes)...)
	findings = append(findings, lintProtectUnset(envelopes, opts)...)
	findings = append(findings, lintNamespaceWideGrant(envelopes)...)

	providerFindings, err := runProviderLints(envelopes, g, resolve)
	if err != nil {
		return nil, err
	}
	findings = append(findings, providerFindings...)

	findings = applyWaivers(envelopes, findings)

	sort.SliceStable(findings, func(i, j int) bool { return lint.Less(findings[i], findings[j]) })
	return findings, nil
}

// runProviderLints calls reconciler.DesignLinter.LintDesign once per
// distinct provider Type() present among envelopes that implements it
// (docs/planning/TASK_PROGRESS.md's recorded design decision) — never once
// per Provider envelope, so a manifest with two debezium Providers doesn't
// get the same cross-manifest finding twice.
func runProviderLints(envelopes []resource.Envelope, g *graph.Graph, resolve compatibility.ProviderResolver) ([]Finding, error) {
	seen := map[string]bool{}
	var types []string
	for _, e := range envelopes {
		if e.Kind != "Provider" {
			continue
		}
		t, _ := e.Spec["type"].(string)
		if t == "" || seen[t] {
			continue
		}
		seen[t] = true
		types = append(types, t)
	}
	sort.Strings(types)

	var out []Finding
	for _, t := range types {
		impl, err := resolve(t)
		if err != nil {
			// A provider type present in the manifest failing to resolve is
			// a compatibility-time concern (already caught by validate
			// before lint ever runs) — never lint's own error to surface.
			continue
		}
		linter, ok := impl.(reconciler.DesignLinter)
		if !ok {
			continue
		}
		out = append(out, linter.LintDesign(envelopes, g)...)
	}
	return out, nil
}
