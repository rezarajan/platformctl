package archtest

import (
	"reflect"
	"testing"

	"github.com/rezarajan/platformctl/internal/ports/reconciler"
)

// frozenRequestFields is docs/planning/08 I9's frozen field list on
// reconciler.Request, captured at the moment the generic, engine-backed
// Request.Facts query replaced the one-bespoke-field-per-cross-provider-need
// accretion pattern (SchemaRegistryURL, KafkaBootstrapServers,
// MetricsTargets, CatalogFacts, PrometheusURL, WarehouseFacts, and the
// now-deleted TunnelFacts). Facts is the ONE query surface a NEW
// cross-provider published-fact need must use from here on — see
// reconciler.Request.Facts's own doc comment for the full pattern.
//
// Each entry below is either structural (part of every Request regardless
// of any cross-provider fact concern) or one of the five deprecated
// bespoke-fact wrapper fields I9 kept byte-identical (their removal is
// future work, not this task) or the one graph-resolved field (
// KafkaBootstrapServers) I9 deliberately left out of the Facts migration
// because it is not a *published* fact (ADR 015 scope) — see each field's
// own doc comment on reconciler.Request for the full reasoning.
var frozenRequestFields = map[string]string{
	"Resource":              "structural: the envelope under reconcile/destroy/probe",
	"Runtime":               "structural: the constructed ContainerRuntime",
	"Provider":              "structural: the realizing Provider's own envelope",
	"Secrets":               "structural: resolved spec.secretRefs",
	"Resources":             "structural: the full validated manifest graph",
	"Facts":                 "I9's generic, engine-backed published-fact query surface",
	"Warn":                  "structural: the provider diagnostics channel (docs/adr/031) — a channel, not a fact",
	"SchemaRegistryURL":     "deprecated wrapper over Facts (D1) — kept byte-identical, not removed by I9",
	"KafkaBootstrapServers": "graph-resolved manifest fact (E2) — deliberately NOT a Facts wrapper (not a published fact, ADR 015 scope)",
	"MetricsTargets":        "deprecated wrapper over Facts (C9) — kept byte-identical, not removed by I9",
	"CatalogFacts":          "deprecated wrapper over Facts (D10) — kept byte-identical, not removed by I9",
	"PrometheusURL":         "deprecated wrapper over Facts (C9 completion) — kept byte-identical, not removed by I9",
	"WarehouseFacts":        "deprecated wrapper over Facts (D8) — kept byte-identical, not removed by I9",
}

// TestReconcilerRequestFieldsFrozen is docs/planning/08 I9's accept-bar
// archtest: "forbids NEW bespoke fact fields on Request (list frozen)".
// reflect, not source parsing, is deliberately the mechanism — it
// introspects the actual compiled struct shape (immune to comment/
// whitespace changes, catches a field added via any means) rather than
// re-implementing a Go struct-field parser this repo's other archtests
// don't need. A field appearing on reconciler.Request beyond this frozen
// set must be a documented decision (update this map, explain why, in the
// same commit) — never a silent accretion nobody notices, the exact
// failure mode I9 closes off (docs/planning/08 §7.8 I9's "Why").
func TestReconcilerRequestFieldsFrozen(t *testing.T) {
	t.Parallel()
	typ := reflect.TypeOf(reconciler.Request{})

	got := make(map[string]bool, typ.NumField())
	var extra []string
	for i := 0; i < typ.NumField(); i++ {
		name := typ.Field(i).Name
		got[name] = true
		if _, ok := frozenRequestFields[name]; !ok {
			extra = append(extra, name)
		}
	}
	if len(extra) > 0 {
		t.Fatalf("reconciler.Request has NEW field(s) beyond docs/planning/08 I9's frozen list: %v — "+
			"a new cross-provider published-fact need must be consumed via Request.Facts (the generic "+
			"query), not a new bespoke field. If this field is genuinely structural (like Resource/"+
			"Runtime/Provider) or a deliberate, documented exception (like KafkaBootstrapServers), add "+
			"it to frozenRequestFields in this test with a one-line reason, as a documented decision — "+
			"never silently.", extra)
	}

	// A frozen entry disappearing without this test being updated in the
	// same commit would mean a field was removed by accident (or removed
	// without recording why, the same discipline TunnelFacts's own commit
	// followed: delete the field AND its frozen-list entry together).
	var missing []string
	for name := range frozenRequestFields {
		if !got[name] {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		t.Errorf("frozen field(s) %v no longer exist on reconciler.Request — if intentionally removed "+
			"(like TunnelFacts in I9), that removal should already have deleted the corresponding "+
			"frozenRequestFields entry in this same commit; this failure means it wasn't.", missing)
	}
}
