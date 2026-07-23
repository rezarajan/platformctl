// Conformance scoping decision (docs/planning/08 E6 done-note's recorded
// follow-up; ADR 028's fake-honesty rule, docs/contributing/
// provider-authoring.md §6): openlineage's ONLY Kind ("Provider") cannot
// reach Ready against the fake runtime. Reconcile's waitAPIReady dials the
// Marquez API's real REST endpoint (GET /api/v1/namespaces, expects 200)
// before ever returning — a real application-layer HTTP dial with no
// dialer/transport seam the fake can serve honestly without impersonating
// the Marquez API surface, the class of dial ADR 028 §2's fake-honesty
// rule and the provider-authoring guide's §6 keep out of the fast tier (a
// hand-built HTTP mock would need pinning against a real Marquez server's
// observed behavior before its "green" meant anything — this repo has no
// such pinned harness, and building one is out of this retrofit's scope).
// Covered instead by the Docker integration suite (cmd/platformctl's
// lineage scenarios).
//
// conformance.Run is therefore never called here — it would require
// Reconcile to reach Ready, which is unreachable without a real dial, and
// this task's own brief is explicit: never a fabricated pass. openlineage
// declares no capability interface beyond the base reconciler.Provider (no
// SpecValidator, no BindingOptionsValidator — grep of openlineage.go's own
// method set confirms it), so unlike grafana/prometheus/trino/ingress's
// own scoped-out files in this retrofit, there is no fast-tier-provable
// subset at all here — this file exists purely to record that scoping
// decision, following the same documented-scoping-over-fabricated-pass
// discipline internal/adapters/providers/redpanda's own exemplar
// established for its EventStream Kind.

package openlineage
