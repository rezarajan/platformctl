// Conformance scoping decision (docs/planning/08 E6 done-note's recorded
// follow-up; ADR 028's fake-honesty rule, docs/contributing/
// provider-authoring.md §6): nessie cannot reach a fast-tier-provable Ready
// determination for either of its Kinds.
//
//   - Provider (reconcileInstance): waitAPIReady dials nessie's real REST
//     API (GET /api/v2/config, expects 200) before Reconcile ever returns —
//     a real application-layer HTTP dial with no dialer/transport seam the
//     fake can serve honestly without impersonating the Nessie API surface.
//   - Catalog (reconcileCatalog): beyond the identical waitAPIReady call,
//     ensureBranch/branchExists require STATEFUL REST semantics (create a
//     branch, then observe it as existing) — a meaningfully deeper "fake
//     HTTP admin API" than a bare status-code check, exactly what ADR 028
//     §2's fake-honesty rule would require pinning against a real Nessie
//     server's observed behavior before trusting a green result.
//
// Both are out of this retrofit's scope; covered instead by the Docker
// integration suite (cmd/platformctl's lakehouse scenarios).
//
// conformance.Run is therefore never called here — it would require
// Reconcile to reach Ready, which is unreachable without a real dial, and
// this task's own brief is explicit: never a fabricated pass. nessie
// declares no error-returning capability interface beyond
// CatalogCapableProvider.SupportedCatalogEngines (a value-returning method,
// not error-returning — nothing for a CapabilityCheck to exercise), so
// unlike grafana/prometheus/trino/ingress's own scoped-out files in this
// retrofit, there is no fast-tier-provable subset at all here — this file
// exists purely to record that scoping decision, following the same
// documented-scoping-over-fabricated-pass discipline
// internal/adapters/providers/redpanda's own exemplar established for its
// EventStream Kind.

package nessie
