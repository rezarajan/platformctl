# Remediation Audit Ledger

Audit of the repository against
`docs/planning/07-production-grade-docker-runtime-gap-analysis.md` (the gap
analysis): every resolved/open/deferred item verified against current code,
tests, schemas, documentation, examples, and contracts, plus an architecture
coherence review (layering, provider/runtime boundary, extensibility).

**Audit date:** 2026-07-16. **Audited revision:** `ae99505`.
**Rules:** production code is not modified by this audit; each confirmed
issue becomes one bounded finding file a lower-tier model can implement
without making architectural decisions.

## How to resume

Findings are numbered `F-NNN-<slug>.md` in discovery order. The checklist
below is the authoritative progress record — resume at the first unchecked
section. A section is checked only when every claim in it was verified
against the repo (not against the doc's own text).

## Audit coverage checklist

- [x] Setup: ledger created
- [x] Gate 0 §0.1 canonical names/namespaces/IDs
- [x] Gate 0 §0.2 ambiguous bare-name references
- [x] Gate 0 §0.3 external lifecycle enforcement
- [x] Gate 0 §0.4 removed/renamed resource handling
- [x] Gate 0 §0.5 machine-readable output contract
- [x] Gate 0 §0.6 renderer escaping
- [x] Gate 0 §0.7 Docker bind address + ownership
- [x] Gate 1 §1.1 ContainerRuntime contract
- [x] Gate 1 §1.2 runtime drift equivalence
- [x] Gate 1 §1.3 GC/orphan inspection (deferred — disposition verified)
- [x] Gate 1 §1.4 state durability
- [x] Gate 2 §2.1 provider drift equivalence
- [x] Gate 2 §2.2 provider-specific bugs
- [x] Gate 2 §2.3 lakehouse contract
- [x] Gate 2 §2.4 ingress/egress
- [x] Gate 2 §2.5 security baseline
- [x] Gate 3 §3.1 schema/provider validation (open items — claims verified)
- [x] Gate 3 §3.2 test coverage gaps (open items — claims verified)
- [x] Gate 3 §3.3 docs/public-surface sync
- [x] Gate 3 §3.4 contributor contract (open — no completion claims)
- [x] Cross-Runtime Portability section (Kubernetes adapter claims)
- [x] Stage-gate checkboxes (Gates 0–3 summary claims)
- [x] Architecture: layering invariant (domain/ports ↛ adapters)
- [x] Architecture: provider/runtime boundary + capability seams
- [x] Architecture: schema ↔ docs ↔ generated-reference consistency

## Findings index

| ID | Severity | Status | Title |
|---|---|---|---|
| [F-001](F-001-probe-secrets-unresolved-false-drift.md) | High | Confirmed | `drift`/`status` probe Connect Bindings without resolved secrets → guaranteed false `ConnectorConfigDrift` |
| [F-002](F-002-graph-output-flag-contract.md) | Medium | Confirmed | `graph -o json` emits non-JSON to stdout, violating the §0.5 machine-output contract |
| [F-003](F-003-inventory-for-structured-output.md) | Low | Confirmed | `inventory --for <tool> -o json` emits prose to stdout (§0.5 contract) |
| [F-004](F-004-stale-generated-reference-docs.md) | Medium | Confirmed | Committed `docs/reference/` not regenerated after schema changes (deletionPolicy, runtime.type) |
| [F-005](F-005-compatibility-test-imports-adapter.md) | Low | Confirmed | `compatibility_test.go` imports a concrete adapter — layering-invariant exception without recorded waiver |
| [F-006](F-006-provider-schema-network-description.md) | Low | Confirmed | `provider.json` `runtime.network` described as docker-specific; Kubernetes adapter consumes it as the Namespace |
| [F-007](F-007-serverid-formula-migration-drift.md) | Low | Confirmed | `database.server.id` formula change makes pre-existing MySQL connectors report config drift until re-applied (undocumented migration note) |
| [F-008](F-008-probe-tcp-reachable-not-skippable.md) | Low | Confirmed | `TestProbeTCPReachable` still hard-fails in loopback-restricted runners; §3.2 lists it open but the doc's own validation-signal section claims it "should be made skippable" — unimplemented |
| [F-009](F-009-just-check-gofmt.md) | Low | Confirmed | `just check` cannot fail on unformatted files (`gofmt -l` exits 0) — §3.2 open item verified still broken |
| [F-010](F-010-k8s-namespace-collision-with-cluster-namespaces.md) | Medium | Confirmed | Kubernetes adapter refuses pre-existing *unmanaged* namespaces, but `EnsureNetwork("default")` and similar system names produce a hard error only at apply time; no validate-time guard |
| [F-011](F-011-dbt-psql-single-database-assumption.md) | Low | Confirmed | `inventory --for dbt/psql` picks the last postgres Source/endpoint when several exist — nondeterministic for multi-database platforms |
| [F-012](F-012-fake-runtime-hostaddr-inconsistency.md) | Info | Recorded | Fake runtime reports observed ports even when the container was never "started" — divergence from Docker semantics is acceptable but undocumented in the conformance contract |

## Architecture assessment (summary)

Recorded in [ARCHITECTURE-ASSESSMENT.md](ARCHITECTURE-ASSESSMENT.md):
layering invariant holds in production code (one test-only exception,
F-005); ports are runtime-agnostic with documented per-runtime differences;
capability seams are consistent; the concrete risks are contract drift
between generated docs and schemas (F-004) and the machine-output contract
regressing as new flags are added (F-002, F-003 — same root cause).
