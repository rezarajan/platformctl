# Remediation Audit Ledger

Audit of the repository against
`docs/planning/07-production-grade-docker-runtime-gap-analysis.md` (the gap
analysis): every resolved/open/deferred item verified against current code,
tests, schemas, documentation, examples, and contracts, plus an
architecture coherence review (layering, provider/runtime boundary,
extensibility).

**Audit date:** 2026-07-16. **Audited revision:** `ae99505`.
**Rules:** production code is not modified by this audit; each confirmed
issue is one bounded finding file a lower-tier model can implement without
making architectural decisions. Findings that were *disproven* during the
audit are recorded in
[ARCHITECTURE-ASSESSMENT.md §4](ARCHITECTURE-ASSESSMENT.md) so the same
ground is not re-audited.

## How to resume

Findings are numbered `F-NNN-<slug>.md` in confirmation order. The
checklist below is the authoritative progress record — resume at the first
unchecked section. A section is checked only when every claim in it was
verified against the repo (not against the doc's own text). This pass
completed all sections; a future audit re-opens the checklist at a new
revision.

## Audit coverage checklist (revision `ae99505` — complete)

- [x] Gate 0 §0.1 canonical names/namespaces/IDs — verified (schema
      patterns, Go-side validation, escaped v2 state keys + migration,
      project-scoped labels; dedicated tests exist)
- [x] Gate 0 §0.2 ambiguous refs — verified (indexed slice resolution +
      explicit ambiguity errors; observers resolve kind-scoped to Provider
      in `graph.Build`)
- [x] Gate 0 §0.3 external lifecycle — verified (`ExternalConfigurer`
      enforced in engine; `TestExternalProviderRefUsesConfigureExternal`)
- [x] Gate 0 §0.4 removal/rename — verified (authoritative deletes,
      `ActionOrphanUnknown` refusal, rename + provider-type-change tests)
- [x] Gate 0 §0.5 machine-readable output — **FAILED for three paths** →
      [F-001]; all other commands verified parseable on success/no-op/
      cancelled/error paths (live repro)
- [x] Gate 0 §0.6 renderer escaping — verified (hex ids, adversarial golden
      tests)
- [x] Gate 0 §0.7 bind address + ownership — verified (loopback default,
      unmanaged refusal, live integration tests exist and are real)
- [x] Gate 1 §1.1 runtime contract — verified (ports/aliases/pull
      policy/files/logs + conformance); deferrals accurate
- [x] Gate 1 §1.2 drift equivalence — verified (fake DeepEqual, observed
      ports subtest)
- [x] Gate 1 §1.3 GC (deferred) — disposition accurate (safety mechanisms
      exist; tooling absent as stated)
- [x] Gate 1 §1.4 state durability — fsync verified in `localfile.Save`;
      deferrals accurate
- [x] Gate 2 §2.1 drift equivalence table — verified line-by-line against
      probes (incl. nessie branch check) → one gap: [F-006] migration note
- [x] Gate 2 §2.2 provider bugs — verified (escaping tests use real driver
      parsers; serverID per-connector; BindingOptionsValidator seam;
      deletionPolicy end-to-end)
- [x] Gate 2 §2.3 lakehouse contract — verified → gaps: [F-010] plurality,
      [F-001] `--for -o json`
- [x] Gate 2 §2.4 ingress/egress — dispositions verified (Connection seam
      is docs/design/002; host-audience probes exist incl.
      through-forwarder)
- [x] Gate 2 §2.5 security baseline — verified (no `latest` images remain;
      `Endpoint.Insecure` on all nine publishers; no-secret-persistence
      audit re-performed) → [F-005] schema description drift
- [x] Gate 3 §3.1 (open) — claims consistent
- [x] Gate 3 §3.2 (open) — verified still-broken: [F-007], [F-008]; CI's
      own gofmt gate is correct
- [x] Gate 3 §3.3 docs sync — [F-002] generated reference stale, [F-003]
      README stale
- [x] Gate 3 §3.4 (open) — no completion claims to audit
- [x] Cross-Runtime Portability — adapter code matches every mapping claim;
      k8s conformance skip-guard verified → [F-009] refusal-message UX
- [x] Stage-gate checkboxes — Gate 0 one unsupported item ([F-001]); Gates
      1–2 supported; verdict table in the assessment
- [x] Architecture: layering — production clean; test exception → [F-004]
- [x] Architecture: provider/runtime boundary + capability seams — coherent
      (assessment §2, §6)
- [x] Architecture: schema ↔ docs ↔ generated reference — [F-002], [F-005]

## Findings index

Ordered by severity, then discovery.

| ID | Severity | Title |
|---|---|---|
| [F-001](F-001-machine-output-contract-violations.md) | Medium | `graph -o json`, `validate -o json`, `inventory --for -o json` emit non-JSON — Gate 0 stage-gate claim unsupported |
| [F-002](F-002-stale-generated-reference-docs.md) | Medium | Committed `docs/reference/` not regenerated after schema changes (`deletionPolicy` missing; kubernetes described as unavailable) |
| [F-003](F-003-readme-cli-surface-stale.md) | Low | README CLI table stale: graph flag/description wrong, inventory missing |
| [F-004](F-004-test-files-import-concrete-adapters.md) | Low | Application-layer test files import concrete adapters — invariant exception without recorded waiver |
| [F-005](F-005-provider-schema-network-description.md) | Low | `provider.json` `runtime.network` described as docker-specific; Kubernetes consumes it as the Namespace |
| [F-006](F-006-serverid-migration-drift-note.md) | Low | `database.server.id` formula change → one-time drift on pre-existing MySQL connectors, undocumented |
| [F-007](F-007-probe-tcp-reachable-not-skippable.md) | Low | `TestProbeTCPReachable` hard-fails in loopback-restricted runners |
| [F-008](F-008-just-check-cannot-fail-on-gofmt.md) | Low | `just check` exits 0 with unformatted files (reproduced) |
| [F-009](F-009-k8s-network-refusal-error-guidance.md) | Low | Kubernetes namespace refusal message names no remedy; collision with system namespaces surfaces only at apply |
| [F-010](F-010-toolconfig-multi-database-last-wins.md) | Low | `inventory --for` picks the last endpoint of a kind when several exist |

## Suggested implementation order

1. F-001 (contract fix; unblocks re-checking the Gate 0 stage-gate box)
2. F-005 then F-002 (schema description, then one regeneration pass + sync
   test)
3. F-003 (after F-001 — documents its outcome)
4. F-007, F-008 (independent, trivial)
5. F-004, F-006, F-009, F-010 (independent)

## Architecture assessment

[ARCHITECTURE-ASSESSMENT.md](ARCHITECTURE-ASSESSMENT.md) — layering verdict
(clean in production code), boundary review, stage-gate verdict table,
disproven hypotheses (§4, incl. the probe-secrets false alarm), recorded
risks without bounded tasks (§5: Connect GET/config secret-echo dependency,
k8s subPath update semantics, fake-runtime port timing, `--format` vs `-o`
consolidation), and extensibility spot-checks (§6).
