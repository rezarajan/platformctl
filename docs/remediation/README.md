# Remediation Audit Ledger

Audit of the repository against
`docs/planning/07-production-grade-docker-runtime-gap-analysis.md` (the gap
analysis): every resolved/open/deferred item verified against current code,
tests, schemas, documentation, examples, and contracts, plus an
architecture coherence review (layering, provider/runtime boundary,
extensibility).

**Audit date:** 2026-07-16, audited revision `ae99505`.
**Implementation date:** 2026-07-17 — all 10 findings resolved.
**Rules:** each confirmed issue is one bounded finding file, implemented
behind its own commit, with `docs/planning/07` updated in the same pass so
its claims track the code.

## Status: all findings resolved

| ID | Severity | Title | Commit |
|---|---|---|---|
| [F-001](F-001-machine-output-contract-violations.md) | Medium | `graph -o json`, `validate -o json`, `inventory --for -o json` emit non-JSON | `0e12bca` |
| [F-002](F-002-stale-generated-reference-docs.md) | Medium | Committed `docs/reference/` not regenerated after schema changes | `5bcb7b6` |
| [F-003](F-003-readme-cli-surface-stale.md) | Low | README CLI table stale: graph flag/description wrong, inventory missing | `e7db2d7` |
| [F-004](F-004-test-files-import-concrete-adapters.md) | Low | Application-layer test files import concrete adapters | `7210711` |
| [F-005](F-005-provider-schema-network-description.md) | Low | `provider.json` `runtime.network` described as docker-specific | `5bcb7b6` |
| [F-006](F-006-serverid-migration-drift-note.md) | Low | `database.server.id` formula change — undocumented one-time drift | `694a5dd` |
| [F-007](F-007-probe-tcp-reachable-not-skippable.md) | Low | `TestProbeTCPReachable` hard-fails in loopback-restricted runners | `df308c0` |
| [F-008](F-008-just-check-cannot-fail-on-gofmt.md) | Low | `just check` exits 0 with unformatted files | `df308c0` |
| [F-009](F-009-k8s-network-refusal-error-guidance.md) | Low | Kubernetes namespace refusal message names no remedy | `08be62b` |
| [F-010](F-010-toolconfig-multi-database-last-wins.md) | Low | `inventory --for` picks the last endpoint of a kind when several exist | `4d08404` |

Each finding file's own **Status** line carries implementation detail —
what actually shipped, any wrinkle discovered mid-implementation, and the
verification performed (live-cluster/live-daemon checks where applicable).
Two findings surfaced *additional* staleness while being implemented, fixed
in the same commit:

- **F-002** discovered `secretreference.md` carried hand-written
  rotation-behavior prose with no home in the schema description — a naive
  regeneration would have deleted it. Moved into the schema description
  instead (multi-paragraph); `docsgen.go` gained `description()` (preserves
  paragraph breaks) and `firstParagraph()` (index summary column) to
  support it without breaking the index table. Also fixed
  `docs/planning/03`'s stale "vault (future)" comment (Vault shipped in
  Phase 6, gated).
- **F-006** discovered §2.2's entire seven-item checklist had never been
  ticked despite every item being fixed in the earlier Gate 2 close-out
  (`09e1b61`) — the stage-gate summary referenced the fixes without
  updating the section it summarized. Corrected in the same commit.

## How to resume a future audit

This ledger is closed for revision `ae99505`→`4d08404`. A future audit
should:

1. Pick a new revision to audit.
2. Re-run the coverage checklist below against that revision (not against
   this file's claims).
3. Number new findings starting at F-011, in a new ledger entry — don't
   reuse F-001..F-010 for unrelated issues even if some are still open at
   the new revision (re-open the specific finding file instead, with a new
   dated status line).

### Audit coverage checklist (revision `ae99505` — complete)

- [x] Gate 0 §0.1–§0.7 — all verified; §0.5 claim was unsupported ([F-001],
      now fixed and re-verified)
- [x] Gate 1 §1.1–§1.4 — verified; deferrals accurate
- [x] Gate 2 §2.1–§2.5 — verified; §2.2's checklist was stale ([F-006],
      now fixed)
- [x] Gate 3 §3.1–§3.4 — open items re-verified as genuinely open or fixed
      ([F-007], [F-008]); §3.3 sync gaps fixed ([F-002], [F-003], [F-005])
- [x] Cross-Runtime Portability — adapter code matches every mapping claim;
      [F-009] UX gap fixed
- [x] Stage-gate checkboxes — Gate 0's unsupported item corrected
- [x] Architecture: layering — production clean; test exception now
      documented ([F-004])
- [x] Architecture: provider/runtime boundary + capability seams — coherent
- [x] Architecture: schema ↔ docs ↔ generated reference — sync test added
      ([F-002]) so this class of drift fails CI going forward

## Architecture assessment

[ARCHITECTURE-ASSESSMENT.md](ARCHITECTURE-ASSESSMENT.md) — unchanged by the
implementation pass (it recorded the audit's findings and verified-OK
results as of `ae99505`; still accurate as the description of what was
true then and the reasoning behind the findings above).

## Final verification (2026-07-17, revision `4d08404`)

```
gofmt -l .                    # clean
go vet ./... && go vet -tags integration ./...   # clean
go test ./...                 # all pass
just check                    # now actually gates on gofmt (F-008) — passes
```

Live-verified during implementation (not just unit-tested): F-001's three
CLI paths against a built binary; F-002's regeneration diffed and
drift-guarded; F-007/F-008 reproduced failing, then reproduced fixed;
F-009 against a real Kubernetes cluster (minikube).
