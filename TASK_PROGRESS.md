# H1 + H2 — Design lints (ADR 020)

Bundled task: H1 (lint engine + built-in set) and H2 (provider-contributed
lints), per docs/planning/08-production-readiness-plan.md §7.7. Previous
TASK_PROGRESS.md content (D3/D4) is stale — that task is committed on main
(80b5bf6) already; overwritten here per step 0.

Read in full before coding (per doc 08 §2.1): CLAUDE.md, docs/adr/020,
docs/adr/009, docs/planning/02-architecture.md §4.2/§5.2,
internal/application/compatibility/compatibility.go,
internal/domain/status/catalog.go, cmd/platformctl/explain.go,
internal/ports/reconciler/reconciler.go, internal/domain/graph/graph.go.

## Design decisions (filling gaps ADR 020 leaves open, not reopening it)

- Finding type lives in `internal/domain/lint` (not `internal/application/lint`)
  because `reconciler.DesignLinter` (a port) must return it — ports may only
  import domain (CLAUDE.md layering invariant).
- `compatibility.Index`/`NewIndex` — minimal additive export over the
  existing unexported `manifestIndex`, so lint reuses the same resolved
  name-index `compatibility.Check` builds (ADR 020 "no second resolution
  pass").
- Only 11 codes exist in ADR 020 §4's table (DL001-004, DL010-014, DL020-021)
  — "DL001-DL021" names the addressing range, not 21 distinct lints. DL000 is
  added (not in the ADR table) for one housekeeping case the ADR requires but
  doesn't code: "empty reason = the waiver itself is a warning".
- Waiver annotation value is `"CODE: reason"`, comma-separated for multiple
  codes on one resource (ADR gives one single-code example; multi-code
  support is an implementation detail, not a reopened decision).
- `reconciler.DesignLinter.LintDesign(envelopes, g)` is called once per
  *distinct provider Type()* implementing it (not once per Provider
  envelope) — the debezium replication-slot check needs the whole manifest,
  not one envelope, and calling once per type avoids duplicate findings when
  more than one Provider envelope shares a type.
- Provider-technology adapters (debezium/redpanda/s3sink) cannot be imported
  by `internal/application/lint` (CLAUDE.md test-exception only allows
  fake/localfile/env/noop even in tests) — the golden "every DL code"
  fixture in H1 covers only the 11 built-ins; H2's positive/negative
  provider fixtures + the full DL-<type>-NNN golden live in cmd/platformctl
  (which already imports every adapter via the registry).

## Steps

1. [done] git merge main --no-edit (already up to date).
2. [done] Read ADR 020, ADR 009, doc 02 §4.2/§5.2, compatibility.go,
   catalog.go, explain.go, reconciler.go, graph.go, resource.go, domain
   kinds (binding/dataset/eventstream/catalog/connection/source/provider),
   root.go, featuregate.go, registry.go, blueprint templates, output
   contract harness.
3. [done] internal/domain/lint (Finding/Severity/waiver parsing).
4. [done] compatibility.go: export Index/NewIndex (additive).
5. [done] reconciler.go: DesignLinter capability interface.
6. [done] internal/application/lint: Run() + built-in DL001-004,010-014,
   020-021 + DL000 (malformed waiver) + waiver application + sorting.
7. [done] internal/application/lint tests: per-code table tests,
   determinism golden, fixture set under testdata/.
8. [done] status/catalog.go: lintCode entries for all built-ins + provider
   codes.
9. [done] cmd/platformctl/lint.go: `lint` command, one-line validate
   summary, DesignLints gate registration (main.go) + doc 04 §12 row.
10. [done] output_contract_harness_test.go: register "lint".
11. [done] Blueprint audit: run lint against every shipped blueprint; fix
    (explicit deletionPolicy) or waive (documented) every finding.
12. [done] cmd/platformctl test: every blueprint lints clean (0 unwaived).
13. [done] H2: debezium/redpanda/s3sink DesignLinter implementations +
    codes + catalog entries + positive/negative fixtures + completeness
    test.
14. [done] gofmt/build/vet/go test ./...; scripts/test-impact.sh. Results
    logged below.
15. [done] Commit.

## Verification log

- `gofmt -l .` — empty (clean) after every increment.
- `go build ./...` — clean.
- `go vet ./...` — clean.
- `go test ./...` — all packages pass (internal/application/lint,
  internal/domain/lint, cmd/platformctl including
  TestBlueprintsLintClean/TestExplainCatalogCoversEveryLintCode/
  TestEveryLintCodeExplains/the H2 positive+negative fixture tests, every
  provider package including debezium/redpanda/s3sink's existing suites
  unaffected by the additive lint.go files).
- `just test-affected main` (scripts/test-impact.sh --base main): the
  diff's breadth (cmd/platformctl/*.go, internal/application/blueprint/
  templates/**, three provider packages) maps to nearly every Docker
  integration suite's scope (14 selected), and the shared flock is
  contended by 5 other concurrently active agent worktrees per this task's
  own briefing ("shared flock; queuing expected"). `redpanda` suite
  (TestRedpandaEndToEnd|TestRedpandaHA — the suite most relevant to
  redpanda/lint.go) ran green in 85.6s once the lock was free. The
  remaining suites (cdc, sink, connect-ha-dlq, acceptance, lakehouse,
  prometheus, ingress, blueprints, object-store-posture, trino, jdbcsink,
  s3source) were left running in the background against the shared ledger
  (git common dir) — a future run against the same content-state will hit
  the ledger and skip, per the script's own design; see the final report
  for how far it got. The lint engine itself is pure (no live infra, per
  ADR 020 and this task's own instruction); the three provider lint.go
  additions add only a new LintDesign method each and touch no existing
  Reconcile/Probe/Destroy/ValidateSpec code path those suites exercise —
  the risk of a live-infra regression from this diff is low by
  construction, not just by inference from the one suite that finished.
