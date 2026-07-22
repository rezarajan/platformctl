# E4 (explain catalog) + G7 (test-impact economy hardening) — progress

Task: docs/planning/08 §7 E4 + §7.6 G7. Branch: this worktree
(worktree-agent-aa460448ab61c2a3d). Resume from here + `git log`.

(Replaces this file's previous contents, which were the already-merged
C4+D7 task's checkpoint.)

File ownership: cmd/platformctl/explain*.go, internal/domain/status
(catalog additions only), internal/application/docsgen (additive),
scripts/test-impact.sh (outside the map heredoc only), internal/archtest,
.github/workflows/ci.yml. Do NOT touch internal/adapters/providers/**,
docs/onboarding/, README, docs/adr/018, connection/dataset schemas.

## Step plan and status

1. [done] `git merge main --no-edit` — fast-forward-merged cleanly
   (b9edeb8 -> 39efc80, C4/D7/D9/E1 landed on main). No conflicts.
2. [done] Read: doc 08 E4 + G7 entries in full, doc 02 §5.5 (lineage),
   internal/domain/status/{reasons.go,status.go} (74 Reason constants + 4
   ConditionTypes), internal/application/docsgen/{docsgen.go,site.go},
   cmd/platformctl/{root.go,init.go,output_contract_harness_test.go},
   internal/cliutil/cliutil.go, internal/archtest/reason_literal_test.go
   (style precedent), scripts/test-impact.sh in full, doc 06 §10,
   .github/workflows/ci.yml.
3. [done] E4 implementation:
   - [done] `internal/domain/status/catalog.go`: `CatalogEntry` struct +
     `Catalog []CatalogEntry` — 4 ConditionType entries + one entry per
     Reason constant (74), grouped/ordered exactly as reasons.go's own
     section comments. Verified 8 dynamic-prefix reasons by grepping
     actual call sites (fmt.Sprintf/string-concat), not just doc-comment
     wording: ReasonConnectorState, ReasonConnectWorkerMissing,
     ReasonPartitionCountMismatch, ReasonReplicationFactorMismatch,
     ReasonRetentionMismatch, ReasonBrokerMissing, ReasonBrokerNotJoined,
     ReasonNodeMissing — all others are literal/complete reasons (e.g.
     ReasonTopicMissing, ReasonLifecycleRuleDrift/VersioningDrift,
     ReasonWorkerCountMismatch are NOT prefixes, confirmed by their call
     sites using the constant as-is, no fmt.Sprintf).
   - [done] `internal/archtest/explain_catalog_test.go`:
     `TestExplainCatalogCoversEveryReason` parses reasons.go's AST
     (go/parser, not regex) for every `Reason* = "value"` const, diffs
     against `status.Catalog`'s reason tokens both directions (missing +
     orphan/typo'd + duplicate detection);
     `TestExplainCatalogCoversEveryConditionType` for the 4 ConditionTypes;
     `TestDeclaredReasonsDetectsMissingCatalogEntry` self-proof (mirrors
     reason_literal_test.go's pattern).
   - [done] Live-proved the guard test against REAL reasons.go (not just
     the synthetic self-proof): temporarily deleted the ReasonNoDrift
     catalog entry, ran `go test ./internal/archtest/... -run
     TestExplainCatalogCoversEveryReason -v` — failed naming
     `ReasonNoDrift = "NoDrift"` exactly as designed, then restored the
     file (not committed).
   - [done] `cmd/platformctl/explain.go`: `newExplainCmd` — exact match,
     then dynamic-prefix match (query has entry's Token as a literal
     prefix, e.g. pasting "PartitionCountMismatch(3!=5)" from `status`
     output), then case-insensitive Token-prefix, then case-insensitive
     substring — each stage stops at a unique hit; ambiguous/empty
     -> candidate list + ExitValidation. `-o json|yaml` structured
     (`explainOutput{query,matched,entry,candidates}`).
   - [done] `cmd/platformctl/root.go`: registered `newExplainCmd(a)` in
     `newRootCmd`'s AddCommand list; status footnote (only table mode,
     only when any resource's Ready != "True") pointing at `explain`.
   - [done] `cmd/platformctl/output_contract_harness_test.go`: registered
     "explain" scenario (exact match + ambiguous fallback, both -o json).
   - [done] `internal/application/docsgen/docsgen.go`: `renderExplainCatalog()`
     — imports `internal/domain/status` (application->domain, layering-legal),
     renders `Catalog` grouped by Area into `explain.md`, linked from
     `index.md`. Regenerated `docs/reference/` (`go run ./cmd/platformctl
     docs build --out docs/reference`) — `TestGeneratedReferenceInSync`
     green.
   - [done] Manually verified end-to-end: `explain WALNotLogical` (exact),
     `explain DriftDetected` (ConditionType), `explain
     "PartitionCountMismatch(3!=5)"` (dynamic-prefix paste), `explain
     Broker` (ambiguous, 4 candidates, exit 3), `explain zzz` (no match,
     exit 3); `status` footnote appears pre-apply (Unknown/NotApplied) and
     disappears once all resources are Ready=True, and is absent under
     `-o json`.
4. [done] G7 implementation:
   - [done] Parsed scripts/test-impact.sh's suite map (id|scope|cmd) with
     a throwaway Python prototype to find EVERY currently-uncovered
     integration Test* function before writing the Go guard test, so the
     guard's exemption list would be accurate on day one rather than
     discovered by trial and error. Found 23 pre-existing gaps (see
     Finding below) — real bugs in the map's -run filters, NOT
     something introduced by this task.
   - [done] `internal/archtest/test_impact_completeness_test.go`: parses
     the suite map straight from scripts/test-impact.sh's own source text
     (heredoc between `cat <<'EOF'` / `EOF` inside `suites() {`) —
     `parseSuiteMap`; extracts each suite's -run regex (quoted-or-bare) +
     target dirs (recursive `/...` vs exact) — `coveringSuites` mirrors
     real `go test -run`'s unanchored-substring semantics; enumerates
     every `func Test*` in cmd/platformctl/*_integration_test.go + every
     other `//go:build integration`-tagged file repo-wide —
     `integrationTestFuncs`; fails naming misses not on
     `integrationTestExemptions` (also flags stale exemptions naming a
     test that no longer exists). `TestParseSuiteMapAndCoverage`
     self-proof against synthetic input (mirrors reason_literal_test.go's
     style).
   - [done] Proved it live against the REAL map: wrote a throwaway
     `cmd/platformctl/zzz_fixture_integration_test.go` with
     `TestZzzTotallyUnmappedFixture`, ran
     `go test ./internal/archtest/... -run
     TestIntegrationSuiteMapCoversEveryTest -v` — failed naming
     `TestZzzTotallyUnmappedFixture@cmd/platformctl` exactly as designed,
     then deleted the fixture file (never staged/committed;
     `git status --short` confirmed clean after).
   - [done] `--prune <days>` flag in scripts/test-impact.sh, added
     entirely OUTSIDE the suites() heredoc (flag parsing + a standalone
     prune-and-exit branch using `find -mtime +N -delete`, run before the
     diff-selection logic). Documented in the script's own header comment
     and doc 06 §10 (pure append, items 7-9 — no existing text modified;
     guard hook did not block it). Manually verified: seeded the real
     shared ledger with a fresh + an artificially-40-day-old fixture
     entry, `--prune 30` removed only the old one (output: "pruned 1
     ledger entry(ies)..."), confirmed via `ls`.
   - [done] `.github/workflows/ci.yml`: `integration` job's checkout now
     `fetch-depth: 0` (so `origin/main` resolves for the diff); its single
     step now branches on `github.event_name == 'pull_request'` ->
     `scripts/test-impact.sh --base origin/main`, else ->
     `scripts/test-impact.sh --full`. `integration-k8s` job (RBAC minting,
     kind cluster, kubeconfig steps) untouched — verified via `git diff
     .github/workflows/ci.yml` that only the `integration` job's checkout
     step and its one run: block changed. YAML syntax verified
     (`python3 -c "import yaml; yaml.safe_load(...)"`).
5. [done] Verify: gofmt clean; `CGO_ENABLED=0 go build -trimpath
   -buildvcs=false ./cmd/platformctl` OK; `go vet ./...` and `go vet -tags
   integration ./...` both OK; `go test ./...` fully green (includes the
   two new archtest completeness tests + explain harness scenario).
   `scripts/test-impact.sh --base main --print` selected 11 suites
   (redpanda, cdc, sink, connect-ha-dlq, acceptance, lakehouse,
   prometheus, ingress, blueprints, object-store-posture, trino) — NOT
   just gc-state-ops, because this diff touches internal/domain/status
   (catalog.go), which is in SHARED_CORE and thus in scope for every
   suite that includes it. Running the full selected set live now (see
   Verification results below for the outcome).
6. [done] Live 11-suite impact run complete, fully green (see
   Verification results). Final commit: squashed the two WIP commits
   (`git reset --soft 39efc80` — the step-1 merge fast-forwarded to
   main's tip, so the staged diff vs 39efc80 is exactly this task's
   changes) into the single task commit with the required subject
   (C2/C4 squash precedent). GPG signing verified working beforehand.

**STATUS: COMPLETE.** E4: 78-entry catalog (4 ConditionTypes + 74
reasons, 1:1 with reasons.go), explain command + harness + docsgen page,
status footnote, completeness enforced by archtest. G7: guard test
(proven via temporary unmapped fixture), --prune, CI split
(PR=--base origin/main, main push=--full).

## Finding recorded per doc 08 §2.1 (not silently worked around)

G7's completeness guard, run against the REAL current
scripts/test-impact.sh map (not just my new guard test's synthetic
fixture), surfaces **23 pre-existing integration tests with no suite
coverage** — genuine gaps in the map that predate this task:

- `internal/adapters/state/s3`: TestConformance, TestForceUnlock,
  TestLockReclaimsAfterExpiry — the `state-s3` suite's `-run
  'TestSharedState'` filter applies uniformly across BOTH its target dirs
  (`./internal/adapters/state/...` and `./cmd/platformctl/`), so it never
  matches this package's own conformance/lock tests even though the
  directory is in scope.
- `internal/adapters/runtime/docker`: TestEnsureNetworkRefusesUnmanagedExisting,
  TestEnsureVolumeRefusesUnmanagedExisting, TestImagePullAuthPullsFromPrivateRegistry,
  TestNetworkAliasResolvesInNetwork, TestOutOfBandKillSurfacesUnhealthy,
  TestPublishedPortBindsToLoopbackByDefault, TestPullPolicyNeverFailsFastOnAbsentImage
  — the `docker-conformance` suite's `-run Conformance` filter only runs
  Conformance-named tests in that directory; these are real tests in the
  same directory/package the suite's scope already claims to cover.
- `cmd/platformctl` (10): TestDockerProviderEndToEnd,
  TestDriftDetectsDebeziumConnectorConfigMismatch,
  TestDriftDetectsMariaDBReplicationCredentialMismatch,
  TestDriftDetectsRedpandaRetentionMismatch, TestExternalSourceEndToEnd,
  TestImportEndToEnd, TestRedpandaKubernetesEndToEnd,
  TestRedpandaKubernetesPortForwardEndToEnd,
  TestValidateFailsFastOnBadKubernetesContext,
  TestValidatePassesWithReachableKubernetesCluster,
  TestValidateRefusesKubernetesRuntimeWhenGateExplicitlyDisabled — no
  suite row's `-run` pattern matches these names at all.
- `internal/adapters/secrets/kubernetes` (TestResolveLiveCluster) and
  `internal/adapters/secrets/vault` (TestVaultResolve) — neither
  directory is referenced by ANY suite row.

Per the task's explicit file-ownership boundary ("keep your edits to the
script OUTSIDE the map heredoc... two provider agents will each append
suite rows... your guard test reads whatever rows exist at run time"), I
cannot fix the map itself (any heredoc edit risks conflicting with
concurrently active provider agents appending rows). Resolution: all 23
are recorded on the guard test's in-test exemption list, grouped by the
four causes above with a reason string each, exactly the escape hatch
G7's Do line describes ("or be on an explicit exemption list with a
reason"). Flagged here for the maintainer to fix the map itself in a
follow-up (each is a one-line `-run` pattern widening, not a design
problem).

## Gotchas for a resuming session

- Work ONLY in this worktree
  (`/home/cascadura/git/platformctl/.claude/worktrees/agent-aa460448ab61c2a3d`).
- docs/planning guard hook: additive edits only (doc 06 §10 append for
  `--prune`) — if it blocks even a pure append, record here instead of
  retrying more than once (per MEMORY.md).
- internal/adapters/providers/**, docs/onboarding/, README, docs/adr/018,
  connection/dataset schemas belong to other agents — read-only.
- scripts/test-impact.sh: NEVER edit inside the `suites() { cat <<'EOF' ...
  EOF }` heredoc — flags/pruning logic only, elsewhere in the file.

## Verification results (fill in as gates run)

- `go build ./... && go vet ./...`: green (post E4).
- `go test ./internal/domain/status/... ./internal/archtest/...
  ./internal/application/docsgen/... ./cmd/platformctl/...`: green
  (post E4, pre-G7).
- `gofmt -l .`: clean on all new/touched files.
- Final gates (post-G7, full tree): `gofmt -l .` empty;
  `CGO_ENABLED=0 go build -trimpath -buildvcs=false ./cmd/platformctl`
  OK; `go vet ./...` + `go vet -tags integration ./...` OK;
  `go test ./...` fully green.
- `scripts/test-impact.sh --base main` LIVE run (2026-07-20/21, log:
  scratchpad/test-impact-run.log):
  **impact: 11 selected, 11 ran, 0 deduped, 0 failed (base: main)** —
  redpanda 85.4s, cdc 173.2s, sink 158.7s, connect-ha-dlq 104.6s,
  acceptance 88.9s, lakehouse 182.0s, prometheus 14.4s, ingress 50.0s,
  blueprints 58.9s, object-store-posture 142.3s, trino 137.8s.
  Selection driver: catalog.go lives in internal/domain/status
  (SHARED_CORE), so every SHARED_CORE suite selected — not just
  gc-state-ops. No K8s leg selected (k8s-adapter scope untouched), as
  the task brief predicted.
