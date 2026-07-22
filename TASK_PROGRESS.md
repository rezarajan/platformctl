# Hygiene batch: golangci-lint adoption + G7 exemption absorption — progress

Task: docs/planning/11 recorded follow-ups (golangci-lint adoption;
docs/planning/08 G7 exemption-list absorption). Final commit: one squashed
`chore(quality): adopt tuned golangci config + absorb impact-map exemptions
(doc 11 follow-ups, G7)`.

## Part 1 — golangci-lint adoption

1. [done] Read .claude/rules/go-style.md, docs/planning/11 (no verbatim
   "43 defer-close"/"ST1005" text found in doc 08/11/ADRs — treated the
   task's framing as authoritative; independently re-derived and verified
   both claims against the real repo: 44 `defer close<X>()` reachability-
   teardown closures across the provider adapters (errcheck can't name
   these — no exclude-functions target — count grows to 47 with jdbcsink/
   nessie additions from wave 2/3), and 100+ `fmt.Errorf("<Kind> %q: ...")`
   call sites in internal/domain confirming the ST1005-violating
   capitalized-Kind-prefix convention is real and repo-wide.
2. [done] Installed golangci-lint v2.12.2 via `go run .../v2/cmd/golangci-lint@latest`
   (network available). Probed with `max-issues-per-linter: 0` /
   `max-same-issues: 0` FIRST — the tool's own default caps (50/3) were
   silently truncating the true finding count (337 vs the visible ~46-50);
   final config also sets these to 0 permanently so CI never hides
   findings past the default cap.
3. [done] Full triage of all 337 raw errcheck hits + govet/staticcheck/
   unused/ineffassign, categorized by hand (not by pattern-matching the
   message text alone — verified each category's Go types/return
   signatures with `go doc`/reading the actual call sites):
   - fmt.Fprint/Fprintf/Fprintln (131): CLI/HTTP output writes → exclude-functions.
   - database/sql, pgx.Conn, io.ReadCloser, net.Conn, minio.Object Close (57): →
     exclude-functions, matched by **interface/concrete type**, never
     `io.Closer` broadly (would've also swallowed os.File writes).
   - close<X>()/closeFn() reachability-teardown closures (64, incl. test):
     no resolvable static name for exclude-functions (local `func() error`
     vars) → a `source`-regex exclusions rule scoped to the exact naming
     convention, not "any unnamed return".
   - _test.go-only Close/Unsetenv (79 named + the closures above): → a
     path-scoped exclusions rule for errcheck only (govet/staticcheck/
     unused/ineffassign still fire in tests).
   - os.File Close/Remove in localfile.go + compose/patch.go (5): NOT
     excluded (write-path). localfile.go's 4 are cleanup-after-an-
     already-returned-error → `_ = ` blank-assign, matching the file's own
     existing `_ = d.Sync()` idiom. compose/patch.go's 1 was a REAL GAP
     (the final Close on the WRITE-success path was silently dropped,
     unlike localfile.go's own established convention of checking that
     exact Close) — fixed to check-and-return, cited prominently below.
   - staticcheck: ST1005 disabled (repo convention, cited in the config
     with concrete examples) via `checks: [-ST1005]` — deliberately NOT
     `checks: [all, -ST1005]`, which pulled in ST1000/1003/1021/1022 (21
     new, unrelated stylistic findings) that golangci-lint's own default
     staticcheck set doesn't enable.
   - Real fixes applied (not config tuning): 4x S1016 struct-literal→type-
     conversion (archview.go, engine.go, plan.go, graph.go — verified
     ObserverRef/NameRef are field-identical); SA4004 in s3/bucket.go
     (redundant `break` after a MaxKeys:1 range — deleted, behavior
     unchanged); S1005 blank-discard in manifest/schema.go; QF1012
     Sprintf-into-WriteString in docsgen.go; 2x ineffassign (endpoint_test.go,
     gc_integration_test.go) — both genuinely dead assignments, fixed.
   - Judgment-call nolints (not config, too narrow/contextual for a repo
     rule): 3x SA4000 "identical expressions" — all deliberate same-input-
     twice determinism tests, not copy-paste bugs; 2x ST1008 "error not
     last" on the `run(t, ...) (string, error, int)` test helper — 128
     call sites, reordering is out-of-scope churn; 2x QF1001 De Morgan's
     rewrites on test boolean assertions — left as-is, the negated form
     reads better for "only this one case is denied/out-of-order".
   - QF1008 (remove embedded `.Fake` selector) applied directly, 2 sites.
4. [done] `.golangci.yml` written (v2 format), full repo run: **0
   issues**, both default and `--build-tags=integration` (config's
   `run.build-tags: [integration]` makes the plain `golangci-lint run`
   invocation cover integration-tagged files too — verified via `-v`
   showing "Using build tags: [integration]").
5. [done] REAL BUG FOUND AND FIXED (flagging per instructions — this is a
   genuine gap, not style): `internal/application/compose/patch.go`'s
   `Write()` appended pending .env keys via `f.WriteString` (checked) then
   `defer f.Close()` (UNCHECKED) — inconsistent with
   `internal/adapters/state/localfile/localfile.go`'s own established
   convention of checking exactly this final success-path Close. Fixed to
   check-and-return the Close error, matching localfile.go.
6. [done] Also fixed in passing (found via SA1019, a genuine upstream
   deprecation, not a repo-convention conflict): `internal/adapters/
   runtime/docker/{docker,image}.go` imported `github.com/docker/docker/
   errdefs` (deprecated in favor of `github.com/containerd/errdefs`,
   already an indirect dependency at the same v1.0.0) — swapped both
   imports, `go mod tidy` promoted it to direct. Same functions
   (IsNotFound/IsConflict), verified signatures via `go doc` before
   swapping. Build/vet/tests green after.
7. [done] `.claude/rules/go-style.md` updated to reflect the new
   `.golangci.yml` (no longer says "no committed config").
8. [done] `.github/workflows/ci.yml`: new `lint` step in the `unit` job
   running the pinned golangci-lint version (matches step 2's version).

## Part 2 — G7 exemption absorption

9. [done] Read the 11-entry exemption list (internal/archtest/
   test_impact_completeness_test.go) + scripts/test-impact.sh's full
   suite map. For each entry, widened an existing row's `-run` or added a
   new row (never left an exemption in place — every 2026-07-22 entry
   turned out mappable):
   - state-s3: widened `-run` to add TestConformance|TestForceUnlock|
     TestLockReclaimsAfterExpiry (the filter previously applied uniformly
     to both target dirs, missing the s3 package's own tests).
   - docker-conformance: widened `-run` to add the 6 non-Conformance-named
     tests in the same package/dir already in scope.
   - cdc: widened `-run` to add TestDriftDetectsDebeziumConnectorConfigMismatch|
     TestDriftDetectsMariaDBReplicationCredentialMismatch (Docker-only,
     verified no requireK8s/K8s dependency in the test file).
   - redpanda: widened `-run` to add TestDriftDetectsRedpandaRetentionMismatch|
     TestRedpandaKubernetesEndToEnd|TestRedpandaKubernetesPortForwardEndToEnd;
     scope gained the two new K8s testdata dirs these tests actually use.
   - NEW row `docker-acceptance` (internal/adapters/runtime/docker +
     placeholder provider + testdata/docker-scenario): TestDockerProviderEndToEnd
     (the Phase-1 exit-criteria acceptance test — semantically doesn't fit
     any existing row, so a new row beats a forced widen).
   - NEW row `k8s-validate` (internal/adapters/runtime/kubernetes +
     featuregate + rbac): the three TestValidate*Kubernetes* tests
     (`validate` command's K8s-context/gate behavior — cmd/platformctl-
     scoped, so can't just widen k8s-adapter's own dir-only row).
   - k8s-adapter: added `internal/adapters/secrets/kubernetes` to both
     scope and the `go test` dir list (no -run filter on this row already,
     so TestResolveLiveCluster is now covered without a separate filter).
   - NEW row `secrets-vault` (internal/adapters/secrets/vault):
     TestVaultResolve — self-contained Docker-based Vault dev-server test,
     its own row since nothing else touches that package.
10. [done] `integrationTestExemptions` emptied to `map[string]string{}`
    (kept, not deleted, per its own doc comment, as the documented home
    for the next genuinely-unmappable entry) — every 2026-07-22 entry
    absorbed, none left. Before: 11 entries (in 4 clusters). After: 0.
11. [done] `go test ./internal/archtest/...` green
    (TestIntegrationSuiteMapCoversEveryTest + TestParseSuiteMapAndCoverage
    + the package's other guards). `bash scripts/test-impact.sh --print
    --base main` parses clean (26 suite rows now, up from 24).
12. [done] Live verification, flock-wrapped on the shared
    `/tmp/platformctl-itest.lock` (queued behind other concurrently
    active agents' suites — doc 06 §10 rule 4), against
    KUBECONFIG=/tmp/claude-1000/platformctl-rbac/platformctl.kubeconfig
    for the K8s legs. Required (widened/new rows with a K8s leg):
    - `k8s-validate` (new row): PASS, 7.635s
      (g7-live-secrets-k8s.log's sibling — see g7-live-*.log at worktree
      root; not committed, matches the repo's existing i6-live-runs.log
      precedent of leaving evidence logs untracked).
    - `redpanda` widen (TestDriftDetectsRedpandaRetentionMismatch +
      TestRedpandaKubernetesEndToEnd + TestRedpandaKubernetesPortForwardEndToEnd):
      PASS, 51.1s total.
    - `k8s-adapter`'s added `internal/adapters/secrets/kubernetes` dir
      (TestResolveLiveCluster): PASS, 0.041s.
    Bonus (Docker-only; task's gate only required citing CI's full sweep
    for these, but Docker was free so ran them anyway for stronger
    evidence):
    - `cdc` widen (2 drift tests): PASS, 35.5s.
    - `docker-conformance` widen (7 tests): PASS, 23.0s.
    - `docker-acceptance` (new row): PASS, 0.55s.
    - `secrets-vault` (new row): PASS, 1.80s.
    - `state-s3` widen (TestConformance/TestForceUnlock/TestLockReclaimsAfterExpiry
      + the pre-existing TestSharedState* pair, all in one row): PASS,
      4.6s (s3 package) + 2.4s (cmd/platformctl).
    All 8 suite runs green — every widened/new G7 row verified live, not
    just cited.

## Gates run so far

- gofmt clean (`gofmt -l .` empty)
- `go build ./...`, `go vet ./...`, `go vet -tags integration ./...`,
  `go build -tags integration ./...` all clean
- `golangci-lint run` (plain; config's `run.build-tags` covers
  `-tags integration` files too, verified via `-v`): 0 issues, re-checked
  immediately before commit with the pinned version (v2.12.2)
- UNFILTERED `go test ./... ; echo true-exit=$?` = 0, re-checked
  immediately before commit (after all suite-map/archtest edits)
- `go test ./internal/archtest/...` green (completeness guard +
  self-proof tests); `bash scripts/test-impact.sh --print --base main`
  parses (26 suite rows)
- K8s-leg live suite runs: all green (see step 12)

## Done

All gates green; squashed commit follows this checkpoint.
