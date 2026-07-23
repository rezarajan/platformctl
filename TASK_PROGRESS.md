# I10 + I11 (doc 08 §7.8) — task progress

Worktree: agent-aa8eb596a6e2a933a. Branch: worktree-agent-aa8eb596a6e2a933a.
Started from main @ abbbd1b (already up to date, `git merge main --no-edit`
was a no-op).

## Plan

- I10: `internal/application/manifest.FragmentCheck` (new, exported, thin
  wrapper over the existing unexported `compiledFragments`/
  `validateBlockAgainstSchema`) + `internal/archtest/fragment_completeness_test.go`
  walking cmd/platformctl/testdata/** (excl. negative-corpus), examples/**,
  internal/application/blueprint/templates/**.
- I11: `internal/application/engine.Engine.Log func(format,args)` ->
  `Logger *slog.Logger`; `logf` re-implemented on slog; new `logAction`
  helper carrying resource/action/outcome/duration attrs at the 15
  reconciliation-action call sites (Apply's processEntry + Destroy).
  cmd: `--log-format json|text` persistent flag, `cmd/platformctl/logging.go`
  (textLineHandler for byte-compatible default), `newEngine()` wires
  `Logger: newEngineLogger(os.Stderr, a.logFormat)`, `eng.Log = nil` ->
  `eng.Logger = nil` in apply's Reporter-owns-output branch.

## Status

- [x] Read docs/planning/08 I10+I11 specs, 06 §2.1/§10, fragment.go,
      engine.go logf call sites, root.go engine wiring, output-contract
      harness.
- [x] I10 implementation (`manifest.FragmentCheck` +
      `internal/archtest/fragment_completeness_test.go`)
- [x] I10 proof (deleted httpsPort from ingress fragment -> fails naming
      cmd/platformctl/testdata/ingress-tls-scenario/manifests.yaml and
      the field; restored, no diff)
- [x] I11 implementation (`Engine.Logger *slog.Logger`, `logAction`,
      `cmd/platformctl/logging.go`, `--log-format` flag,
      `(*app).newEngine(stderr io.Writer)`)
- [x] I11 cmd-level JSON test (`cmd/platformctl/logging_test.go`,
      TestLogFormatJSONEmitsStructuredEventsPerAction +
      TestLogFormatTextIsByteCompatible)
- [x] README + doc 01 NFR-12 additive note
- [x] gofmt/build/vet (both tag sets) — clean
- [x] go test ./... (unfiltered) — true-exit=0
- [x] scripts/test-impact.sh --print --base main citation — 23 selected
      (SHARED_CORE includes internal/application/engine); 17 confirmed
      k8s-free by grepping for requireK8s/k8sruntime. usages, launched in
      background (--only, nohup, log at
      scratchpad/i10-i11-itest-sweep.log). 6 excluded
      (k8s-validate, redpanda, connect-ha-dlq, lakehouse, chaos-k8s,
      ingress — all mix in K8s subtests): the minted minimal-RBAC
      kubeconfig (/tmp/claude-1000/platformctl-rbac/platformctl.kubeconfig)
      had expired and re-minting a fresh token was denied by the
      permission classifier (credential materialization) — flagged in
      the final report as a deviation, not silently worked around.
- [x] Done-notes under I10/I11 in doc 08
- [ ] final squashed commit (GPG timeout expected; staged +
      COMMIT_MSG.txt fallback)
