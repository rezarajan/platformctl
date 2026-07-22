# I5 — Duplication debt with drift risk (doc 08 §7.8)

Two behavior-preserving refactors. See docs/planning/08-production-readiness-plan.md §7.8 I5.

## Step 0 — setup
- [x] `git merge main --no-edit` — merged cleanly (fast-forward-ish merge
      commit onto b3a1f21, brought in B4 audit batch + I4 doc updates).
- [x] Read doc 06 §2.1 (bookkeeping), §10 (integration economy).

## Step 1 — debezium↔jdbcsink resolution dedup
- [x] Diffed the two ~60-line blocks byte-for-byte
      (debezium.go:283-345, jdbcsink.go:312-378). Findings:
      - variable naming (src/srcEnv vs tgt/tgtEnv) — cosmetic only.
      - config key: `replicationSecretRef` vs `credentialsSecretRef` —
        the expected, spec-called-out divergence.
      - error message text differs (jdbcsink's mentions "target" and
        `b.TargetRef`; provider-name and config-key literals differ) —
        cosmetic, not a logic/behavior divergence.
      - NO semantic/behavioral divergence found beyond the above — no
        drift bug to report.
- [x] Added `internal/adapters/providers/providerkit/endpoint.go`:
      `ResolveEndpoint` (dbHost/dbPort/preflight/connSecretRef) +
      `ResolveEndpointCredentials` (connSecretRef-then-provider-key
      fallback). Both return `ok bool`, no error — callers keep their
      own caller-specific error wording verbatim (zero text change).
- [x] `internal/adapters/providers/providerkit/endpoint_test.go` — unit
      tests covering every branch of the fallback chain.
- [x] debezium.go/jdbcsink.go call the new helpers; local per-file logic
      removed.

## Step 2 — ProbeReachable machinery dedup
- [x] New package `internal/adapters/runtime/probe` — pinned image
      constant, dial script, exec-then-ephemeral-probe algorithm shape,
      ctx-aware `Dialable` (docker's semantics — the keeper).
- [x] docker.go: switched to `probe.Dialable` (byte-identical behavior).
- [x] reachability.go (kubernetes): switched to `probe.Dialable` — this
      is the one deliberate behavior change (was ctx-ignoring/hardcoded
      2s, now ctx-aware/deadline-capped, matching docker).

## Step 3 — verification
- [x] gofmt / go vet / go build (plain + `-tags integration`) — clean.
- [x] `go test ./... ; echo true-exit=$?` — true-exit=0.
- [x] `internal/archtest` suite green (incl. TestIntegrationSuiteMapCoversEveryTest
      after the test-impact.sh scope edit).
- [ ] `bash scripts/test-impact.sh --base main` under
      `KUBECONFIG=/tmp/claude-1000/platformctl-rbac/platformctl.kubeconfig`
      — launched in background, log at
      /tmp/claude-1000/-home-cascadura-git-platformctl/3ff96d5f-6a0c-4676-8628-0810b1d9fe68/scratchpad/i5-test-impact.log
- [x] Additive Done-note under I5 in doc 08.
- [ ] Final commit: GPG signing timed out twice (both WIP attempt and
      final attempt). Per task fallback instructions: all changes are
      `git add -A` staged, and the intended commit message is written to
      COMMIT_MSG.txt in the worktree root. Next session: `git commit -F
      COMMIT_MSG.txt` once GPG is unlocked (per user memory: "GPG lapses
      periodically — user unlocks"), then verify `git log -1` and rm
      COMMIT_MSG.txt.

## Step 4 — report
- [x] Final commit hash (or COMMIT_MSG.txt fallback), helper signatures,
      divergence findings (none beyond the expected config-key name),
      suite timings, deviations — see final assistant report.
