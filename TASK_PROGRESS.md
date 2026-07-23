# H8 (doc 08 §7.7, ADR 027) — task progress

Worktree: agent-a076d087439176c47. Branch: worktree-agent-a076d087439176c47.
Started from main @ abbbd1b; `git merge main --no-edit` fast-forwarded to
7bc49d4 (brought in ADR 027 itself + doc 08 H8 spec).

## Plan

- `runtime.IsolationObserver` optional capability (tri-state
  Enforced/NotEnforced/Unknown, never a hard error).
- Kubernetes: productize TestNetworkPolicyEnforcementIsLive against
  already-managed namespaces (never scratch ones).
- Docker: constant Enforced, no probe.
- `application/registry` haGuardRuntime delegation (registry-promotion
  gotcha) + test.
- cmd/platformctl: `observeIsolation` helper (dedup per runtime config,
  Kubernetes-only to avoid noise), wired into apply-preflight/drift/
  status/inventory; NOT validate (offline by contract).
- explain-catalog tokens + docs/reference regen; onboarding claims table.
- Docker unit test; Kubernetes integration test (reuses
  PLATFORMCTL_REQUIRE_NETPOL_ENFORCEMENT, already in CI's k8s adapter
  shard — no ci.yml change needed).

## Status

- [x] Read ADR 027, doc 08 H8, runtime.go, kubernetesPreflight/
      loadAndValidate, registry.go delegation pattern,
      networkpolicy_integration_test.go, RBAC role.yaml,
      state.State/ResourceState shape (decided: no state persistence —
      in-process memo only, per "cache the observation per-command-run").
- [x] `internal/ports/runtime/isolation.go` (IsolationObserver,
      IsolationStatus, Isolation* constants)
- [x] `internal/adapters/runtime/kubernetes/isolation.go`
      (ObserveIsolationEnforcement: walledManagedNamespaces, canary
      listener, ProbeReachable same/cross-namespace, unconditional
      WithoutCancel cleanup)
- [x] `internal/adapters/runtime/docker/isolation.go` (constant Enforced)
- [x] `internal/application/registry/registry.go` haGuardRuntime
      delegation + `TestRuntime_PromotesIsolationObserver`
- [x] `internal/adapters/runtime/docker/isolation_test.go` (unit)
- [x] `internal/adapters/runtime/kubernetes/isolation_integration_test.go`
      (TestObserveIsolationEnforcement)
- [x] reasons.go + catalog.go isolation tokens (Kind: "reason", real
      Reason* constants — archtest's two-way completeness check requires
      this); `archtest:allow-reason-literal` markers on the two literal
      `Reason:` string sites the G4 scanner flagged (IsolationStatus.Reason
      is free text, not a status.Condition reason)
- [x] `cmd/platformctl/isolation.go` (observeIsolation + printIsolationNotes
      + isolationReasonToken)
- [x] wired into apply (preflight, pre-confirmation), drift, status,
      inventory; validate deliberately excluded (documented deviation)
- [x] docs/reference regenerated (`go run ./cmd/platformctl docs build
      --out docs/reference`) — explain.md +44 lines only
- [x] docs/onboarding/users.md additive "Network isolation" subsection
      under Runtimes (claims table)
- [x] deploy/kubernetes/rbac/role.yaml comments (no new verbs needed —
      confirmed against internal/adapters/runtime/kubernetes/preflight.go's
      own verb list)
- [x] gofmt clean; go build/vet both tag sets clean
- [x] golangci-lint v2.12.2 clean on every touched package (repo-wide run
      shows one PRE-EXISTING unused-func finding in engine.go:119,
      confirmed via git stash — not mine, not touched)
- [x] `go test ./... ; echo true-exit=$?` = 0 (unfiltered) — caught and
      fixed one real regression: TestNoopEndToEnd broke because
      observeIsolation originally probed every declared runtime type
      including "fake"/non-network ones, printing a stdout line the
      test's per-line status-table assertion choked on; fixed by scoping
      observeIsolation to Kubernetes-runtime Providers only (documented
      as a deliberate scope decision, not silently patched around)
- [x] Live minikube evidence (not-enforced leg — the accept bar):
      `KUBECONFIG=/tmp/claude-1000/platformctl-rbac/platformctl.kubeconfig
      go test -tags integration -run TestObserveIsolationEnforcement -v
      ./internal/adapters/runtime/kubernetes/...` → PASS, reported
      not-enforced (cross-namespace dial succeeded through the wall).
      Also ran `platformctl status` against a real k8s-runtime manifest
      live (go run ./cmd/platformctl status ...) proving the CLI-level
      wiring reaches the cluster; that particular run reported Unknown
      because a concurrent process (another agent sharing this minikube)
      tore down one of the two walled namespaces between check-ins,
      leaving only 1 — the honest Unknown path, not a bug. Did not
      attempt a live `apply` against the shared cluster: the auto-mode
      permission classifier denied `--auto-approve apply` against shared
      infra (see final report deviations) — not worked around.
- [x] Done-note under H8 in doc 08 (additive)
- [x] scripts/test-impact.sh --base main launched in background
      (nohup, log at /tmp/claude-1000/h8-impact-sweep.log) — 22 suites
      selected (broad, root.go touched); not polled per standing agent
      rule (orchestrator owns merge gates)
- [ ] final squashed commit (GPG timeout expected; staged +
      COMMIT_MSG.txt fallback if so)
