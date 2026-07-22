# I4: Reconcile/Probe symmetry + Connection-target ordering (doc 08 §7.8)

Branch: worktree-agent-aa3b8d094e3a2a974 (isolated worktree, main untouched).
Commit series (reviewable as two logical changes):
1. `fix(providers): reconcile verifies serving before Ready — wireguard,
   ingress-docker, proxy (I4)` — settle-polls + unit tests + e2e drift
   assertions + doc note.
2. `fix(graph): managed Connection target host orders after the in-set
   upstream it names (I4 follow-up)` — the ordering gap the settle-polls
   exposed (first sweep: ingress + lakehouse-K8s failed because a
   Connection reconciled before the Provider its target names).

## What landed (commit 1 — providers)
1. wireguard: extracted `probeTunnelServing` (shared by Probe + new
   `waitTunnelServing` settle-poll) before Ready in `reconcileConnection`.
2. ingress/docker.go: `waitRouteServing` (reuses
   `probeThroughRoute`/`probeThroughRouteTLS`) before Ready in
   `reconcileConnectionDocker`.
3. proxy: `waitForwarderServing` (reuses `probeThroughForwarder`) before
   Ready; socat self-dial HealthCheck added to ContainerSpec.
4. No new status reasons — timeouts return bare errors (redpanda
   waitTopicSettled pattern). Settle/poll durations are package vars so
   tests shrink them.
5. Unit tests: wireguard_test.go, proxy_test.go (new files),
   ingress/route_settle_test.go.
6. e2e: `drift` zero-drift assertion immediately after `apply` in
   wireguard_integration_test.go + ingress_integration_test.go (NFR-11).

## What landed (commit 2 — graph ordering fix)
`internal/domain/graph/graph.go` (Build): for each MANAGED Connection
(external: false), the `spec.target` host part (net.SplitHostPort; raw
string fallback) resolves against a `byRuntimeName` index
(naming.RuntimeObjectName per in-set resource, metadata name indexed too
against future divergence, namespace-scoped) and adds a
Connection→upstream edge. Semantics:
- Host matching nothing = external address → no edge, NO error (lenient
  where refFields is strict — deliberate).
- Self-naming target → no self-edge.
- Edge closing a loop → existing cycle detection reports it (never
  silently skipped).
Unit tests: graph_test.go `TestManagedConnectionTargetOrdersAfterNamedUpstream`,
`TestExternalConnectionTargetAddsNoEdge`,
`TestManagedConnectionTargetMatchingNothingAddsNoEdge`,
`TestManagedConnectionSelfTargetAddsNoSelfEdge`,
`TestManagedConnectionTargetCycleIsReportedNotSkipped`.

Ordering visibly changes in testdata only: ingress-scenario
(nessie→ing-test-nessie, minio→ing-test-minio), ingress-k8s-scenario,
ingress-tls-scenario (2 of 3 Connections; internal-upstream targets an
out-of-set fixture → correctly no edge), ingress-tls-k8s-scenario.
examples/ + blueprint templates: all targets are external placeholders →
no ordering change.

## Verification (re-run after the graph fix, all green)
- `gofmt -l .` empty; `go build ./...`, `go vet ./...` exit 0.
- `go build/vet -tags integration ./cmd/platformctl/...` exit 0.
- `go test ./... ; echo true-exit=$?` → true-exit=0 (unfiltered).
- `platformctl lint examples/lakehouse` unaffected.

## Impact sweep (round 2)
Round 1 results: wireguard GREEN (32.0s); ingress + lakehouse-K8s FAILED —
root cause was the pre-existing ordering gap fixed in commit 2, not the
settle-polls.
Round 2: `bash scripts/test-impact.sh --base main` now selects 16 suites
(SHARED_CORE includes internal/domain): redpanda, cdc, sink,
connect-ha-dlq, acceptance, lakehouse, prometheus, monitoring, ingress,
blueprints, object-store-posture, trino, jdbcsink, s3source, wireguard,
compose — ledger dedupes any whose scoped content-state already passed.
Launched in the background under the shared flock with
`KUBECONFIG=/tmp/claude-1000/platformctl-rbac/platformctl.kubeconfig`
(minted minimal-RBAC, never ambient admin). Log:
`/tmp/claude-1000/-home-cascadura-git-platformctl/3ff96d5f-6a0c-4676-8628-0810b1d9fe68/scratchpad/i4-test-impact-round2.log`

Orchestrator/next session: check that log's final
`impact: N selected, N ran, ...` line before merging. Per doc 06 §10 rule
7, do not `git worktree remove` this worktree while the sweep is running
(`pgrep -af <worktree-path>`).
