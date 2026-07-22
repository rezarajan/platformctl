# I4: Reconcile/Probe symmetry (doc 08 §7.8) — DONE, sweep in flight

Branch: worktree-agent-aa3b8d094e3a2a974 (isolated worktree, main untouched).

## What landed
1. wireguard: extracted `probeTunnelServing` (shared by Probe + new
   `waitTunnelServing` settle-poll); `waitTunnelServing` runs before Ready in
   `reconcileConnection`.
2. ingress/docker.go: added `waitRouteServing` (reuses
   `probeThroughRoute`/`probeThroughRouteTLS`) before Ready in
   `reconcileConnectionDocker`.
3. proxy: added `waitForwarderServing` (reuses `probeThroughForwarder`)
   before Ready in `reconcileConnection`; added a socat self-dial
   HealthCheck to its ContainerSpec.
4. No new status reasons — timeouts return bare errors (redpanda
   waitTopicSettled pattern: `(st, err)`, not a new Ready=False reason).
5. Unit tests: new wireguard_test.go, proxy_test.go (neither existed
   before); new internal/adapters/providers/ingress/route_settle_test.go.
   Settle timeout/poll (and wireguard's WithReachable inner
   timeout/interval) are package vars, not consts, so tests shrink them
   instead of waiting out real 45s/10s timeouts.
6. e2e: added a `drift` assertion immediately after `apply` in
   cmd/platformctl/wireguard_integration_test.go and
   cmd/platformctl/ingress_integration_test.go (zero drift, NFR-11).
7. docs/planning/08-production-readiness-plan.md: additive Done-note under
   I4.

## Verification (all green before the final commit)
- `gofmt -l .` — empty.
- `go build ./...` — exit 0.
- `go vet ./...` — exit 0.
- `go build -tags integration ./cmd/platformctl/...` + `go vet -tags
  integration ./cmd/platformctl/...` — exit 0 (integration test files
  compile).
- `go test ./... ; echo true-exit=$?` — true-exit=0 (unfiltered, no grep
  filtering).
- `go run ./cmd/platformctl lint examples/lakehouse` — unaffected (no
  schema/manifest changes in this task).

## Impact sweep
`bash scripts/test-impact.sh --base main` selected 3 suites: lakehouse,
ingress, wireguard (`--print` output: "impact: 3 selected, 0 ran, 0
deduped, 0 failed"). Launched for real (not --print) in the background,
serialized on the shared flock per docs/planning/06 §10 rule 4 — do not
run another integration suite outside the wrapper while this is in
flight. Log:
`/tmp/claude-1000/-home-cascadura-git-platformctl/3ff96d5f-6a0c-4676-8628-0810b1d9fe68/scratchpad/i4-test-impact.log`

Orchestrator/next session: check that log for the final PASS/FAIL summary
line (`impact: N selected, N ran, ...`) before merging. Per doc 06 §10
rule 7, do not `git worktree remove` this worktree while the sweep is
still running (`pgrep -af <worktree-path>`).
