# I6: KubernetesRuntime GA-parity evidence — progress

Task: docs/planning/08 §7.8 I6. Test-only; no production code changes.
Final commit: one squashed
`test(k8s): chaos mid-apply-kill + Connect HA/DLQ on Kubernetes (I6)`.

## Steps

1. [done] Read task spec (doc 08 I6), Docker originals
   (`cmd/platformctl/chaos_integration_test.go`'s
   `TestChaosApplyKilledMidRun`, `cmd/platformctl/
   connect_ha_dlq_integration_test.go`'s
   `TestConnectWorkersHAAndDeadLetterQueue`), doc 06 §8 (minimal-RBAC)
   and §10 (integration economy).
2. [done] **Finding #1 (deviation clause, doc 08 §2.1):** live-probed a
   `workers: 2` debezium Provider on `runtime: kubernetes` against the
   real cluster. Confirmed `providerkit.ReachableURLs`' per-ordinal
   addressing (`runtime.OrdinalName(name, i)` -> `EnsureReachable`) has
   no Kubernetes analogue for the Deployment (`StableIdentity: false`)
   shape debezium/s3sink's `workers > 1` opts into (docs/adr/004):
   every ordinal fails to resolve, so a Binding wired to such a
   Provider fails outright at apply:
   `no member of "probe-hadbz-dbz" (2 ordinals) is currently reachable`.
   Real, currently-open production gap (both debezium and s3sink; shared
   code path). Recorded additively in docs/planning/07 (per-runtime
   differences, dated finding under the open multi-replica checkbox) and
   in doc 08 I6's Done-note. Consequence: the K8s Connect test is scoped
   to D6 (DLQ) in full + native Deployment self-heal after an
   out-of-band single-worker kill — not C3's "second worker keeps
   serving" claim. Probe env destroyed cleanly (EXIT=0).
3. [done] `cmd/platformctl/testdata/chaos-k8s-scenario/manifests.yaml` —
   K8s mirror of `testdata/cdc-scenario` (redpanda+postgres+debezium,
   single-instance — smallest CDC shape, doc 06 §10 rule 5).
   `access: node-port` throughout.
4. [done] `cmd/platformctl/testdata/connect-ha-dlq-k8s-scenario/
   manifests.yaml` — K8s mirror of `testdata/connect-ha-dlq-scenario`,
   single-worker per finding #1 (rationale in the file header).
5. [done] `cmd/platformctl/chaos_kubernetes_integration_test.go` —
   `TestKubernetesChaosApplyKilledMidRun`.
6. [done] `cmd/platformctl/connect_ha_dlq_kubernetes_integration_test.go`
   — `TestKubernetesConnectDeadLetterQueueAndWorkerResilience`.
   **Finding #2 (live, fixed in-test):** host-side produce/consume
   against the legacy single-broker redpanda shape on K8s hangs dialing
   the broker's advertised loopback sentinel
   (redpanda.advertisedAddr "127.0.0.1:<kafkaPort>", docs/adr/017
   §a.4) — metadata-only calls work via the seed broker, produce
   follows the advertised address. Fixed with the provider's own
   dialer-redirect trick (chdlqk8sRedirectDialer: every dial goes to
   the EnsureReachable-resolved tunnel address; correct for exactly one
   broker). Two live failures diagnosed to get here
   (scratchpad logs connect-ha-dlq-k8s-run1.log / -run1b.log).
7. [done] `scripts/test-impact.sh`: `chaos-k8s` row added (scope
   `internal/adapters/runtime/kubernetes
   cmd/platformctl/testdata/chaos-k8s-scenario SHARED_CORE`);
   `connect-ha-dlq` row's scope extended with
   `internal/adapters/runtime/kubernetes` +
   `cmd/platformctl/testdata/connect-ha-dlq-k8s-scenario`, -run pattern
   extended with `|TestKubernetesConnectDeadLetterQueueAndWorkerResilience`.
   `go test ./internal/archtest/...` green. `--print --base main`
   selects exactly the two suites.
8. [done] Gates: gofmt clean, `go vet ./...` clean, build (plain +
   `-tags integration`) clean, unfiltered
   `go test ./... ; echo true-exit=$?` = 0.
9. [in-progress, detached] Live evidence — ALL FOUR legs run
   sequentially by one detached script:
   **log: `i6-live-runs.log` (worktree root), nohup PID 1400362**,
   script at scratchpad/i6-live-runs.sh. Order: run 1 of both suites
   via `bash scripts/test-impact.sh --base main` (ledger-records
   greens; also re-runs the Docker connect-ha-dlq leg, same suite),
   then run 2 of each K8s test via direct
   `flock /tmp/platformctl-itest.lock go test ...` (the ledger would
   dedupe an identical suite re-run). KUBECONFIG exported to the minted
   minimal-RBAC kubeconfig. Queued behind other agents' sweeps on the
   shared flock — expected; the merge gate reads the log and
   transcribes the four timings into doc 08 I6's Done-note.
   Prior partial evidence: chaos-k8s already passed once live
   (66.63s test / 130s wall, scratchpad/chaos-k8s-run1.log) before the
   finding-#2 fix (which is outside that test's code path).
10. [done] doc 08 I6 Done-note appended (additive; passed the guard
    hook) — names both findings, the suite rows, the i6-live-runs.log
    hand-off, and the GA-decision consequence.
11. [done] Commit attempted; GPG timed out → per fallback: everything
    STAGED + `COMMIT_MSG.txt` at worktree root. Orchestrator commits
    with `git commit -F COMMIT_MSG.txt` once GPG is unlocked.

## Live-run evidence log

- chaos-k8s run 1: **PASS 66.63s** (130s wall;
  scratchpad/chaos-k8s-run1.log, EXIT=0)
- chaos-k8s run 2: **PASS 63.91s** (511s wall incl. flock queue;
  scratchpad/chaos-k8s-run2.log, EXIT=0)
  → TestKubernetesChaosApplyKilledMidRun is GREEN TWICE live already.
- connect-ha-dlq (K8s) runs 1+2: pending in `i6-live-runs.log`
  (worktree root; sections labeled, grep `exit=`) — the two earlier
  attempts failed on finding #2 (advertised-sentinel produce hang),
  fixed since; the detached script runs the fixed test. The script also
  re-runs chaos-k8s (run 1 via the impact script — ledger-records the
  green — and a direct run) — surplus evidence, harmless.
  Merge gate transcribes the final timings into doc 08 I6's Done-note.

## Deviations

- Accept's "suite rows selected by a K8s-adapter-only change
  (`test-impact.sh --print` proof)" was demonstrated structurally (both
  rows' scope columns contain `internal/adapters/runtime/kubernetes`;
  `--print` at this diff selects exactly the two rows via their testdata
  scopes) but the literal adapter-file-touch `--print` run was NOT
  performed: modifying a tracked k8s-adapter file while the detached
  evidence runs compute scope-hashes from this same worktree would
  corrupt the ledger keys being recorded. One `--print` command at the
  merge gate (touch any k8s adapter file, run, revert) completes it.
- The Docker `TestConnectWorkersHAAndDeadLetterQueue` leg re-runs inside
  run 1 (it shares the extended connect-ha-dlq suite row) — not an I6
  requirement, but the row is the unit the impact script runs.
