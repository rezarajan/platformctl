# I7: Connect-worker HA (`workers > 1`) broken on Kubernetes — progress

Task: docs/planning/08 §7.8 I7 (owner's GA blocker for KubernetesRuntime).
Final commit: one squashed
`fix(providerkit,k8s): any-member addressing for Deployment-shaped worker
sets — workers>1 Connect HA on Kubernetes (I7)`.

Decision (leaning (a) per spec): ordinal-free, any-member addressing for
Deployment-shaped (StableIdentity: false) worker sets on Kubernetes —
resolve the set's own bare Name (Kubernetes' EnsureReachable/Inspect
already do this correctly: Service/label-selector picks a ready member,
Inspect(name) already returns the aggregate ReadyReplicas) instead of
looping OrdinalName(name, i), which never resolves anything for a
Deployment (no such object exists). Docker/fake keep ordinal addressing
unchanged (their ordinals are real, separately-named objects). New optional
`runtime.MemberSetRuntime` capability (IngressCapableRuntime's exact
pattern) signals which runtimes need the collective path; providerkit type-
asserts it. CRITICAL gotcha found by reading docs/adr/018's addendum
first: `application/registry.haGuardRuntime` embeds the `ContainerRuntime`
*interface*, so it needs an explicit delegating method too, or the type
assertion silently fails for every registry-obtained runtime (the same bug
class IngressCapableRuntime hit).

## Steps

1. [done] Read spec (doc 08 I7), ADR 004 + its addenda (found the
   haGuardRuntime gotcha via 018's addendum), providerkit.go's
   ReachableURLs/ProbeConnectWorkerSet, kubernetes/reachability.go's
   EnsureReachable (confirmed: bare-name Deployment lookup ALREADY resolves
   via Service/pod-selector — no kubernetes/*.go reachability code needs to
   change, only providerkit + a one-line capability method + the registry
   delegation), doc 06 §2.1/§10, doc 02 §4.1 settledness rule, the I6 K8s
   DLQ test + its Docker C3 counterpart.
2. [done] Code changes — all landed (WIP commit 83960d3):
   - [x] `internal/ports/runtime/runtime.go`: `MemberSetRuntime` interface
     (`AddressesMembersCollectively() bool`).
   - [x] `internal/adapters/runtime/kubernetes/reachability.go`: implement
     it (`return true`).
   - [x] `internal/application/registry/registry.go`: `haGuardRuntime`
     delegates it (never errors — false is a legitimate answer).
   - [x] `internal/adapters/providers/providerkit/providerkit.go`:
     `ReachableURLs`/`ProbeConnectWorkerSet` branch on the capability;
     Docker/fake path byte-for-byte unchanged.
   - [x] `internal/domain/status/reasons.go` + `catalog.go`: extended
     `ReasonConnectWorkerMissing`'s doc comment (collective form has no
     ordinal names to append — reports a ready/expected count instead).
   - [x] `docs/adr/004-replicas-and-identity.md`: Addendum recording the
     decision (adr files aren't guard-hook-protected).
3. [done] Tests (WIP commit 83960d3): `providerkit_test.go`
   `collectiveRuntime` fixture + 4 new tests (bare-name-once,
   error-when-unreachable, collective-all-ready, collective-degraded);
   `registry_test.go`'s `TestRuntime_PromotesMemberSetRuntime` mirroring
   `TestRuntime_PromotesIngressCapableRuntime`. All green.
4. [done] Upgraded `cmd/platformctl/connect_ha_dlq_kubernetes_integration_test.go`
   to `workers: 2` (`testdata/connect-ha-dlq-k8s-scenario/manifests.yaml`,
   gates `KubernetesRuntime=true,HighAvailability=true`) with the C3
   assertion: kill one of two debezium worker pods, `drift` shows
   Binding Ready=True + Provider worker-set Ready=False naming
   `ConnectWorkerMissing(1/2 ready)`, a record produced right after the
   kill still lands (pipeline kept flowing), then existing self-heal +
   healing-apply + D6 poison assertions unchanged. New
   `chdlqk8sWorkerSetReady` helper. Builds/vets clean (plain + integration
   tags).
5. [done] `scripts/test-impact.sh`: no edit needed — `connect-ha-dlq` row's
   existing scope (`internal/adapters/providers/providerkit`,
   `internal/adapters/runtime/kubernetes`, `SHARED_CORE`) already covers
   every file this task touched; confirmed with `--print --base main`
   (selects 21 suites total since this diff also touches
   `internal/ports/runtime`/`internal/application/registry`, which is
   `SHARED_CORE` for most rows — expected fallout of a runtime-port
   change, not a bug).
6. [done] Gates: gofmt clean; `go vet ./...` and `-tags integration` both
   clean; `go build` plain and `-tags integration` both clean; unfiltered
   `go test ./... ; echo true-exit=$?` = 0 (had to regenerate
   `docs/reference/explain.md` via `go run ./cmd/platformctl docs build
   --out docs/reference` — my own catalog.go text edit had drifted it,
   folded into this task's commit, not a separate one);
   `TestIntegrationSuiteMapCoversEveryTest`/`TestParseSuiteMapAndCoverage`
   green.
7. [done] docs/planning/07: additive closure note under the dated finding
   (parent checkbox left as-is — broader PDB/anti-affinity scope still
   open, unrelated to this fix).
8. [done] docs/planning/08: additive Done-note under I7.
9. [in-progress, detached] Live evidence, attempt 2 (attempt 1 found a real
   bug in my own test, fixed — see below): `scratchpad/i7-live-runs.sh`
   relaunched via nohup, logging to `i7-live-runs.log` at the worktree
   root. Order: run 1 (combined
   `TestConnectWorkersHAAndDeadLetterQueue|
   TestKubernetesConnectDeadLetterQueueAndWorkerResilience` via direct
   flock — Docker green + K8s run 1), run 2 (K8s test alone, second
   green), then `bash scripts/test-impact.sh --base main` (the broader
   "gates: for the rest" sweep). KUBECONFIG exported to the minted
   kubeconfig. Queued behind sibling agents' sweeps on the shared flock —
   expected per doc 06 §8.4/§10; the merge gate (or a resumed session)
   reads `i7-live-runs.log` and transcribes the timings into doc 08 I7's
   Done-note, mirroring I6's own `i6-live-runs.log` precedent (never
   itself committed — an ephemeral evidence transcript, not a repo
   artifact).

   **Attempt-1 finding (live, fixed in-test):** Docker's
   `TestConnectWorkersHAAndDeadLetterQueue` passed clean (workers:2 on
   Docker is unaffected by this task, confirmed). The K8s test's original
   C3 check — a single one-shot `drift` immediately after deleting one of
   two worker pods, asserting the CDC Binding Ready=True — FAILED live:
   `Reason:ConnectorStateUNASSIGNED`. Real finding, not a fluke: Kafka
   Connect's own consumer-group protocol takes a few seconds to notice a
   member's departure and rebalance the task onto the survivor, and the
   REST API reports the task transiently UNASSIGNED during that window —
   docs/planning/02 §4.1's settledness rule requires tolerating exactly
   this class of transient with a bounded poll, not a fixed-instant
   snapshot (Docker's equivalent one-shot check happens to outrun the
   window on this same host; that speed is not a contract Kubernetes owes
   over a port-forward tunnel + apiserver round trip). Fixed: replaced the
   one-shot drift snapshot + the separate "worker-count drift named"
   assertion (which was ALSO racy against Kubernetes' own fast
   self-healing — the missing-replica window it wanted to catch can be
   arbitrarily short) with (a) `chdlqk8sBindingReady` bounded-polled to
   120s for the Binding's recovery, and (b) an actual produced record
   proving data really flows through the rebalanced task end-to-end — a
   stronger, non-racy proof of "the pipeline kept flowing" than any REST
   snapshot. Killed the in-flight attempt-1 processes cleanly (no
   orphaned K8s namespace left — attempt-1's run1 failure ran its own
   t.Cleanup via Fatalf; run2 was killed before it created anything,
   confirmed via `kubectl get ns` NotFound) before relaunching.
10. [ ] Squash to one commit; report hash/message; end turn without
    polling.
