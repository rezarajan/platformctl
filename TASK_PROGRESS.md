# C2 — Redpanda multi-broker clusters and replicated topics: progress

**STATUS: COMPLETE.** All steps done, full verification green on both
runtimes; squashed into the single task commit (subject
`feat(redpanda): multi-broker clusters and replicated topics (C2)`).
This file is the doc-08 §2.1 step-0 checkpoint artifact for the task.

Task: docs/planning/08 §5 C2. Branch: this worktree. Resume from here + `git log`.

## Step plan and status

1. **[done] Merge main (pre-work)** — fast-forwarded to db88965 (G3/G5 splits)
   at session start. NOTE: main has since moved again (D2 merged, touches
   `internal/application/engine/engine.go` resolveSchemaRegistryURL +
   `internal/application/compatibility/compatibility.go`); a second
   `git merge main --no-edit` is REQUIRED before final verification —
   resolve keeping both D2's and C2's compatibility changes (C2 added the
   `EventStream` case in `checkResourceCapabilities` + the
   `streamReplicationStub` tests).
2. **[done] ADR 017** — `docs/adr/017-redpanda-multibroker-and-replica-state.md`
   (+ index row in docs/adr/README.md). Key decisions:
   - `brokers` declared (any N>=1) opts into ordinal-set shape
     (`Replicas: N, StableIdentity: true`); unset keeps legacy single
     container byte-for-byte. 1→3 = same-shape in-place scale; legacy→brokers
     = shape transition, refused by C1 guards (destroy/recreate).
   - Port amendment (§a.2): StableIdentity selects the set shape at
     ReplicaCount()==1 too. Conformance pin
     `ReplicaSet_ShapeTransition_Refused` amended (collapse target is now
     StableIdentity:false), new pins `ReplicaSet_StableIdentitySingleOrdinal`,
     `ReplicaSet_SingleToSetRefused`, `ReplicaSet_OrdinalInNetworkDNS`,
     `ReplicaSet_EntrypointReplaces_OnSet` (all in
     internal/ports/runtime/conformance/{replicas,entrypoint}.go).
   - 3→1 scale-down refused at reconcile (no destructive-flag plumbing —
     recorded §a.5). Observed-ordinal probe, not state.
   - State (question b): NO per-ordinal state entries; aggregate
     ResourceState + additive providerState (brokers, comma internalAddr,
     per-ordinal `kafka-<i>` endpoint facts). No state version bump.
   - Gate at validate: `checkHighAvailabilityGate` in cmd/platformctl/root.go
     (loadAndValidate), NOT inside SpecValidator (no gate access — recorded
     §a.8 as the deviation from doc 08's literal wording; closes ADR 004's
     deferred accept line).
3. **[done] Runtime/adapters** — dispatch `StableIdentity` → set path at any
   count: docker.go, fake.go, kubernetes/container.go; mirror single→set
   guards (docker/fake); k8s statefulset.go now maps Entrypoint→Command
   (live-caught bug, pinned); k8s container.go `ensureOrdinalServices`
   (per-ordinal ClusterIP Service, selector statefulset.io/pod-name,
   PublishNotReadyAddresses — makes short ordinal DNS real on K8s, ADR 004's
   claim; live-caught).
4. **[done] Domain/schema/docs** — eventstream.Replication (+
   ReplicationFactor()), schemas/v1alpha1/{eventstream,provider}.json,
   docs/planning/03 additive blocks (Provider brokers para + EventStream
   replication comment incl. odd-factor note), docs/reference regenerated
   (`go run ./cmd/platformctl docs build --out docs/reference`) — MUST
   re-regen after the last eventstream.json edit (odd-factor sentence) —
   [next].
5. **[done] Provider** — redpanda.go: brokersDeclared, clusterCmdScript
   (bash -c, HOSTNAME-derived node-id, ordinal-0 seed), reconcileBrokerSet
   (scale-down refusal, EnsureContainer StableIdentity, waitClusterFormed,
   brokerSetProviderState), probeBrokerSet (BrokerMissing/BrokerNotJoined),
   topicDial/clusterDial, Destroy via ListManagedVolumes prefix match,
   KafkaBootstrapAddress comma list, ValidateStreamReplication (> brokers +
   even-factor refusal), ValidateSpec (brokers int>=1, port-pin +
   schemaRegistry refusals). kafka.go: adminClient dial-MAP + seeds,
   ensureTopic RF + INVALID_REPLICATION_FACTOR bounded retry, probeTopic RF,
   countJoinedBrokers. reasons.go: BrokerMissing, BrokerNotJoined,
   ReplicationFactorMismatch.
6. **[done] Capability + compatibility** — reconciler.StreamReplicationValidator;
   compatibility.go EventStream case; stub tests in compatibility_test.go.
7. **[done] Gate at validate** — checkHighAvailabilityGate + call in
   loadAndValidate; tests cmd/platformctl/ha_gate_test.go (4 tests, green).
8. **Verification so far (all green unless noted):**
   - gofmt/build/vet/`go test ./...` green (before the odd-factor edit —
     re-run pending).
   - Docker conformance (integration) green ×3 runs incl. all new pins (15s).
   - K8s conformance green ×2 under minimal-RBAC kubeconfig (320s, 365s) —
     kubeconfig at scratchpad/platformctl.kubeconfig (mint per
     deploy/kubernetes/rbac/README.md; minikube CA at
     ~/.minikube/ca.crt, token via `kubectl create token platformctl -n
     platformctl-system`).
   - **Docker HA e2e GREEN**: cmd/platformctl/redpanda_ha_integration_test.go
     `TestRedpandaHAEndToEnd` (16.5s): 3-broker Ready, RF-3 via admin API,
     produce/consume before/during/after out-of-band broker kill,
     BrokerMissing drift naming ordinal, re-apply heal, idempotent re-apply
     "no changes", clean destroy (containers+volumes+network).
   - **K8s HA e2e GREEN at brokers:3/replication:3** (69s, minimal-RBAC
     kubeconfig; minikube fits 3 brokers): STS cluster Ready, RF-3 verified
     via admin API over per-ordinal port-forwards, produce/consume
     before/during/after out-of-band pod delete (StatefulSet controller
     heals — documented per-runtime difference), idempotent re-apply "no
     changes", clean destroy. Three live-caught bugs were fixed en route:
     (i) statefulset.go dropped Entrypoint→Command; (ii) ordinal short-name
     DNS needs per-ordinal Services; (iii) redpanda refuses EVEN replication
     factors ("must be odd") → validate refusal added; K8s scenario must be
     brokers:3/replication:3 (2/2 can NEVER work) — manifest+test consts
     still say 2 → [next].
9. **[done] a–c**: scenario at 3/3, docs regenerated, unit suite green,
   K8s e2e green (see above).
   **[status of d–e]**
   d. [done, commit dba1d7c] main (D2) merged clean; both compatibility
      features verified present; unit suite green post-merge.
   e. [DONE — GREEN] Full integration sweep: run 3 (clean env, fresh 6h
      token) EXIT=0, all 39 packages ok — cmd/platformctl 898s (whole
      acceptance/chaos/lakehouse/CDC/sink suite incl. both new HA e2e
      tests), runtime/kubernetes 390s (conformance incl. new pins, minimal
      RBAC), runtime/docker re-run uncached green (20s). gofmt clean,
      go vet (both tag sets) clean. Runs 1–2's three failures were
      environmental (load flake / expired token / daemon contention) and do
      not reproduce: full log at scratchpad/sweep-full.log. Historical
      detail of run 1–2 triage below.
      [superseded triage] Full integration sweep. Run 1 (concurrent-load): all
      packages green EXCEPT k8s TestVolumeSizingAndStorageClass (passes
      standalone — load flake) and cmd/platformctl verdict truncated. Run 2
      (cmd only, later): 3 fast (~1.4s) failures — K8s example test (token
      expired mid-suite: 4h TTL) and both sink tests (docker build of
      s3sink image failed transiently; builds fine standalone). Token
      re-minted (6h). Run 3 (authoritative, clean env, full ./...) writing
      complete log to scratchpad/sweep-full.log — check `EXIT=` line and
      non-ok package lines; if only environmental flakes recur, rerun the
      affected tests individually to prove them green before judging.
   OLD-d. `git merge main --no-edit` (D2) — resolve compatibility.go keeping both.
   e. Full sweep: gofmt/build/vet, `go test ./...`,
      `just test-integration` with KUBECONFIG=minted (never ambient admin).
   f. doc-08 C2 status note (additive insertion after C2 Accept block) —
      include: gate-at-validate closure note, K8s leg sizing/status, the
      three live catches; do NOT tick stage exit criteria.
   g. Final commit, subject:
      `feat(redpanda): multi-broker clusters and replicated topics (C2)`
      body: what was verified + deviations (SpecValidator-gate wording §a.8;
      1→3 semantics per ADR 017 §a.1) + note C1's deferred gate-at-validate
      accept line closed. Trailer: Co-Authored-By: Claude Fable 5
      <noreply@anthropic.com>. If GPG times out: leave staged, write
      COMMIT_MSG.txt, report.

## Gotchas for a resuming session

- Work ONLY in this worktree; run tests here (cwd resets here).
- docs/planning guard hook: pure insertions only (never touch an existing
  line, incl. lines added earlier in this same uncommitted diff).
- K8s test runs MUST use KUBECONFIG=scratchpad/platformctl.kubeconfig
  (doc 06 §8 rule 4 — never ambient admin); token may expire (4h) — re-mint.
- The engine's spec-hash no-op means adapter-template fixes don't propagate
  to an existing STS: delete the namespace before re-testing.
- runDrift in chaos_integration_test.go has no --feature-gates; HA test uses
  its own runDriftGated.
