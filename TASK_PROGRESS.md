# C4 (object-store production posture) + D7 (Dataset lifecycle) — progress

Task: docs/planning/08 §5 C4 + §7(D) D7. Branch: this worktree
(worktree-agent-a9b28e633efb9f169). Resume from here + `git log`.

(Replaces this file's previous contents, which were the already-merged C2
task's checkpoint — see `git log --oneline -- TASK_PROGRESS.md` /
`ff16dae`/`b9edeb8` for that history if ever needed.)

Bundled because they own the same files (internal/adapters/providers/s3,
internal/domain/dataset, schemas/{dataset,provider}.json).

## Design decision (recorded per task instructions)

**No ExternalConfigurer needed for the Dataset half of C4.** Studied doc 03
§3.3's table + ADR 005's "already fully supported" precedent (external
Source + CDC works today with zero ExternalConfigurer, because the Binding
referencing the external Source is itself NOT external — only the Source
it targets is). Mirrored exactly for s3:

- `Provider(type: s3, external: true, connectionRef: ...)` — the PROVIDER
  is external. Per doc 03 §3.3's Provider row, this always takes the
  generic no-provider path (`engine.reconcileExternal` / `isExternalNoProvider`)
  — connectionRef reachability verified, s3's own `Reconcile`/`Probe`/
  `Destroy` for kind "Provider" are never even called. No code needed here;
  already fully generic.
- `Dataset(providerRef: <that external Provider>, bucket, format)` — the
  DATASET itself is NOT external (no `external: true` on the Dataset). It
  therefore takes the ORDINARY (non-external) reconcile path — `s3.Provider
  .Reconcile`'s `case "Dataset"` — exactly like any managed Dataset. Because
  doc 03 §3.3's "external+providerRef requires ExternalConfigurer" rule
  is keyed off the DATASET's own `external` flag, not its realizing
  Provider's, this combination needs no capability gap closed at all.
- The only real code gap: `reconcileDataset`/`Probe`/`Destroy` (Dataset
  case) assumed a managed running container reachable by
  `naming.RuntimeObjectName(req.Provider)`. Teaching them to detect
  `cfg.External` on `req.Provider` and, when true, resolve the S3 endpoint
  + credentials from the Provider's own `connectionRef` (a Connection or
  bare SecretReference, resolved from `req.Resources` — mirrors exactly how
  `debezium.buildDesiredConnector` resolves an external Source's
  `connectionRef`, the proven in-repo precedent) is the actual substance of
  this half.
- s3sink Bindings: already fully supported via existing
  `options.endpoint` + `configuration.credentialsSecretRef` — zero s3sink
  code changes needed (file-ownership boundary respected; verified by
  reading, not editing, s3sink.go's `objectStoreEndpoint`).

This closes doc 03 §3.3's capability-gap note only insofar as the Dataset
side never needed it; the gap itself (no shipped ExternalConfigurer
implementor) remains open — recorded, not silently worked around.

## Step plan and status

1. [done] Merge main — already up to date (this worktree's branch base
   already includes C2/redpanda multi-broker at b9edeb8; no new merge
   needed — verified `git merge main --no-edit` says "Already up to date").
2. [done] Read: doc 08 C4/D7 entries, doc 03 §3.3 + §4 (Provider) + §8
   (Dataset), ADR 005, reconciler.go (ExternalConfigurer + Request),
   engine.go (isExternalNoProvider/reconcileExternal/
   reconcileExternalWithProvider), debezium.go's external-Source connectionRef
   resolution (the precedent), redpanda.go in full (StableIdentity
   brokers pattern — the template for minio `nodes`), s3.go/bucket.go,
   dataset.go, provider.go, connection.go, root.go's
   checkHighAvailabilityGate, minio-go v7 lifecycle/versioning API,
   s3sink.go's objectStoreEndpoint (read-only, not owned).
3. [in-progress] Implement:
   - [done] `internal/domain/provider`: External/ConnectionRef fields
     (mirrors source.Source). `go test ./internal/domain/...` green.
   - [done] `schemas/v1alpha1/provider.json`: connectionRef required when
     external (allOf, mirrors dataset.json/connection.json); `nodes`
     documented in configuration description.
   - [done] `internal/adapters/providers/s3/s3.go`: external-Provider dataset
     addressing (resolveDatasetDial/externalStoreDial helpers); `nodes`
     StableIdentity multi-node MinIO (reconcileInstanceSet/probeInstanceSet,
     ValidateSpec refusing 2-3/port-pins, Destroy volume cleanup); dispatch
     wiring in Reconcile/Probe/Destroy for all three shapes (legacy,
     node-set, external). backup.go's newClient calls updated for the new
     `secure bool` param.
   - [done] `internal/adapters/providers/s3/bucket.go`: lifecycle rule +
     versioning ensure (`ensureLifecycle`)/diff (`probeLifecycleDrift`) via
     minio-go lifecycle API (read-modify-write, preserves sibling Datasets'
     rules on a shared bucket); `ensureBucketAt` (external, single-shot,
     mirrors managed `ensureBucket`'s wait-then-create).
   - [done] `internal/domain/status/reasons.go`: ReasonLifecycleRuleDrift,
     ReasonVersioningDrift, ReasonNodeMissing, ReasonNodeUnreachable.
   - [done] `cmd/platformctl/root.go`: checkHighAvailabilityGate generalized
     to `haReplicaFields = []string{"brokers", "nodes"}`.
   - [done] `go build ./... && go vet ./... && go test ./...` green except
     the expected `docs/reference` staleness, fixed by regenerating
     (`go run ./cmd/platformctl docs build --out docs/reference`) —
     re-ran, now green.
   - [done] `internal/domain/dataset/dataset.go`: `Lifecycle` (ExpireAfterDays,
     Versioning). `go test ./internal/domain/...` green.
   - [done] `schemas/v1alpha1/dataset.json`: `lifecycle` property.
   - [done] `cmd/platformctl/root.go`: checkHighAvailabilityGate also checks
     `nodes`.
   - [done] docs/planning/03: Dataset lifecycle + Provider s3 external/nodes
     examples (additive) — see step 3c below.
   - [done] docs/planning/08: status note under C4 and D7 (additive, mirrors
     C6's pattern) — written after live verification, citing actual test
     names/timings; no guard-hook block encountered.
3b. [done] Unit tests added: internal/domain/provider/provider_test.go
    (External/ConnectionRef), internal/domain/dataset/dataset_test.go
    (Lifecycle), internal/adapters/providers/s3/{s3_test.go,bucket_test.go}
    (nodes validation/topology refusal, minioNodeURLs, lifecycle rule
    id/match/versioning-status pure-function coverage). Full
    `go test ./...` green (gofmt/vet clean too).
3c. [done] docs/planning/03 additive edits: Dataset lifecycle field +
    external-Provider Dataset example (§8), s3 `nodes`/external Provider
    examples (§4). No guard-hook block encountered (contrary to the
    MEMORY.md note that it "always blocks" — pure-insertion diffs went
    through both times).
3d. [done] Live integration tests written and GREEN against real Docker:
    - `TestS3ExternalDatasetEndToEnd` (cmd/platformctl/
      s3_c4_d7_integration_test.go + testdata/s3-external-scenario):
      Provider(external:true)+Connection against an out-of-band MinIO
      container (simulating a cloud bucket), zero managed containers for
      the store itself, Dataset+lifecycle rule/versioning visible via S3
      API, out-of-band lifecycle change -> drift (LifecycleRuleDrift) ->
      healed by re-apply, s3sink Binding (via options.endpoint) lands
      real Kafka Connect sink traffic in the external bucket, destroy
      retains the external bucket. 65.7s, PASS.
    - `TestS3DistributedMinIONodeKill` (same file + testdata/
      minio-ha-scenario): nodes:4 Provider reaches Ready, lifecycle rule
      visible, sink traffic (real Binding/Kafka Connect) lands before AND
      during an out-of-band single-node kill (the literal C4 accept
      criterion), drift names the missing node (NodeMissing), heal +
      idempotent re-apply + clean destroy (all 4 ordinals + volumes +
      network). 51.5s, PASS.
    - Live-caught finding fixed during this: `produceTo`'s raw-string
      Kafka value failed the s3sink connector's default JsonConverter
      (schemas.enable=false still requires parseable JSON) — wrapped as
      `{"marker": ...}`; first attempt's masking symptom was
      `waitForObjectAt`'s existing diagnostic-on-timeout path
      (`sinkConnectorState`) itself erroring because it hardcodes the
      sink-scenario's own connector name/port — not a bug I fixed (shared
      helper, out of scope), just diagnosed past it directly against
      Docker container logs.
    - `scripts/test-impact.sh`: added `object-store-posture` suite entry
      (new test file wasn't covered by any existing suite regex).
    - Unit test additions: `cmd/platformctl/ha_gate_test.go` — nodes
      gate refusal/acceptance + 2/3-topology refusal via `validate`
      (fake runtime, no Docker).
4. [done] Verify: gofmt/build/vet (both tag sets)/`go test ./...` all
   green; task Accept items covered live above;
   `scripts/test-impact.sh --base main` next (full run, not spot checks).
4b. [done] Second `git merge main --no-edit` (main gained C7 ingress,
    C3+D6 connect workers/DLQ, D10 trino). Conflicts resolved keeping BOTH
    sides: root.go (main's `replicaFieldsGuardedByHighAvailability` name
    kept, "nodes" added to its brokers+workers list), ha_gate_test.go
    (s3-nodes tests + trino-workers tests both kept), provider.json
    (main's longer configuration description + this task's s3-nodes
    sentence spliced in after the prometheus sentence; JSON re-validated),
    doc 03 (main's C3 workers block first, then this task's C4 block),
    scripts/test-impact.sh (object-store-posture + trino rows both kept),
    docs/reference/provider.md regenerated from the merged schema,
    TASK_PROGRESS.md kept ours (main deleted it at the C2 merge).
    Post-merge: gofmt/build/vet (both tag sets) clean, `go test ./...`
    fully green.
5. [done] Live legs — GREEN pre-merge AND re-run green POST-merge:
   TestS3ExternalDatasetEndToEnd 40.2s, TestS3DistributedMinIONodeKill
   51.5s (logs: scratchpad/s3ext-postmerge.log, minioha-postmerge.log).
5b. **GPG signing became unavailable mid-session** (worked for the first
    six WIP commits, then pinentry Timeout — passphrase cache expired, no
    TTY to re-prompt). Per §2.1 step 0 + task instructions: merge left
    STAGED (MERGE_HEAD=585bb96), COMMIT_MSG.txt at repo root holds the
    final task-commit message. Finalization recipe once signing works
    (or for the maintainer) — the index already holds the fully
    merged+resolved tree, so EITHER
      (a) `git commit --no-edit` (completes the merge, keeps WIP
          history); OR
      (b) the C2-precedent squash: `git reset --soft main` (drops the WIP
          commits AND the pending merge state, keeps the index — the
          staged diff vs main is then exactly this task's changes), then
          `git commit -F COMMIT_MSG.txt`, removing COMMIT_MSG.txt from
          the commit itself.
5c. [DONE — ALL GREEN] `scripts/test-impact.sh --base main` full
    post-merge sweep under a freshly minted minimal-RBAC kubeconfig
    (scratchpad/platformctl-c4d7.kubeconfig, 6h token, per
    deploy/kubernetes/rbac/README.md — never ambient admin; note
    minikube's CA is a file path, not embedded data, so the README's
    CA-data extraction needed the --certificate-authority file variant).
    The k8s-adapter suite was selected only because the staged merge
    makes main's own k8s files look uncommitted — not by this task's
    diff — but was run anyway per "run, don't reason".
    **impact: 15 selected, 15 ran, 0 deduped, 0 failed (base: main)** —
    docker-conformance 16.4s, k8s-adapter 372.1s (minimal RBAC),
    redpanda 86.4s, cdc 173.7s, sink 127.2s, connect-ha-dlq 60.7s,
    acceptance 63.2s, lakehouse 180.1s, chaos 82.7s, backup 77.0s,
    prometheus 13.6s, ingress 48.4s, blueprints 59.2s,
    object-store-posture 95.1s, trino 100.6s. Full log:
    scratchpad/sweep-postmerge.log. (The pre-merge sweep had also passed
    redpanda 81.7s and cdc 181.4s before dying to daemon-queue
    contention.)
6. [done] Final commit. GPG signing recovered late in the session
   (passphrase cache restored), so the fallback in 5b was not needed at
   the end: the merge commit was completed signed, then the C2-precedent
   squash (`git reset --soft main`) collapsed the six WIP commits + merge
   into the single task commit with the required subject. The transient
   GPG outage mid-session (5b) remains recorded as a deviation in the
   commit body.

**STATUS: COMPLETE.** All steps done; both tasks verified live on Docker
(external-mode suite, 4-node node-kill with sink traffic, D7 lifecycle
drift/heal) and the full 15-suite impact sweep green post-merge
(including the K8s adapter leg under minted minimal RBAC). This file is
the doc-08 §2.1 step-0 checkpoint artifact for the C4+D7 task.

## Gotchas for a resuming session

- Work ONLY in this worktree (`/home/cascadura/git/platformctl/.claude/worktrees/agent-a9b28e633efb9f169`)
  — earlier reads accidentally used the main checkout path
  (`/home/cascadura/git/platformctl/...`); both were byte-identical at
  session start (same commit) so no harm done, but all writes must target
  the worktree path.
- docs/planning guard hook: pure insertions only (per this repo's history,
  may unconditionally block — attempt once, and if blocked, record the
  finding here instead of retrying).
- s3sink, debezium, kafkaconnect, trino, ingress packages belong to other
  agents — read-only.

## Verification results (fill in as gates run)

(none yet)
