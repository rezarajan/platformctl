# TASK_PROGRESS — C3 (distributed Kafka Connect workers) + D6 (dead-letter queues)

Supersedes the C2 checkpoint previously at this path (C2 is done, merged to
main as `ff16dae`/`3acabcb`-equivalent history — verified via
`git merge main --no-edit` = "Already up to date" at session start).

Resume protocol: read this file + `git log --oneline -20` first. Steps marked
`[done]` are committed (WIP commits are fine per doc 08 §2.1 step 0). Continue
from the first `[next]`/`[in-progress]` step.

## Design decisions (read before touching code)

### C3
- `spec.configuration.workers: N` on debezium/s3sink Provider -> realized as
  `ContainerSpec{Replicas: N, StableIdentity: false}` (ADR 004's D10-shaped
  branch -- "no stable identity, Connect rebalances" per doc 08 C3). This is
  the FIRST real consumer of the `StableIdentity: false, Replicas > 1` path
  (D10/Trino hasn't shipped yet) -- reuses `providerkit.EnsureInstance`
  unmodified (it passes `Container.Replicas`/`StableIdentity` through
  untouched; Connect workers are stateless, no `Volume`).
- Undeclared `workers` (absent from configuration) keeps the pre-C3
  single-container path byte-for-byte -- same opt-in-by-declaration pattern
  as ADR 017 Sec a.1 (mirrored, not reused code, since redpanda's opt-in is
  StableIdentity:true-shaped and this is the false-shaped sibling).
- `internal/adapters/kafkaconnect`: every REST function's signature changes
  from `baseURL string` to `baseURLs []string` -- tries each in order,
  returns first success, joins errors if all fail (`tryEach` helper). This
  is the "REST calls try each live worker" failover doc 08 names literally.
- Address resolution: provider-side `workerURLs(ctx, rt, name, workers)`
  helper (debezium.go and s3sink.go each get their own copy -- small,
  provider-specific http port constant, not worth a providerkit extraction
  per G1's "more parameters than lines saved" bar) mirrors redpanda's
  `clusterDial`: workers<=1 -> single bare-name `EnsureReachable` (today's
  exact `reachableURL`, wrapped in a 1-element slice); workers>1 -> iterate
  ordinals 0..N-1, `EnsureReachable` each, skip failures, error only if zero
  reachable. Resolved once per Reconcile/Probe/Destroy call (fresh per call,
  same granularity as redpanda's topicDial/clusterDial -- NOT re-resolved
  inside kafkaconnect's own internal 90s retry loops, which stay
  pre-existing hand-rolled retries, now cycling the given address list each
  iteration instead of a single URL). This satisfies ADR 015's "per-attempt
  re-resolution stays inside runtime.WithReachable" instruction: the
  re-resolve primitive is the provider-layer EnsureReachable call at the top
  of each Reconcile/Probe/Destroy, not a new bespoke loop inside
  kafkaconnect (which has no runtime.ContainerRuntime access and shouldn't
  gain one -- narrow package scope, ADR 008).
- Probe (Provider kind, workers>1): per-ordinal `Inspect` (found && Running)
  -- mirrors `redpanda.probeBrokerSet`'s presence check (no group-membership
  equivalent exists in the Kafka Connect REST API to check further). Missing
  ordinals -> drift reason `ConnectWorkerMissing(<ordinals>)` (new status
  reason, mirrors `ReasonBrokerMissing`).
- Gate: `checkHighAvailabilityGate` in cmd/platformctl/root.go extended to
  scan both `configuration.brokers` (existing) and `configuration.workers`
  (new) on any Provider, naming whichever field triggered it in the error
  message (doc 08 C3 explicit instruction -- "make the message name the
  field that triggered it").
- Schema: `options: additionalProperties: true` already permits
  `configuration.workers` with no schema-file change; Go `ValidateSpec` on
  both providers gains the integer->=1 check (mirrors redpanda's `brokers`
  check). Doc 03 gets an additive `workers` paragraph mirroring the
  `brokers` one.

### D6
- `Binding.spec.options.deadLetter: {stream, tolerance}` parsed in
  `internal/domain/binding` (`Binding.DeadLetter *DeadLetter`), not left as
  a raw map read at each call site -- mirrors how `SourceRef`/`TargetRef`
  etc. are typed accessors. Structural validation (mode must be sink,
  stream required, tolerance in {all,none}, default "all") lives in
  `binding.validate()` so every `FromEnvelope` caller gets it for free.
- Existence check (`deadLetter.stream` must resolve to an EventStream
  in-graph) lives in `internal/application/compatibility.Check` -- NOT a new
  `graph.Build` edge. `graph.Build`'s `refFields` are a fixed, generic,
  top-level list (providerRef/sourceRef/targetRef/connectionRef/secretRef)
  shared uniformly by every Kind; `deadLetter.stream` is nested inside
  `spec.options`, mode-scoped (sink Bindings only), and provider-consumed --
  adding a special case to the generic graph walker for one nested field of
  one Kind/mode is exactly the "engine-block introspection the plan
  deliberately avoids" per this task's own instruction. Chosen instead: a
  compatibility-level existence check only (same error-message family as
  the existing sourceRef/targetRef "does not resolve" checks).
  **Ordering consequence, documented here and at the check's call site**:
  no dependency edge means `graph.TopologicalLevels` does not guarantee the
  DLQ EventStream reconciles before the sink Binding -- under
  ParallelReconciliation they may land in the same level. This is safe in
  practice because Kafka Connect's own framework creates the DLQ topic
  itself (via the worker's internal AdminClient) if missing, using
  `errors.deadletterqueue.topic.replication.factor` -- s3sink sets this from
  the resolved DLQ EventStream's own `spec.replication` when the engine
  happens to have it in `req.Resources` (always true -- Resources is the
  full validated set regardless of reconcile order), else "1". The
  platform-managed EventStream's own partition/retention config "wins" once
  it reconciles (same apply or a later one); until then Connect's
  auto-created topic (1 partition, the given RF) serves. This is a known,
  documented limitation, not a silent gap.
- s3sink `desiredConnectorConfig`: when `b.DeadLetter != nil`, adds
  `errors.tolerance`, `errors.deadletterqueue.topic.name` (=
  `b.DeadLetter.Stream` -- an EventStream's name IS its Kafka topic name,
  the same convention `redpanda.reconcileTopic` already uses),
  `errors.deadletterqueue.topic.replication.factor`,
  `errors.deadletterqueue.context.headers.enable: "true"` (diagnostic
  value, DLQ records carry the original topic/partition/offset/exception).
  Zero behavior change when `deadLetter` is unset (existing keys untouched).
  `connectorConfigDrift` covers the new keys for free (diffs the whole map).
- debezium is CDC-only (Source->EventStream); `deadLetter` is a sink-mode
  concept per doc 08 D6 and `binding.validate()` refuses it on any other
  mode, so debezium never sees it.

## Steps

0. [done] TASK_PROGRESS.md created (this file; supersedes the stale C2
   checkpoint that lived at this path).
1. [done] Read CLAUDE.md, doc 08 Sec2.1/C3/D6, ADR 004/015/017/009,
   reconciler.go, redpanda.go+kafka.go (opt-in pattern reference),
   debezium.go, s3sink.go, kafkaconnect/connect.go, binding.go,
   compatibility.go, graph.go, root.go gates, providerkit, status reasons,
   doc 03 Sec4/Sec7, existing integration test patterns
   (redpanda_ha_integration_test.go, sink_integration_test.go).
2. [done] internal/adapters/kafkaconnect: multi-address failover
   (`baseURLs []string`, `tryEach` helper); unit tests with httptest
   multi-server failover. Also added `providerkit.ReachableURLs` (shared
   by debezium+s3sink, byte-identical port 8083 usage — G1's bar for a
   providerkit extraction).
3. [done] status reasons: add `ReasonConnectWorkerMissing`.
4. [done] debezium.go: `workers` config (`workersDeclared`), `reconcileWorker`
   Replicas/StableIdentity:false path (Container.Replicas: workers), providerState
   echoes `workers` when declared, `workerURLs` helper (wraps
   providerkit.ReachableURLs), `probeWorkerSet` (per-ordinal presence,
   ConnectWorkerMissing(...) reason), ValidateSpec workers int>=1 check,
   wired new kafkaconnect `[]string` signature through
   reconcileConnector/ConfigureLineage/Destroy(Binding)/Probe/
   connectorConfigDrift. Unit tests added (TestReconcileWorkerWorkersReplicaSet,
   TestReconcileWorkerWorkersUndeclaredIsSingleContainer, TestValidateSpecWorkers).
   VERIFIED: gofmt/go vet/go test ./internal/adapters/providers/debezium/...
   all green in the WORKTREE checkout (note: earlier in this session `cd
   /home/cascadura/git/platformctl && ...` accidentally targeted the
   *main* checkout, not this worktree — Bash's default cwd IS already the
   worktree; do not `cd` to the absolute main-checkout path when verifying).
   Committed c87cf8d.
5. [done] s3sink.go: same `workers` treatment (workersDeclared/workerURLs,
   Replicas field, providerState echo, ValidateSpec check, ProbeConnectWorkerSet
   wired) + `applyDeadLetterConfig` (errors.tolerance/deadletterqueue.topic.name/
   .replication.factor resolved from the named EventStream in req.Resources
   when present else "1"/.context.headers.enable) called from
   desiredConnectorConfig when b.DeadLetter != nil. Wired new kafkaconnect
   `[]string` signature everywhere. Unit tests added (workers ×3,
   DeadLetter translation ×3). VERIFIED in worktree: gofmt/go vet/
   `go test ./internal/adapters/providers/s3sink/...` green. Committed 777347c.
6. [done] binding.go: `DeadLetter{Stream, Tolerance}` struct, parsed from
   `spec.options.deadLetter` in FromEnvelope (tolerance defaults "all"),
   validated in `validate()` (mode must be sink, stream required, tolerance
   ∈ {all,none}). Unit tests in binding_test.go (new file). VERIFIED:
   `go test ./internal/domain/binding/...` green. Also ran full
   `go build ./... && go test ./...` — ALL GREEN across the whole repo at
   this point (recorded here since it's a good checkpoint). Committed 777347c.
7. [done] compatibility.go: `checkDeadLetterQueue` (structural existence
   check only, no graph edge — ordering story documented in the function's
   own doc comment + here). Wired into Check() for every Binding, right
   after checkSchemaFormat. Unit tests: TestDeadLetterQueueExistingStreamAccepted,
   TestDeadLetterQueueMissingStreamRejected (the literal D6 Accept item),
   TestDeadLetterQueueOnCDCBindingRejected. Committed f0b3a69.
8. [done] root.go: `replicaFieldsGuardedByHighAvailability = []string{"brokers",
   "workers"}`, checkHighAvailabilityGate scans all of them, error names
   whichever field triggered it (`spec.configuration.%s: %d`). Unit tests
   in ha_gate_test.go (TestValidateRefusesWorkersWithoutHighAvailabilityGate,
   TestValidateAcceptsWorkersWithHighAvailabilityGate). Committed f0b3a69.
9. [done] doc 03: additive `workers` paragraph+example after the redpanda
   `brokers` block (§4), additive `### 7.4 spec.options.deadLetter` section
   before §8 (full ordering-story note included). schemas/v1alpha1/{binding,
   provider}.json description strings updated (workers, deadLetter) —
   these ARE schema-file edits (description text only, no shape change,
   since `configuration`/`options` are already free-form maps), so
   `docs/reference/*` needed regen: ran
   `go run ./cmd/platformctl docs build --out docs/reference`,
   TestGeneratedReferenceInSync green after. Both doc-guard-hook edits to
   docs/planning/03 succeeded on the first (additive-only) attempt — no
   retry needed. Committed 69819e0.
   VERIFIED at this point: `gofmt -l .` empty, `go build ./...`,
   `go vet ./...`, `go test ./...` all green across the WHOLE repo.
10. [done] Unit tests already added per-step above (debezium_test.go,
    s3sink_test.go, binding_test.go, compatibility_test.go, ha_gate_test.go,
    providerkit_test.go, connect_test.go).
11. [done] gofmt/build/vet/go test ./... green (see step 9's verification
    note; re-confirmed again after step 9's commit).
11.5 [done] CORRECTNESS FIX found while designing the integration test (not
    caught by unit tests since the fake runtime doesn't simulate real
    Docker port collisions): `workers > 1` combined with a pinned
    `connectPort` would give every ordinal the IDENTICAL deterministically-
    derived HostPort (ordinalContainerSpec copies ContainerSpec.Ports
    verbatim across ordinals), so ordinal 1's container create would fail
    with a real port-already-allocated error on live Docker — exactly
    ADR 004's documented "fixed HostPort cannot combine with Replicas > 1"
    limitation, which redpanda's brokers path already closes (ADR 017
    §a.4) but C3 had not yet. Fixed: `connectPorts(cfg, name, workers)` in
    both debezium.go/s3sink.go leaves HostPort unset (0, Docker/K8s
    auto-assign per ordinal) when workers > 1, exactly mirroring
    `redpanda.reconcileBrokerSet`; `ValidateSpec` now refuses a
    `connectPort` pin combined with `workers` (mirrors ADR017 §a.4's
    refusal for kafkaPort/adminPort/schemaRegistryPort + brokers). Unit
    tests added (TestValidateSpecWorkersRefusesConnectPortPin ×2).
    Committed c1d3b8e. This is exactly the kind of live-Docker-only defect
    doc 08's conformance-ratchet policy (ADR 015 F6) exists to catch —
    recorded here since it was caught by *designing* the integration test,
    before ever running it live.
12. [in-progress] Integration test: testdata/connect-ha-dlq-scenario + a combined
    test file covering both Accept lists — debezium workers:2 CDC kill-test
    (kill one ordinal out-of-band, `status`/probe without `apply` still
    reports the CDC Binding RUNNING); s3sink + DLQ EventStream poison-record
    test (produce a poison record directly to the source EventStream's own
    topic — bypasses CDC entirely, simpler infra — verify it lands in the
    DLQ topic, connector stays RUNNING, a subsequent valid record still
    lands in S3/MinIO). Docker confirmed available at session start
    (`docker info`/`docker ps` both worked; other agents' containers
    visible — shared daemon, queuing expected per doc 06 §10).
    Wrote cmd/platformctl/testdata/connect-ha-dlq-scenario/manifests.yaml
    (redpanda single-broker kafkaPort:19693, postgres port:15745, debezium
    workers:2 NO connectPort pin, minio port:19501, s3sink connectPort:18685
    + deadLetter Binding option) and
    cmd/platformctl/connect_ha_dlq_integration_test.go
    (TestConnectWorkersHAAndDeadLetterQueue). Added the `connect-ha-dlq`
    suite row to scripts/test-impact.sh (G7 completeness guard).

    RUN 1 (80.44s): everything through the poison-record/heal cycle
    PASSED; the final `destroy` call FAILED — DeleteConnector against the
    CDC Binding's connector got "every Kafka Connect worker address failed
    (2 tried)": one worker answered HTTP 500 with body "IO Error trying to
    forward REST request: java.net.ConnectException: Connection refused"
    (Connect's own internal REST-forwarding between distributed-mode
    workers, one candidate trying to reach the other), the other "read:
    connection reset by peer". A REAL live-caught defect (not an artifact
    of the test), squarely doc 08's conformance-ratchet territory (ADR 015
    F6): `DeleteConnector`'s single-pass `tryEach` had no retry-through-
    transient the way `PutConnectorConfig` already did.
    FIX (commit 66fab13): extracted `retryTransient` (shared bounded
    retry-on-transient-error loop) out of PutConnectorConfig; widened
    `isTransientPutError` -> `isTransientConnectError` to also recognize
    "connection reset by peer"/"broken pipe"/"EOF" alongside the existing
    HTTP 409/"connection attempt failed"/"Connection refused" set (the 500
    body case already matched via the "Connection refused" substring);
    wrapped `DeleteConnector` in the same primitive (30s budget, shorter
    than Put's 90s — destroy shouldn't hang as long on a truly broken
    worker). Unit tests added: TestIsTransientConnectErrorRecognizesForwardingFailure
    (pins the exact live-caught message shapes), TestRetryTransientRetriesThenSucceeds,
    TestRetryTransientReturnsNonTransientImmediately, TestDeleteConnectorFailsOverOnForwardingError.
    Full repo gofmt/build/vet/`go test ./...` green after. Committed 66fab13.
    RUN 2 with the fix: **PASS, 58.54s** — full sequence green: apply
    (2 debezium ordinals up, sink connector RUNNING), pre-poison valid
    record landed in MinIO, out-of-band kill of datascape-chdlq-dbz-1,
    `drift` (no apply) reported CDC Binding Ready=True via the survivor +
    Provider drift ConnectWorkerMissing(datascape-chdlq-dbz-1), healing
    apply restored the ordinal, poison record landed in the DLQ topic,
    sink connector stayed RUNNING, post-poison valid record landed in
    MinIO, live connector config carries errors.tolerance=all +
    errors.deadletterqueue.topic.name=chdlq-attendance-events-dlq, destroy
    fully clean (all 14 resources ok, zero leftover chdlq containers).
    The F6 ratchet pin for the live-caught bug is
    TestIsTransientConnectErrorRecognizesForwardingFailure (kafkaconnect
    unit level — the bug class IS expressible at the client level, so the
    pin lands there, in the same commit as the fix: 66fab13).
13. [done] scripts/test-impact.sh --base main — ALL GREEN, exit 0:
    9 selected, 9 ran, 0 deduped, 0 failed. Environment hygiene done
    first (no leftover datascape-* containers except other agents' live
    trino set — untouched; pinned images cached in digest form). K8s leg
    not selected by the impact map (this diff touches no k8s-adapter
    scope — doc 06 §10 rule 6), correctly skipped.
14. [done] doc 08 C3/D6 Done notes appended (additive, guard hook
    accepted); WIP commits squashed (`git reset --soft` to the C2 merge
    base) into the single final commit
    "feat(connect): distributed workers (C3) + dead-letter queues (D6)".

## Verification log

- gofmt -l . : empty. go build ./... : OK. go vet ./... (default + -tags
  integration): OK. go test ./... : all packages green (re-run at every
  increment; final run post-doc-sync).
- TestGeneratedReferenceInSync: green after docs/reference regen.
- connect-ha-dlq standalone run 1: FAIL at destroy (live-caught
  DeleteConnector transient-forwarding bug — fixed in commit 66fab13);
  run 2: PASS 58.54s; run 3 (under the ledger wrapper, in the sweep
  below): PASS 54.75s.
- scripts/test-impact.sh --base main (branch gate, flock-serialized,
  2026-07-21): redpanda 78.1s ok; cdc 173.0s ok; sink 128.6s ok;
  connect-ha-dlq 54.8s ok; acceptance 64.4s ok; lakehouse 173.2s ok;
  backup 72.9s ok; prometheus 14.4s ok; blueprints 62.7s ok.
  impact: 9 selected, 9 ran, 0 deduped, 0 failed (base: main), exit 0.
  Evidence recorded in the shared ledger — the merge gate can cite these
  scope-hashes instead of re-running.
