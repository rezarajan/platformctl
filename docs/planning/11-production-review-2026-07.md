# 11 — Production review (2026-07): goal tracking and findings

**Goal (project owner, 2026-07-22):** review docs for scope/requirement
gaps across software-engineering areas and amend; then review the entire
codebase for conformance, fixing bugs, future hazards, and anything short
of GA-for-production. Named focus areas: (a) real data-engineer
production scenarios — cloud-managed databases behind secured
connectors (AWS/GCP/Azure), databases reachable only through a VPN into
a VPC, done zero-trust / blast-minimized (egress confined to the
Connection seam); (b) state-machine and sequenced-operation correctness
under async execution — no race conditions, no timing dependence on
machine speed; (c) coding-practice quality. Done when the code is usable
for production to the requirements the scope has built out.

**Protocol:** this file is the resumable progress record (doc 06 §2.1
step 0 applied at goal scope). Every phase/task below gets a status line
when touched; work lands as commits on main referencing this doc. A
fresh session resumes from this file + `git log` alone.

## Phase 0 — close the in-flight wave (precondition)

- [x] C9 merge (commit `57c4d39` ready; gate = agent's 18-suite sweep,
      log task `b57tylbfz`, running as of goal-set). Closes Stage C →
      owner decision: KubernetesRuntime GA + v1.3.0.
- [x] D8 merge (branch `worktree-agent-a91e4af8eee00bb01` @ `3f63b8d`,
      reviewed; targeted gate after merge).

## Phase A — docs review: gaps in scope/requirements (in progress)

Sweep docs 01–10 + ADRs 001–024 against the goal's focus areas; record
each gap here with a disposition (amend doc / new task / out-of-scope
with reason). Initial gap register, from wave knowledge (to be verified
and extended by the full pass):

| # | Gap | Evidence | Disposition (proposed) |
|---|---|---|---|
| A1 | `Connection.spec.via` is schema-accepted + capability-validated but consumed by no provider — the owner's VPC/VPN scenario (egress to a private database only through the tunnel) is exactly this seam | ADR 023 "Scope" records the deviation | Task: wire `via` into managed-Connection realization (forwarder egress routed through the tunnel provider's container/network — blast-minimized: only the Connection's entrypoint can reach the tunnel) |
| A2 | Outbound TLS is unmodeled: `spec.tls` is managed-only (terminates TLS at *our* entrypoint); connecting to a TLS-required cloud-managed DB (server-cert verification, client certs / cloud IAM auth) has no declared shape | domain/connection validate: "spec.tls is only meaningful on managed connections" | Doc 03 + ADR addendum: external-endpoint TLS posture (verify/CA pinning at the consumer or forwarder), explicitly scope cloud-IAM auth in/out |
| A3 | Cloud "secured connector" scenario (e.g. Cloud SQL Auth Proxy, AWS RDS Proxy, Azure Private Link) not described anywhere as a supported topology, though External Connection + secretRef composes for the simple case | docs 03 §8.2, onboarding | Amend docs with a worked scenario page; identify any missing fields (e.g. proxy sidecar as a Provider) |
| A4 | Async-correctness requirements are implicit (fixed case-by-case: redpanda settle `93fbf14`, K8s port-forward races) — no stated invariant that Ready must mean settled/serving, no catalog of wait/probe patterns | doc 08 F3 "ready means serving" exists for F3 only | Promote to an NFR in doc 01 + engineering rule in doc 02; Phase B audits against it |
| A5 | Timing-dependence review (slow vs fast machines): timeouts/cadences are constants (e.g. `topicSettleTimeout=45s`) with no stated budget rationale | providers' probe code | Phase B audit item; doc the timeout policy |
| A6 | Zero-trust runtime posture (policy H3–H6, mediation ADR 022) is sequenced but unimplemented — GA claims must not imply it | doc 04 §12 gates | Verify gate table + README make maturity honest; no new work beyond H-sequence |

- [x] Full docs pass (each doc 01–10, ADR index) against goal areas
- [x] Amendments committed (additive; guard-compatible)

## Phase B — codebase conformance review (blocked on Phase 0)

Fan-out review (sonnet agents, file-ownership fenced) once main is
static. Dimensions, each producing findings verified before fixing:

- [x] B1 async/state-machine correctness: every wait/poll/settle/probe
      path in engine + providers + runtimes (races, unsettled-Ready,
      machine-speed dependence)
- [x] B2 GA-bar audit: every gate marked GA/enabled vs its actual
      test evidence and error-path behavior
- [x] B3 production-scenario walkthroughs: external cloud DB (TLS,
      secured connector), VPC-via-tunnel (A1 outcome), each exercised
      end-to-end or explicitly documented as gapped
      (closed by I1+I2 e2e + doc 03 §8.2.4 + README scenario gallery)
- [x] B4 coding practices: error wrapping, context propagation,
      goroutine lifecycle/leaks, lint debt beyond golangci defaults
- [x] B5 fix waves for confirmed findings, impact-gated per doc 06 §10
      (I4 merged 55708ee, I5 merged 5f792b1)

## Log

- 2026-07-22: goal set; this file created. Phase 0 sweep running
  (redpanda green 93.7s at C9 state). Phase A register seeded from wave
  knowledge; full docs pass starting.
- 2026-07-22: A2 verified and PROMOTED to code gap (production-blocking
  for the owner's cloud-DB scenario): `sslmode=disable` is hardcoded in
  internal/adapters/providers/postgres/sql.go:27 (admin connection),
  internal/adapters/providers/debezium/debezium.go:620 (preflight; the
  connector config sets no database.sslmode at all), and
  internal/adapters/providers/jdbcsink/jdbcsink.go:657; the mysql/mariadb
  paths declare no TLS parameter either. No provider can reach a
  TLS-requiring database (all cloud-managed engines). Sequenced as doc 08
  I2. A1 (consume Connection.spec.via) sequenced as doc 08 I1. A4
  verified: doc 01 NFR table (NFR-1..10) has no settledness/async
  invariant — amendment pending Phase A completion (single batch).
  Full-docs inventory pass delegated (in flight).
- 2026-07-22: Phase 0 closed. C9 merged at e69f1b4 (STAGE C COMPLETE;
  18-suite sweep 0 failed at its exact content-state). D8 merged at
  5b91048 (both-append with C9's PrometheusURL; targeted trino+lakehouse
  gate running, ledger-recorded on green). Owner decisions now open:
  KubernetesRuntime GA + v1.3.0 tag. Phase B unblocked once the D8 gate
  is green.
- 2026-07-22: Phase A COMPLETE. Full docs inventory (delegated) verified
  the register: A6 ruled out (gate table matches main.go exactly), A1/A2
  held up, plus new findings. Amendments landed in one batch: NFR-11
  Settledness added to doc 01 (I3 executed) + scale-envelope note;
  `validate` now REFUSES Connection.spec.via until I1 (compatibility.go
  + TestConnectionViaNotConsumedRefused — closes the silent-security-
  no-op); ADR 025 scopes cloud IAM DB auth out (auth-proxy topology is
  the supported pattern); doc 03 §8.2.4 worked cloud-DB walkthrough
  (+ fixed duplicate §8.2.2 numbering); README CLI table gained
  lint/explain/add/wire/expose + jdbcsink/s3source/wireguard in
  Highlights, and TestREADMECLISurfaceInSync now guards the F-003
  recurrence class both directions; users.md gained the mid-apply-crash/
  stuck-lock runbook entry; Stage H exit-criterion 1 ticked (third
  "checkbox truth" instance); doc 10 narrative caught up; ADR index
  reordered. Remaining from inventory, deliberately NOT doc work:
  ContainerProvider stale code comment (B4 nit).
- 2026-07-22: Phase B1 audit report received: 4 findings, ONE class —
  Reconcile sets Ready from a weaker signal than Probe verifies
  (wireguard CONFIRMED tunnel-up-without-handshake; ingress-docker and
  proxy same asymmetry, narrower windows; ingress-k8s symmetric-shallow,
  deferred). Clean: redpanda/postgres/mysql/s3/trino/prometheus/nessie/
  openlineage/debezium/jdbcsink/s3sink/kafkaconnect/both runtimes/
  engine; no goroutine leaks; 2 time.Sleep both test-harness-only.
  Fix wave next: reuse each provider's existing probe fn in reconcile
  before Ready (findings 1-3); finding 4 recorded as deferred with
  reason.
- 2026-07-22: Phase B4 audit received and first fix batch landed. Fixed
  directly (this commit): CONFIRMED shell injection in mysql
  backup/restore — manifest database name was interpolated into the
  job container's `sh -c` (a container holding root DB + object-store
  creds); now rides DATASCAPE_BACKUP_DATABASE env var expanded quoted,
  postgres's pattern, pinned by a hostile-name unit test. Also:
  specHash marshal errors now propagate in both runtimes (idempotency
  contract, finding 5); docker log demux truncation is annotated
  (finding 4); stale ContainerProvider comment fixed (finding 6); three
  dead functions removed (finding 7); .claude/rules/go-style.md no
  longer claims a nonexistent .golangci.yml — adopting a tuned lint
  config is a RECORDED FOLLOW-UP. Remaining B4 findings 2+3 (debezium↔
  jdbcsink resolution dedup into providerkit; docker↔k8s probe-helper
  dedup + k8s dialable ctx-awareness) sequenced as doc 08 I5. B4 clean
  areas: SQL parameterization, secret hygiene (fingerprints only,
  0600 modes), HTTP client timeouts, atomic state writes, no init()/
  mutable package state.
- 2026-07-22: Phase A+B4 batch integration gate GREEN: 18/18 suites,
  0 failed (base 5b91048) — mysql injection fix, specHash propagation,
  via refusal all verified live incl. docker-conformance (17.3s) and
  k8s-adapter under minimal RBAC.
- 2026-07-22: I4's strictness EXPOSED A REAL ORDERING GAP (the review's
  async-correctness thesis validated end-to-end): `Connection.spec.target`
  is a plain host:port string, so graph.Build never ordered a managed
  Connection after the resource its target names — apply order was
  arbitrary (observed live: Connection/minio at [4/6] before its
  upstream Provider at [6/6]; the old blind-Ready masked it, the new
  settle-poll honestly failed in 46s). Fix in flight on the I4 branch:
  a target-host→resource dependency edge in graph.Build (warehouseRef-
  edge precedent), with cycle detection left LOUD, plus unit ordering
  tests. Wireguard leg of I4 was green (32.0s) incl. the NFR-11
  zero-drift-after-apply assertion.
- 2026-07-22: Phase B2 audit received. ALL 8 GA gates SOLID (incl. the
  chaos mid-apply-kill and node-kill evidence; only reachable panic is a
  build-defect guard; docs make no overclaims). KubernetesRuntime:
  conditional-GA recommendation — the runtime port, RBAC discipline, and
  the GA provider set are proven (conformance suite, minted minimal-RBAC
  CI job on every PR, HA-on-K8s live, both examples e2e on a real
  cluster), but there is NO K8s chaos/mid-apply-kill test and NO K8s
  DLQ/Connect-HA test (sequenced as doc 08 I6); an unconditional GA
  claim is not yet honest — a scoped GA (runtime lifecycle + GA
  providers) with a release-notes scope line is defensible NOW, owner's
  call. Mechanical fixes applied this commit: K8s adapter dir added to
  redpanda/cdc/lakehouse impact scopes (gap 3), new `external-import`
  suite row unexempts TestImportEndToEnd/TestExternalSourceEndToEnd
  (ExternalResourceConfiguration's e2e was unreachable by every CI -run
  filter), doc 07 now documents the Sysctls Docker-only drop (gap 4).
- 2026-07-22: I4 FULLY VERIFIED — round-2 sweep 15/16 (graph edge fixed
  the ingress ordering failure) + targeted lakehouse-K8s re-run green
  (157.9s) after the proxy settle fix (mirror Probe's guard: dial-through
  only where the runtime publishes a host address; container-health is
  the bar on ClusterIP/port-forward K8s — keeps reconcile/Probe symmetry
  bidirectional). I5 sweep 13/13 green. Both branches staged behind the
  GPG lapse (COMMIT_MSG.txt in each worktree). MERGE QUEUE (execute when
  GPG unlocks): (1) commit this staged main batch; (2) in
  .claude/worktrees/agent-aa3b8d094e3a2a974: `git commit -F
  COMMIT_MSG.txt`, then re-sign 858277c (rebase --exec 'git commit
  --amend --no-edit -S' or merge as-is and note it); (3) in
  .claude/worktrees/agent-a76e72e4b87175348: `git commit -F
  COMMIT_MSG.txt`; (4) merge I4 then I5 into main (expect trivial
  overlap: both touch runtime adapters — I5 moved docker's dialable into
  internal/adapters/runtime/probe; I4 does not touch that code); (5)
  post-merge: unfiltered unit run; integration evidence already in the
  ledger at each branch's content-state — merged-state delta gate:
  docker-conformance + k8s-adapter + ingress + wireguard + lakehouse +
  cdc + jdbcsink minimum, ledger-deduped.
- 2026-07-22: GPG unlocked; merge queue FLUSHED. I4 merged (55708ee,
  series re-signed) and I5 merged (5f792b1, suite-map conflict composed
  by per-row scope union). Merged-state delta gate launched. I3's doc 02
  half completed (settledness engineering rule §4.1). Next wave: I1
  (via consumption), I2 (outbound DB TLS), I6 (K8s GA-parity evidence),
  H3 (policy engine) — E5 deliberately held until I1/I2 merge (it
  restructures every provider's validation and would conflict).
- 2026-07-22: ORCHESTRATOR FULL-REPO REVIEW (owner-requested, done
  in-session while wave 2 runs). Verified in order: git state clean, all
  recent commits signed, origin up to date; worktrees = exactly the 4
  wave-2 agents; hooks (guard-planning-docs, guard-agent-model,
  fmt-and-lint) present and wired; unlock markers gitignored; justfile ↔
  CLAUDE.md accurate; docs/reference in sync; CI workflow wires
  test-impact both tiers; remediation ledger has no open items; ledger
  185 entries healthy. FIXED during review: (1) checkbox-truth 4th
  recurrence — Stage C's five exit criteria + Stage D's parquet
  criterion + E8's release-artifacts criterion were all unchecked with
  evidence complete; ticked with citations. (2) stale merged branch
  fix/k8s-external-ingress-networkpolicy deleted. (3) go.mod: accepted
  tidy's direct classification of parquet-go (E9's manual indirect
  revert made every future tidy dirty). (4) LIVE ISSUE caught: the
  minted RBAC token had expired mid-wave — k8s-adapter, redpanda, cdc
  legs of the I4+I5 delta sweep failed on credentials, not code;
  re-minted 8h, re-runs queued and ledger-recorded on green. (5) The
  class is now mechanically dead: scripts/test-impact.sh gained a
  k8s_preflight that fails fast with the re-mint pointer when
  KUBECONFIG can't authenticate. Notable non-issue: v1.1.0 tag absent
  (v1.0.0 → v1.2.0) — historical numbering, not an error.
- 2026-07-22: I4+I5 merged-state evidence COMPLETE: k8s-adapter 363.0s,
  cdc green (incl. K8s example leg), redpanda green 81.6s — all
  ledger-recorded at the merged content-state. One load-flake observed
  and cleared on retry: TestRedpandaHAKubernetesEndToEnd's
  produce-during-kill window (bounded 90s, NFR-11-compliant shape)
  exceeded deadline once at host load 3.9 with four agents compiling
  (failing run 140.7s vs green 81.6s). Not a regression (suite green at
  both branch states; neither I4's graph edge nor I5's resolution dedup
  touches this scenario). If it recurs under normal load, review the
  window budget as its own task — do not ad-hoc bump it.
- 2026-07-22: I6 reported. Chaos-on-K8s GREEN TWICE (66.6s/63.9s test
  time) — mid-apply-kill recoverability proven on Kubernetes. MAJOR
  FINDING (live-reproduced): C3 Connect-worker HA `workers > 1` fails
  hard at apply on Kubernetes — ordinal addressing vs Deployment shape
  mismatch (doc 08 I7 sequenced with the fix decision framed as an ADR
  004 addendum; Docker unaffected). The K8s DLQ test is single-worker
  by necessity: proves D6 DLQ + Deployment self-heal, NOT C3's
  second-worker claim. GA brief updated: unconditional KubernetesRuntime
  GA requires either a workers>1-on-K8s carve-out in release notes or
  I7 fixed first. DLQ runs 1+2 executing in I6's detached script; merge
  gate transcribes timings.
- 2026-07-22: I2 reported — WAVE 2 CODE-COMPLETE (all four agents).
  Outbound DB TLS shipped: tls.mode require/verify-ca/verify-full +
  caSecretRef on external Connections (absent = plaintext back-compat);
  debezium postgres full support, jdbcsink full support both engines,
  providerkit TLS seam with CA bundles file-mounted into workers;
  ExternalDatabaseTLS gate (Alpha/enabled). Honest scope boundaries
  recorded (debezium-mysql Java truststore; admin conns never dial
  external DBs). e2e green twice incl. wrong-CA negative. All four
  branches staged behind GPG; merge order on unlock: I1 → I2 → H3 → I6.
- 2026-07-22: ORCHESTRATOR CWD INCIDENT (recorded for honesty): during
  the I1 diff review a `cd` into its worktree persisted; the I7/doc-11
  edits and a COMMIT_MSG.txt overwrite landed in the agent's worktree
  instead of main. Fully recovered: worktree restored to the agent's
  state (doc 11 restored, my I7 section stripped, its commit message
  reconstructed from report+TASK_PROGRESS), edits re-applied on main.
  No commits were lost (GPG lapse prevented any wrong commit). Lesson:
  worktree inspection uses `git -C`/absolute paths, never `cd`.
- 2026-07-22: WAVE 2 MERGED. I1 a4a16ef, I2 92ef738, H3 f8507fb, I6
  f5c725f — all four unit-verified true-exit=0 per merge; I6's four
  live legs transcribed (chaos-k8s 66.6/63.9s + 164s; dlq-k8s green
  twice + 269s; impact run 992s ledger-recorded); gate-table and doc 08
  conflicts both-append composed. Worktrees cleaned (I6's evidence log
  preserved in session scratchpad). Merged-state gate sweep launched
  (--base 159b80d). Stage I now: I1-I6 done, I7 open. Stage H: H1-H3
  done, H4-H6 open. Stage E: E5→E6→E7 now UNBLOCKED (I1/I2 merged).
  Owner decisions: KubernetesRuntime GA (chaos+DLQ evidence complete;
  workers>1 carve-out or I7 first), ExternalResourceConfiguration GA,
  v1.3.0 tag. Note for history: the four agent COMMIT_MSG.txt files
  were lost to a pipe-masked exit (the documented class, this time by
  the orchestrator) — messages reconstructed from reports, marked as
  such in each commit body.
- 2026-07-22: OWNER DECISION recorded: KubernetesRuntime does NOT GA
  until HA is feature-complete — I7 is the gate. WAVE 3 launched: I7
  (workers>1 fix + ADR 004 addendum + 2-worker K8s DLQ/HA test), E5
  (provider schema fragments + typed option validation + negative
  corpus — the E-chain opener, now unblocked), H4 (zero-trust pack CI'd
  against examples + governance onboarding), and a hygiene batch
  (.golangci.yml adoption follow-up + G7 exemption-list absorption).
  E6→E7, H5→H6 queue behind their dependencies next wave.
- 2026-07-22: CI failure (owner-reported) FIXED BY ORCHESTRATOR directly:
  TestRedpandaHAEndToEnd on Docker — drift-after-heal reported
  ProbeFailed because a DescribeConfigs shard hit the just-restarted
  broker while it accepted TCP but closed during ApiVersions
  negotiation. Root cause: 93fbf14 fixed the RECONCILE side
  (waitTopicSettled before Ready) but the PROBE side stayed single-shot
  — a transport error became an instant verdict. Fix:
  retryTransientProbe in the redpanda provider (errors = undetermined,
  retried within a 15s bounded window; verdicts clean-or-drifted return
  immediately, so real drift is never masked; persistent failure
  surfaces the honest last error). Unit-pinned (TestRetryTransientProbe:
  transient-then-clean, immediate-verdict, persistent-error,
  ctx-cancel); live redpanda suite re-run queued+ledger-recorded. The
  earlier K8s produce-during-kill flake (I8) is this race's sibling in
  the TEST client — still open, distinct fix.
- 2026-07-22: OWNER DECISION: zero-trust pack treated as the prod
  starter it is (option 1) — drop the non-exemptible
  secrets-from-vault-or-k8s twin (exemptible forbid-env-secret-backend
  covers the fact; orgs flip exemptible locally per ADR 021's tailoring
  frame), keep protect-data non-exemptible with the examples'
  known-baseline. To be applied at H4's merge gate with an ADR 021
  addendum; Stage H criterion 2 pack-half ticked then.
- 2026-07-22: E5 reported (commit 732500c): 24 provider/engine/options
  schema fragments (Go-composed by discriminator, core Kind schemas
  untouched), 19-class negative corpus all green, docsgen renders
  per-provider reference from fragments. LATENT BUG found+closed:
  Source engine `database` was apply-time-only (ADR 011 violation) —
  now required at validate; zero shipped manifests needed edits.
  Honest scoping note: 5 providers' single-field checks kept as
  defense-in-depth. Stage E exit criterion 1 ticked by the agent with
  evidence. Sweep launched (16 suites, queued).
- 2026-07-22: WAVE 3 MERGED — I7 6a00f42 (GA blocker closed: workers>1
  any-member addressing, run 1 + run 2 127.1s green live), E5 31ba711
  (fragments + corpus + latent ADR 011 bug closed), H4 d09cf1d (pack
  applied; OWNER'S one-rule-per-fact decision executed in-merge — ADR
  021 addendum, non-exemptible secrets twin removed, pack = 11 rules,
  Stage H criterion 2 ticked), hygiene 940549f (golangci 0 issues at
  merged state, exemption map EMPTY, two real fixes). I8 closed (run 1
  111.9s @load 3.08 + run 2 89.4s, both green under load); the CI
  probe-side fix validated on the same runs. All worktrees cleaned;
  wave-3 merged-state gate sweep launched. REMAINING: E6→E7, H5→H6;
  owner decisions now fully unblocked — KubernetesRuntime GA (HA
  feature-complete per the owner's bar), v1.3.0,
  ExternalResourceConfiguration GA.
- 2026-07-22: ORCHESTRATOR FULL-CODEBASE SYSTEMS PASS (owner-requested,
  personally executed; ~37k LOC surveyed structurally, core seams read
  in full). VERDICT: the hexagonal architecture is genuinely load-bearing
  — ports import only domain (archtest-enforced), every cross-runtime
  behavior sits behind ContainerRuntime + conformance, providers are
  stateless-per-call. Third-party provider readiness is REAL but gated
  on one wall (below). Findings and dispositions:
  (1) FIXED NOW — hostport collisions: FNV auto-allocation over 10k
  slots had no detection (~50% birthday-collision odds at ~120
  components; failure surfaced as a cryptic bind error). domain/hostport
  now records every claim; the engine fails apply with both names + the
  pin remedy. Determinism untouched.
  (2) FIXED NOW — S3 state lease: fixed-TTL with no renewal meant an
  apply outlasting the TTL silently lost its lock mid-run (concurrent
  writer corruption window). Lease now renews at TTL/3 (ETag-matched so
  a reclaimed lease is never overwritten; transient failures degrade to
  the old behavior, never worse). renewLoop unit-pinned.
  (3) SEQUENCED (I9) — the Request fact-field accretion is the one
  modularity wall for third-party providers: each new cross-provider
  fact patches engine+port. Generic facts query specced; E6 must teach
  the generic form.
  (4) BUILD-VS-BUY AUDIT: healthy. Real FOSS delegated where it counts
  (franz-go/kadm, pgx, client-go, official docker SDK, minio-go,
  santhosh-tekuri/jsonschema, caddy-as-ingress, cobra, goldmark, huh).
  Deliberate hand-rolls reviewed and ACCEPTED with reasons: vault KV2
  client (91 lines vs the heavyweight official SDK; env-token only),
  dbjob FIFO pipeline (sh -c + exit-file protocol is the fragile spot —
  acceptable while backup is Alpha; NOTE: revisit with a supervised
  sidecar pattern or restic-class tool if backup targets GA), S3 lease
  (now renewal-hardened; a future multi-operator posture should adopt a
  real coordination service rather than extending it further — recorded,
  ADR 003's own boundary).
  (5) NFR-4 NOTE: "structured events" are the Reporter interface + logf
  prose; adopting stdlib log/slog behind the existing seam would make
  the claim literal (S-size, zero deps) — recorded as a follow-up, not
  sequenced (CLI UX is the current consumer and is well-served).
  (6) EDGE-CASE CLASSES swept and confirmed CLOSED by the wave's fixes:
  settle-vs-probe asymmetry (I4/CI fix), ordering-by-name gaps (I4 graph
  edge), per-ordinal addressing (I7), forward-staleness (I8), token
  expiry (preflight guard), spec-hash silent fallback (B4), port
  collisions + lease expiry (this pass). No further instance of any
  class found in the survey.
  (7) README REWRITTEN for current state: capability map grouped by
  user concern, updated architecture diagram (both runtimes, 16+
  providers, lint/policy pipeline, connectivity plane, facts), provider
  maturity table, scenario gallery (cloud DB TLS, auth-proxy, VPN-via,
  ingress TLS, monitoring, governance, backup), and a
  write-your-own-provider recipe naming providerkit + conformance +
  fragments. TestREADMECLISurfaceInSync green.
- 2026-07-22: wave-3 gate caught the LAST heal-window sibling:
  BrokerNotJoined(2!=3) drift right after a heal apply that had waited
  for membership — waitClusterFormed asked ONE broker's metadata view
  (kadm ListBrokers routes to a single broker) while drift's fresh
  client asked another still catching up; Kafka metadata propagation is
  eventually consistent BETWEEN brokers. Fix: waitClusterFormed now
  requires the MINIMUM view across every member (per-seed clients) ≥ n
  — a settle bar no same-instant probe can disagree with from any
  vantage; an erroring member counts as view 0 (correct mid-rejoin).
  Probe stays a point-in-time observation. Live proof run queued. The
  heal-window class is now closed at reconcile-membership, reconcile-
  topic, probe-transport, and test-client layers.
- 2026-07-22: min-view settle proven live (full redpanda suite 131.6s
  GREEN incl. both K8s HA tests). Wave-3 gate then caught the K8s
  workers:2 DLQ test timing out at ConnectorStateUNASSIGNED — root
  cause is Kafka Connect ITSELF: incremental cooperative rebalancing
  parks a departed worker's tasks for scheduled.rebalance.max.delay.ms
  (default FIVE MINUTES, tuned for rolling upgrades) awaiting its
  return; I7's 120s poll passes only when the replacement pod rejoins
  fast. Product-level fix (not a test budget bump): all three managed
  Connect worker providers (debezium, s3sink, s3source) now set
  CONNECT_SCHEDULED_REBALANCE_MAX_DELAY_MS=10000 — C3's promise is
  RUNNING-through-loss, worker restarts are reconcile-driven, so fast
  reassignment is the correct posture. Live connect-ha-dlq proof (both
  runtimes) queued.
- 2026-07-22: FINAL GATE: 24 selected, 18 ran, 4 deduped (targeted
  proofs honored by the ledger), 2 failed — both root-caused and fixed:
  (1) TestLockReclaimsAfterExpiry: MY lease renewal made "skip release"
  no longer model a dead holder (a live holder now correctly renews
  forever) — the store gained a stopRenewal seam and the test simulates
  death properly (renewal goroutine gone, lease left to expire).
  (2) TestIngressTLSEndToEnd: E5's ingress fragment omitted httpsPort —
  a field exercised ONLY by integration testdata, invisible to E5's
  examples/blueprints sweep. Fragment fixed; a systematic
  used-keys-vs-fragments sweep over ALL testdata+examples+blueprints
  confirmed this was the only gap (the one other hit is the negative
  corpus's intentional typo fixture doing its job). Both suites re-proof
  queued. Blind-spot lesson: fragment completeness must be checked
  against every manifest the repo ships, not just examples — a unit
  test doing the sweep would make this class impossible (small
  follow-up, noted).
- 2026-07-22: DAY CLOSED FULLY VERIFIED. Final-gate fix re-proofs green
  (ingress 118.8s incl. the TLS scenario; state-s3 4.8s incl. the
  dead-holder reclaim) and ledger-recorded. Every suite in the map is
  green at the current content-state. Main carries waves 1-3 merged,
  the systems pass, and today's seven root-cause fixes; unpushed,
  awaiting the owner. Open: owner decisions (KubernetesRuntime GA — HA
  now feature-complete per the owner's bar; v1.3.0;
  ExternalResourceConfiguration GA) and the sequenced tail (E6→E7,
  H5→H6, I9, small follow-ups: fragment-completeness unit sweep, slog,
  dbjob revisit-before-backup-GA).
- 2026-07-22: HANDOFF SNAPSHOT (pushed). The review goal's remaining
  work, fully specced in doc 08 for any agent to pick up cold:
  E6 (provider-author guide proof — teach I9's generic facts form),
  E7 (retire ContainerProvider gate), H5 (domains, ADR 022 Ring 0-1),
  H6 (mediated connections, OpenZiti), I9 (generic facts query on
  Request — land BEFORE third-party provider work), I10
  (fragment-completeness unit sweep), I11 (slog/NFR-4), I12 (dbjob
  hardening — blocks BackupRestore GA). Owner decisions open:
  KubernetesRuntime GA (evidence complete), v1.3.0 tag
  (docs/releasing.md), ExternalResourceConfiguration GA. Process rules
  for the next orchestrator: doc 06 §2.1 (checkpointing) + §8.4
  (minimal RBAC, mint 8h) + §10 (impact economy, one flock);
  memory/active-wave-handoff.md lists the operational lessons
  (no idle-polling, bounded watchers, git -C for worktrees, unfiltered
  exit checks, kill sweeps WITH their watchers).
- 2026-07-22: TIMED-POLL CENSUS (owner directive: ready-means-serving on
  ANY environment). Production code: exactly ONE time.Sleep — a poll
  cadence inside a bounded condition loop (conformance
  waitReadyReplicas); zero fixed-sleep functionality. Test corpus: 38
  sleeps, all but four are bounded-poll cadences; the four violations
  FIXED: conformance entrypoint sleep-then-assert (the live CI failure —
  now WaitHealthy-bounded, proven green on both adapters, K8s 374.0s),
  phase5's 5s initdb assumption (now pg_isready poll), drift-config's
  500ms PUT-propagation assumption (now bounded read-back poll), proxy
  test fixtures' 2s session holds (now hold-until-cleanup channels —
  timeless). Clock-condition waits (lease TTL expiry, deadline-in-past)
  and yield-based concurrency fixtures audited and kept — they are not
  machine-speed dependent. SYSTEMIC: DATASCAPE_WAIT_SCALE added at three
  chokepoints (WaitHealthy x2 adapters, WithReachable, and every settle
  deadline) — deadlines bound failure reporting only; any environment
  widens them with one knob (doc 02 §4.1 records the rule). Unfiltered
  unit true-exit=0; golangci 0 issues; scale=0.01 clamp exercised.
- 2026-07-22: CI parallelization (owner request) + two fixes. Fix: my
  phase5 pg_isready poll targeted the wrong container name
  (ext-attendance-db vs the test's actual datascape-ext-outofband-pg) —
  the bounded poll burned its full window on a nonexistent container;
  corrected and proven live (suite 15.2s, was 121s failing). CI:
  test-impact.sh gained --list (JSON suite ids) and --only <ids>; the
  serial 20-min integration job is now integration-plan (selects once)
  + a per-suite matrix — each suite on its own runner with its own
  daemon (the flock exists for SHARED local daemons only), wall-clock ≈
  slowest suite + setup, failures isolated per-job. integration-k8s
  sharded into two parallel kind clusters (adapter conformance vs cmd
  scenarios). Stale CI comments fixed (removed-twin policy rule, old
  job shape).
- 2026-07-22: WAVE 4 (closeout) LAUNCHED — owner: "get all remaining
  tasks finished and out the door", merge gates reviewed against ADRs by
  the orchestrator. In flight: I9 (generic Facts query; TunnelFacts
  migrated end-to-end, field list frozen by archtest), I10+I11 batched
  (fragment guard + slog), I12 (dbjob hardening, ADR 007 addendum
  decision first), H5 (domains, ADR 022 Rings 0-1, byte-identical
  undeclared-domain pin). Queued behind dependencies: E6 (guide teaches
  I9's generic form) → E7 (truth sweep + ContainerProvider retirement),
  H6 (mediated connections, needs H5). Merge order on green: I10+I11
  (smallest surface) → I12 → I9 → H5; then wave 4b.
- 2026-07-22: DECOUPLING VERIFICATION (owner-directed, orchestrator-
  executed): can core facilities (routing, access policy) change with
  ZERO provider edits? Verdict by facility:
  PASS — addresses/dialing (ADR 015: EnsureReachable/WithReachable,
  providers never construct addresses; proven by K8s adapter arriving
  with no provider changes). PASS — access policy (H3: validate-time,
  provider-invisible; B7 K8s isolation: runtime-side). PASS — port
  posture (loopback-default binding landed runtime-side, zero provider
  edits). PASS — published facts (I9's query; accretion frozen). PASS —
  runtime capabilities (optional interfaces + registry promotion; the
  ADR 018 promotion gotcha is the known, documented coupling point).
  PASS — secrets (engine-resolved into Request; backends invisible).
  FAIL — NETWORK IDENTITY: providerkit.Network hardcodes the shared
  name, 23 provider Networks: sites choose networks themselves, each
  provider EnsureNetworks its own, and the engine duplicates the
  literal a third time (engine.go ~802) — the construct-by-convention
  class ADR 015 banned for addresses, alive for network names. H5's
  segmentation would have forced every provider to become domain-aware.
  REMEDIATION (redirected into H5 mid-flight, before wrong code landed):
  the name providers pass becomes the LOGICAL platform-network token
  (zero provider edits, byte-for-byte); internal/domain/naming owns the
  single logical→concrete mapping (default domain → unchanged = the
  byte-identical pin; explicit overrides pass verbatim, pin-wins
  semantics); the engine's per-resource Runtime construction gains a
  decorating ContainerRuntime translating the token at every
  network-name-accepting port method; the engine's duplicated literal
  folds into the same authority. H5's acceptance now includes the
  zero-provider-diff proof and a decoupling archtest. Once landed, the
  invariant holds across every audited facility.
- 2026-07-22: OWNER REQUIREMENT captured as ADR 026 + doc 08 H7:
  graph-scoped access — least privilege at RESOURCE granularity,
  compiled from the declared reference graph (the graph IS the
  access-request set; refs are namespace-qualified, so the owner's
  A→{B/X,C/Y} vs A→{B/X} example is expressible today with zero new
  declarations). Wide grants become explicit, policy-visible
  declarations. Realization rides the H5 decorator chokepoint (zero
  provider edits, archtest-pinned) — per-edge NetworkPolicies on K8s,
  per-edge networks on Docker (scale bounds documented). Gate
  GraphScopedAccess Alpha/disabled. Sequenced AFTER H5 merges; the
  zero-trust progression is now: namespace walls → domain walls →
  graph-scoped reachability → identity-attested edges (H6), all from
  one graph.
- 2026-07-22: LATENT SAFETY BUG (found by H5's Ring-0 work, fixed by
  orchestrator immediately): manifest.envelopeFrom's field-by-field
  metadata decoder NEVER decoded metadata.protect — the NFR-3 protect
  refusal was inert for every real manifest since the feature shipped,
  while engine-level tests stayed green by constructing Envelopes
  directly (the loader was the untested seam). Fixed + pinned through
  the REAL loader (TestLoadDecodesMetadataProtect). Consistency note:
  this also explains H4's observation that no shipped example sets
  protect — nobody could have noticed it not working. H5 report also
  notes: K8s NetworkPolicy enforcement is SKIP-only on both the local
  minikube and CI kind clusters (default CNIs don't enforce) — B7's
  known caveat; follow-up recorded: put a policy-enforcing CNI (Calico)
  on the CI kind cluster to make B7/H5/H7 enforcement live-proven.
