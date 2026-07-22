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
- [ ] B3 production-scenario walkthroughs: external cloud DB (TLS,
      secured connector), VPC-via-tunnel (A1 outcome), each exercised
      end-to-end or explicitly documented as gapped
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
