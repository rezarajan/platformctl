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

- [ ] Full docs pass (each doc 01–10, ADR index) against goal areas
- [ ] Amendments committed (additive; guard-compatible)

## Phase B — codebase conformance review (blocked on Phase 0)

Fan-out review (sonnet agents, file-ownership fenced) once main is
static. Dimensions, each producing findings verified before fixing:

- [ ] B1 async/state-machine correctness: every wait/poll/settle/probe
      path in engine + providers + runtimes (races, unsettled-Ready,
      machine-speed dependence)
- [ ] B2 GA-bar audit: every gate marked GA/enabled vs its actual
      test evidence and error-path behavior
- [ ] B3 production-scenario walkthroughs: external cloud DB (TLS,
      secured connector), VPC-via-tunnel (A1 outcome), each exercised
      end-to-end or explicitly documented as gapped
- [ ] B4 coding practices: error wrapping, context propagation,
      goroutine lifecycle/leaks, lint debt beyond golangci defaults
- [ ] B5 fix waves for confirmed findings, impact-gated per doc 06 §10

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
