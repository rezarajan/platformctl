# 12 — Path to Production: from "runs on a dev machine" to "tech firms rely on it"

## 0. What this document is

The single, actionable plan to take Datascape (d7s / `platformctl`) from its
current state — a functionally complete control plane proven on one
developer's Docker and, this session, a local Kubernetes cluster — to a
system a technology firm can trust to run **production** data platforms,
with absolute confidence in its **design, developer experience, value-add,
and stability**.

It is written so that **either a human or an agent** can pick up any task
with no context beyond the linked ADR/spec and this document. Read §1 before
starting anything. It complements — does not replace — docs/planning/08
(which took the system to feature-complete); doc 12 takes it to
*trustable-in-production*.

## 1. How to use this (humans and agents)

### 1.1 The task contract

Every task in §5 carries: an **ID**, a one-line **scope**, explicit
**acceptance criteria**, **dependencies**, a **size** (S ≤ 1 day, M ≤ 1
week, L > 1 week / decompose), and a suggested **owner** (human / agent /
either). A task is **DONE** only when its acceptance criteria are met **and**
the standing bar holds:

- tests added (unit + the relevant conformance/property suite);
- `CGO_ENABLED=0 go build ./...`, `go test ./...` (true exit 0), `go vet`
  (plain + `-tags integration`), and golangci all clean;
- schema/docs kept in sync in the same commit (CLAUDE.md rules);
- the layering invariant holds (`internal/domain`/`internal/ports` import no
  adapter);
- commits are unsigned (`git -c commit.gpgsign=false`), full messages; the
  owner finalizes with a signed commit before push.

"Shipped means no defects": a task is not done because it compiles — it is
done when its acceptance criteria are *verified*.

### 1.2 Agent pick-up protocol

1. Read `CLAUDE.md`, the task's linked ADR, and the task's acceptance.
2. Check the **phase gate** (§4): do not start a task whose dependencies are
   unmet or whose phase gate is not open.
3. If the task defines new cross-runtime behavior, extend the **conformance
   suite** (§6) — new behavior without a machine-checked property is not
   done.
4. For L tasks, checkpoint incrementally (`TASK_PROGRESS.md` + WIP commits)
   so a dead session resumes without repeating work.
5. Never pattern-match Docker/cluster state in teardown (named-only); never
   mint K8s tokens yourself (ask the human); never `cd` into worktrees.

### 1.3 Definition of task IDs

Workstream prefixes: **FV** formal verification & materialization, **RT**
runtime breadth & scale, **REL** reliability & operations, **SEC** security
& compliance, **DX** developer experience, **VAL** value & capability, **GA**
release & graduation.

## 2. Current state (baseline, 2026-07-24)

- **Proven:** the full HA zero-trust lakehouse capstone applies to `Ready`
  on **Docker** (26/26, all zero-trust + HA + broker-loss proofs green) and
  on **Kubernetes** (26/26, mediated CDC through the mesh, HA StatefulSets,
  idempotent self-healing under a real OOM chaos event). The just-works DX
  (ADR 035), include-members project composition, and portable per-provider
  runtime overrides all land.
- **Not yet proven / known gaps:** materialization is *carefully coded*, not
  *formally verified*; zero-trust network isolation currently leans on the
  CNI (a minikube Calico that did not enforce NetworkPolicy exposed this —
  ADR 037 REACH); no real multi-node cluster, no Terraform runtime, no Helm
  materializer; storage size/tier not yet declarable (ADR 036, 10Gi/default
  everywhere); most feature gates are Beta, a few Alpha; upgrade/migration,
  DR drills, continuous reconcile, multi-tenancy, supply-chain, and audit
  are partial or absent.

## 3. Definition of Done — the GA gate ("tech firms rely on it")

1.0-GA is declared only when **all** hold (this is the §6 GA6 checklist):

- **Design (verified):** every registered `(provider, runtime)` materializer
  passes the CAP/REP/IDN/REACH core-equality + TIER-ordering conformance and
  the differential (same-Intent-across-runtimes) suite; the Alloy/TLA+ model
  checks clean; zero-trust REACH is enforced by the mesh overlay and proven
  by the negative probe on Kubernetes-in-Docker, Docker-in-Docker, and
  Kubernetes-in-VM.
- **Stability:** survives node/broker/network-partition/control-plane-restart
  faults (self-heal or fail-safe) in a CI chaos gauntlet; upgrade vN→vN+1
  preserves state and running platforms with zero data loss; a full backup →
  restore DR drill passes on both runtimes; continuous reconcile heals drift.
- **Security:** zero-trust, secrets (Vault), and RBAC/multi-tenancy at GA;
  external pen-test findings resolved; releases signed with SBOM +
  provenance; no secret material in state or logs (scanner-proven); audit log
  of every control-plane action.
- **DX:** a new data engineer stands up a real platform from the docs alone
  within the onboarding benchmark; every failure path is actionable; the
  blueprint library covers the common platform shapes on both runtimes.
- **Value:** the provider/connector matrix at GA covers a real firm's data
  platform (multi-source CDC, ≥2 lake formats + Iceberg query, catalog,
  lineage, external orchestration) end-to-end in CI.
- **Release:** semver + API-stability + deprecation policy published, `v1`
  API cut; distribution via ≥2 channels incl. a `platformctl` Helm chart;
  cross-runtime CI matrix (Docker + real K8s + Terraform) gates every PR;
  documented support + security-disclosure process.

## 4. Phased sequencing (the critical path)

Phases gate *confidence*; independent workstreams parallelize within and
across them, but a phase's **exit gate** must be green before the next
phase's gate-dependent tasks start.

| Phase | Theme | Contains | Exit gate |
|---|---|---|---|
| **P1** | Formal correctness foundation (design spine) | FV1–FV7 | Core providers' materialization formally verified on Docker+K8s; REACH negative probe green on the three nested substrates; storage size CAP-preserved, tier degrades explicitly |
| **P2** | Runtime breadth & real-cluster hardening | RT1–RT4, FV8 | HA survives a **node** loss on a real ≥3-node cluster; Terraform route passes conformance; Helm-backed redpanda pilot green with rendered+materialized assertions |
| **P3** | Reliability & operations (stability) | REL1–REL6 | DR restore drill + upgrade-without-downtime + chaos gauntlet all pass in CI; continuous reconcile heals drift |
| **P4** | Security & compliance | SEC1–SEC5 | Pen-test resolved; zero-trust + secrets + RBAC/multi-tenancy at GA; releases signed + SBOM + provenance; audit log complete |
| **P5** | DX, value & docs (adoption) | DX1–DX5, VAL1–VAL4 | Docs-only bring-up works within the onboarding benchmark; provider/connector matrix at GA; blueprint library on both runtimes |
| **P6** | Release & GA | GA1–GA6 | §3 checklist all green → **1.0-GA** |

Parallelism notes: SEC4 (supply chain), DX1 (docs), REL4 (observability), and
GA4 (CI matrix) accrue continuously from P1. P4 security work may begin
during P2/P3 wherever its deps allow. The one strict spine is **P1 → P2**:
nothing downstream can claim "confidence in design" until the materialization
is verified.

## 5. Workstreams & task backlog

### FV — Formal verification & materialization (ADR 037; the design spine)

| ID | Title & scope | Acceptance | Deps | Size | Owner |
|---|---|---|---|---|---|
| **FV1** | Intent value types + `C ⊎ B` partition for redpanda, minio, postgres, mysql, debezium, s3sink, trino, nessie, openziti | A domain `Intent` type per provider; a documented C(invariant)/B(best-effort) classification; unit tests asserting minimality (no field derivable from others) | ADR 037 | L | either |
| **FV2** | `Materializer` port + registry selection by project runtime; container port stays the default | A provider can register `Mat_p`; engine selects by `project.Runtime.Type` or refuses with the ADR 037 message; existing providers byte-unchanged on the default path; archtest still green | FV1 | L | either |
| **FV3** | Alloy (or TLA+) model of CAP/REP/IDN/REACH | Model in `formal/`; checks clean at a defined scope; CI runs it; a documented map from model predicates → Go conformance assertions | ADR 037 | L | human-led + agent |
| **FV4** | Property-based + **differential** conformance suite | gopter/rapid suite: random Intents materialized on each registered runtime, asserting CAP/REP/IDN + TIER ordering; differential check (same Intent → equal `C_p` projections on Docker+K8s); CI-gated; a violated invariant fails CI | FV1, FV2 | L | either |
| **FV5** | REACH enforced by the mesh overlay, substrate-agnostic (ADR 034); NetworkPolicy demoted to defense-in-depth | The **negative probe** (an unauthorized path fails) passes on K8s-in-Docker, Docker-in-Docker, and K8s-in-VM; a non-enforcing CNI does **not** weaken enforcement; a materializer that cannot guarantee REACH does not register for that runtime | ADR 034, FV2 | L | either |
| **FV6** | Storage size/tier (ADR 036) as the first `C ⊎ B` instance | `VolumeSpec` gains tier; runtime schema carries a tier→class map; per-provider default sizes; `StableIdentity` volumeClaimTemplate consults it; size is CAP-preserved (differential test), tier degrades explicitly on Docker with a surfaced fact | FV1, ADR 036 | M | either |
| **FV7** | Cold-start-aware readiness deadlines | Per-provider readiness deadline scales with cold-start cost (image/JVM/replicas), separate from steady-state liveness, tunable; the M7 JVM providers reach Ready on a cold cluster without a spurious failure | — | M | either |
| **FV8** | Helm-backed K8s materializer pilot (redpanda), rendered + materialized assertions | redpanda HA on real K8s via a pinned official chart; **rendered** assertion parses replicas/size/ordinals/mesh from `helm template`; **materialized** assertion verifies the live STS; behind a gate | FV2, FV4 | L | either |

### RT — Runtime breadth & scale

| ID | Title & scope | Acceptance | Deps | Size | Owner |
|---|---|---|---|---|---|
| **RT1** | Real multi-node K8s validation (not minikube) | Capstone on a ≥3-node cluster with a policy-enforcing CNI; anti-affinity/PDB/topology-spread verified; HA survives a **node** loss (not just a pod); REACH negative probe green with the CNI enforcing too | FV5 | L | human (cluster) + agent (tests) |
| **RT2** | Terraform runtime materializer (the third route) | ≥1 provider materializes via Terraform and passes the conformance + differential suite for its `C_p` | FV2 | L | either |
| **RT3** | Scale & load testing | A large scenario (100s of resources) applies within a stated reconcile-latency SLO; ParallelReconciliation graduated after soak | FV4 | M | either |
| **RT4** | Per-runtime external-endpoint resolution (first-class) | An external Source's Connection resolves correctly on Docker + K8s + Terraform **without diverging the shared plane** (generalizes the M7 orders-DB target/`targetNetworks` divergence) | FV2 | M | either |

### REL — Reliability & operations (stability)

| ID | Title & scope | Acceptance | Deps | Size | Owner |
|---|---|---|---|---|---|
| **REL1** | Upgrade & migration | An upgrade test vN→vN+1 preserves state + running platforms; a data-bearing provider version bump rolls with zero loss; state-schema migrations covered | — | L | either |
| **REL2** | Backup/restore GA + DR drills | Scheduled backup + full **restore drill** in CI on both runtimes; RPO/RTO stated; DR runbook; BackupRestore graduated | REL1 | L | either |
| **REL3** | Continuous reconciliation / operator mode + drift heal | A controller mode continuously reconciles; an out-of-band change is detected and healed within an interval; DriftDetection graduated | — | L | either |
| **REL4** | Observability GA | `platformctl` exports its own metrics; a reconcile is traceable end-to-end (logs+traces); managed-platform dashboards; SLOs; MonitoringStackProvider graduated | — | M | either |
| **REL5** | Chaos/resilience gauntlet | CI suite: node loss, broker loss, network partition, control-plane restart, mid-reconcile kill, partial-apply recovery — every scenario self-heals or fails safe; documented | RT1 | L | either |
| **REL6** | State backend production hardening | Two concurrent operators cannot corrupt state (locking); a remote backend supported; SharedStateBackend graduated | — | M | either |

### SEC — Security & compliance

| ID | Title & scope | Acceptance | Deps | Size | Owner |
|---|---|---|---|---|---|
| **SEC1** | Zero-trust GA + external pen-test | REACH enforcement proven on all target substrates; pen-test findings resolved; ZeroTrust/MediatedConnections/PolicyEngine/GraphScopedAccess/LabelScopedAccess graduated | FV5, RT1 | L | human-led + agent |
| **SEC2** | Secrets GA (Vault) + rotation | VaultSecretBackend graduated; secret rotation without downtime; a scanner proves no secret material in state or logs | — | L | either |
| **SEC3** | RBAC & multi-tenancy | Per-team projects; a tenant cannot reach another's resources; the minimal-RBAC kubeconfig model hardened + documented | — | L | either |
| **SEC4** | Supply chain | Releases signed; SBOM emitted; all images digest-pinned (finish); SLSA provenance attestation | — | M | either |
| **SEC5** | Audit logging & compliance surface | Every apply/destroy/config action audit-logged; encryption at rest + in transit; data-residency knobs; a SOC2-shaped control mapping | REL4 | M | either |

### DX — Developer experience & adoption

| ID | Title & scope | Acceptance | Deps | Size | Owner |
|---|---|---|---|---|---|
| **DX1** | Documentation completeness | Getting-started, provider reference (auto-gen, sync-checked), architecture, runbooks, troubleshooting, migration guides; docs CI checks links + schema-sync; a new engineer stands up the capstone from docs alone | — | L | either |
| **DX2** | Error-message quality pass | A rubric (what/why/how-to-fix); a review of every error return; ADR 031 diagnostics channel consistent | — | M | either |
| **DX3** | Blueprint & example library | Streaming-only, batch lakehouse, and ML-feature-store blueprints — all apply to Ready on Docker+K8s, all lint-clean; DesignLints graduated | DX1 | M | either |
| **DX4** | Plan/dry-run & tooling UX | Accurate plan diffs; dry-run; JSON schemas published for editor validation (LSP-ready) | — | M | either |
| **DX5** | Onboarding-time benchmark | A reproducible time-to-first-working-platform benchmark, met under a stated target | DX1 | S | either |

### VAL — Value & capability coverage

| ID | Title & scope | Acceptance | Deps | Size | Owner |
|---|---|---|---|---|---|
| **VAL1** | Provider coverage to GA | Graduate the Beta providers that pass conformance; add high-demand ones (a compute engine — Flink/Spark; dbt; cloud sinks — Snowflake/BigQuery/Delta) | FV4 | L (ongoing) | either |
| **VAL2** | Connector & format coverage | ≥2 lake formats (e.g. Parquet/Avro + Iceberg/Delta) demonstrated healthy alongside Iceberg query | VAL1 | M | either |
| **VAL3** | External orchestration first-class | Dagster/Airflow integration guides + a deployment that reads/writes the platform end-to-end in CI | VAL1 | M | either |
| **VAL4** | Governance plane | Lineage across every binding; a policy-governed access example; data-contract surface | — | M | either |

### GA — Release engineering & graduation

| ID | Title & scope | Acceptance | Deps | Size | Owner |
|---|---|---|---|---|---|
| **GA1** | Feature-gate graduation plan | Every gate has explicit Alpha→Beta→GA criteria + schedule (doc 04 §12); core gates driven to GA as their soak completes | — | M (ongoing) | either |
| **GA2** | Versioning & compatibility policy | Published semver + API-stability + deprecation policy; the `datascape.io/v1alpha1` → `v1` API cut | — | M | human-led + agent |
| **GA3** | Distribution | Install via ≥2 channels (binary, container, package manager) + a `platformctl` Helm chart for cluster-hosted control plane | — | M | either |
| **GA4** | CI/CD maturity | Cross-runtime CI matrix (Docker + real K8s + Terraform) gates every PR; release automation; the integration-test economy (doc 06 §10) scaled to the matrix | RT1, RT2 | M | either |
| **GA5** | Support & operations model | Documented issue-triage, SLAs, security-disclosure, and community process | — | S | human |
| **GA6** | **GA sign-off gate** | The §3 Definition-of-Done checklist all green → declare **1.0-GA** | all | — | human |

## 6. Formal verification track (called out)

The design-confidence claim rests on artifacts, not assurances:

1. **The model** (`formal/`, Alloy or TLA+): Intent algebra + materialization
   relation; CAP/REP/IDN/REACH as model-checked theorems. Proves the design
   cannot satisfy the `Materializer` interface while violating a core
   equality. *Owner-reviewed; changing a core equality starts here.* (FV3)
2. **The conformance suite** (Go, property-based): the same predicates,
   generated over random Intents, run for every `(provider, runtime)` — the
   mechanized proof the *implementation* refines the model. (FV4)
3. **The differential check:** same Intent on two runtimes ⇒ equal `C_p`
   projections. Directly enforces "the same intent means the same thing on
   Docker and K8s." (FV4)
4. **The REACH negative probe:** an unauthorized path is *confirmed to fail*
   on every nested substrate. Security is proven, never assumed. (FV5)
5. **The Helm hand-off:** rendered assertion (pre-apply) + materialized
   assertion (post-apply) bracket every external chart. (FV8)

CI gates on 2–5; 1 runs in CI at its defined scope. No `(provider, runtime)`
route is GA until all five are green for it.

## 7. Non-negotiables / risk register

- **REACH never degrades.** Zero-trust reachability is the one invariant with
  no best-effort tier; enforced by the mesh overlay on every substrate, or
  the runtime is refused. (ADR 037)
- **Intent is never silently misrepresented.** Core equalities (size,
  replicas, identity, reachability) hold exactly on every runtime;
  best-effort degradations (tier, affinity) are always surfaced, never traded
  against the core.
- **The layering invariant.** No adapter import in `domain`/`ports`; the
  `Materializer` port holds runtime specifics (incl. Helm) out of the domain.
- **Operational safety.** Named-only teardown (never pattern-match runtime
  state); protected/data-bearing resources refuse destructive ops; commits
  unsigned, owner signs; no self-minted K8s tokens.
- **Top risks:** (a) formal-model scope too small to catch a real violation →
  mitigate with the differential + property suites as the belt to the model's
  suspenders; (b) external Helm chart drift → pin + bracket with assertions;
  (c) over-formalization stalling delivery → model the core only, pilot on
  redpanda/minio first; (d) real-cluster/CNI variance → RT1 on a policy-
  enforcing CNI plus FV5's substrate-agnostic mesh enforcement.

## 8. Milestones & suggested cadence

- **1.0-beta** — P1 + P2 exit gates green (design verified; real-cluster HA;
  three runtimes conformant). The "confidence in design" milestone.
- **1.0-rc** — P3 + P4 exit gates green (stability + security). The "a firm
  can run it" milestone.
- **1.0-GA** — P5 + P6 (§3 all green). The "a firm can *rely* on it"
  milestone.

Each phase closes on its gate, not a date; agents and humans draw the next
ready task (deps met, gate open) from §5 and run the §1.1 contract to done.
