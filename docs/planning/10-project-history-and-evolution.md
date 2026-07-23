# Project History & Evolution — the consolidated record

Written 2026-07-20. This document is the single sequential narrative of how
Datascape (`platformctl`) got from an experimental prototype to its current
state: every phase, stage gate, audit, and course-correction, **with the
reasoning that drove each change**. It consolidates what was previously
scattered across `checkpoint.md`, `errors.md`, `feature-requests.md` (now
archived under `docs/history/`), the stage-gate close-out notes in docs
07–09, and the design notes.

It is a **historical record, not a contract**: the contracts live in docs
01–03, the live work backlog in doc 08. When this document and a contract
doc disagree, the contract doc wins; when it and git history disagree, git
wins. Commit hashes are given so every claim is checkable.

---

## 0. Origin — the experimental phase and the production rebuild

The project began as `project-datascape`, an experiment with a much larger
kind vocabulary (`DatabaseClass`/`DatabaseInstance`, `ConnectorClass`,
`CDCClass`/`CDCInstance`, `RelationalSource`, `ObjectStore`,
Kubernetes-shaped volume kinds, and more). The production rebuild was
deliberately **greenfield with respect to design**: the planning package
(docs 00–06, committed at `847a5f5`) did not assume any of the experimental
layout survived.

The founding consolidation decisions (full rationale in doc 00's table):

- **Five class/instance kinds collapsed into one `Provider`** (`type` +
  `runtime` + `configuration`) — the class/instance split solved a problem
  `providerRef` + `runtime` already solved more simply.
- **`Source` became one kind with an engine-keyed, provider-extensible
  sub-block** (`spec.engine: postgres` + `spec.postgres: {...}`) — a new
  engine is a schema fragment plus a provider declaration, never a core
  schema change.
- **Kubernetes-shaped storage kinds deferred** — abstracting over storage
  before a second runtime existed would have been premature generality.
  (Validated later: the Kubernetes adapter needed only a `VolumeSpec`
  namespace hint, not a storage vocabulary — see §6.)
- **Compatibility as provider capability, not type system** — capability
  interfaces (`CDCCapableProvider`, ...) checked at `validate` with an
  exactly-specified error shape (doc 02 §5.2).
- **Lineage observed, never synthesized** — Datascape forwards a
  `LineageEndpoint` connection fact to a `LineageAware` provider; it never
  constructs lineage semantics (doc 01 NG7, guiding principle 7).
- **The three-layer invariant** — resource model / provider / runtime, with
  domain and ports never importing adapters. Every subsequent audit
  re-verified this; it has never been violated in production code.

## 1. Phases 0–5 — the committed path to v1.0.0

Executed as thin vertical slices per doc 04, each phase closing with
runnable, tested exit criteria (evidence per phase recorded in
`docs/history/checkpoint.md`):

| Phase | Shipped | Key commits |
|---|---|---|
| 0 Foundations | domain/ports/application skeleton, fake runtime, localfile state, plan golden tests | `847a5f5` |
| 1–2 Docker runtime + Redpanda | `ContainerRuntime` against real Docker; conformance suite; first real provider | `639e782` |
| 3 CDC + lineage mechanism | postgres + debezium providers, compatibility checks, `observers`/`LineageAware` proven against a fake | `f7777d5` |
| 4 Object-storage sink | s3/minio + s3sink providers, `Dataset`, sink Bindings | `5c8aeb5` |
| 5 External/Imported/Drift — **v1.0.0** | `import`, external configure-only paths, `drift`, engine-level NFR-3 guard | `62bac0b`, `329c000`, `493a744` |

One design correction happened **before** v1.0.0 was declared, and a second
followed immediately after it in the same spirit — both project-owner-
prompted, both recorded because they changed shipped contract shapes:

- **The taxonomy revision** (`docs/adr/001`, commit `87d6189`): the
  draft `Binding.mode → Kind pair` table was a *function*; real platforms
  need a *relation* (a database is a legitimate sink; an object store a
  legitimate source). Asset kinds were redefined role-neutrally, direction
  lives only in `sourceRef`/`targetRef`, and the `sink → Source` /
  `ingest` pairings shipped as schema-stable capability seams with no
  provider — because shapes are the GA contract and implementations are
  not. Had the function-shaped table gone GA, database-as-sink would have
  been a breaking change instead of additive provider work.
- **Nouns before providers** (`552dfd9`, `df3fa93`, days after the
  `493a744` release commit — the first application of the pattern the
  Catalog/Connection remodel then formalized, §2): technologies realize
  provider-agnostic kinds; they never become kinds themselves.

v1.0.0's definition of done (doc 05 §9) was fully automated: the
10-resource acceptance scenario in CI against real Docker, NFR checks
(determinism golden files, zero-mutation re-apply, NFR-3 safety, SIGKILL
recoverability, 4-minute performance budget) as tests, and seven gates at
GA.

## 2. Phases 6 and 6.5 — scale-out and the orchestrator-ready remodel

Phase 6 delivered parallel reconciliation and the vault/file secret
backends (`c05f781`). Phase 6.5 (project-owner direction, design note 002)
aimed platformctl at being the infrastructure a Dagster-style orchestrator
runs against — MySQL/MariaDB, Nessie, Marquez (the formerly-optional
openlineage provider), and a stable-entrypoint proxy.

**The mid-flight remodel is the most instructive pivot in the repo's
history** (design 002's addendum): the first cut shipped Nessie as a bare
provider type and proxy routes as provider configuration. Owner review
redirected it before release on two grounds:

1. **Model first.** Two provider-agnostic kinds were introduced instead:
   `Catalog` (engine-discriminated, mirroring `Source`) and `Connection`
   (the stable-entrypoint noun — managed forwarder or external address
   record, credentials always via `secretRef`). Bindings against external
   Sources consume the Connection automatically. This is the repeatable
   rule: *technologies realize nouns; they never become nouns.*
2. **"Soak" was retired as a product term** — stage names must not leak
   into manifests, examples, or the roadmap; the feature set was baseline
   GA-track functionality (`examples/lakehouse/`).

A post-6.5 hardening sweep (from the then-live `errors.md` /
`feature-requests.md`, both archived with per-item write-ups) delivered:
secret preflight + `--env-file`, secret-rotation fingerprints, honest
external reachability probing, the architecture `graph`, `inventory` with
endpoint facts, Docker-style progress output, the searchable docs site,
immutable versioned provider profiles (`f4f77d3`), and deterministic
auto-allocated host ports (`8b6097b`). The **validate-time completeness
contract** crystallized here: a manifest set that validates must not be
able to half-apply into a mis-wired platform (schema → kind validation →
graph → compatibility/capability → `SpecValidator` → feature gates).

## 3. The production gap analysis — doc 07 and Gates 0–3

With v1.0.0 shipped, a full review (2026-07-15, doc 07) asked what stands
between "demo-correct" and "production-grade Docker runtime". It produced
four stage gates, worked in close-out passes:

- **Gate 0 — foundation correctness** (`a2c1484`, closed 2026-07-16):
  canonical identity (namespaced `resource.Key`, DNS-label names, escaped
  v2 state keys with a migration path, project-scoped ownership labels);
  central `ExternalConfigurer` enforcement in the engine; authoritative
  apply-deletes with `ActionOrphanUnknown` refusal for legacy state;
  machine-output contract; collision-resistant hex renderer ids;
  loopback-default binds and refusal of unmanaged same-name objects.
- **Gate 1 — Docker production runtime** (closed 2026-07-16): restart
  policy, resource limits, security context, log config, network aliases,
  observed-port inspection (`ContainerState.Ports`/`HostAddr`), file-mounted
  secrets (`ContainerSpec.Files` + `ReadFile` — bootstrap passwords out of
  inspectable env), pull policy + digest pinning, state-dir fsync.
  Explicit, reasoned deferrals: registry auth (→ Stage A1), GC tooling
  (→ A2), state doctor (→ A3), host-path mounts (permanently unsupported —
  not portable across runtimes).
- **Gate 2 — lakehouse/pipeline completeness** (closed 2026-07-16,
  `09e1b61` + close-out): credential escaping fixed everywhere (round-trip
  tested against real drivers), unique Debezium `server.id` (a behavioral
  migration — `docs/upgrade-notes.md`), `BindingOptionsValidator`,
  `deletionPolicy: retain|delete` on data-bearing kinds, drift probes
  upgraded from liveness to **desired-configuration equivalence** (the
  per-provider table in doc 07 §2.1), `inventory --for <tool>` config
  rendering, pinned images, explicit `Insecure` endpoint labeling.
  Recorded decisions: Iceberg tables belong to external tools (modeling
  them would re-implement engine DDL); JSON is the supported sink format
  until a schema registry ships (→ D1/D2).
- **Gate 3 — contribution readiness**: deliberately left open; its items
  became Stage E.

## 4. The remediation audit — docs held to the code's standard

A dedicated audit (2026-07-16/17, `docs/remediation/`, findings
F-001–F-010, all resolved) checked every doc-07 claim against the code. Its
lasting lessons, beyond the ten fixes (machine-output violations, stale
generated docs, a stale README table, test-layering waivers, `just check`
that couldn't fail, ...):

- **Stale docs are architectural debt** — contributors copy the wrong
  patterns. The fix class was mechanized where possible:
  `TestGeneratedReferenceInSync` fails CI when `docs/reference/` drifts
  from `schemas/`; the schema-change rule (schema edit ⇒ doc 03 update in
  the same commit) is a standing convention in `CLAUDE.md`.
- **Stage-gate summaries must update the checklists they summarize**
  (F-006: Gate 2's §2.2 checklist sat unticked for a day after every item
  was fixed) — the origin of this repo's habit of auditing checkbox truth.
- The `ARCHITECTURE-ASSESSMENT` also recorded **verified-OK** results and
  disproven hypotheses, so future audits don't re-plow the same ground.

## 5. Doc 08 — the stage-gated production readiness plan

Doc 08 (2026-07-17, `1eabffa`) superseded per-phase planning: every open
item from docs 00–07, checkpoint, and errors got exactly one home in
Stages A–E (later F), each task executable in isolation. **Doc 08 is the
live, sequential, stage-gated action plan**; this history records what has
closed.

### Stage A — operational hardening (closed 2026-07-18; v1.1 milestone)

`0513bd1`…`da3abe5`: registry auth for private images (A1),
`gc plan|apply` (A2), `state inspect|doctor|repair` + a formal migration
chain (A3), the S3 shared-state backend with lease locking (A4, design
note 003 — S3 chosen because the product already depends on object
storage; no new dependency class), `metadata.protect` deletion guard (A5),
the external-lifecycle kind-by-kind audit (A6 — documented in doc 03
§3.3), the generic machine-output harness (A7), out-of-band config-drift
tests (A8), MariaDB CDC coverage (A9), and the digest-pinning workflow
(A10 — `scripts/pinned-images.txt` + a scheduled refresh job).

### Phase 7 / Stage B — Kubernetes to Beta (closed 2026-07-19; v1.2 milestone)

The phase existed to test the project's central bet: a second runtime
adapter lands **without changing any provider**. The bet held — with
precisely-documented exceptions that became the most valuable data the
project has produced:

- The adapter (`internal/adapters/runtime/kubernetes`) passed the same
  conformance suite as Docker, live against minikube, and ran the
  unmodified redpanda provider end-to-end (`7d16e53`, doc 07
  Cross-Runtime). One genuine port defect surfaced (`VolumeSpec` needed a
  namespace hint) and one translation bug (Docker `Cmd` → K8s `Args`, not
  `Command`) — found only by running a real provider, which became the
  "conformance proves the port, only real providers prove the
  translation" lesson.
- B1–B9 (`2f12e17`…`5da8367`) delivered access modes
  (port-forward | node-port | load-balancer | in-cluster), observed
  endpoints, sized/classed volumes with a persistence proof, the
  Kubernetes SecretStore backend, a minimal RBAC posture proven by running
  CI under it, cluster preflight, NetworkPolicy parity with Docker's
  isolation, and the full example scenarios on a real cluster.
  `KubernetesRuntime` graduated Alpha → Beta, enabled by default.
- Live testing kept finding bugs no suite caught — thirteen of them
  (K1–K13, cataloged in doc 09 §1) — which triggered the next audit.

### The doc 09 audit — five failure classes, one missing plane (2026-07-19)

`b507c3b`. The thirteen Kubernetes bugs plus four earlier Docker ones
reduced to five recurring classes: (1) network topology leaked into
dependents (ten call sites hardcoding `127.0.0.1`), (2) exists ≠ ready ≠
reachable conflated, (3) under-declared intent a permissive runtime
tolerates, (4) runtime-object identity by convention, (5) contract tests
that prove the port but not the translation. The synthesis: the
**connectivity/discovery plane was never named as a layer**, so its logic
precipitated into whichever provider needed it that day. Each class got a
systems-level fix (Stage F) placed so *a provider author cannot
reintroduce the class* — compiler, conformance suite, or the capability
simply isn't expressible.

### Stage F — segregation readiness (closed 2026-07-20)

`6a0526b`…`87a3b4e`:

- **F1** — addresses became unconstructible: `DialAddress()`'s managed
  loopback guess deleted (external Connections expose `ExternalAddress()`
  only — docs 08/09 planned this under the name `DeclaredAddress`), one shared `WithReachable` helper (resolve → call → re-resolve
  per retry), and an architecture test banning loopback literals in
  providers/domain.
- **F2** — explicit `PortBinding.Audience: host | internal`; the
  `HostPort: 0` magic value retired; the fake runtime became the *strict
  interpreter* (stricter than Kubernetes), so under-declaration fails in
  `go test ./...`.
- **F3** — `EnsureReachable` contract hardened: returned addresses are
  currently dialable; adapters absorb async races (NodePort iptables,
  port-forward listen, initdb socket-only window).
- **F4** — one naming authority + endpoint facts; consumers resolve facts,
  never re-derive names (`cf73246`).
- **F5** — **the largest post-v1.0.0 contract change**: `reconciler.Request`
  (envelope, runtime, realizing Provider, resolved secrets, validated
  resource set) replaced the accreting `*Aware` setter interfaces
  (`ba68b26`). Providers are now stateless per call and the surface is
  serializable — the prerequisite for Phase 8's out-of-process plugins.
  Doc 02 §4.2 documents the current shape.
- **F6** — the conformance ratchet: entrypoint-faithfulness and
  delayed-listen subtests back-filled (`c5aead4`, `87a3b4e`); the standing
  policy is recorded in doc 06 §8 — *a bug found only by live testing
  lands with a contract-level reproduction in the same commit, or a
  documented per-runtime difference in doc 07 when the semantic can't be
  expressed at the port.*

Two post-Stage-F fixes are the ratchet's first exemplars (both
2026-07-20, RCAs in `docs/history/errors.md`):

- **NetworkPolicy vs. external access modes** (`05eeddd`): the B7
  default-deny wall silently dropped the very NodePort/LoadBalancer
  traffic B1's access modes exist to admit (SNAT'd sources never match
  `allow-same-namespace`). Kubernetes-only, inexpressible at the port —
  documented as a per-runtime difference in doc 07 per the ratchet's
  secondary branch, with a per-container `datascape-allow-external-<name>`
  policy as the fix.
- **`RemoveNetwork` while workloads remain** (`ca9d719`): Docker refuses
  to remove an in-use network; the K8s adapter deleted the whole
  namespace, cascading over siblings and unmanaged workloads. Expressible
  at the port — pinned in the conformance suite as
  `RemoveNetwork_refuses_while_container_attached` on all three adapters,
  per the ratchet's primary branch.

### Stages C, D, E — in progress

- **C5 decided** (design note 005, `8f74dd8`): managed databases stay
  single-node (+ backup/restore + drift-heal); production HA databases
  integrate as `external: true` Sources through the Connection seam.
  Reimplementing Patroni/Galera is not a plane platformctl should own; the
  note enumerates exactly what would change if a managed HA mode is ever
  added, so "not yet" never hardens into "not possible".
- **D9 decided** (design note 006, `19e5fbd`): Trino ships first as the
  compute-engine provider (D10 spec added to doc 08) — read path completes
  the lakehouse story, and Trino's statelessness avoids partial-ownership
  problems; Flink deferred as application-adjacent. Engine
  *infrastructure* is in scope; job execution never is (NG1).
- **E1 shipped** (`03230fa`, `a87d01a`): `platformctl init` blueprint
  scaffolding — init → apply → Ready on a Docker-only machine with zero
  manifest edits, verified end-to-end.
- **Three tasks have parallel implementations on unmerged branches**
  (agent worktrees under `.claude/worktrees/`, all based on main at
  `a87d01a`), discovered and principles-reviewed during the 2026-07-20/21
  documentation consolidation: **C1** (replicas/stable identity, with
  design note 004 — reviewed clean on layering/Stage-F, four Medium
  findings fixed on-branch in `5fd4ac3`), **D1** (Redpanda schema
  registry + Avro/Protobuf — reviewed near-clean, its one gate-boundary
  finding fixed on-branch in `2a05bd4`), and **C6** (backup/restore —
  **not merge-ready**: both accept-criterion round-trips fail against real
  Docker, with F1/F4 violations in the engine's address handling; the
  required fixes are itemized in doc 08's C6 status note). The C6 outcome
  is the review system working as designed: the Stage-F invariants gave
  the review concrete, checkable rules, and the violations were caught
  before merge rather than on a cluster. Merge decisions rest with the
  project owner; all three branches will conflict trivially in doc 04
  §12's gate table (restructured 2026-07-20) and touch `main.go`/
  `reconciler.go` in ways that need sequenced rebases. *(Closure,
  2026-07-21: C1 merged after its full Kubernetes integration suite ran
  green on live minikube; D1 merged after its live Avro run surfaced and
  fixed two more real defects — a missing-converter image requirement and
  hyphenated-DNS-label names being illegal in Avro namespaces; C6's
  rework proceeded on its branch.)*
- Everything else in Stages C/D/E remains open in doc 08; §10 there is the
  sequencing. The headline remaining work: the HA scenarios built on C1,
  ingress/TLS/monitoring, Parquet on D1's registry, and the
  provider-author contract (E5/E6, deliberately sequenced after F5
  stabilized the contract they would document).

## 6. Process evolution — how the repo is worked, and why

- **Planning docs are a contract, mechanically guarded.** The
  `guard-planning-docs.sh` PreToolUse hook has evolved deliberately:
  block-everything → allow checkbox toggles for completed work
  (`e4042a6`) → allow purely additive edits (recording facts about shipped
  behavior is not revising the plan) → allow new-document creation and an
  explicit, marker-file maintenance unlock (2026-07-20 consolidation).
  Modifying existing contract text still defaults to human-only.
- **Agent infrastructure is checked in** (doc 06): `CLAUDE.md` under 200
  lines with the one invariant; path-scoped rules; subagents
  (provider-implementer, compatibility-reviewer, integration-test-runner,
  docker-verifier, schema-doc-sync); a fmt/lint PostToolUse hook.
- **Checkbox truth is audited, not assumed** — twice a close-out summary
  referenced fixes without ticking the checklist it summarized (Gate 0
  §0.5, Gate 2 §2.2); both were caught by audits and corrected. When a
  stage closes, its exit criteria get ticked in the same pass as the
  evidence.
- **Deferrals carry reasons.** Every open item is either mapped to a task
  (doc 08 §9), deferred with a recorded rationale, or explicitly
  designated permanently-unsupported. Nothing is silently dropped.
- **Design notes are decision records.** Anything that changes a shipped
  shape, adds a dependency class, or draws a scope line gets a numbered
  note under `docs/adr/` with options considered and follow-ups —
  including superseded first cuts (002's addendum), kept as history.
  On 2026-07-21 the directory was formalized as an ADR set
  (`docs/design/` → `docs/adr/`): the six original notes kept their
  numbers, 007 stayed reserved for C6's backup/restore design, and ADRs
  008–016 were written retroactively so the standing architectural
  decisions (layering, capabilities, lineage, validation, determinism,
  safety, gates, the connectivity plane, the Request contract) exist as
  findable records rather than archaeology across the planning package.
  The same pass added doc 08 §2.1 (a literal task-execution protocol for
  lower-capability agent sessions) and §7.6 Stage G (the structural-debt
  register from the 2026-07-21 code survey).

### 5.x The 2026-07-22 wave — Stages C and D close; guardrails and composition land

A six-agent orchestrated wave (doc 06's process at full stretch) merged,
in one day: the WireGuard tunnel provider (D5, ADR 023 — negative
reachability proven in-network), the design-lint engine + provider lints
(H1+H2, ADR 020 — `platformctl lint`, 15 codes, blueprints lint-clean),
interactive composition (E9, ADR 024 — `add`/`wire`/`expose` compiling
to manifests, huh v2 confined by archtest), the monitoring completion
(C9 — postgres/mysql exporters with dedicated least-privilege users +
grafana; **Stage C closed**), and `Catalog.spec.warehouseRef` (D8 —
WarehouseFacts on the stateless Request, closing D10's inference gap).
Two composition-with-lints co-evolution bugs were caught at merge gates,
not after: composed output tripping DL020, and an archtest sweeping
agent worktrees. Evidence discipline: one 18-suite impact sweep at the
final content-state (0 failed) instead of per-merge sweeps — the ledger
economy (doc 06 §10) working as designed. The same day, the owner opened
the **production review** (doc 11): Phase A promoted three findings to
Stage I tasks — `spec.via` consumption (I1; until then `validate`
refuses the field rather than silently realizing an untunneled
forwarder), outbound database TLS (I2; `sslmode=disable` was hardcoded
in every database-facing consumer), and the Settledness NFR-11 (I3,
applied to doc 01). ADR 025 scoped cloud IAM database auth out
(composable today via the clouds' own auth proxies; doc 03 §8.2.4 is
the worked walkthrough).

### 5.y The production review saga (2026-07-22 → 2026-07-23) — waves 2-4, ADR 026/027/028, and the no-compromise pass

Doc 11's production review (opened 2026-07-22, goal: real production
scenarios, async/state-machine correctness, GA-bar honesty) ran as a
sequence of agent waves gated by the same merge discipline as the earlier
stages, but compressed into roughly a day and a half of wall-clock time
because each wave's findings immediately re-sequenced the next.

**Wave 2** merged I1 (`spec.via` consumption, blast-minimized tunnel
egress), I2 (outbound database TLS), H3 (the typed-rule policy engine,
ADR 021), and I6 (the unconditional-GA gaps `KubernetesRuntime` needed).
The owner decision that followed: `KubernetesRuntime` does not GA until
high availability is feature-complete — I7 became the gate.

**Wave 3** closed I7 (the GA blocker), landed E5 (schema fragments) and
H4 (the zero-trust policy pack, one-rule-per-fact per owner direction),
and a hygiene pass. The orchestrator then ran a systems pass directly
(not agent-delegated) fixing seven root-cause issues found by exercising
the whole system together rather than one suite at a time — hostport
collisions between concurrently-run suites, S3 state-lease renewal plus
a dead-holder test seam, a minimum-view cluster settle race, transient
probe-retry hardening, I8's forward re-resolve, Connect's 10s rebalance
delay, and an ingress fragment's `httpsPort` — and rewrote the README for
the platform's then-current state. Every suite was green at that
content-state; a GA-caveat sweep (owner: "no GA without conclusive
evidence") then closed two more items live: Kubernetes NetworkPolicy
enforcement was PROVEN, not assumed — the CI `kind` cluster had been
running the default `kindnet` CNI, which never enforces `NetworkPolicy`
at all, so every isolation test had been silently skipping its
enforcement assertion; switching CI to Calico plus a live
cross-namespace negative-probe test
(`TestNetworkPolicyEnforcementIsLive`) closed the gap, and
`ContainerSpec.Sysctls` on Kubernetes changed from a silent drop to a
named refusal (doc 07's loud-refusal clause, finally implemented).

**The ADR 027 pivot.** Mid-review, the owner posed a design challenge
directly: network-layer enforcement authenticates *location*, not
*workload*, and its guarantee varies by fabric (a non-enforcing CNI, a
Docker network's topology-as-ACL, a future cloud security group) — so
any zero-trust claim resting on it is not a guarantee at all. **ADR 027
(enforcement layering)** is the answer: two explicit layers with
different contracts. Layer 1 — identity-attested, mutually-authenticated
edges, realized by a `MediationProvider` port (ADR 022 Ring 2 promoted
from "a mesh feature" to THE authoritative guarantee; OpenZiti is the
first adapter, never the architecture) — is the only configuration
allowed to say "zero-trust," and it is substrate-independent by
construction (identical on Docker, Kubernetes, and any future
Terraform-provisioned infrastructure, because enforcement travels with
the workload). Layer 2 — the platform's compiled network objects
(Docker networks, Kubernetes `NetworkPolicy`, future security groups) —
is demoted to best-effort defense-in-depth, *observed, never assumed*:
`status`/`preflight`/`validate` report exactly what a live probe found
(`enforced` / `NOT ENFORCED by this cluster's CNI` / `unknown`), because
an unverifiable claim is treated as false. ADR 027's claims table is now
the only phrasing this repo's docs are allowed to use. The same review
pass also produced **ADR 026** (graph-scoped access — least privilege at
resource granularity, compiled from the declared reference graph itself,
so a wide grant becomes an explicit, policy-visible declaration instead
of an implicit side effect of sharing a network) and sequenced its H7
realization.

**Wave 4** implemented the ADR 027 pivot: H8 (the Layer 2 honesty probe
— capability, `status`/`preflight` surfacing, `validate` warnings) and
H6-as-amended (the `MediationProvider` port plus the OpenZiti adapter,
SPIFFE-aligned workload identity minted from the naming authority,
per-edge authorization compiled from the ADR 026 graph, an anti-coupling
archtest ensuring nothing outside the `openziti` package imports it by
name) merged together with I9-I12 (evidence-backed completions of
earlier-deferred items) and **H5** (domains and cross-domain policy, ADR
022 Rings 0–1: `metadata.domain`, Ring 0 validate-time cross-domain
policy denial, Ring 1 per-domain network segmentation realized entirely
in the engine's per-request runtime decorator so that — per an
owner-directed mid-task architecture correction — zero code under
`internal/adapters/providers` becomes domain-aware). Stage I closed
completely (I1–I12).

**The no-compromise pass (2026-07-23).** The owner's standing rule for
the review — "recorded deviations are debts, not resolutions" — drove a
final pass converting every "accepted with reasons" into a real fix
rather than a documented limitation: H5's domain-of-record deviation (the
decorator had keyed network naming on the *dependent* resource's declared
domain rather than the *realizing Provider's* — a cross-domain-broker
EventStream would have dialed the wrong network) became a coherence
check at validate time (ADR 022 addendum: runtime addressing follows the
provider's domain; the dependent's own declared domain governs
graph/policy edges only); ADR 026's Docker per-edge scale bound was
recomputed and killed (deterministic `/28` subnets from a supernet
support thousands of edges — the earlier "tens" ceiling was a default
address-pool exhaustion artifact, not a structural limit); I13 replaced
restore's detect-after-replay posture with verify-then-promote (a scratch
database plus an atomic swap, so corruption never touches the live
target); I14 solved Grafana admin-credential rotation via
`grafana-cli reset-admin-password` over the runtime's own exec
capability, closing what had been recorded as a vendor limitation; I15
brought backup/restore to Kubernetes (same-pod Jobs sharing a FIFO via
`emptyDir`), making runtime parity a `BackupRestore` GA requirement. A
`TestGeneratedReferenceInSync`-style discipline surfaced one latent
safety bug along the way: `manifest.envelopeFrom`'s metadata decoder had
never wired `metadata.protect` through from the real manifest loader — the
NFR-3 protect refusal had been inert for every real manifest since it
shipped, invisible because engine-level tests constructed `Envelope`
values directly rather than through the loader; fixed and pinned through
the real loader path.

**ADR 028 (test tiering, accepted 2026-07-23)** closed the review's own
process debt: local development had settled into 20-minute sweeps
followed by 20-minute CI runs, with a recurring pass-local/fail-CI
pattern traced to environment-timing divergence (warm local caches and
idle daemons hide races that CI's cold runners expose). The decision:
three tiers — a fast tier (`just test`, unit + contract-against-fakes,
`t.Parallel()` throughout, budget-guarded at ≤60s/test) as the only thing
a developer waits for; a deep tier (`just test-deep`, the existing
impact-mapped integration suites) for pre-push confidence; CI as the
arbiter (the full stress matrix, Calico-enforced Kubernetes, race
detector, on every PR). E6 (the provider-author contract) is re-scoped as
the fast tier's missing middle — a reconciler conformance suite against
fakes, in milliseconds, so providers stop leaning on e2e for their basic
lifecycle evidence — and a new task J1 owns the mechanical restructure
(parallelism audit, the CI budget guard, the `just test`/`test-deep`
split documented for developers).

**E7** (this task) closed the review's remaining documentation debt:
retiring the test-only `ContainerProvider` gate (kept the placeholder
provider as a test fixture once evidence showed it load-bearing for the
Docker and Kubernetes segmentation suites — H5's own accept scenario),
writing the one-paragraph-per-stage support-level commitment doc 04
lacked, sweeping for stale zero-trust language against ADR 027's claims
table, and this catch-up section. It also surfaced, but deliberately did
not fix (out of its own scope), a regression in H5's own Docker/Kubernetes
segmentation test fixtures: the ADR 022 addendum coherence check above
was never actually applied to `cmd/platformctl/testdata/domains-scenario`
despite a same-day review-log entry claiming it had been — both
segmentation integration tests fail at validate time on current `main`.
Recorded in doc 08's H5 section as a tracked follow-up, not silently
absorbed into this narrative as settled.

## 7. Where knowledge lives

| You want... | Read |
|---|---|
| What the product is / isn't; principles | doc 01 |
| Layering, ports, capability contracts, error shapes | doc 02 (+ `CLAUDE.md` for the invariant) |
| Every kind, field by field | doc 03; generated per-schema reference in `docs/reference/` |
| Phase history + feature-gate master table | doc 04 |
| What v1.0.0 committed to | doc 05 |
| How to work on this repo (process, agents, models) | doc 06 |
| Docker-runtime gap analysis; per-runtime differences ledger | doc 07 (historical record; open items live in doc 08) |
| **The live, stage-gated action plan** | **doc 08** |
| Why Stage F exists; the failure-class analysis | doc 09 |
| The full story, with reasoning | this document |
| Decision records | `docs/adr/` |
| The closed doc-audit ledger | `docs/remediation/` |
| Phase-by-phase evidence, resolved errors, delivered requests | `docs/history/` |
| Behavioral migrations operators will notice | `docs/upgrade-notes.md` |
