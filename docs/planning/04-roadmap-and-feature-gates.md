# Datascape — Phased Roadmap & Feature Gates

## 1. Roadmap philosophy

- **Thin vertical slices.** Every phase ends with something a user can actually run
  end-to-end — never "just the domain model" or "just the CLI skeleton" in isolation.
- **Docker-first, on purpose.** Docker validates the resource model cheaply before any second
  runtime is attempted. Kubernetes/Terraform/external-API adapters are real, planned, and
  explicitly *not* v1.
- **Gate, don't branch.** New providers/behaviors ship disabled-by-default behind a feature gate
  in the same release they're built, rather than living on a long-lived feature branch. This
  keeps `main` always releasable.
- **The exit criteria are the spec.** Each phase below has a Definition of Done a reviewer can
  actually check, not a vibe.

## 2. Phase overview

**Status (2026-07-20):** Phases 0–6.5 are complete and verified
(`docs/history/checkpoint.md` records the evidence per phase); their
exit-criteria checklists below are retained as historical record. Phase 7 is
**complete** — the Kubernetes runtime closed doc 08's Stage B and graduated
to Beta (enabled by default); GA is targeted at Stage C close. Post-v1.0.0
production-readiness work — high availability, routing/TLS, monitoring,
backup, pipeline-completeness providers, DX/contribution readiness, and the
segregation-readiness fixes — is planned as stage-gated, individually
actionable tasks in
[08-production-readiness-plan.md](08-production-readiness-plan.md) (Stages
A–F; A, B, and F are closed), which supersedes per-phase detail for
everything after Phase 6.5. The full historical narrative, with the
reasoning behind each stage and pivot, is
[10-project-history-and-evolution.md](10-project-history-and-evolution.md).

| Phase | Theme | Primary outcome |
|---|---|---|
| 0 | Foundations | Domain model, ports, manifest validation, dependency graph, CLI skeleton, local state store — no real infrastructure yet. |
| 1 | Docker Runtime Adapter | `ContainerRuntime` fully implemented against real Docker: networks, volumes, containers, health checks. |
| 2 | Redpanda Provider | First real, end-to-end managed resource: `EventStream` on Docker via Redpanda. |
| 3 | Postgres + Debezium CDC + Lineage mechanism | The full CDC scenario: `Source` (Postgres) → `Binding` (Debezium CDC) → `EventStream` (Redpanda), plus the `observers`/`LineageAware` mechanism proven against a fake provider. |
| 4 | Object Storage Sink | `Provider(type: s3\|minio)`, `Dataset`, `Binding(mode: sink)` — CDC now has somewhere durable to land. |
| 5 | External/Imported Resources + Drift Detection | `import` command, external-resource configure-only paths, drift detection, safety-flag enforcement hardened. **v1.0.0 is declared at the close of this phase.** |
| 6 | Scale-out (post-v1.0.0) | `s3` write-path parallelism, `vault` secret backend, and — optionally — a real `openlineage`-backed provider (e.g. Marquez). |
| 7 | Kubernetes Runtime (future) | Second runtime adapter; proves the provider/runtime split for real. |
| 8 | External/Terraform Adapter + Plugin SDK (future) | Out-of-process provider plugins; non-container runtime port; external SaaS API adapter (e.g., Confluent Cloud). |

Phases 0–5 are the committed path to a production-grade v1.0.0. Phases 6–8 are the credible next
steps, sketched so v1's design doesn't box them out, but not committed deliverables here.

## 3. Phase 0 — Foundations

**Objectives:** establish the domain/ports/adapters skeleton; make `validate` and `plan` work
against a no-op provider so the graph, diffing, and state-writing machinery is exercised before
any real infrastructure is involved. Build the domain packages using the final shapes from this
package (engine-extensible `Source`, mode-aware `Binding`, the `lineage.LineageEndpoint` type)
from the outset.

**Deliverables:**
- `domain/resource`, `domain/status`, `domain/graph`, and kind packages for `Provider`, `Source`,
  `EventStream`, `Binding`, `SecretReference` (schema + validation only — `Dataset` schema
  drafted but not required to validate yet).
- `ports/runtime`, `ports/reconciler`, `ports/state`, `ports/secretstore` interfaces, each with a
  contract test suite.
- `adapters/runtime/fake`, `adapters/state/localfile`, `adapters/secrets/env`, a trivial `noop`
  provider adapter used only for testing the engine.
- `application/manifest`, `application/plan`, `application/engine`, `application/registry`,
  `application/featuregate`.
- CLI: `validate`, `plan`, `apply`, `status`, `graph` runnable end-to-end against the `noop`
  provider and the fake runtime.

**Exit criteria:**
- [ ] A manifest set using only `noop`-typed `Provider`s can be validated, planned, applied, and
      shows `Ready` in `status`, with state persisted and reloadable.
- [ ] Re-running `apply` performs zero state mutations (NFR-2, verified by a test asserting no
      `StateStore.Save` diff).
- [ ] A cyclic `providerRef`/`sourceRef` graph is rejected by `validate` with a clear error.
- [ ] Golden-file test for `plan` output exists and passes (NFR-1 baseline).
- [ ] `ports/runtime` and `ports/state` each have a passing conformance suite run against their
      fake implementation.

**Feature gates introduced:** `CoreReconciler` (Alpha → GA in this phase).

## 4. Phase 1 — Docker Runtime Adapter

**Objectives:** implement `ContainerRuntime` for real against the Docker Engine API.

**Deliverables:**
- `adapters/runtime/docker`: `EnsureNetwork`, `EnsureVolume`, `EnsureContainer`, `WaitHealthy`,
  `Inspect`, `Remove`, `RemoveNetwork`, `RemoveVolume`, `ListManaged`.
- Datascape-owned labeling scheme (`io.datascape.managed-by`, `io.datascape.generation`) applied
  to every created object, enforced so `ListManaged`/destroy never touch unlabeled resources.
- Integration test suite (`//go:build integration`, `just test-integration`) running the
  conformance suite from Phase 0 against the real Docker adapter.

**Exit criteria:**
- [ ] Docker adapter passes the same `runtime.ConformanceSuite` the fake adapter passes.
- [ ] A manifest with a Docker-typed `Provider` (still backed by a placeholder/no-op reconciler
      technology used only to prove the runtime) creates a real network, volume, and container,
      waits for health, and reports `Ready`.
- [ ] `destroy` removes exactly what was created, verified by diffing the Docker daemon's object
      list before/after.
- [ ] Killing a managed container out-of-band and re-running `plan`/`status` surfaces it as not
      healthy (drift detection is still basic here; the full `drift` command lands in Phase 5).

**Feature gates introduced:** `DockerRuntime` (Alpha → GA end of Phase 5).

## 5. Phase 2 — Redpanda Provider

**Objectives:** first real technology provider; `EventStream` becomes fully meaningful.

**Deliverables:**
- `adapters/providers/redpanda`: reconciles a broker container (via the Docker runtime) and,
  post-health, creates/updates topics and retention settings via the Redpanda admin API.
- `EventStream` Probe implementation for drift/status (topic exists, partition count matches,
  retention matches).

**Exit criteria:**
- [ ] `platformctl apply` against a `Provider(type: redpanda)` + `EventStream` manifest set
      produces a healthy, running Redpanda broker with the declared topic and retention.
- [ ] Changing `partitions` and re-applying updates the topic without recreating the broker.
- [ ] `destroy` tears down the broker container, its network, and its volume cleanly.
- [ ] Idempotent re-apply verified (zero mutating calls on unchanged manifests).

**Feature gates introduced:** `RedpandaProvider` (Alpha → GA end of Phase 5).

## 6. Phase 3 — Postgres + Debezium CDC + Lineage mechanism

**Objectives:** the full worked CDC scenario end-to-end, plus proving the `observers`/
`LineageAware` mechanism.

```mermaid
sequenceDiagram
    participant Engine as Reconciliation Engine
    participant Docker as Docker Runtime
    participant RP as Redpanda Provider
    participant PG as Postgres Provider
    participant DBZ as Debezium Provider
    Engine->>Docker: EnsureNetwork(datascape)
    Engine->>RP: Reconcile(EventStream)
    RP->>Docker: EnsureContainer(redpanda)
    Docker-->>RP: healthy
    RP->>RP: CreateTopic(attendance-events)
    Engine->>PG: Reconcile(Source)
    PG->>Docker: EnsureContainer(postgres)
    Docker-->>PG: healthy
    PG->>PG: EnableLogicalReplication + CreateReplicationUser
    Engine->>DBZ: Reconcile(Binding, mode=cdc)
    DBZ->>Docker: EnsureContainer(connect)
    Docker-->>DBZ: healthy
    DBZ->>DBZ: RegisterConnector + VerifyConnectorState
    Note over Engine,DBZ: If Binding.metadata.observers names a lineage Provider,<br/>Engine resolves its LineageEndpoint and calls DBZ.ConfigureLineage<br/>(DBZ is LineageAware; forwards to Debezium's own native OpenLineage integration)
```

**Deliverables:**
- `postgres` provider: reconciles a Postgres container, enables logical replication
  (`wal_level=logical`), creates a replication role/user via `SecretReference`-sourced
  credentials.
- `debezium` provider: reconciles a Kafka Connect (Debezium) container, registers a connector via
  its REST API using `Binding.spec.options`, polls connector state (`RUNNING`/`FAILED`) and
  surfaces it as `status.conditions`.
- `application/compatibility`: the structural mode↔Kind check and the
  `CDCCapableProvider.SupportedSourceEngines()` capability check, enforced at `validate`/`plan`.
- `domain/lineage.LineageEndpoint`, the `LineageAware` interface, and engine wiring to resolve
  `observers` and forward endpoints.
- `debezium` implements `LineageAware`: when registering a connector, if a `LineageEndpoint` was
  forwarded, it sets Debezium's own `openlineage.integration.enabled` and endpoint configuration
  — a real integration (Debezium's native support), not a stub, but a real lineage *backend* to
  point it at is not required to exist yet.
- A fake `LineageAware` test provider, used to prove the mechanism in unit/contract tests without
  standing up Marquez.
- Full dependency ordering across `Provider → Source → EventStream → Binding` proven by the
  engine's topological execution.

**Exit criteria:**
- [ ] The example manifest set (Provider×3, Source, EventStream, Binding) applies cleanly from
      empty state to fully `Ready` in under the NFR-8 time budget.
- [ ] `platformctl status` shows `Ready=True` for every resource, including the `Binding`
      (meaning: connector verified `RUNNING`, not merely "container started").
- [ ] Re-running `apply` with no changes performs zero mutating calls across all three providers.
- [ ] `destroy` tears everything down in reverse dependency order with no orphaned containers,
      networks, or volumes.
- [ ] A change to `Binding.spec.options.tables` updates the running connector's configuration
      without recreating the Postgres or Redpanda containers.
- [ ] A `Binding(mode: cdc)` with `metadata.observers: [{name: some-fake-lineage-provider}]`
      results in the fake provider receiving a correctly-populated `LineageEndpoint` in a test.
- [ ] A `Binding` referencing a `Provider` that does not implement `CDCCapableProvider`, or whose
      `SupportedSourceEngines()` doesn't include the `Source`'s engine, fails at `validate` with
      the documented error shape, not at `apply`.
- [ ] An `observers` entry on a resource whose provider does not implement `LineageAware`
      produces the `LineageEndpointDeclaredNotConsumed` informational condition and does not
      block `Ready`.

**Feature gates introduced:** `PostgresProvider`, `DebeziumCDCProvider`, `CDCBinding` (Alpha →
GA end of Phase 5), plus **`LineageObservability`** (Alpha, default disabled, since it's a
mechanism with no required real backend yet; graduates to Beta once a real `openlineage`-backed
provider exists and has been run against it — tracked in Phase 6, not required for v1.0.0).

## 7. Phase 4 — Object Storage Sink

**Objectives:** give CDC (and, later, anything else) somewhere durable to land, and establish
the `Dataset`/sink contract future orchestrator providers will build against.

**Deliverables:**
- `adapters/providers/s3` (targeting an S3-API-compatible service; MinIO is the reference
  target): reconciles the object-store container, and `Dataset` reconciliation (bucket/prefix
  existence, format metadata).
- `adapters/providers/s3sink`: a Kafka-Connect-based S3 sink connector provider, implementing
  `SinkCapableProvider`; reconciles its own Connect worker container, registers a sink connector
  reading from the `EventStream`'s topic and writing to the `Dataset`'s bucket/prefix.
- `Binding.spec.mode: sink` fully implemented in the engine and `application/compatibility`.

**Exit criteria:**
- [ ] Extending the Phase 3 manifest set with a `minio` `Provider`, an `attendance-raw` `Dataset`,
      and an `attendance-events-to-lake` `Binding(mode: sink)` reaches `Ready` end-to-end:
      Postgres → Debezium → Redpanda topic → sink connector → objects landing in MinIO.
- [ ] Changing `Dataset.spec.format` (where the sink connector supports the new format) updates
      the connector without recreating the broker, database, or object store.
- [ ] A `Binding(mode: sink)` referencing a `Provider` that isn't `SinkCapableProvider`, or whose
      `SupportedSinkFormats()` doesn't include the `Dataset`'s format, fails at `validate`.
- [ ] `destroy` tears down the sink connector, the object store, and its data cleanly (subject to
      the same managed/external safety rules as everything else).
- [ ] Idempotent re-apply verified across all newly-added resources.

**Feature gates introduced:** `ObjectStoreProvider` (Alpha → GA end of Phase 5), `SinkBinding`
(Alpha → GA end of Phase 5).

## 8. Phase 5 — External/Imported Resources, Drift Detection

**Objectives:** complete the three-lifecycle model and make drift a first-class, actionable
signal rather than an afterthought.

**Deliverables:**
- `platformctl import` command and the `Imported` lifecycle path through the engine.
- `Source.spec.external: true` fully honored end-to-end: a `Binding` can register a Debezium
  connector against an externally-declared Postgres `Source` without Datascape ever attempting
  to create/delete that database.
- `platformctl drift` command; drift surfaced in `plan`/`status` as a distinct condition
  (`DriftDetected`).
- Hardened enforcement of NFR-3: a dedicated engine-level guard (not per-provider convention)
  blocking any delete call against an `External`-lifecycle resource absent both required flags.

**Exit criteria:**
- [ ] Importing a pre-existing, out-of-band-created Docker container as a `Source`'s backing
      Postgres instance results in `Ready` status without any create call being issued.
- [ ] A `Binding` against an `external: true` `Source` reconciles the connector but `destroy
      --include-external` without the destructive-action flag refuses, with a clear message.
- [ ] Manually stopping a managed container and running `platformctl drift` reports it; running
      `platformctl plan` (not `apply`) does not restart it; running `apply` does.

**Feature gates introduced:** `ImportedResources` (Alpha), `ExternalResourceConfiguration`
(Alpha → Beta), `DriftDetection` (Alpha → Beta). `CDCBinding`, `SinkBinding`, `DockerRuntime`,
`RedpandaProvider`, `PostgresProvider`, `DebeziumCDCProvider`, and `ObjectStoreProvider` all
graduate to GA at the close of this phase.

**This phase's completion is the v1.0.0 declaration point.**

## 9. Phase 6 — Scale-out (post-v1.0.0)

- `ParallelReconciliation`: concurrent execution within a topological level, bounded by
  `--parallelism`.
- `vault` `SecretStore` backend.
- **Optional:** `adapters/providers/openlineage` — a provider that stands up a real lineage
  backend (e.g., Marquez) and, combined with Phase 3's `LineageAware` mechanism, produces an
  actual end-to-end lineage demo. Not required for v1.0.0 — the natural place to build it once
  there's time, not a gap in v1.0.0.

**Feature gates:** `ParallelReconciliation`, `VaultSecretBackend` (Alpha); `LineageObservability`
graduates Alpha → Beta here, contingent on the `openlineage` provider actually being built and
exercised.

## 9.5 Phase 6.5 — Orchestrator-ready infrastructure (post-v1.0.0, before Kubernetes)

Added post-v1.0.0 by project-owner direction (see
docs/adr/002, the stage's design note): let the engine build the core
infrastructure real orchestrators (Dagster and friends) run against, while
users operate the orchestrator themselves. **Model first:** the stage
extends the resource model with two provider-agnostic kinds before any
provider code — technologies realize nouns, they never become nouns.

**Resource-model deliverables:**
- `Catalog` kind: a table/metadata catalog as an engine-discriminated noun
  (`spec.engine: nessie | hive | glue | ...`), mirroring `Source`'s
  extensibility exactly. Capability-checked at validate via
  `CatalogCapableProvider.SupportedCatalogEngines()`.
- `Connection` kind: a first-class, non-secret "how to reach a system"
  record (address + `secretRef`), with two lifecycles from one shape —
  managed (a stable platform-owned entrypoint forwarding to where the
  system lives) and external (a plain address record). `connectionRef`
  fields resolve to a `Connection` first, `SecretReference` as the v1.0.0
  shorthand. Capability-checked via
  `ConnectionCapableProvider.SupportedConnectionSchemes()`.

**Provider deliverables:**
- `mysql` provider (also registered as `mariadb`): instance lifecycle,
  Source reconciliation (database + replication-capable user, binlog
  verified); Debezium connector class resolved per `Source.spec.engine`.
- `nessie` provider: realizes `Catalog(engine: nessie)` — instance
  container plus catalog-level reconciliation (default branch) against the
  Iceberg REST API.
- `openlineage` provider (the Phase 6 optional item, now built): Marquez +
  dedicated Postgres; endpoint published in provider state for
  `metadata.observers`. `LineageObservability` graduates Alpha → Beta.
- `proxy` provider: realizes managed `Connection`s as per-Connection TCP
  forwarder containers on the shared network and the host. Bindings against
  external Sources consume the Source's Connection automatically (endpoint
  + credentials). Tunnel chaining for VPC reach is deliberately deferred;
  the `Connection` kind is the seam it lands behind.
- `ImportedResources` graduates to Beta/enabled (its Phase 6 graduation).
- `examples/lakehouse/`: the orchestrator-ready stack with a README mapping
  every resource to the endpoint Dagster/Metabase connect to, exercising
  managed, imported, and external lifecycles side by side.

**Exit criteria:**
- [ ] The lakehouse example applies to Ready: MinIO + Catalog(nessie) +
      Marquez + Postgres + MySQL + a managed Connection + an external
      Source consumed through it.
- [ ] Nessie and Marquez REST APIs answer on their published endpoints; the
      Catalog's declared default branch exists.
- [ ] A connection through the managed entrypoint reaches the external
      database end-to-end (CDC Binding RUNNING against it).
- [ ] Idempotent re-apply, drift healing, and clean destroy hold for every
      new kind and provider (same bar as phases 1–4).

**Feature gates introduced:** `MySQLProvider`, `NessieProvider`,
`OpenLineageProvider`, `ProxyProvider` (Alpha, enabled — this stage is
their hardening period). `LineageObservability` Alpha → Beta,
`ImportedResources` Alpha → Beta.

## 10. Phase 7 — Kubernetes Runtime Adapter (Stage B complete, Beta)

**Status update (2026-07-16):** started. `internal/adapters/runtime/kubernetes`
implements `ContainerRuntime` against a real cluster (client-go), passes the
same conformance suite the Docker adapter passes (run live against
`minikube`), and reconciled the real `redpanda` provider end-to-end through
`platformctl apply` with **zero changes to any provider package** —
confirming the design decision this phase exists to prove. Full findings,
including one real bug found only by running an unmodified provider
end-to-end (Docker's `Cmd` maps to Kubernetes `Args`, not `Command`) and one
real port-boundary gap found and fixed (`VolumeSpec` needed a `Networks`
hint because PersistentVolumeClaims are namespace-scoped and Docker volumes
are not), are recorded in
`docs/planning/07-production-grade-docker-runtime-gap-analysis.md`'s
"Cross-Runtime Portability" section — read that before resuming this phase.

The one `VolumeSpec.Networks` field was the only port change required; no
`redpanda`/`postgres`/`debezium`/`s3`/`s3sink` provider *logic* changed
(6 provider files got a one-line, mechanical addition of the same field at
their existing `EnsureVolume` call site) — the design bet this phase exists
to test held.

No storage-vocabulary reintroduction (`StorageClass`/`PersistentVolume`-
equivalent Kinds) was needed: the existing `VolumeSpec` expresses the
Kubernetes adapter's volume model (a `PersistentVolumeClaim` per named
volume) once it carried a namespace hint.

Stage B (docs/planning/08 §4) closed all of the above: external
reachability via per-Provider access modes (port-forward | node-port |
load-balancer | in-cluster, B1), observed bind-address/published-port
inspection so `inventory` tells the truth (B2), storage sizing/class and a
persistence-across-update proof (B3), a Kubernetes SecretStore backend
(B4), a minimal RBAC posture proven sufficient by running the full K8s
suite under it in CI (B5), connection preflight with named remedies (B6),
NetworkPolicy parity with Docker's network isolation (B7), and the full
cdc-attendance/lakehouse example scenarios verified end-to-end against a
real cluster (B8). See docs/planning/08 §4 for the verification detail
behind each item and `deploy/kubernetes/rbac/README.md` for the RBAC
posture itself.

**Feature gates:** `KubernetesRuntime` (Beta, enabled by default as of
Stage B close).

**Task breakdown:** Stage B (B1–B9) took the adapter to Beta; Stage C (C1
replicas/stable identity and the HA scenarios built on it) takes it toward
GA. Phase 7 closes with Stage B's exit criteria held (docs/planning/08 §4).

## 11. Phase 8 — External/Terraform Adapter, Out-of-Process Provider Plugins (future)

- A narrower, non-container runtime port for adapters that don't map to "run a container"
  (external SaaS APIs, Terraform-managed infrastructure).
- Provider plugin protocol (gRPC, versioned, Terraform-provider-inspired) so third parties can
  ship providers without a core-binary release.

**Feature gates:** `TerraformRuntimeAdapter` (Alpha), `OutOfProcessProviderPlugins` (Alpha).

## 12. Feature gate master table

| Gate | Introduced | Stage at introduction | Default | Graduation target |
|---|---|---|---|---|
| `CoreReconciler` | Phase 0 | Alpha | enabled | GA in Phase 0 |
| `DockerRuntime` | Phase 1 | Alpha | enabled | GA end of Phase 5 (v1.0.0) |
| `RedpandaProvider` | Phase 2 | Alpha | enabled | GA end of Phase 5 (v1.0.0) |
| `PostgresProvider` | Phase 3 | Alpha | enabled | GA end of Phase 5 (v1.0.0) |
| `DebeziumCDCProvider` | Phase 3 | Alpha | enabled | GA end of Phase 5 (v1.0.0) |
| `CDCBinding` | Phase 3 | Alpha | enabled | GA end of Phase 5 (v1.0.0) |
| `LineageObservability` | Phase 3 | Beta (since Phase 6.5) | enabled | graduated: the openlineage (Marquez) provider shipped in Phase 6.5 and is exercised |
| `ObjectStoreProvider` | Phase 4 | Alpha | enabled | GA end of Phase 5 (v1.0.0) |
| `SinkBinding` | Phase 4 | Alpha | enabled | GA end of Phase 5 (v1.0.0) |
| `ImportedResources` | Phase 5 | Beta (since Phase 6.5) | enabled | graduated per its Phase 6 intent |
| `ExternalResourceConfiguration` | Phase 5 | Beta | enabled | GA (the Phase 6 target has not been taken — an explicit graduation decision is still pending) |
| `DriftDetection` | Phase 5 | Beta | enabled | graduated Beta at Phase 5 close |
| `ParallelReconciliation` | Phase 6 | Alpha | disabled | — |
| `VaultSecretBackend` | Phase 6 | Alpha | disabled | — |
| `KubernetesRuntime` | Phase 7 | Beta (08 Stage B/B9) | enabled | GA in Stage C |
| `TerraformRuntimeAdapter` | Phase 8 | Alpha | disabled | — |
| `OutOfProcessProviderPlugins` | Phase 8 | Alpha | disabled | — |
| `SharedStateBackend` | 08 Stage A (A4) | Alpha | disabled | Beta once used by CI itself |
| `MySQLProvider` | Phase 6.5 | Beta (since 08 Stage A close) | enabled | GA after real-use soak |
| `NessieProvider` | Phase 6.5 | Beta (since 08 Stage A close) | enabled | GA after real-use soak |
| `OpenLineageProvider` | Phase 6.5 | Beta (since 08 Stage A close) | enabled | GA after real-use soak |
| `ProxyProvider` | Phase 6.5 | Beta (since 08 Stage A close) | enabled | GA after real-use soak |
| `KubernetesSecretBackend` | 08 Stage B (B4) | Beta (08 Stage B/B9) | enabled | GA with KubernetesRuntime |
| `HighAvailability` | 08 Stage C (C1) | Alpha | disabled | Beta after C2/C3 soak (guards Replicas > 1; validate-time enforcement arrives with C2 per docs/adr/004) |
| `SchemaRegistrySupport` | 08 Stage D (D1) | Beta (since D2, 2026-07-21) | enabled | graduated per the recorded intent when D2 landed |
| `BackupRestore` | 08 Stage C (C6) | Alpha | disabled | Beta after restore drills in CI |
| `MonitoringStackProvider` | 08 Stage C (C9) | Alpha | disabled | Beta after real-use soak (core slice only — see 08 C9's status note for explicit deferrals) |
| `IngressProvider` | 08 Stage C (C7) | Alpha | disabled | Beta after real-use soak; see docs/adr/018 |
| `TrinoProvider` | 08 Stage D (D10) | Alpha | disabled | Beta after real-use soak; defaults off (unlike the enabled-Alpha Phase 6.5 precedent) because a query engine accepting arbitrary SQL from whoever can reach its coordinator port is a meaningfully different risk profile — docs/adr/006-compute-engines.md |
| `JDBCSinkProvider` | 08 Stage D (D3) | Alpha | disabled | Beta after real-use soak; defaults off, matching the IngressProvider/TrinoProvider posture (a new provider exposing a new capability surface — writes into a database — defaults off until soaked) |
| `IngestProvider` | 08 Stage D (D4) | Alpha | disabled | Beta after real-use soak; same posture as `JDBCSinkProvider` |
| `TLSTermination` | 08 Stage C (C8) | Alpha | disabled | Beta after real-use soak; independent of `IngressProvider`'s own gate (a Connection can stay plaintext even once ingress routing graduates) — see docs/adr/018 addendum |
| `TunnelProvider` | 08 Stage D (D5) | Alpha | disabled | Beta after real-use soak; defaults off — a new provider granting `NET_ADMIN` and opening a routed path into a private network is a meaningfully different risk profile from the Phase 6.5 enabled-Alpha precedent — docs/adr/023-wireguard-tunnel.md |
| `DesignLints` | 08 Stage H (H1) | Alpha | enabled | Beta once blueprints + examples are lint-clean for a release (docs/adr/020-design-lints.md) — read-only reporting, so it defaults on; the gate switches `validate`'s one-line summary off, not the `lint` command itself |
| `ExternalDatabaseTLS` | 08 Stage I (I2) | Alpha | enabled | Beta after real-use soak; enabled-by-default because it's a purely additive opt-in field (`Connection.spec.tls.mode` on an external Connection) — absent, every consumer's plaintext dial is byte-for-byte unchanged, so there is no new attack surface to soak disabled — docs/adr/025-cloud-iam-database-auth.md |
| `PolicyEngine` | 08 Stage H (H3) | Alpha | disabled | Beta after the zero-trust pack soaks in this repo's own CI (docs/adr/021-policy-engine-zero-trust.md) — unlike `DesignLints`, disabled means a full no-op (no directory read, no evaluation), and an enabled deny blocks validate/plan/apply/destroy, so this defaults off |
| `MediatedConnections` | 08 Stage H (H6) | Alpha | disabled | Beta after the owner-scenario dual-runtime accept suite soaks — docs/adr/022 (Ring 2), docs/adr/027 (Layer 1, the authoritative zero-trust plane). A new provider running its own control plane (a pinned OpenZiti controller + router) and minting cryptographic identity is a meaningfully different risk profile from the Phase 6.5 enabled-Alpha precedent, matching `TunnelProvider`'s posture |
| `GraphScopedAccess` | 08 Stage H (H7) | Alpha | disabled | Beta after the dual-runtime negative-proof suite soaks — docs/adr/026 (Layer 2 of docs/adr/027's zero-trust progression). Flips reachability semantics for every existing manifest set the instant it's on (a resource that previously reached its whole domain/shared network now reaches only what it declared), so it defaults off until soaked, per ADR 026's own Gate section |
| `LabelScopedAccess` | 08 Stage K (K2) | Alpha | disabled | Beta once the composed H9-style scenario passes on both runtimes (ADR 033 decision 6) — docs/adr/033: gives the policy vocabulary (`matchEdge.selector`, `match.selector`) the same label-granularity resolution the reference graph already has. Gate-off is byte-identical: a rule using either selector form is skipped entirely, never partially evaluated |

**`ContainerProvider` retired (2026-07-23, 08 E7):** the row above is
removed, not marked disabled — the gate itself no longer exists in
`main.go`. Evidence review (`grep 'type: container'` across
`cmd/platformctl/testdata`) found the placeholder provider genuinely
load-bearing for three integration suites (`docker-acceptance`,
`domains` — H5's Kubernetes segmentation scenario in particular), so
per the retirement decision the *gate* is retired while the *provider*
stays: `container` now registers ungated, exactly like `noop`
(`cmd/platformctl/main.go`'s `defaultWiring`). It was never a
manifest-authoring surface for real pipelines, so there is no user-
facing maturity signal to stage — a feature gate exists to stage that
signal, not to gate a test fixture from itself. See
`docs/history/checkpoint.md` item 4 and `docs/adr/014-feature-gate-
strategy.md`.

Gates planned by the production-readiness backlog (`HighAvailability`,
`IngressProvider`, `TLSTermination`, `MonitoringStackProvider`,
`BackupRestore`, `SchemaRegistrySupport`, `JDBCSinkProvider`,
`IngestProvider`, `TunnelProvider`) are tracked with their introduction
points and graduation intents in
[08-production-readiness-plan.md](08-production-readiness-plan.md) §8;
append each to this table in the commit that lands it.

Gate mechanics: `--feature-gates=Name=true,Other=false` on the CLI, or a `featureGates:` block in
a config file; `application/registry` consults the gate before constructing a provider/runtime,
failing fast with a message naming the gate and its current default if disabled.

Table semantics (clarified 2026-07-20): the Stage and Default columns state the
**current** registration in `cmd/platformctl/main.go` (the K8s rows set this
precedent by updating in place at B9); "Stage at introduction" survives in the
Introduced column's phase reference. This table and `main.go`'s
`gates.Register` calls must agree — that is the sync the review checks.

### 12.1 Support-level commitments (added 2026-07-23, 08 E7)

Doc 07 §3.3 flagged that the maturity table above states a stage but never
spelled out what each stage *commits to*. This is the one authoritative
statement; README's Provider maturity table (and any other doc) should
point here rather than restate it:

- **Alpha** — the shape (schema fields, CLI surface, gate name) may change
  between minor releases without a deprecation window; a gate defaults
  **disabled** unless its row explicitly records a reasoned exception
  (e.g. `DesignLints`, `ExternalDatabaseTLS` — each row states why).
  Enabled-Alpha is reserved for a declared hardening period with a named
  graduation trigger (Phase 6.5's precedent), never the default posture
  for a brand-new capability surface. Test coverage: exercised by at
  least one integration suite (docker and/or Kubernetes leg) or, absent
  that, unit/contract coverage against the fake runtime — a gate with
  zero automated coverage anywhere is a gap, not an accepted state.
  Behavior with the gate off is byte-for-byte unchanged for any manifest
  that doesn't opt in (docs/adr/014's behavioral contract) — this holds
  regardless of stage.
- **Beta** — the shape is stable (schema fields and CLI surface do not
  change without a deprecation window); behavior may still change as real
  use surfaces edge cases. Defaults **enabled**. Test coverage: a
  dedicated integration suite exists and runs in CI on every affecting
  change (the impact map in `scripts/test-impact.sh`), not just at
  merge time.
- **GA** — stable; both shape and behavior changes follow a deprecation
  window. Defaults enabled, no opt-out gate (the gate itself may still
  exist for historical/documentation reasons but is no longer meaningful
  to disable). Test coverage: integration suite plus inclusion in the
  acceptance-scenario / example set (docs/planning/05 §6-class coverage).

A gate's row in the master table above is the per-gate instantiation of
this contract — read the row's own Graduation-target column for *when*
the next transition triggers, and this section for *what each stage
means* once there.

## 13. Versioning & release strategy

- Binary semver (`platformctl` releases: `v0.1.0`, `v0.2.0`, ...) tracks phases loosely but is
  not 1:1 — a phase may span multiple binary releases if it's large.
- `apiVersion` maturity (alpha/beta/GA per Kind) is tracked **independently** of binary semver —
  a `v0.3.0` binary can simultaneously support `EventStream` at GA and `Dataset` at alpha.
- **v1.0.0 is declared at the close of Phase 5** — object storage and the lineage mechanism are
  both required, GA-by-v1.0.0 deliverables, not post-v1.0.0 additions.

## 14. Cross-cutting risk register

| Risk | Impact | Mitigation |
|---|---|---|
| Docker Engine API version skew across developer machines | `EnsureContainer` behaves inconsistently | Pin a minimum supported Docker API version; conformance suite run in CI against a matrix of Docker versions. |
| Debezium/Kafka Connect connector registration is inherently async and occasionally flaky | Flaky `Ready` status, flaky CI | `DebeziumCDCProvider`'s `Probe` polls connector state with bounded backoff before reporting `Progressing` vs. `Degraded`; golden-file/e2e tests use generous, documented timeouts, not tight retries. |
| Determinism (NFR-1) is easy to violate accidentally (e.g., map iteration order, timestamps leaking into hashes) | Silent flaky `plan` diffs, eroding trust in the core value prop | Golden-file tests from Phase 0 onward; spec hashing goes through a canonicalization step (sorted keys, no timestamps) reviewed as part of every provider PR. |
| Scope creep back toward orchestration (Warehouse/Table/Pipeline) | Re-blurs the boundary the product brief explicitly draws | These kinds are simply absent from the schema directory in v1 — there's no code path that would let them sneak back in without a deliberate schema addition and a new scoping conversation. |
| Safety defaults get "helpfully" relaxed under time pressure late in Phase 3/4/5 | A `destroy` accidentally deletes an external resource | The guard lives in the engine, not per-provider — there is exactly one place to review, not N places to audit. |
| Building `LineageObservability` without a real backend to test against could mean the mechanism looks correct in unit tests but breaks on first real integration | False confidence in a v1.0.0-adjacent mechanism | Explicitly scoped as Alpha, not GA, in v1.0.0 — the requirements doc (NG6) and this roadmap both say the real-backend integration is optional, precisely so its immaturity is visible rather than hidden behind a green checkmark. |
| Running a second Kafka Connect worker (for the S3 sink) doubles Connect-related operational surface for what might be mergeable later | Slower cold-start (NFR-8), more moving parts to debug | Accepted for v1.0.0 per the open question in the requirements doc; `SinkCapableProvider` is the seam that makes consolidating onto a shared worker later a contained change, not a redesign. |
