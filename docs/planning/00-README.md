# Datascape — Production Rebuild: Planning Package

This is the canonical planning package for taking `project-datascape` out of its experimental
phase and into a production-grade v1. It is greenfield with respect to design — it does not
assume the current `internal/` layout, resource-kind list, or CLI surface survive unchanged.

## How to read this package

1. **[01-product-requirements.md](01-product-requirements.md)** — what Datascape is, who it's
   for, what it must do, what it explicitly will not do.
2. **[02-architecture.md](02-architecture.md)** — how it's built: module layout, domain/ports/
   adapters, the reconciliation engine, state management, capability matching, lineage
   observation, CLI.
3. **[03-resource-model-reference.md](03-resource-model-reference.md)** — the actual API: every
   Kind, every field, the managed/external/imported lifecycle taxonomy, status conditions.
4. **[04-roadmap-and-feature-gates.md](04-roadmap-and-feature-gates.md)** — the phased build-out,
   phase-by-phase exit criteria, and the feature gate system.
5. **[05-v1-first-version-spec.md](05-v1-first-version-spec.md)** — the precise, testable
   definition of "production-grade first version," including the worked acceptance scenario.
6. **[06-agentic-execution-guide.md](06-agentic-execution-guide.md)** — how to actually build
   this with Claude Code and other coding agents: repo structure, standing bookkeeping tasks,
   pre-coding review checklist, and model selection per task type.
7. **[07-production-grade-docker-runtime-gap-analysis.md](07-production-grade-docker-runtime-gap-analysis.md)**
   — the post-v1.0.0 gap analysis and stage gates (Gates 0–3), including the
   cross-runtime (Kubernetes) portability findings. Analysis record; its open
   items are worked through the backlog below.
8. **[08-production-readiness-plan.md](08-production-readiness-plan.md)** — the
   current stage-gated backlog (Stages A–E) of individually actionable tasks
   taking v1.0.0 to a production data-pipeline platform: operational
   hardening, Kubernetes to Beta/GA, HA/routing/TLS/monitoring/backup,
   pipeline-infrastructure providers, and DX/contribution readiness.

## The one diagram that explains everything else

```mermaid
flowchart TB
    A["Data-Platform Resource Model<br/>(Source, EventStream, Binding, Dataset)"]
    B["Provider Implementation<br/>(Redpanda, Postgres, Debezium, S3, Confluent...)"]
    C["Runtime / Deployment Environment<br/>(Docker, Kubernetes, External API, Terraform...)"]
    A -->|"providerRef"| B
    B -->|"runtime.type"| C
```

Everything in this package exists to keep these three layers from collapsing into each other.
The resource model must never know about Docker. The Docker adapter must never know what a
topic or a replication slot is. The provider is the only thing that understands both a
technology's semantics *and* how to ask a runtime to host it.

## Key design decisions

These decisions constrain everything else in this package. If any is wrong for your intent, say
so before treating the rest as settled.

| Decision | What it means | Why |
|---|---|---|
| **Collapse `*Class`/`*Instance` pairs into `Provider`** | `DatabaseClass`, `ConnectorClass`, `CDCClass`, `DatabaseInstance`, `CDCInstance` are retired. A single `Provider` kind (`type` + `runtime` + `configuration`) replaces all five. | The class/instance split was solving a problem (reusable policy vs. concrete deployment) that `providerRef` + `runtime` already solves more simply. |
| **Retire Kubernetes-shaped volume kinds for v1** | `StorageClass`, `PersistentVolume`, `PersistentVolumeClaim`, `VolumeMountBinding` are deferred, not deleted from the vocabulary. Docker-native volumes are managed internally by the Docker runtime adapter. | Mimicking Kubernetes storage abstractions before there's a second runtime to abstract *over* is premature generality. |
| **`Source` is one Kind with an extensible, engine-keyed sub-block** | `spec.engine: postgres` plus `spec.postgres: {...}`; a new engine is a schema fragment and a provider declaration, not a core schema change. | Collapsing `RelationalSource` into `Source` only works if the result is genuinely extensible per-provider — a discriminator plus an opaque, provider-owned sub-block delivers that without a Kind per technology. |
| **`Warehouse`, `Table`, `Pipeline`, `LineageSink`, `AuditStore` are out of v1 scope entirely** | Not modeled at all in v1. | These describe *what happens to data after it lands* — orchestration/transformation territory, explicitly out of scope. |
| **Compatibility is a provider capability, not a type-system guarantee** | `CDCCapableProvider.SupportedSourceEngines()` and `SinkCapableProvider.SupportedSinkFormats()` are consulted at `validate`/`plan` time, with a documented error shape. | Wiring a `Source` to a `Provider` that can't actually speak that engine is a configuration mistake, not a type error — catch it early, with a clear message. |
| **Lineage is observed, never synthesized** | Datascape resolves a lineage backend's `LineageEndpoint` (connection details only) and hands it to a provider that implements `LineageAware`. It never constructs a Job/Run/Dataset fact itself. | Real lineage tools (OpenLineage) model *job execution*, produced by the tool doing the work. Datascape reconciling infrastructure is not a job execution. Where a tool already does this natively — Debezium ships its own OpenLineage integration — Datascape's job shrinks to forwarding configuration, nothing more. |
| **Object storage ships in v1.0.0, mechanism-complete; lineage integration does not have to** | `Provider(type: s3\|minio)` + `Dataset` + `mode: sink` `Binding`s are required v1.0.0 deliverables. The `LineageAware`/`observers` *mechanism* is required and tested; a concrete lineage-backend provider (e.g., one that stands up Marquez) is optional. | CDC into a stream with nowhere durable to go is an incomplete platform story, and `Dataset` is also the input contract future orchestrator-facing providers (Dagster, etc.) will need. Lineage's mechanism matters now; its integration value depends on tools this project doesn't yet reach. |
| **Keep `SecretReference`** | `spec.backend` ∈ {`env`, `file`, `kubernetes`, `vault`}, `spec.keys` is a logical key list. Secrets are always references, never inline values. | This was already right in the experimental phase. |
| **Keep the Kubernetes-familiar envelope** | `apiVersion`/`kind`/`metadata`/`spec`/`status` stays. | Developers already pattern-match this instantly; the goal is not to be Kubernetes, but to borrow its literacy. |
| **CLI binary stays `platformctl`** | Project/product name is Datascape; the compiled binary and command surface remain `platformctl`. | No functional reason to rename; renaming has real cost for zero design benefit. |
