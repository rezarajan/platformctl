# Datascape — Product & Technical Requirements

Status: Canonical — this is the authoritative requirements document.

## 1. Vision

> Datascape declares and reconciles data-platform resources independently of where they run,
> with Docker as a first-class local and single-node runtime target.

Datascape gives a developer a Kubernetes-familiar way to say "I need an event stream, a CDC
pipeline off my Postgres database, and an object store" and have that platform actually exist —
locally today, elsewhere later — without hand-rolling Compose files, connector JSON, and
bootstrap scripts every time.

It is a **control plane for data infrastructure**, not a job orchestrator, not a transformation
engine, and not a general-purpose container platform. It stops exactly at "the infrastructure
exists, is healthy, and is configured" — what runs *through* that infrastructure (dbt models,
Airflow DAGs, Spark jobs) is someone else's job.

## 2. Problem statement

Standing up a realistic local data platform — a message broker, CDC off a relational database,
object storage, the glue between them — currently means either hand-writing Compose files and
bootstrap scripts that drift from what's actually running, with no concept of desired-state
reconciliation, or adopting a full Kubernetes + Helm + operator stack, which is enormous overhead
for a single developer or CI, and still doesn't model *data-platform* concepts (topics,
replication slots, connectors), only generic workloads.

There is no equivalent of "a Kubernetes manifest, but for the data platform, that also knows how
to run itself locally." That's the gap. A CDC flow that lands events in a broker and stops is
also an incomplete platform story — a realistic local data platform needs a durable place for
that data to land, which is why object storage is part of the committed v1.0.0 path rather than
a later add-on.

## 3. Goals

- **G1.** Provide a declarative resource model for data-platform concepts (`Source`,
  `EventStream`, `Binding`, `Dataset`, `Provider`) that is meaningful independent of where those
  resources are deployed.
- **G2.** Make Docker a fully-realized, actively-reconciled runtime target — not merely a
  Compose-file generator — including post-startup application-level configuration (topic
  creation, replication slots, connector registration).
- **G3.** Support three distinct resource lifecycles in the same model: **managed** (Datascape
  creates and operates it), **external** (Datascape configures something that already exists),
  and **imported** (Datascape discovers and adopts existing state).
- **G4.** Make the system safe to re-run: `apply` is idempotent, `destroy` never touches
  external or imported resources without an explicit, separate opt-in.
- **G5.** Make the system's behavior legible: a `plan` step always precedes `apply`, status is
  always inspectable, and drift from declared state is detectable and reported.
- **G6.** Design the provider/runtime split so that a second runtime adapter (Kubernetes,
  Terraform, an external SaaS API) can be added later without changing any existing resource
  kind or provider's semantics.
- **G7.** Provide a durable, provider-agnostic sink target (`Dataset`, backed by an
  S3-API-compatible `Provider`) so that stream-based resources (`EventStream`) have somewhere
  real to land data, and so that future orchestrator-facing providers (e.g. a Dagster provider)
  have a stable, already-proven input contract to build against.
- **G8.** Provide a narrow, optional mechanism for a provider to be told about a lineage
  backend's connection details, without Datascape ever constructing lineage semantics itself.
  This is a mechanism-provision goal, not an integration-completeness goal — see NG6.

## 4. Non-goals

- **NG1.** Datascape does not schedule or execute data processing jobs (dbt, Airflow, Spark
  jobs, etc.). It provisions the infrastructure those tools run on top of; it does not run them.
- **NG2.** Datascape is not a general-purpose container orchestrator. It does not aim to
  replace Docker Compose, Kubernetes, or Nomad for arbitrary workloads — only for the specific,
  named data-platform resource kinds it defines.
- **NG3.** Datascape does not provide a multi-tenant control plane, RBAC, or a hosted service in
  v1. It is a CLI operated by a single developer or a CI pipeline against a single target
  environment at a time.
- **NG4.** Datascape does not provide a web UI in v1.
- **NG5.** Datascape does not attempt cross-environment promotion (dev → staging → prod
  pipelines) in v1. One set of manifests targets one environment per invocation.
- **NG6.** Datascape does not ship a production-grade, fully integrated lineage backend provider
  (e.g., a hardened Marquez provider) in v1.0.0. The `observers`/`LineageAware` mechanism must
  exist and be provably correct against a test double; a real backend integration is a candidate
  for a later phase, not a v1.0.0 blocker.
- **NG7.** Datascape does not implement its own lineage semantics (Job/Run/Dataset event
  construction). Where a provider's underlying tool has native lineage support (as Debezium does
  for OpenLineage), forwarding a connection endpoint to that tool is in scope; reconstructing
  what the tool already does is not.

## 5. Guiding principles

1. **The resource model must never become Docker-specific.** If a field, behavior, or type only
   makes sense for Docker, it belongs in the Docker runtime adapter, not in `Source`,
   `EventStream`, or `Binding`.
2. **A provider owns technology semantics; a runtime owns execution mechanics.** The Redpanda
   provider knows what a topic and a retention policy are. The Docker adapter knows how to run a
   container and does not know what a topic is.
3. **Determinism is a feature, not an aspiration.** The same manifests plus the same prior state
   must always produce the same plan. Where live infrastructure introduces inherent
   non-determinism (a container's actual health at this instant), that non-determinism must be
   confined to `status`, never leak into `plan` output ordering or diffing.
4. **Safety defaults to caution.** `destroy` defaults to managed-only. Deleting an external or
   imported resource requires a distinct, explicit flag. Secrets are never printed, logged, or
   persisted in plaintext.
5. **Kubernetes-familiar, not Kubernetes-compatible.** Borrow the envelope
   (`apiVersion`/`kind`/`metadata`/`spec`/`status`) and the reconciliation mental model for
   literacy. Do not chase CRD/controller-runtime compatibility as a goal in itself.
6. **Every phase ships something runnable end-to-end.** No phase is "just the domain model" or
   "just the CLI skeleton" without a demonstrable, working vertical slice by its exit criteria.
7. **Observation is not participation.** A mechanism that lets Datascape tell another system
   "something happened" must never require Datascape to know what that other system will do with
   the information, or to fabricate details on its behalf. The `observers` mechanism forwards
   connection facts; it does not construct domain-specific event payloads for tools Datascape
   doesn't operate.

## 6. Primary users

| Persona | Need |
|---|---|
| **Data/platform engineer, local dev** | Stand up a realistic local replica of pieces of the data platform (broker, CDC, object storage) without hand-maintaining Compose files. |
| **CI pipeline** | Provision an ephemeral, reproducible data-platform stack for integration tests, then tear it down deterministically. |
| **Platform team building internal tooling** | Use Datascape's resource model and provider interfaces as the reconciliation substrate for an internal developer platform, potentially targeting Kubernetes later. |

## 7. Functional requirements

| ID | Requirement | Priority |
|---|---|---|
| FR-1 | Parse and validate YAML/JSON manifests against the resource schema for each supported `apiVersion`/`kind`, producing actionable errors (file, line where feasible, field path). | Must |
| FR-2 | Build a dependency graph across resources from `providerRef`, `sourceRef`, `targetRef`, `connectionRef` and detect cycles before any reconciliation begins. | Must |
| FR-3 | Compute a `plan`: diff desired manifest state against last-known state (and, where cheap, live runtime state) and present create/update/delete/no-op per resource in dependency order, without mutating anything. | Must |
| FR-4 | Execute `apply`: reconcile resources in dependency order, respecting the plan, updating state as each resource settles, continuing or halting on a per-resource failure per a documented policy. | Must |
| FR-5 | Execute `destroy`: tear down **managed** resources in reverse dependency order by default; require an explicit flag to include external/imported resources, and even then only within what the provider is permitted to configure (never delete data the user marked external). | Must |
| FR-6 | Report `status`: per-resource condition set (`Ready`, `Progressing`, `Degraded`, `DriftDetected`, ...) plus a rollup for the whole manifest set. | Must |
| FR-7 | Support `import`: given a description of an existing resource, adopt it into state as **imported** without recreating it. | Should |
| FR-8 | Detect drift: on `plan` or a dedicated `drift` command, compare live runtime/provider state against last-applied state and report divergence without auto-correcting unless `apply` is explicitly run. | Should |
| FR-9 | Resolve secrets exclusively through `SecretReference`, from at least an environment-variable backend and a file backend in v1; never accept inline secret values in `spec`. | Must |
| FR-10 | Provide a Docker runtime adapter implementing network, volume, and container reconciliation, including health-check waiting, sufficient for at least the Redpanda, Postgres, Debezium, MinIO, and S3-sink providers. | Must |
| FR-11 | Provide a Redpanda provider capable of reconciling brokers and topics end-to-end on the Docker runtime. | Must |
| FR-12 | Provide a Postgres provider capable of reconciling a database instance, enabling logical replication, and provisioning a replication user, on the Docker runtime. | Must |
| FR-13 | Provide a Debezium/Kafka-Connect-based CDC provider capable of registering and verifying a connector that binds a `Source` to an `EventStream` via a `Binding`. | Must |
| FR-14 | Provide human-readable (table) and machine-readable (JSON) output modes for `plan` and `status`, selectable via flag, so CI can consume output programmatically. | Must |
| FR-15 | Provide a feature-gate mechanism so unstable providers/runtimes/behaviors can ship disabled-by-default and graduate without a breaking release. | Must |
| FR-16 | Provide an object-store provider (`type: s3` or `type: minio`) capable of reconciling a bucket/prefix-backed `Dataset` on the Docker runtime. | Must |
| FR-17 | Support `Binding.spec.mode: sink`, reconciling a stream-to-object-store data path (`EventStream` → `Dataset`) via a sink-capable provider. | Must |
| FR-18 | Enforce `Binding` compatibility structurally by `mode` (which resource Kinds `sourceRef`/`targetRef` may resolve to) and by provider capability (`CDCCapableProvider.SupportedSourceEngines()` for `cdc`, `SinkCapableProvider.SupportedSinkFormats()` for `sink`), surfaced as a `validate`-time error, never discovered only at `apply`. | Must |
| FR-19 | Support `metadata.observers` on any data-plane resource, resolving each named entry to a `Provider`'s connection details and, if the owning resource's provider implements `LineageAware`, forwarding those details before/during reconciliation. | Must (mechanism); a concrete lineage-backend provider is Should. |
| FR-20 | An `observers` entry referencing a provider that does not implement `LineageAware` is a no-op, not an error — surfaced only as an informational status annotation, never blocking reconciliation. | Must |

## 8. Non-functional requirements

| ID | Requirement |
|---|---|
| NFR-1 (Determinism) | Given identical manifests and identical prior state, `plan` output is byte-identical modulo explicitly-live fields (timestamps, observed health). |
| NFR-2 (Idempotency) | Running `apply` twice in a row with no manifest changes results in zero mutating calls to any runtime/provider on the second run. |
| NFR-3 (Safety) | No `destroy` invocation without an explicit flag ever issues a delete call against a resource marked `external`. This is enforced in the reconciliation engine, not left to individual providers to remember. |
| NFR-4 (Observability) | Every reconciliation action is logged as a structured event (resource, action, outcome, duration) sufficient to reconstruct what happened without re-running. |
| NFR-5 (Portability) | The CLI is a single static binary for macOS and Linux (amd64/arm64), with no runtime dependency beyond Docker itself when the Docker runtime is in use. |
| NFR-6 (Extensibility) | Adding a new provider requires implementing one interface and registering it; it must not require changes to the domain layer, the CLI, or the state format. |
| NFR-7 (Testability) | Every port (`Runtime`, `Provider`, `StateStore`, `SecretStore`) has a fake/in-memory implementation usable in unit tests, and a shared conformance test suite that both fakes and real adapters must pass. |
| NFR-8 (Performance) | Cold-start reconciliation of the full worked acceptance scenario (10 resources — see the v1 spec) completes in under 4 minutes on a typical developer laptop with images already pulled. |
| NFR-9 (Recoverability) | State is written atomically (temp file + rename, or transactional store); a crash mid-apply never corrupts state into an unreadable file. |
| NFR-10 (Mechanism correctness without a real backend) | The `observers`/`LineageAware` path must be fully testable — and tested — using a fake `LineageAware` provider, so its correctness does not depend on a real lineage backend being implemented first. |
| NFR-11 (Settledness) | A resource reported `Ready` answers its declared protocol at that moment, and a probe run immediately after `apply` reports no drift. Wait loops poll an observable condition under an overall deadline (with an honest timeout error naming the last observed state); fixed-duration sleeps that assume completion are forbidden — correctness must not depend on machine speed. (Added by the 2026-07 production review, doc 08 I3; the redpanda settle fix `93fbf14` is the motivating instance.) |
| NFR-12 (NFR-4 made literal) | NFR-4's "structured event" is not a metaphor: `--log-format json` emits one `encoding/json`-parseable `log/slog` event per reconciliation action, carrying `resource`/`action`/`outcome`/`duration` as attributes (plus the same prose `--log-format text`, the default, renders byte-for-byte unchanged). (Added by the 2026-07 production review, doc 08 I11.) |

**Scale envelope (2026-07 note):** NFR-8's budget is stated for the
10-resource acceptance scenario only. Behavior at 100s of resources
(sequential per-resource reconciliation, the local JSON state file, the
graph walk) is *not yet characterized* — treat that as a known,
deliberate gap until a scale test exists (tracked by the production
review, doc 11), not an implicit promise.

## 9. Constraints and assumptions

- Docker (or a Docker-API-compatible engine) is assumed present for the Docker runtime adapter;
  Datascape does not manage the Docker daemon's lifecycle itself.
- v1 targets single-machine/single-node scenarios. Multi-node Docker Swarm-style orchestration
  is out of scope.
- The CLI operates against one state file/backend per invocation; there is no v1 requirement for
  concurrent multi-user access to the same state (a simple advisory lock is sufficient).
- The v1.0.0 object-store provider targets an S3-API-compatible service running on Docker (MinIO
  is the reference target); it does not target a hosted cloud object store as a first-class
  scenario (nothing prevents it working against one via `external: true`, but it isn't a tested
  v1.0.0 path).

## 10. Success criteria for v1

Treated as an acceptance gate, detailed fully in
[05-v1-first-version-spec.md](05-v1-first-version-spec.md):

- A user can author `Provider`, `Source`, `EventStream`, `Binding`, and `Dataset` manifests
  describing a Redpanda + Postgres + Debezium CDC flow landing in MinIO/S3, run
  `platformctl plan`, `platformctl apply`, watch it come up healthy, run `platformctl status`
  and see `Ready`, then `platformctl destroy` and have it cleanly torn down — all against the
  Docker runtime, all without hand-written Compose files or bootstrap scripts.
- Re-running `apply` with no changes performs zero mutating operations.
- Killing a container out-of-band and re-running `plan` surfaces drift.
- A `metadata.observers` entry on the CDC `Binding`, pointed at a fake test `LineageAware`
  provider, results in that provider receiving the correct `LineageEndpoint` — proving the
  mechanism without requiring a real Marquez/OpenLineage backend to exist.

## 11. Open questions

Flagged rather than silently resolved:

| Question | Recommended default | Why it's still open |
|---|---|---|
| Local state backend format: flat JSON file vs. embedded KV store (BoltDB/SQLite)? | Flat JSON file at `.datascape/state.json` for v1; abstracted behind `StateStore` so this is swappable later. | JSON is simpler to inspect/debug during early development; an embedded KV store buys transactional writes "for free" but adds a dependency. |
| Out-of-process provider plugins (Terraform-provider-style, over gRPC) — when? | Not in v1; compiled-in Go registry only. Revisit at Phase 8. | Matters once third parties want to ship providers independently of the core binary's release cycle — premature before there's a second consumer of the interface. |
| Should `destroy --include-external` be able to delete external resources at all, or only ever de-configure them? | De-configure only; never issue a destructive delete against a resource the user declared `external: true`. | This is a safety-critical default; worth explicit sign-off rather than assuming. |
| Does v1.0.0 need more than one sink-capable connector technology (e.g., both a Kafka-Connect-based S3 sink and a Redpanda-native sink)? | Ship exactly one (a Kafka-Connect-based S3 sink connector) for v1.0.0. | Depends on whether Redpanda's own sink tooling is simpler to operate than a second Kafka Connect worker — worth a short spike before committing. |
| Should the sink connector share the Kafka Connect worker already running for Debezium, or run its own? | Run its own container for v1.0.0 (simpler, no new sharing mechanics). | Sharing a worker reduces container count but introduces a new kind of cross-provider dependency that hasn't been designed yet. |
