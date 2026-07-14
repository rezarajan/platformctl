# Datascape — v1.0.0 "Production-Grade First Version" Specification

This document is the precise, testable definition of what ships as `v1.0.0`. It corresponds to
the close of **Phase 5** in the roadmap. If a reviewer can't check a box below against a running
binary, it isn't done.

## 1. What "v1.0.0" means here

A from-scratch implementation of the layered model (resource / provider / runtime) described in
this planning package, with:

- The Docker runtime fully implemented.
- Five real providers: Redpanda, Postgres, Debezium (CDC), an S3-API-compatible object store
  (MinIO reference), and a Kafka-Connect-based S3 sink connector.
- All three resource lifecycles (managed, external, imported) working, not just modeled.
- Drift detection as a real, runnable command.
- Safety guarantees (no destructive action on external resources without explicit, separate
  opt-in) enforced in the engine, tested, not merely documented.
- The `observers`/`LineageAware` lineage-observation mechanism working and tested against a fake
  provider — a real lineage backend is optional, not required.

## 2. In-scope Kinds

| Kind | apiVersion maturity at v1.0.0 |
|---|---|
| `Provider` | GA |
| `Source` | GA (engine-extensible shape) |
| `EventStream` | GA |
| `Binding` | GA (`mode: cdc` and `mode: sink`; `mode: batch` reserved, unimplemented) |
| `SecretReference` | GA (backends: `env`, `file`; `kubernetes`/`vault` accepted by schema, resolution not yet implemented — fails fast with a clear error if referenced) |
| `Dataset` | GA |

## 3. In-scope Providers

| Provider `type` | Capability at v1.0.0 |
|---|---|
| `redpanda` | Broker lifecycle on Docker; topic create/update (partitions, retention); status/drift probe. |
| `postgres` | Instance lifecycle on Docker; logical replication enablement; replication user provisioning via `SecretReference`; status/drift probe. |
| `debezium` | Kafka Connect instance lifecycle on Docker; connector registration/update against a `Binding`; connector state verification (`RUNNING`/`FAILED`) surfaced as conditions; implements `LineageAware` — forwards a resolved `LineageEndpoint` into Debezium's own native OpenLineage configuration when `metadata.observers` names a lineage provider. |
| `s3` / `minio` | Object-store instance lifecycle on Docker; `Dataset` (bucket/prefix) reconciliation; status/drift probe. |
| `s3sink` | Kafka-Connect-based sink connector lifecycle on Docker; registers/updates a sink connector per a `Binding(mode: sink)`; implements `SinkCapableProvider`. |
| `openlineage` | **Optional, not required.** Schema-accepted; a reference implementation (e.g., standing up Marquez) may or may not exist at v1.0.0 sign-off — does not block it either way. |

## 4. In-scope Runtime

Docker only (`runtime.type: docker`), via the Docker Engine API. `kubernetes`, `external`,
`terraform` runtime types are accepted by schema for forward compatibility but rejected at
registry-construction time with a "planned, not yet available" error — never silently ignored.

## 5. CLI surface

```
platformctl validate ./platform/
platformctl plan ./platform/ [-o table|json]
platformctl apply ./platform/ [--auto-approve]
platformctl status ./platform/ [-o table|json|yaml]
platformctl drift ./platform/
platformctl destroy ./platform/ [--include-external] [--include-imported] [--yes-i-understand-this-is-destructive]
platformctl import <kind>/<name> --from <descriptor>
platformctl graph ./platform/ [-o dot|mermaid]
platformctl docs build
platformctl docs serve
```

## 6. Acceptance scenario (worked example, as a runnable test)

This is the primary end-to-end acceptance test for v1.0.0. Example manifests live under
`examples/cdc-attendance/`:

```yaml
# provider-redpanda.yaml
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: local-redpanda
spec:
  type: redpanda
  runtime: {type: docker, network: datascape}
  configuration: {image: docker.redpanda.com/redpandadata/redpanda:v24.2.1}
---
# provider-postgres.yaml
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: local-postgres
spec:
  type: postgres
  runtime: {type: docker, network: datascape}
  configuration: {image: postgres:16}
  secretRefs: [postgres-replication-creds]
---
# provider-debezium.yaml
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: postgres-cdc
spec:
  type: debezium
  runtime: {type: docker, network: datascape}
  configuration: {image: debezium/connect:2.7}
---
# provider-minio.yaml
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: local-minio
spec:
  type: minio
  runtime: {type: docker, network: datascape}
  configuration: {image: minio/minio:RELEASE.2026-06-01}
---
# provider-s3sink.yaml
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: s3-sink
spec:
  type: s3sink
  runtime: {type: docker, network: datascape}
  configuration: {image: debezium/connect:2.7}
---
# secret.yaml
apiVersion: datascape.io/v1alpha1
kind: SecretReference
metadata:
  name: postgres-replication-creds
spec:
  backend: env
  keys: [username, password]
---
# source.yaml
apiVersion: datascape.io/v1alpha1
kind: Source
metadata:
  name: student-database
spec:
  engine: postgres
  providerRef: {name: local-postgres}
  postgres: {database: studentdb}
---
# eventstream.yaml
apiVersion: datascape.io/v1alpha1
kind: EventStream
metadata:
  name: attendance-events
spec:
  providerRef: {name: local-redpanda}
  partitions: 6
  retention: {duration: 7d}
---
# dataset.yaml
apiVersion: datascape.io/v1alpha1
kind: Dataset
metadata:
  name: attendance-raw
spec:
  providerRef: {name: local-minio}
  bucket: raw-events
  prefix: attendance/
  format: parquet
---
# binding-cdc.yaml
apiVersion: datascape.io/v1alpha1
kind: Binding
metadata:
  name: student-db-to-events
  observers:
    - name: test-lineage-fake        # a fake LineageAware-compatible Provider used only in the acceptance test
spec:
  mode: cdc
  sourceRef: {name: student-database}
  targetRef: {name: attendance-events}
  providerRef: {name: postgres-cdc}
  options:
    tables: ["students", "attendance"]
    snapshotMode: initial
---
# binding-sink.yaml
apiVersion: datascape.io/v1alpha1
kind: Binding
metadata:
  name: attendance-events-to-lake
spec:
  mode: sink
  sourceRef: {name: attendance-events}
  targetRef: {name: attendance-raw}
  providerRef: {name: s3-sink}
  options:
    format: parquet
```

**Test procedure and acceptance checks (10 resources total):**

1. `platformctl validate examples/cdc-attendance/` → exits 0.
2. `platformctl plan examples/cdc-attendance/` against empty state → shows 10 resources, all
   `create`.
3. `platformctl apply examples/cdc-attendance/ --auto-approve` → reconciles in dependency order:
   network → Redpanda → topic → Postgres → replication → Debezium → connector → verification →
   MinIO → bucket → sink connector → verification (MinIO/sink branch is independent of the
   Postgres/Debezium branch and eligible for concurrent reconciliation once parallel
   reconciliation is enabled).
4. `platformctl status examples/cdc-attendance/` → all 10 resources `Ready: True`.
5. The fake `test-lineage-fake` provider records that it received a `LineageEndpoint` with the
   expected URL — proving FR-19/FR-20 without a real lineage backend.
6. `platformctl apply` again, unchanged manifests → zero mutating calls across all five real
   providers (Redpanda, Postgres, Debezium, MinIO, S3 sink).
7. Manually stop the MinIO container → `platformctl drift` reports it on the `Dataset`; `plan`
   does not restart it; `apply` does.
8. Re-run the scenario with `student-database` marked `external: true` and `destroy
   --include-external` **omitted**: the Postgres container is left running; running it again
   **with** `--include-external` but without `--yes-i-understand-this-is-destructive` still
   refuses.
9. Kill the `platformctl apply` process mid-run (e.g., after Redpanda is up but before Postgres
   starts) and confirm `.datascape/state.json` remains valid JSON reflecting exactly the
   resources that completed.
10. `platformctl destroy examples/cdc-attendance/ --auto-approve` → all managed resources removed
    in reverse dependency order, no leftover Datascape-labeled Docker objects of any kind.

## 7. Non-functional acceptance bars

| Bar | Check |
|---|---|
| Determinism (NFR-1) | Golden-file test: `plan` output for this exact manifest set against a fixed prior-state fixture is byte-identical across repeated runs. |
| Idempotency (NFR-2) | Step 6 above, automated. |
| Safety (NFR-3) | Step 8 above, automated. |
| Recoverability (NFR-9) | Step 9 above, automated. |
| Performance (NFR-8) | Steps 1–4 complete in under 4 minutes on a reference laptop spec, with images pre-pulled. |
| Mechanism correctness without a real backend (NFR-10) | A contract/unit test asserts that a `LineageAware`-implementing fake provider receives a `LineageEndpoint` with the correct `URL`/`Namespace` when `metadata.observers` names a resolvable `Provider`, and that a non-`LineageAware` provider's resource still reaches `Ready` with the informational `LineageEndpointDeclaredNotConsumed` condition, never a failure. |
| Sink correctness | A golden-file or live check confirms objects actually land under `raw-events/attendance/` in the expected format after the sink connector runs against real CDC traffic (at minimum: the initial snapshot). |

## 8. Explicitly out of scope for v1.0.0

- Kubernetes runtime adapter, Terraform/external SaaS API runtime adapter, out-of-process
  provider plugins (Phases 7–8).
- Any job/orchestration execution (dbt, Airflow, Spark) — never in scope.
- Multi-tenant control plane, RBAC, hosted service, web UI.
- Cross-environment promotion workflows.
- Remote state backends (local file only in v1.0.0; interface supports it later).
- A real, hardened lineage-backend provider (`openlineage`/Marquez or otherwise) — the mechanism
  ships, a reference backend implementation does not have to.
- `Binding.spec.mode: batch` — schema-reserved, not implemented.
- Any orchestrator-facing provider (Dagster, Airflow, etc.) that would *consume* a `Dataset` —
  `Dataset` is being built now specifically so that work has something stable to target later,
  but the orchestrator provider itself is out of scope for v1.0.0.

## 9. Definition of Done checklist for `v1.0.0`

- [ ] All Phase 0–5 exit criteria (roadmap doc §3–8) are individually checked off.
- [ ] The 10-resource acceptance scenario in §6 passes in CI against a real Docker daemon on
      every commit to `main`.
- [ ] Every NFR in §7 has an automated check, not a manual one.
- [ ] `DockerRuntime`, `RedpandaProvider`, `PostgresProvider`, `DebeziumCDCProvider`,
      `ObjectStoreProvider`, `SinkBinding`, `CDCBinding` feature gates are GA. `LineageObservability`
      remains Alpha at v1.0.0 by design — this is expected, not a gap to close before release.
- [ ] `platformctl docs build` generates a reference site covering every GA Kind and Provider
      `type` from the `schemas/` directory with no manual doc-writing step.
- [ ] A first-time user can go from `git clone` to a fully `Ready` CDC-to-object-storage pipeline
      using only `examples/cdc-attendance/` manifests and the README quickstart — no undocumented
      steps.

## 10. Suggested next step after this package is approved

Scaffold Phase 0 directly from the architecture doc: the domain packages, the ports with their
conformance suites, and a `noop` provider — enough to make `validate`/`plan`/`apply`/`status`
real against fake infrastructure before touching Docker at all. See
[06-agentic-execution-guide.md](06-agentic-execution-guide.md) for how to structure that work
using Claude Code and other coding agents.
