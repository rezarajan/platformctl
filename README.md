<div align="center">

# 🌐 Datascape

### `platformctl` — declarative data infrastructure on container runtimes

*Describe your data platform as resources. Plan the diff. Apply it. Watch a
Postgres → Debezium → Redpanda → S3 pipeline reach `Ready` from a directory
of YAML.*

[![CI](https://github.com/rezarajan/platformctl/actions/workflows/ci.yml/badge.svg)](https://github.com/rezarajan/platformctl/actions/workflows/ci.yml)
[![Go](https://img.shields.io/badge/go-1.22%2B-00ADD8?logo=go&logoColor=white)](go.mod)
[![Runtime](https://img.shields.io/badge/runtime-Docker-2496ED?logo=docker&logoColor=white)](#architecture)
[![Status](https://img.shields.io/badge/phases-0–4_verified-brightgreen)](docs/planning/04-roadmap-and-feature-gates.md)

</div>

---

Datascape treats the *infrastructure of a data platform* — databases, event
streams, CDC connectors, object storage, sinks — the way Kubernetes treats
workloads and Terraform treats cloud resources: a **typed resource model**,
a **deterministic plan**, an **idempotent reconciliation engine**, and
**drift-aware status**, all from one static binary.

```console
$ platformctl apply ./platform/ --auto-approve
ok   Provider/local-redpanda        (create) in 2.7s
ok   EventStream/attendance-events  (create) in 213ms
ok   Provider/local-postgres        (create) in 2.7s
ok   Provider/postgres-cdc          (create) in 6.7s
ok   Source/student-database        (create) in 53ms
ok   Binding/student-db-to-events   (create) in 254ms   # Debezium connector: RUNNING
...
applied: 14 succeeded, 0 failed, 0 skipped
```

## ✨ Highlights

- **Declarative, diff-driven** — `plan` is computed purely from manifests +
  recorded state (never live probing), so it is deterministic and
  reviewable; `apply` reconciles in dependency order and re-applying an
  unchanged set makes **zero mutating calls**.
- **A real pipeline, end-to-end** — five providers ship today: Redpanda,
  Postgres (logical replication), Debezium CDC, MinIO/S3, and a
  Kafka-Connect S3 sink. Rows inserted into Postgres land as objects in a
  bucket with nothing hand-wired in between.
- **Capability-checked bindings** — a `Binding(mode: cdc)` against a
  provider that can't do CDC, or a `sink` to a format the connector can't
  write, fails at `validate` with a precise error — not at 2 a.m. during
  `apply`.
- **Safety in the engine, not in conventions** — external resources are
  never destroyed without explicit, separate opt-in flags; failed destroys
  block teardown of their dependencies; unmanaged Docker objects are never
  touched (everything Datascape owns is labeled).
- **Secrets stay out of manifests** — `SecretReference` resources resolve
  through pluggable backends (`env` today); specs carry names, never values.
- **Lineage-aware by design** — `metadata.observers` forwards a resolved
  `LineageEndpoint` to providers that consume one (Debezium's native
  OpenLineage integration), and degrades to an informational condition when
  they don't.
- **Feature-gated evolution** — every provider ships behind a gate
  (`--feature-gates=Name=true|false`), so `main` is always releasable.

## 🏗 Architecture

Strict hexagonal layering — the entire design hangs on one invariant:
**domain and ports never import an adapter.**

```mermaid
flowchart LR
    subgraph CLI["cmd/platformctl"]
        V[validate] --> P[plan] --> A[apply / destroy / status]
    end
    subgraph Application["internal/application"]
        M[manifest] --> G[graph] --> C[compatibility] --> PL[plan] --> E[engine]
        R[registry + feature gates]
    end
    subgraph Ports["internal/ports"]
        RT[ContainerRuntime]
        RC[reconciler.Provider]
        ST[StateStore]
        SS[SecretStore]
    end
    subgraph Adapters["internal/adapters"]
        D[runtime/docker]
        RP[redpanda]
        PG[postgres]
        DBZ[debezium]
        S3[s3 / minio]
        SNK[s3sink]
        LF[state/localfile]
        ENV[secrets/env]
    end
    CLI --> Application --> Ports
    D -.implements.-> RT
    RP & PG & DBZ & S3 & SNK -.implement.-> RC
    LF -.implements.-> ST
    ENV -.implements.-> SS
```

| Layer | Rule |
|---|---|
| `internal/domain` | Imports nothing else in this repo. Resource kinds, graph, lineage types. |
| `internal/ports` | Interfaces only (+ conformance suites). Imports `domain`. |
| `internal/adapters` | Implement ports; may import third-party SDKs. Every adapter passes its port's conformance suite. |
| `cmd/platformctl`, `application/registry` | The **only** places allowed to import concrete adapters. |

### The resource model

Six kinds, one worked scenario:

```
Source(postgres) ──Binding(mode: cdc)──▶ EventStream ──Binding(mode: sink)──▶ Dataset(bucket/prefix)
      │                    │                  │                  │                    │
  Provider(postgres)  Provider(debezium)  Provider(redpanda)  Provider(s3sink)   Provider(minio)
                                                                          SecretReference(env) ⤴
```

`Binding` is the connective tissue: its `mode` fixes which Kinds it may join
(`cdc`: Source→EventStream, `sink`: EventStream→Dataset), and the referenced
provider must declare the matching capability interface
(`CDCCapableProvider`, `SinkCapableProvider`) — both enforced at `validate`.

## 🚀 Quickstart

**Prerequisites:** Go 1.22+, a running Docker daemon.

```sh
git clone https://github.com/rezarajan/platformctl && cd platformctl
just build        # → bin/platformctl
```

Run the full CDC-to-object-storage acceptance scenario:

```sh
# one-time: the sink Connect image (stock images ship no S3 sink plugin)
docker build -t datascape-s3sink-connect:local examples/cdc-attendance/s3sink-image/

# credentials resolve from the environment — see examples/cdc-attendance/README.md
export DATASCAPE_SECRET_POSTGRES_ADMIN_CREDS_USERNAME=admin
export DATASCAPE_SECRET_POSTGRES_ADMIN_CREDS_PASSWORD=admin-pw
export DATASCAPE_SECRET_POSTGRES_REPLICATION_CREDS_USERNAME=repl
export DATASCAPE_SECRET_POSTGRES_REPLICATION_CREDS_PASSWORD=repl-pw
export DATASCAPE_SECRET_MINIO_ROOT_CREDS_USERNAME=minioadmin
export DATASCAPE_SECRET_MINIO_ROOT_CREDS_PASSWORD=minioadmin-pw

bin/platformctl validate examples/cdc-attendance/
bin/platformctl apply    examples/cdc-attendance/ --auto-approve
bin/platformctl status   examples/cdc-attendance/
```

Insert a row, watch it land in the lake:

```sh
psql postgres://admin:admin-pw@localhost:15432/studentdb \
  -c "CREATE TABLE students (id serial PRIMARY KEY, name text);
      INSERT INTO students (name) VALUES ('alice'), ('bob');"

mc alias set local http://localhost:19000 minioadmin minioadmin-pw
mc ls --recursive local/raw-events/       # objects appear within ~30s
```

Tear it all down (reverse dependency order, labeled objects only):

```sh
bin/platformctl destroy examples/cdc-attendance/ --auto-approve
```

## 🖥 CLI surface

| Command | What it does |
|---|---|
| `validate <dir>` | Schema + graph (cycles) + Binding capability checks. No state, no runtime calls. |
| `plan <dir>` | Deterministic diff of manifests vs. state. Exit `1` when changes are pending. |
| `apply <dir>` | Reconcile in topological order; state persisted after every resource. |
| `status <dir>` | Per-resource `Ready`/conditions/lifecycle from recorded state. |
| `graph <dir> -o dot\|mermaid` | Render the dependency DAG. |
| `destroy <dir>` | Reverse-order teardown. `--include-external` additionally requires `--yes-i-understand-this-is-destructive`. |

Global flags: `--state-file` (default `.datascape/state.json`),
`--feature-gates`, `-o table|json|yaml`.

## 🧪 Development

```sh
just build             # CGO_ENABLED=0 static build
just test              # unit + contract tests (no Docker)
just test-integration  # real Docker: runtime conformance + Redpanda/CDC/sink e2e
just check             # gofmt + go vet (both build-tag variants)
```

The integration suite stands up real Postgres, Debezium, Redpanda, and MinIO
containers on non-default host ports and verifies the roadmap's exit
criteria literally — including "re-apply makes zero mutating calls" and
"destroy leaves no orphans".

**Adding a provider:** implement `reconciler.Provider` (plus capability
interfaces you support), register it in `application/registry` wiring behind
a feature gate, and cover it with an integration test. The engine hands you
your `Provider` resource, resolved secrets, and the full resource set via
optional `*Aware` interfaces — see `internal/adapters/providers/redpanda`
for the smallest complete example.

## 📚 Documentation

The `docs/planning/` package is the source of truth:

| Doc | Contents |
|---|---|
| [01-product-requirements](docs/planning/01-product-requirements.md) | What Datascape is (and deliberately isn't). |
| [02-architecture](docs/planning/02-architecture.md) | Layering, ports, capability interfaces, error contracts. |
| [03-resource-model-reference](docs/planning/03-resource-model-reference.md) | Every Kind, field by field. |
| [04-roadmap-and-feature-gates](docs/planning/04-roadmap-and-feature-gates.md) | Phases 0–8 with checkable exit criteria. |
| [05-v1-first-version-spec](docs/planning/05-v1-first-version-spec.md) | The precise v1.0.0 definition of done. |

**Roadmap status:** Phases 0–4 (foundations, Docker runtime, Redpanda,
CDC + lineage mechanism, object-storage sink) are complete and verified
end-to-end. Phase 5 (imported/external lifecycles, drift detection) is in
progress; its completion declares v1.0.0.

---

<div align="center">
<sub>Built docker-first on purpose: the resource model is validated against
the cheapest real runtime before Kubernetes/Terraform adapters (phases 7–8)
make it portable.</sub>
</div>
