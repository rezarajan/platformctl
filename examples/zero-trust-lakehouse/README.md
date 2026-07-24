# Zero-Trust Lakehouse — a complete, provable data platform

This example stands up a **production-shaped, HA, zero-trust data
platform** declared entirely as resources in data-platform **planes**
(folders) and brought up with one `platformctl apply`. It is the
reference for "what just-works Datascape looks like" (docs/adr/035).

> ## ✅ Verified live on Docker — 2026-07-24
>
> `platformctl apply examples/zero-trust-lakehouse` brings the whole set
> up from the plane folders: 26 resources to `Ready`, zero-trust on with
> no `--feature-gates` for it, all 5 checks in `test-zero-trust.sh`
> passing (positive mediated-CDC path, an unauthorized-identity dial
> refused, dark-DB posture), and the HA broker-loss kill-test surviving
> (see [High availability](#high-availability)). The plane-folder layout
> loads via the project's explicit `spec.resources` member list (the
> Helm/Kustomize include pattern, docs/adr/035 / M7): the root
> `datascape.yaml` names its planes; each plane's own `datascape.yaml`
> names its files.

## What it proves

| Capability | How this example demonstrates it |
|---|---|
| **Just-declare-it DX** | ONE `datascape.yaml` (project root) declares the runtime; every Provider omits `spec.runtime`; every port except three Dagster-facing ones is auto-allocated; zero-trust is on with no `--feature-gates`; no labels, no hand-written policy anywhere. |
| **Data-platform planes** | Resources are organized into folders that read like a data platform: `platform/`, `sources/`, `cdc/`, `sinks/`, `catalog/`, `query/`, `lineage/` — not a numbered flat file dump. |
| **Multiple sources** | A dark, mediated Postgres `orders` DB and a managed MySQL `events` DB, both captured. |
| **CDC into a data lake** | Debezium captures both databases → Redpanda (HA, 3 brokers) → raw JSON in MinIO (HA, 4-node erasure-coded). |
| **Iceberg query** | A Trino coordinator (HA, 2 workers) wired to the Nessie catalog fronting the MinIO warehouse. |
| **Lineage** | Marquez (OpenLineage) receives lineage from every CDC binding. |
| **Zero-trust, by default, from the graph alone** | The dark `orders` DB is reachable only through an identity-mediated OpenZiti Connection; the CDC worker is auto-wired to it by graph-scoped derivation (docs/planning/08 M5) with **no label, no policy, no `--feature-gates`** — the declared graph *is* the allow-set (M6). |
| **High availability, as a deliberate lever** | `brokers: 3` (Redpanda), `nodes: 4` (MinIO), `workers: 2` (Trino) — the one explicit gate this example needs, `--feature-gates HighAvailability=true`, alongside `TrinoProvider=true` (a provider-technology gate, unrelated to zero-trust/HA — Trino ships Alpha/disabled independent of this ADR). |
| **External orchestration** | Trino publishes a fixed host port a Dagster deployment connects to; MinIO/Kafka publish auto-allocated-but-stable ports discoverable via `platformctl inventory` (see [Known adaptations](#known-adaptations) for why those two aren't pinned). |

## Plane structure

```
examples/zero-trust-lakehouse/
├── datascape.yaml         # Project: ONE runtime (docker), zero-trust on
├── platform/               # secrets + the zero-trust mesh
│   ├── 00-secrets.yaml     # every SecretReference this project uses
│   └── 01-mesh.yaml        # openziti Provider + the mediated Connection
├── sources/                 # ingestion origins
│   ├── 01-providers.yaml   # events-mysql (the ONLY managed source Provider)
│   └── 02-sources.yaml     # Source orders (external, dark) + events
├── cdc/                     # change data capture
│   ├── 01-redpanda.yaml    # streaming backbone, HA: brokers: 3
│   ├── 02-debezium.yaml    # Kafka Connect worker realizing both CDC legs
│   ├── 03-streams.yaml     # EventStream orders-events, events-events
│   └── 04-bindings.yaml    # Binding(mode: cdc) x2
├── sinks/                   # the lake's landing zone
│   ├── 01-providers.yaml   # lake-minio (HA: nodes: 4), lake-s3sink
│   ├── 02-datasets.yaml    # orders-raw, events-raw, warehouse
│   └── 03-bindings.yaml    # Binding(mode: sink) x2
├── catalog/                 # the Iceberg REST catalog
│   ├── 01-provider.yaml    # nessie
│   └── 02-catalog.yaml     # Catalog(engine: nessie, warehouseRef: warehouse)
├── query/                   # Iceberg query engine
│   └── 01-provider.yaml    # trino (HA: workers: 2; port pinned for Dagster)
├── lineage/                 # lineage capture
│   └── 01-provider.yaml    # openlineage (Marquez)
└── k8s/
    └── datascape.yaml       # the SAME planes, runtime: kubernetes instead
```

Every plane is a folder; nothing outside `platform/` sets a label, a
policy, a port (except Trino's), or a runtime block. `sources/` has no
`postgres` Provider — `orders` is external and dark, mediated entirely
through `platform/`'s Connection; that omission is deliberate, not a gap
(see the comment at the top of `sources/01-providers.yaml`).

## Prerequisites

- Docker (~15+ containers across the HA sets; JVMs for
  Nessie/Marquez/Trino/Connect). Recommended 8+ GB free RAM, 4+ CPUs — HA
  triples the Redpanda footprint and quadruples MinIO's versus the
  original single-container build.
- `platformctl` on your PATH (or point at a built binary).
- Python 3 (`test-zero-trust.sh` parses JSON).

## Run it

```bash
cd examples/zero-trust-lakehouse

# 1. Credentials — copy, fill with strong values, and source.
cp .env.example .env && $EDITOR .env
set -a; . ./.env; set +a

# 2. Build the CDC/sink connector image (bundles the Avro converter build
#    this project no longer uses directly, but the s3sink personality
#    still needs — see Known adaptations).
docker build -t datascape-s3sink-connect:local ./s3sink-image

# 3. Stand up the "external" dark orders database (your real DB in prod;
#    a stand-in here, on an isolated network with NO host port).
./setup-external-db.sh

# 4. Apply. TWO gates, neither about zero-trust: HighAvailability (the
#    owner's deliberate HA lever — brokers/nodes/workers) and TrinoProvider
#    (a provider-technology gate, Alpha/disabled independent of this ADR).
platformctl apply . \
  --state-file ./ztl.state.json --auto-approve \
  --feature-gates HighAvailability=true,TrinoProvider=true

# 5. Confirm every resource is Ready.
platformctl status . --state-file ./ztl.state.json \
  --feature-gates HighAvailability=true,TrinoProvider=true
```

No `--policies`. No `MediatedConnections`/`GraphScopedAccess`/
`PolicyEngine`/`LabelScopedAccess` gates. No labels anywhere in the
manifest set. That absence *is* the proof docs/adr/035 exists to make.

## Zero-trust

Zero-trust is on by default the moment `datascape.yaml` exists
(docs/adr/035 decision 3) — no flag turns it on; `--no-zero-trust` is the
only way to turn it off. With it on:

- **The dark `orders` source is identity-mediated automatically.**
  `platform/01-mesh.yaml` declares one openziti Provider and one
  Connection — no labels, no ports. `sources/02-sources.yaml`'s `orders`
  Source references that Connection via `connectionRef`; that reference
  alone is the only thing that authorizes the CDC leg in `cdc/` to reach
  it. Graph-scoped derivation follows the reference transitively to the
  mediation tunneler (docs/planning/08 M5), so the Debezium worker is
  auto-wired onto the tunnel's network — the fix that used to require a
  hand-written, label-selector `spec.access` grant on the Debezium
  Provider (the prior build's "KNOWN GAP") is now unconditional and
  automatic.
- **There is no policy file.** The declared graph (Connections + Bindings)
  IS the complete allow-set (docs/planning/08 M6) — nothing widens beyond
  it. A hand-written policy could still narrow or annotate a declared
  edge, but this example needs none, so it has none.
- **`spec.access` wide grants are refused outright**, not just unneeded —
  `checkZeroTrustNoWideGrants` (cmd/platformctl/root.go) rejects any
  `spec.access` entry under zero-trust with "declare a Connection or
  Binding to what X needs to reach instead." That refusal is exactly why
  no plane in this example carries one.

`test-zero-trust.sh` proves: (1) the
legitimate mediated CDC path reaches connector state RUNNING; (2) a
canary holding a different, unauthorized Ziti identity is refused the
dial; (3) the dark DB has no host port and is on no network any other
container in this project shares.

## High availability

The one deliberate scaling lever this example takes, `--feature-gates
HighAvailability=true`:

| Component | Declared | What it proves |
|---|---|---|
| `cdc/01-redpanda.yaml` | `brokers: 3` | A 3-broker Raft cluster; `orders-events`/`events-events` are `replication: 3`, not scaled-up-but-unreplicated. |
| `sinks/01-providers.yaml` | `nodes: 4` | A 4-node erasure-coded MinIO cluster (distinct topology from `nodes: 1`; 2-3 has no supported shape). |
| `query/01-provider.yaml` | `workers: 2` | A Trino coordinator with 2 independent query-execution workers. |

The live proof (run against a Docker apply of this example): `docker ps`
shows 3 `lake-redpanda-*` broker containers, 4 `lake-minio-*` node
containers, and a `lake-trino` coordinator + 2 worker containers. Killing
even the **leader** broker for the orders CDC topic (`docker kill
lake-redpanda-2`) leaves the orders CDC path serving — the connector stays
`RUNNING`, a row inserted into the dark DB *after* the kill is still
captured end-to-end, and the platform keeps reporting `Ready=True` for
`lake-redpanda`, `lake-debezium`, and the `orders-to-events` Binding.

That survival is not automatic from `brokers: 3` alone — it required a fix
found by running this very proof: Kafka Connect's per-table CDC data topics
**and** the connect worker's own internal state topics (config/offset/
status) are now created replicated to the target EventStream's declared
`replication` (`internal/adapters/providers/debezium`), where they were
previously hardcoded to a single replica and a broker loss stranded the
data. A single-broker project is unaffected (replication stays 1).

Scope note: the fully broker-loss-proven leg is the **orders** (Postgres)
CDC path. The second **events** (MySQL) leg additionally keeps a Debezium
`schema-history` topic that Debezium auto-creates at replication factor 1
(the connector exposes no per-connector override in this version); it is
not part of the broker-loss claim above and would need the cluster's
`default_topic_replications` raised to be equally resilient.

## Known adaptations (found verifying this example against the current code)

Building this example against the real provider adapters (not just the
architecture docs) surfaced constraints the plan didn't anticipate — all
deliberate, documented product behavior, not bugs, but each changes what
this example can literally do:

1. **A pinned host port cannot combine with a multi-broker/multi-node
   declaration.** `spec.configuration.kafkaPort` + `brokers` and
   `spec.configuration.port` + `nodes` are BOTH refused at validate
   ("each broker's/node's host port is auto-assigned") — by design, since
   each ordinal needs its own port and there is exactly one to pin. So
   only Trino's coordinator port (unaffected — workers carry no host port
   of their own) stays pinned for Dagster; MinIO's S3 endpoint and
   Redpanda's Kafka bootstrap are auto-allocated but **stable across
   applies** (`internal/domain/hostport` is deterministic per component
   name) — discover them with `platformctl inventory` rather than reading
   them off a manifest.
2. **Redpanda's built-in schema registry does not yet support
   multi-broker.** `schemaRegistry: enabled` combined with `brokers` is
   refused ("not yet supported together ... docs/adr/017, follow-ups").
   Since MinIO's Aiven parquet writer requires schema-carrying Avro input,
   this HA build's two CDC legs land as plain JSON rather than the prior
   build's Avro→Parquet leg; the platform's "multiple lake formats" story
   is now raw JSON (CDC/sink path) + Iceberg (Trino/Nessie path,
   independent of EventStream/Binding entirely) rather than JSON + Parquet
   + Iceberg.
3. **`spec.runtime` cannot carry `resources` without also carrying
   `type`.** The schema requires `type` inside any `spec.runtime` block
   that appears at all (so an override can be checked against the
   project's own runtime family). The three providers in this example
   that keep an explicit resources override (`lake-debezium`,
   `lake-s3sink`, `lake-trino` — each documented in its own file) also
   restate `type: docker`; every other provider has no `spec.runtime`
   block whatsoever.

## Connect an orchestrator

An external **Dagster** deployment connects to the platform's sources and
sinks over their published host endpoints — no coupling, just connectors.

| Endpoint | How to find it | Dagster use |
|---|---|---|
| Trino | `http://127.0.0.1:16900` (pinned — `query/01-provider.yaml`) | `trino` DBAPI resource (query Iceberg tables) |
| MinIO (S3) | `platformctl inventory . | grep lake-minio` (auto-allocated, stable) | `S3Resource` / IO manager |
| Kafka bootstrap | `platformctl inventory . | grep lake-redpanda` (auto-allocated, stable; 3 broker addresses) | Kafka source/sink connector |

```python
# resources.py — a Dagster deployment reads the lake without any platformctl coupling
from dagster import Definitions
from dagster_aws.s3 import S3Resource
import trino

s3 = S3Resource(
    endpoint_url="http://127.0.0.1:<from inventory>",
    aws_access_key_id="lakeadmin",           # DATASCAPE_SECRET_LAKE_MINIO_ROOT_USERNAME
    aws_secret_access_key="...",             # DATASCAPE_SECRET_LAKE_MINIO_ROOT_PASSWORD
)

def trino_conn():
    return trino.dbapi.connect(host="127.0.0.1", port=16900, user="dagster", catalog="lakehouse")

defs = Definitions(resources={"s3": s3})
```

## Kubernetes variant

`k8s/datascape.yaml` declares the SAME project with `runtime.type:
kubernetes` instead of `docker` — proving one-runtime-per-project +
portability with zero changes to any plane. `platformctl`'s project
loader reads exactly one `datascape.yaml`, at the applied path's own
root, so the swap is a one-line file copy, not a parallel tree:

```bash
cp datascape.yaml datascape.docker.yaml.bak
cp k8s/datascape.yaml datascape.yaml
KUBECONFIG=/path/to/your.kubeconfig \
  platformctl apply . --state-file ./ztl-k8s.state.json --auto-approve \
  --feature-gates HighAvailability=true,TrinoProvider=true
mv datascape.docker.yaml.bak datascape.yaml   # swap back when done
```

Not yet proven live against a real cluster — blocked on the same
recursive-directory-loading gap as the Docker leg above.

## Teardown

```bash
./teardown.sh                       # removes ONLY this example's objects
```

`teardown.sh` destroys the platform (`platformctl destroy`) and then the
external dark DB + its isolated network — by exact name, never touching
anything else on your Docker host.

## Troubleshooting

- **`error: ... declares spec.resources but they resolved to no manifest
  files`:** a plane named in a `datascape.yaml`'s `spec.resources` is
  missing or empty. The root lists its plane directories; each plane's own
  `datascape.yaml` lists its files — check both against the actual layout.
- **`Binding/orders-to-events` fails "connection attempt failed":** the
  dark DB isn't up or its credential doesn't match
  `orders-db-replication`. Re-run `./setup-external-db.sh` (it uses that
  exact secret).
- **`network "ztl-orders-vpc" exists but is not managed`:** an old
  unmanaged network lingers — `docker network rm ztl-orders-vpc` and
  re-run `./setup-external-db.sh` (it labels the network managed).
- **Trino/JVM OOM:** raise Docker's memory; the JVM limits are set per
  provider file, under `spec.runtime.resources`.
