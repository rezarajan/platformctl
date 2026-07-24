# Zero-Trust Lakehouse — a complete, provable data platform

This example stands up a **production-shaped data platform** with one
`platformctl apply` and proves every claim live. It is the reference for
"what a full Datascape platform looks like."

## What it proves

| Capability | How this example demonstrates it |
|---|---|
| **Multiple sources** | A Postgres `orders` DB and a MySQL `events` DB, both captured. |
| **CDC into a data lake** | Debezium captures both databases → Redpanda (Avro, schema-registry-backed) → Parquet in MinIO. |
| **Multiple lake formats** | Raw **Parquet** (the s3sink landing zone) *and* **Iceberg** tables queried through Trino. |
| **Iceberg query** | A Trino coordinator wired to the Nessie catalog; `SHOW CATALOGS` / `SELECT` over Iceberg. |
| **Cataloging** | Nessie (Iceberg REST catalog) fronting the MinIO warehouse. |
| **Lineage** | Marquez (OpenLineage) receives lineage from every CDC binding. |
| **Zero-trust access** | Three independent, *tested* mechanisms — see [Zero-trust](#zero-trust). |
| **External orchestration** | MinIO, Kafka, and Trino publish host endpoints an external **Dagster** deployment connects to as resources/IO managers — see [Connect an orchestrator](#connect-an-orchestrator). |

Every feature here ships **Beta** in this release. Nothing is a mock: the
canary is refused by a real OpenZiti identity check; the policy deny is a
real `validate` exit; the CDC connector really reaches a database it can
only see through an identity-mediated tunnel.

## Architecture

```
                       ┌──────────────────── ztl-orders-vpc (isolated) ─────────┐
                       │   ztl-orders-db  (DARK Postgres — no host port,        │
                       │                   no shared network, no direct route)  │
                       └───────────────▲────────────────────────────────────────┘
                                       │  identity-mediated tunnel only
                        ┌──────────────┴───────────────┐
                        │  mesh (OpenZiti controller +  │   ← router alone crosses
                        │  router)   orders-db-mediated │     into the VPC
                        └──────────────▲────────────────┘
   datascape (shared platform network) │
   ┌───────────────┬───────────────────┴────┬──────────────┬───────────────┐
   │ events-mysql  │ lake-debezium (CDC) ────┤ lake-redpanda│ lake-s3sink   │
   │ (source)      │  orders + events        │  (Avro +     │ (Parquet sink)│
   └───────────────┘                         │  schema reg) └──────┬────────┘
   ┌───────────────┬───────────────┬─────────┴────┐                │
   │ lake-minio    │ catalog-svc   │ lake-trino    │  lake-lineage  ▼
   │ (S3 warehouse)│ (Nessie)      │ (Iceberg query)│  (Marquez)   MinIO
   └──────▲────────┴───────────────┴──────▲────────┘                warehouse
          │ S3                            │ Trino
          └──────── external Dagster ─────┘  Kafka
```

## Prerequisites

- Docker (this stack is ~15 containers; JVMs for Nessie/Marquez/Trino/
  Connect). **Recommended: 6+ GB free RAM, 4+ CPUs.** Every container is
  resource-bounded (`spec.runtime.resources`) so the scheduler places by
  request and JVMs size their heaps from the limit.
- `platformctl` on your PATH (or point at a built binary).
- Python 3 (the test script parses JSON).

## Run it

```bash
cd examples/zero-trust-lakehouse

# 1. Credentials — copy, fill with strong values, and source.
cp .env.example .env && $EDITOR .env
set -a; . ./.env; set +a

# 2. Build the CDC/sink connector image (bundles the Avro converter).
docker build -t datascape-s3sink-connect:local ./s3sink-image

# 3. Stand up the "external" dark orders database (your real DB in prod;
#    a stand-in here, on an isolated network with NO host port).
./setup-external-db.sh

# 4. Apply the platform. Every gate this example uses ships Beta.
GATES=SchemaRegistrySupport=true,MediatedConnections=true,PolicyEngine=true,LabelScopedAccess=true,TrinoProvider=true
platformctl apply . \
  --state-file ./ztl.state.json --auto-approve \
  --policies ./policies --feature-gates "$GATES"

# 5. Confirm every resource is Ready.
platformctl status . --state-file ./ztl.state.json \
  --policies ./policies --feature-gates "$GATES"
```

All 27 resources report `Ready=True`. The `orders` Source shows
`External` / `ExternalEndpointReachable` — reached only through the mesh.

## Zero-trust

Three mechanisms, each independently proven by `./test-zero-trust.sh`:

1. **Identity-mediated dark source.** `orders` is a Postgres with no host
   port and on an isolated network nothing else joins. The only path in is
   the `orders-db-mediated` Connection — an OpenZiti service where every
   hop is mutually authenticated. Debezium dials `orders-db-mediated:16891`
   and never learns the real address. *Proof:* the CDC connector reaches
   `RUNNING` (positive), and a **canary** holding a different, unauthorized
   Ziti identity — enrolled against the same controller, on the same
   network — is **refused** the dial.
2. **Label-scoped policy.** The `orders` Source is `tier: gold` /
   `classification: pii`; `policies/zero-trust-lakehouse.yaml` denies any
   graph edge from a `tier: gold` source to a consumer not carrying
   `clearance: gold`. *Proof:* `validate` on a rogue consumer exits 3 and
   names the firing rule.
3. **Graph-scoped least-privilege networking** (ADR 026) is available as a
   Beta gate (`GraphScopedAccess=true`) for per-edge network isolation.
   It is proven standalone by the `graphscoped` suite; it is *not* stacked
   on the mediated path in this example, because a mediated Connection
   already provides identity-level zero-trust for that edge and the two
   layers are belt-and-suspenders there.

```bash
./test-zero-trust.sh    # after apply — expects 3/3 PASS
```

## Connect an orchestrator

An external **Dagster** deployment connects to the platform's sources and
sinks over their published host endpoints — no coupling, just connectors.

| Endpoint | Host | Dagster use |
|---|---|---|
| MinIO (S3) | `http://127.0.0.1:16898` | `S3Resource` / IO manager (warehouse read/write) |
| Kafka bootstrap | `127.0.0.1:16895` | Kafka source/sink connector (consume CDC topics) |
| Schema Registry | `http://127.0.0.1:16896` | Avro deserialization of CDC events |
| Trino | `http://127.0.0.1:16900` | `trino` DBAPI resource (query Iceberg tables) |

```python
# resources.py — a Dagster deployment reads the lake without any platformctl coupling
from dagster import Definitions
from dagster_aws.s3 import S3Resource
import trino

s3 = S3Resource(
    endpoint_url="http://127.0.0.1:16898",
    aws_access_key_id="lakeadmin",           # DATASCAPE_SECRET_LAKE_MINIO_ROOT_USERNAME
    aws_secret_access_key="...",             # DATASCAPE_SECRET_LAKE_MINIO_ROOT_PASSWORD
)

def trino_conn():
    return trino.dbapi.connect(host="127.0.0.1", port=16900, user="dagster", catalog="lakehouse")

defs = Definitions(resources={"s3": s3})
```

Kafka: point any Dagster Kafka connector at `127.0.0.1:16895` with the
schema registry at `http://127.0.0.1:16896` to consume the `orders-events`
and `events-events` CDC topics directly.

## Teardown

```bash
./teardown.sh                       # removes ONLY this example's objects
```

`teardown.sh` destroys the platform (`platformctl destroy`) and then the
external dark DB + its isolated network — by exact name, never touching
anything else on your Docker host.

## Troubleshooting

- **`Binding/orders-to-events` fails "connection attempt failed":** the
  dark DB isn't up or its credential doesn't match `orders-db-replication`.
  Re-run `./setup-external-db.sh` (it uses that exact secret).
- **`network "ztl-orders-vpc" exists but is not managed`:** an old
  unmanaged network lingers — `docker network rm ztl-orders-vpc` and re-run
  `./setup-external-db.sh` (it labels the network managed).
- **Trino/JVM OOM:** raise Docker's memory; the JVM limits are set in
  `02-providers.yaml` under each provider's `spec.runtime.resources`.
