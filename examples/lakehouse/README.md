# lakehouse — orchestrator-ready infrastructure

The core infrastructure a Dagster (or similar) deployment runs against,
declared as resources and reconciled by `platformctl`: object storage, an
Iceberg **Catalog**, a lineage backend, relational stores, and a stable
**Connection** to an external database — with CDC from that external
database flowing through the platform-owned entrypoint onto a managed
stream. You run the orchestrator yourself; this stands up (and heals, and
tears down) everything it connects to.

```
                        ┌────────────────────────────────────────────┐
   Dagster / Metabase   │                 datascape network          │
   (run by you) ──────► │  MinIO   Catalog(nessie)   Marquez         │
                        │  Postgres   MySQL   Redpanda   Debezium    │
                        │                                            │
   external orders db ◄─┼── Connection "orders-db" (forwarder)       │
   (owned elsewhere)    │        ▲ CDC connector reads through it    │
                        └────────┼───────────────────────────────────┘
                                 └── Binding(cdc): orders → orders-events
```

## What this example demonstrates

- **Provider-agnostic nouns.** The manifests declare a `Catalog` and a
  `Connection`; Nessie and socat are engines/technologies *realizing* them
  (`docs/planning/03-resource-model-reference.md` §8.1–8.2). Swap the
  engine, keep the model.
- **All three lifecycles side by side.** Everything here is Managed except
  the `orders` Source (External, integrated through its Connection). To see
  Imported, adopt any pre-existing container with
  `platformctl import <Kind>/<name> --from <name>` — §3.1 of the reference
  explains when to reach for which.
- **External integration that actually does work.** The CDC Binding
  registers a Debezium connector at the Connection's in-network endpoint
  (`orders-db:15999`) with the Connection's credentials — the platform
  configures *against* the external database without ever owning it
  (§3.2).
- **Lineage by observation.** `observers: [lake-lineage]` on the Binding
  hands Marquez's endpoint to Debezium's native OpenLineage integration.

## Run it

Prerequisites: a Docker daemon; the "external" database (stands in for the
system another team operates — anything answering Postgres on the shared
network works):

```sh
docker network create datascape
docker run -d --name external-orders-db --network datascape \
  -e POSTGRES_USER=orders_ro -e POSTGRES_PASSWORD=orders-pw -e POSTGRES_DB=orders \
  postgres:16 postgres -c wal_level=logical
```

Credentials resolve from the environment (`DATASCAPE_SECRET_<NAME>_<KEY>`):

```sh
export DATASCAPE_SECRET_LAKE_MINIO_ROOT_USERNAME=minioadmin
export DATASCAPE_SECRET_LAKE_MINIO_ROOT_PASSWORD=minioadmin-pw
export DATASCAPE_SECRET_LAKE_PG_ADMIN_USERNAME=admin
export DATASCAPE_SECRET_LAKE_PG_ADMIN_PASSWORD=admin-pw
export DATASCAPE_SECRET_LAKE_MYSQL_ROOT_USERNAME=root
export DATASCAPE_SECRET_LAKE_MYSQL_ROOT_PASSWORD=mysql-root-pw
export DATASCAPE_SECRET_EXT_ORDERS_CREDS_USERNAME=orders_ro
export DATASCAPE_SECRET_EXT_ORDERS_CREDS_PASSWORD=orders-pw

platformctl validate examples/lakehouse/
platformctl apply    examples/lakehouse/ --auto-approve
platformctl status   examples/lakehouse/
```

Generate change traffic in the external database and watch it stream:

```sh
psql postgres://orders_ro:orders-pw@127.0.0.1:15999/orders \
  -c "CREATE TABLE orders (id serial PRIMARY KEY, sku text);
      INSERT INTO orders (sku) VALUES ('sku-1'), ('sku-2');"
# → topic orders-events.public.orders on 127.0.0.1:19096
```

Note the psql address: **the Connection's entrypoint**, not the database's
own — the same address the CDC connector uses in-network.

## Endpoints for your orchestrator

Don't hand-maintain this — `platformctl` generates it from the applied
state, with the SecretReference holding each credential:

```sh
platformctl inventory examples/lakehouse/     # or -o json for tooling
```

The table below is what that produces (abridged):

| What | In-network (Dagster in a container) | From the host |
|---|---|---|
| Iceberg REST catalog | `http://catalog-svc:19120/iceberg` | `http://127.0.0.1:19121/iceberg` |
| Nessie API | `http://catalog-svc:19120/api/v2` | `http://127.0.0.1:19121/api/v2` |
| S3 (warehouse bucket) | `http://lake-minio:9000` | `http://127.0.0.1:19010` |
| OpenLineage (Marquez) | `http://lake-lineage:5000` | `http://127.0.0.1:15100` |
| App Postgres (`appdb`) | `lake-postgres:5432` | `127.0.0.1:15434` |
| MySQL (`eventsdb`) | `lake-mysql:3306` | `127.0.0.1:13306` |
| Kafka (Redpanda) | `lake-redpanda:29092` | `127.0.0.1:19096` |
| External orders db | `orders-db:15999` | `127.0.0.1:15999` |

## Defaults

Fields this example used to spell out that a `platformctl` default already
covers (docs/planning/08 E2 — omitted below; every default stays visible
after apply through the same providerState `inventory`/`state inspect`
already read):

- `spec.runtime.network`: every Provider defaults to `datascape`.
- `spec.configuration.image` on `minio`/`redpanda`/`debezium`: each has a
  pinned default image.
- `spec.configuration.bootstrapServers` on `orders-cdc` (debezium):
  inferred from the manifest graph — both CDC Bindings using it resolve to
  `lake-redpanda`. Pin it explicitly, e.g.
  `bootstrapServers: lake-redpanda:29092`, if that ever becomes ambiguous.
- `spec.nessie.defaultBranch` on the Catalog: defaults to `"main"`.
- `spec.postgres.schema` on a Source: defaults to `"public"`.

## When things drift

Kill anything and reconcile: `platformctl drift examples/lakehouse/`
reports it, `platformctl apply` heals the managed pieces — including the
Connection's forwarder — and never mutates the external database.

## Teardown

```sh
platformctl destroy examples/lakehouse/ --auto-approve
# external-orders-db is left running: it is External. Removing it from
# state too requires the NFR-3 double lock:
#   --include-external --yes-i-understand-this-is-destructive
docker rm -f external-orders-db && docker network rm datascape   # yours to clean
```

## Files

| File | Contents |
|---|---|
| `secrets.yaml` | `SecretReference`s (env backend) — names and keys only, never values |
| `providers.yaml` | The technology layer: minio, nessie, openlineage, postgres, mysql, proxy, redpanda, debezium |
| `catalog-and-connections.yaml` | The `Catalog` noun and the `orders-db` `Connection` |
| `sources-and-datasets.yaml` | Managed sources, the External `orders` source, the warehouse `Dataset` |
| `streams-and-bindings.yaml` | The `EventStream` + CDC `Binding` reading the external database through its Connection |
