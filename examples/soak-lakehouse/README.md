# soak-lakehouse — orchestrator-ready infrastructure

The Phase 6.5 (soak) example: `platformctl` stands up the core
infrastructure a data-pipeline orchestrator runs against — object storage,
an Iceberg REST catalog, an OpenLineage backend, relational stores, and a
stable entrypoint to external systems. **You run Dagster/Metabase yourself**
(not managed in this release, by design) and point them at the endpoints
below.

## Run it

```sh
export DATASCAPE_SECRET_LAKE_MINIO_ROOT_USERNAME=minioadmin
export DATASCAPE_SECRET_LAKE_MINIO_ROOT_PASSWORD=minioadmin-pw
export DATASCAPE_SECRET_LAKE_PG_ADMIN_USERNAME=admin
export DATASCAPE_SECRET_LAKE_PG_ADMIN_PASSWORD=admin-pw
export DATASCAPE_SECRET_LAKE_MYSQL_ROOT_USERNAME=root
export DATASCAPE_SECRET_LAKE_MYSQL_ROOT_PASSWORD=mysql-root-pw
export DATASCAPE_SECRET_LAKE_MARQUEZ_DB_USERNAME=marquez
export DATASCAPE_SECRET_LAKE_MARQUEZ_DB_PASSWORD=marquez-pw
export DATASCAPE_SECRET_EXT_ORDERS_CONN_USERNAME=orders_ro
export DATASCAPE_SECRET_EXT_ORDERS_CONN_PASSWORD=orders-pw

platformctl validate examples/soak-lakehouse/
platformctl apply    examples/soak-lakehouse/ --auto-approve
platformctl status   examples/soak-lakehouse/
```

## Endpoints for your orchestrator

What a Dagster deployment (resources/IO managers) or Metabase connects to
once `status` shows all `Ready`:

| Resource | From your machine | From a container on `datascape` | Notes |
|---|---|---|---|
| `Dataset/warehouse` (S3) | `http://127.0.0.1:19010`, bucket `warehouse`, prefix `iceberg/` | `http://lake-minio:9000` | creds = `lake-minio-root` secret |
| Iceberg catalog (Nessie) | `http://127.0.0.1:19121/iceberg` | `http://lake-catalog:19120/iceberg` | REST catalog URI; API at `/api/v2` |
| OpenLineage (Marquez) | `http://127.0.0.1:15100/api/v1` | `http://lake-lineage:5000` | point `OPENLINEAGE_URL` here; also the target for `metadata.observers` |
| `Source/app-db` (Postgres) | `127.0.0.1:15434`, db `appdb` | `lake-postgres:5432` | creds = `lake-pg-admin` |
| `Source/events-db` (MySQL) | `127.0.0.1:13306`, db `eventsdb` | `lake-mysql:3306` | creds = `lake-mysql-root` |
| `Source/orders-db` (external, via proxy) | `127.0.0.1:15999` | `edge-orders-db:15999` | creds = `ext-orders-conn`; the *real* location only lives in the manifest |

## The proxy entrypoint (and VPCs)

`Provider/edge` gives every external dependency a **platform-owned address
that never changes**: tools connect to the route, and when the external
endpoint moves you edit one line of manifest and re-apply. Update the
route's `target` to your real external database
(`host.docker.internal:5432` reaches your host on Docker Desktop; any
address reachable from the Docker network works on Linux).

If the external system sits behind a VPC: run your VPN/tunnel out-of-band
for now and point the route's `target` at its local end. Each route's `via`
field is schema-reserved for chaining through a future `tunnel`-typed
provider (WireGuard first) — see
`docs/design/002-soak-orchestrator-infrastructure.md` for why that's design-
only in this release.

## Importing infrastructure you already have

Already running a Postgres you want the platform to own going forward?
Declare it in the manifests, then adopt it (never re-created, only probed):

```sh
platformctl import Provider/lake-postgres examples/soak-lakehouse/ --from lake-postgres
```

`ImportedResources` is Beta and enabled by default as of this release.

## What's deliberately not here

- **Dagster / Metabase themselves** — bring your own; this stack is what
  they connect to.
- **CDC** — see `examples/cdc-attendance/` for the Debezium pipeline; both
  examples share the `datascape` network and compose.
- **Tunnel/VPN providers** — designed, not built (design note 002).
