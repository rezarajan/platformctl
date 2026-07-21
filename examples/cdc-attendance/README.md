# cdc-attendance — the v1.0.0 acceptance scenario

The worked example from `docs/planning/05-v1-first-version-spec.md` §6: a
Postgres database whose changes stream through Debezium into a Redpanda
topic, with a Kafka Connect S3 sink landing them as **parquet** objects in
MinIO. Change events are Avro-serialized against Redpanda's built-in
Confluent-compatible schema registry (docs/planning/08 D1/D2) — that
schema-carrying chain is what makes the columnar parquet output possible.

```
Source(student-database) ──Binding(cdc)──▶ EventStream(attendance-events)
                                                    │
                                            Binding(sink)
                                                    ▼
                                          Dataset(attendance-raw)  →  MinIO
```

10 declared resources + 3 secret references, all reconciled by
`platformctl apply` in dependency order.

## Prerequisites

- A running Docker daemon (images are pulled on first apply).
- The Connect worker image, built once and shared by both Connect workers —
  stock Connect images ship neither the S3 sink plugin nor Confluent's Avro
  converter (which the Avro→parquet chain needs on the CDC worker *and* the
  sink worker; see `s3sink-image/Dockerfile` for the exact pinned jars):

  ```sh
  docker build -t datascape-s3sink-connect:local examples/cdc-attendance/s3sink-image/
  ```

- Credentials in the environment (the `env` secret backend resolves
  `DATASCAPE_SECRET_<NAME>_<KEY>`, uppercased, dashes → underscores):

  ```sh
  export DATASCAPE_SECRET_POSTGRES_ADMIN_CREDS_USERNAME=admin
  export DATASCAPE_SECRET_POSTGRES_ADMIN_CREDS_PASSWORD=admin-pw
  export DATASCAPE_SECRET_POSTGRES_REPLICATION_CREDS_USERNAME=repl
  export DATASCAPE_SECRET_POSTGRES_REPLICATION_CREDS_PASSWORD=repl-pw
  export DATASCAPE_SECRET_MINIO_ROOT_CREDS_USERNAME=minioadmin
  export DATASCAPE_SECRET_MINIO_ROOT_CREDS_PASSWORD=minioadmin-pw
  ```

## Run it

The schema-registry chain sits behind the Alpha `SchemaRegistrySupport`
feature gate, so every command passes it explicitly:

```sh
platformctl validate examples/cdc-attendance/ --feature-gates SchemaRegistrySupport=true
platformctl apply    examples/cdc-attendance/ --feature-gates SchemaRegistrySupport=true --auto-approve
platformctl status   examples/cdc-attendance/ --feature-gates SchemaRegistrySupport=true
```

Then generate some change traffic and watch it land (`students` is one of
the tables the CDC Binding declares — see
[Capturing another table](#capturing-another-table)):

```sh
psql postgres://admin:admin-pw@localhost:15432/studentdb \
  -c "CREATE TABLE students (id serial PRIMARY KEY, name text);
      INSERT INTO students (name) VALUES ('alice'), ('bob');"

# parquet objects appear under raw-events/attendance/ within ~30s
mc alias set local http://localhost:19000 minioadmin minioadmin-pw
mc ls --recursive local/raw-events/

# subjects registered by the Avro converters are visible in the registry
curl -s http://localhost:18081/subjects
```

Tear down:

```sh
platformctl destroy examples/cdc-attendance/ --feature-gates SchemaRegistrySupport=true --auto-approve
```

### The schemaless json variant

`parquet` needs the full schema-carrying chain (registry + Avro). To run
the schemaless path instead — no registry, plain JSON objects — set
`Dataset.spec.format: json`, drop `options.format: avro` from
`binding-cdc.yaml`, and drop the `schemaRegistry`/`schemaRegistryPort` keys
from `provider-redpanda.yaml` (then no feature gate is needed). The
integration suite keeps exactly that json variant as a fixture:
`cmd/platformctl/testdata/parquet-sink-scenario/json/` (and the standing
`testdata/sink-scenario/` set).

## When something dies out-of-band

Containers fail, get OOM-killed, or get `docker rm -f`'d by a human. Plain
`status` reports the *recorded* state (that determinism is the tool's
contract); to check reality, probe:

```sh
docker rm -f local-minio          # simulate an external failure

platformctl drift  examples/cdc-attendance/   # observes it: DRIFT=True, exits 1
platformctl status examples/cdc-attendance/   # DRIFT column now reflects the observation
platformctl apply  examples/cdc-attendance/ --auto-approve   # heals: recreates the
                                  # container (its volume, so its data, survived),
                                  # restarts stopped workers and failed connectors
```

`plan` never restarts anything — it only diffs specs. `destroy` converges
even when parts of the platform are already dead: a Dataset whose object
store was killed doesn't strand the teardown.

## Capturing another table

**Only tables declared on the CDC Binding are captured.** The Binding in
`binding-cdc.yaml` declares:

```yaml
  options:
    tables: [students, attendance]
```

which becomes the Debezium connector's `table.include.list`
(`public.students,public.attendance`). Creating some other table and
inserting rows produces **nothing** in the stream or the sink — the
connector filters it out. That's the declarative model: the manifest, not
the database, decides what is part of the platform.

To capture a new table, declare it and re-apply:

```yaml
  options:
    tables: [students, attendance, grades]
```

```sh
platformctl apply examples/cdc-attendance/ --auto-approve
# → ok   Binding/student-db-to-events (update)   — connector reconfigured in
#   place; Postgres, Redpanda, and the Connect workers are not restarted.
```

Rows inserted into `grades` from then on stream to
`attendance-events.public.grades` and land under
`raw-events/attendance/` within ~30s. Omitting `tables` entirely captures
every table in the database.

**Caveat — pre-existing rows:** widening the table list does not re-run the
initial snapshot. Rows that were already in the new table may or may not
replay (it depends on how far the connector's WAL position has advanced),
so treat declared-late tables as streaming-only from the moment of
re-apply. If you need a full backfill, destroy and re-apply the Binding so
its connector snapshots from scratch, or use Debezium incremental
snapshots (not wired up in v1).

## Host ports

Everything shares the `datascape` Docker network internally; these host
ports are published, chosen away from the services' well-known defaults so
the example coexists with whatever Postgres/Kafka/MinIO you already have
running: Redpanda Kafka 19093, schema registry 18081, Postgres 15432,
Debezium Connect 18083, sink Connect 18084, MinIO 19000. Adjust the
`configuration` blocks (`kafkaPort`, `schemaRegistryPort`, `port`,
`connectPort`) if any are taken on your machine.

## Defaults

Fields this example used to spell out that a `platformctl` default already
covers (docs/planning/08 E2 — the manifests here omit them; every default
is still visible after apply, in the same providerState `inventory`/`state
inspect` already read):

- `spec.runtime.network`: every Provider defaults to the `datascape`
  network. Pin `runtime: {type: docker, network: <name>}` for a different
  one.
- `spec.configuration.image` on `redpanda`/`minio` Providers: each has a
  pinned default image. `s3sink` has none (no stock Connect image ships the
  S3 sink plugin), and since the parquet flip (docs/planning/08 D2) the
  `debezium` Provider also pins the shared local build — the stock Debezium
  image lacks the Confluent Avro converter the `format: avro` Binding needs.
- `spec.configuration.bootstrapServers` on the `debezium`/`s3sink` Connect
  workers: inferred from the manifest graph — the Binding using the
  worker resolves to `attendance-events`, whose Provider is
  `local-redpanda`. Pin it explicitly, e.g.
  `bootstrapServers: local-redpanda:29092`, if a worker's Bindings don't
  unambiguously resolve to one broker.
- `spec.postgres.schema` on a `Source`: the CDC connector defaults an
  unset schema to `"public"`. Pin `schema: <name>` for any other schema.

## Deviations from the spec document's sketch

The manifest sketch in 05-v1-first-version-spec.md §6 predates the working
providers; this directory is the runnable version of it:

- `quay.io/debezium/connect:2.7` — Docker Hub stopped receiving Debezium
  2.x tags.
- ~~`Dataset.spec.format: json` instead of `parquet`~~ — **closed by
  docs/planning/08 D2**: the example now lands parquet as the §6 sketch
  drew it, via registry-driven Avro converters. The schemaless json path
  still works as declared (see
  [the schemaless json variant](#the-schemaless-json-variant)).
- Explicit `postgres-admin-creds` / `minio-root-creds` SecretReferences and
  the `superuserSecretRef`/`rootSecretRef`/`replicationSecretRef`/
  `credentialsSecretRef` configuration keys: real instances need bootstrap
  credentials, which the sketch elided.
- `test-lineage-fake` exists as an actual manifest (`type: noop`) so the
  `observers` entry on the CDC Binding resolves and the LineageEndpoint
  forwarding path runs for real.
