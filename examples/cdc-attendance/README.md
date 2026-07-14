# cdc-attendance — the v1.0.0 acceptance scenario

The worked example from `docs/planning/05-v1-first-version-spec.md` §6: a
Postgres database whose changes stream through Debezium into a Redpanda
topic, with a Kafka Connect S3 sink landing them as objects in MinIO.

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
- The sink Connect worker image, built once — stock Connect images ship no
  S3 sink plugin:

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

```sh
platformctl validate examples/cdc-attendance/
platformctl apply    examples/cdc-attendance/ --auto-approve
platformctl status   examples/cdc-attendance/
```

Then generate some change traffic and watch it land:

```sh
psql postgres://admin:admin-pw@localhost:5432/studentdb \
  -c "CREATE TABLE students (id serial PRIMARY KEY, name text);
      INSERT INTO students (name) VALUES ('alice'), ('bob');"

# objects appear under raw-events/attendance/ within ~30s
mc alias set local http://localhost:9000 minioadmin minioadmin-pw
mc ls --recursive local/raw-events/
```

Tear down:

```sh
platformctl destroy examples/cdc-attendance/ --auto-approve
```

## Host ports

Everything shares the `datascape` Docker network internally; these host
ports are published: Redpanda Kafka 9092, Postgres 5432, Debezium Connect
8083, sink Connect 8084, MinIO 9000. Adjust the `configuration` blocks
(`kafkaPort`, `port`, `connectPort`) if any are taken on your machine.

## Deviations from the spec document's sketch

The manifest sketch in 05-v1-first-version-spec.md §6 predates the working
providers; this directory is the runnable version of it:

- `quay.io/debezium/connect:2.7` — Docker Hub stopped receiving Debezium
  2.x tags.
- `Dataset.spec.format: json` instead of `parquet`: the sink connector's
  parquet writer needs schema-carrying records, and this pipeline runs
  schemaless JSON converters end-to-end. Formats `json`/`jsonl`/`csv` work
  as declared.
- Explicit `postgres-admin-creds` / `minio-root-creds` SecretReferences and
  the `superuserSecretRef`/`rootSecretRef`/`replicationSecretRef`/
  `credentialsSecretRef` configuration keys: real instances need bootstrap
  credentials, which the sketch elided.
- `test-lineage-fake` exists as an actual manifest (`type: noop`) so the
  `observers` entry on the CDC Binding resolves and the LineageEndpoint
  forwarding path runs for real.
