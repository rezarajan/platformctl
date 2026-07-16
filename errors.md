# examples/cdc-attendance failure — diagnosed and fixed

**Status: resolved.** The full example lifecycle (apply → CDC traffic →
objects in MinIO → idempotent re-apply → destroy, 14/14 resources) was
re-verified against a Docker daemon that had unrelated stacks holding ports
5432/8083/9000/9092.

## Original logs

```
ok   Provider/local-redpanda (create) in 2.681s
ok   Provider/test-lineage-fake (create) in 1ms
ok   SecretReference/minio-root-creds (create) in 1ms
ok   SecretReference/postgres-admin-creds (create) in 1ms
ok   SecretReference/postgres-replication-creds (create) in 1ms
ok   EventStream/attendance-events (create) in 208ms
ok   Provider/local-minio (create) in 2.573s
ok   Provider/local-postgres (create) in 2.58s
fail Provider/postgres-cdc (create) after 5.579s: container "postgres-cdc" exited before becoming healthy
ok   Provider/s3-sink (create) in 6.698s
fail Dataset/attendance-raw (create) after 1ms: check bucket "raw-events": Get "http://localhost:9000/raw-events/?location=": dial tcp [::1]:9000: connect: connection refused
fail Source/student-database (create) after 2ms: connect to postgres: failed to connect to `user=admin database=postgres`:
        [::1]:5432 (localhost): dial error: dial tcp [::1]:5432: connect: connection refused
        127.0.0.1:5432 (localhost): dial error: dial tcp 127.0.0.1:5432: connect: connection refused
skip Binding/attendance-events-to-lake: a dependency failed
skip Binding/student-db-to-events: a dependency failed
error: 3 resource(s) failed to reconcile
exit status 2
```

## Root causes and fixes

1. **`Provider/postgres-cdc` exited before healthy** — the container from a
   previous attempt was still on disk, attached to a `datascape` network that
   no longer existed (its endpoint was pruned when the network was removed).
   `EnsureContainer`'s spec-hash reuse path restarted it *without verifying
   network attachment*, so Kafka Connect came up unable to resolve
   `local-redpanda` ("No resolvable bootstrap urls") and exited.
   **Fix:** the docker adapter now checks that an existing container is
   attached to every network the spec declares before reusing it; on drift it
   replaces the container. `WaitHealthy` failures now also include the
   container's last log lines, so this class of failure is no longer a black
   box.

2. **`Source/student-database` connection refused on 5432** — the Postgres
   healthcheck was `pg_isready -U <user>`, which answers over the *unix
   socket*. The postgres image's initdb phase runs a temporary
   socket-only server, so the container reported healthy (~2.5s) before the
   real server was listening on TCP; the provider then dialed the host port
   and was refused. **Fix:** the healthcheck forces TCP
   (`pg_isready -h 127.0.0.1`), and Source provisioning additionally
   ping-waits up to 30s before issuing SQL.

3. **`Dataset/attendance-raw` connection refused on 9000** — same
   healthy-vs-reachable gap class, aggravated by dialing `localhost` (which
   can resolve to `::1` where some daemons only publish IPv4). **Fix:**
   every provider now dials `127.0.0.1` explicitly, and bucket operations
   retry for up to 30s after the store reports healthy.

4. **Default host ports collide with real machines** — 5432/8083/9000/9092
   are exactly what any existing Postgres/Connect/MinIO/Kafka occupies. The
   example now publishes on 15432 (Postgres), 18083/18084 (Connect workers),
   19000 (MinIO), 19093 (Kafka); see the example README.

Two destroy-path defects surfaced while re-verifying and are fixed too:
failures during provider/secret resolution were counted but never logged,
and a failed destroy did not block teardown of the resources it depends on
(a failed connector delete no longer lets the broker be removed out from
under the Connect worker — mirroring apply-side dependency blocking).

## External errors are not detected against state, and are thus not reconciled correctly

**Status: resolved.** Drift detection is now a first-class path (the Phase 5
roadmap item, pulled forward):

- **`platformctl drift`** probes every applied resource against the live
  runtime — container present/healthy, topic present, connector RUNNING,
  bucket reachable — records the observed `Ready`/`DriftDetected` conditions
  into state, prints a report, and exits 1 when drift is found. A resource
  whose backing provider is unreachable (the killed-MinIO Dataset case
  below) is reported as drifted with `ProbeFailed`, not left `Ready`:
  unreachable *is* drift.
- **`platformctl status`** gained a `DRIFT` column showing the last recorded
  observation (`-` until the first probe). Plain `status` remains
  state-based by design — determinism is the tool's core contract — but it
  no longer *hides* what `drift` observed.
- **`platformctl apply` heals drift**: resources whose spec is unchanged
  (plan no-op) are probed, and drifted ones are re-reconciled — a manually
  removed container is recreated (its volume, and therefore data, persists),
  a stopped one restarted, a failed connector restarted. `plan` still never
  mutates. Gated by `DriftDetection` (enabled by default; deviation from the
  master table recorded in checkpoint.md).
- **`destroy` converges when infrastructure is already dead**: deleting a
  Dataset/EventStream/Binding whose backing store, broker, or Connect worker
  no longer exists is treated as already done rather than failing forever —
  this is exactly what stranded `Dataset/attendance-raw`,
  `Provider/local-minio`, and `minio-root-creds` in the reported state.
- A latent `status.SetCondition` bug surfaced by this work is fixed:
  conditions were keyed by Type+Reason, so an observation with a different
  reason appended a duplicate condition instead of replacing it.

The whole matrix is enforced by `cmd/platformctl/chaos_integration_test.go`
(chaos monkey: out-of-band kills/stops, drift → plan → heal → drift-clean,
destroy with dead infra, and a SIGKILL mid-apply recoverability check),
which runs in CI's integration job.

### Original report
Manual removal of a container does not get reflected in the platformctl state check; the utility does not check against the current running status of the containers. External failures are not observed and thus impossible to reconcile. Furthermore, in this state when issuing the destroy command, containers are removed but the state does not correctly reflect that. Furthermore, assets like the Dataset and Provider/local-minio show as 'Ready' although the containers are no longer available. This is illogical since, firstly the S3 provider local-minio was killed externally, and thus there is no way for the tool to ascertain the availability of the Dataset/attendance-raw, which resides in S3 in this case.

Expected result: The status command must correctly consider the existing state against the declared/saved state. Things fail all the time, out-of-band from platformctl, and the tool must be designed to expect this. Dependencies must have a proper way of resolving state, and assessing state for assets dependent on parents.

```text
❯ bin/platformctl status examples/cdc-attendance/
RESOURCE                                    READY    REASON              LIFECYCLE
Binding/student-db-to-events                Unknown  NotApplied          Managed
Binding/attendance-events-to-lake           Unknown  NotApplied          Managed
Dataset/attendance-raw                      True     DatasetProvisioned  Managed
EventStream/attendance-events               Unknown  NotApplied          Managed
Provider/postgres-cdc                       Unknown  NotApplied          Managed
Provider/test-lineage-fake                  Unknown  NotApplied          Managed
Provider/local-minio                        True     InstanceHealthy     Managed
Provider/local-postgres                     Unknown  NotApplied          Managed
Provider/local-redpanda                     Unknown  NotApplied          Managed
Provider/s3-sink                            Unknown  NotApplied          Managed
SecretReference/postgres-admin-creds        Unknown  NotApplied          Managed
SecretReference/postgres-replication-creds  Unknown  NotApplied          Managed
SecretReference/minio-root-creds            True     SecretResolvable    Managed
Source/student-database                     Unknown  NotApplied          Managed
```


## Platformctl does not check for required dependencies like environment variables being set

**Status: resolved.** Two fixes:

1. **Secret pre-flight** — `apply` (and `import`) now resolve every declared
   `SecretReference` through the configured store *before* touching any
   infrastructure, via the new `SecretStore.Preflight` capability and
   `Engine.PreflightSecrets`. All missing variables are aggregated into one
   error (not one apply at a time), the command exits with the validation
   code, and **no state file is written** — the platform can never
   half-apply for want of a credential. Example:

   ```
   error: 4 secret(s) cannot be resolved — apply would half-apply the
   platform, so nothing was changed:
     - SecretReference "ext-orders-creds": unset environment variable(s): DATASCAPE_SECRET_EXT_ORDERS_CREDS_USERNAME, DATASCAPE_SECRET_EXT_ORDERS_CREDS_PASSWORD
     - SecretReference "lake-minio-root": unset environment variable(s): ...
   ```

2. **`--env-file`** — a persistent flag on every command loads dotenv-style
   `KEY=VALUE` lines (blank/`#`-comment lines ignored, optional `export`
   prefix and surrounding quotes handled) into the environment before
   secrets are resolved. A value already exported in the shell wins over the
   file. `platformctl apply examples/lakehouse/ --env-file ./lakehouse.env`.

3. **External drift honesty** — the second half of the report: an external
   resource showed `Ready=True Drift=False` even though the connector to it
   failed. `externalConnectionStatus` now does more than confirm the
   connection *resolves*: when the `connectionRef` names a `Connection` with
   an address, it TCP-probes the endpoint (`Connection.DialAddress`). A
   managed forwarder with a dead upstream closes the probe immediately →
   `Ready=False, Drift=True, ExternalEndpointUnreachable`; a live endpoint
   holds the connection → `ExternalEndpointReachable`. Reconcile retries the
   probe for up to 30s (startup races); `drift`/`status` take a single fast
   snapshot. An unreachable external source no longer claims health, and its
   dependent Binding is blocked rather than failing after 90s of connector
   retries.

Enforced by `TestPreflightSecretsAggregates`, `TestApplyRefusesOnMissingSecrets`,
`TestEnvFileLoads`, and `TestProbeTCPReachable`.

### Original report

Running apply on an manifest without setting the required environment variables does not throw an error before running; this safeguard must exist so that the user cannot half-apply their infrastructure. Optionally, and env file can be used to host this information.

``` text
❯ ./bin/platformctl apply examples/lakehouse/
RESOURCE                          ACTION     REASON
Provider/catalog-svc              create     not present in state
Provider/edge                     create     not present in state
Provider/lake-lineage             create     not present in state
Provider/lake-redpanda            create     not present in state
SecretReference/ext-orders-creds  create     not present in state
SecretReference/lake-minio-root   create     not present in state
SecretReference/lake-mysql-root   create     not present in state
SecretReference/lake-pg-admin     create     not present in state
Catalog/lakehouse-catalog         create     not present in state
Connection/orders-db              create     not present in state
EventStream/orders-events         create     not present in state
Provider/lake-minio               create     not present in state
Provider/lake-mysql               create     not present in state
Provider/lake-postgres            create     not present in state
Provider/orders-cdc               create     not present in state
Dataset/warehouse                 create     not present in state
Source/app-db                     create     not present in state
Source/events-db                  create     not present in state
Source/orders                     configure  external resource; configuration differs from last applied
Binding/orders-to-events          create     not present in state

Apply these changes? Only 'yes' is accepted: yes
ok   Provider/catalog-svc (create) in 4.607s
ok   Provider/edge (create) in 2ms
ok   Provider/lake-lineage (create) in 6.977s
ok   Provider/lake-redpanda (create) in 2.706s
fail SecretReference/ext-orders-creds (create) after 0s: SecretReference "ext-orders-creds": key "username" not found (expected env var DATASCAPE_SECRET_EXT_ORDERS_CREDS_USERNAME)
fail SecretReference/lake-minio-root (create) after 0s: SecretReference "lake-minio-root": key "username" not found (expected env var DATASCAPE_SECRET_LAKE_MINIO_ROOT_USERNAME)
fail SecretReference/lake-mysql-root (create) after 0s: SecretReference "lake-mysql-root": key "username" not found (expected env var DATASCAPE_SECRET_LAKE_MYSQL_ROOT_USERNAME)
fail SecretReference/lake-pg-admin (create) after 0s: SecretReference "lake-pg-admin": key "username" not found (expected env var DATASCAPE_SECRET_LAKE_PG_ADMIN_USERNAME)
ok   Catalog/lakehouse-catalog (create) in 122ms
skip Connection/orders-db: a dependency failed
ok   EventStream/orders-events (create) in 207ms
skip Provider/lake-minio: a dependency failed
skip Provider/lake-mysql: a dependency failed
skip Provider/lake-postgres: a dependency failed
skip Provider/orders-cdc: a dependency failed
skip Dataset/warehouse: a dependency failed
skip Source/app-db: a dependency failed
skip Source/events-db: a dependency failed
skip Source/orders: a dependency failed
skip Binding/orders-to-events: a dependency failed
error: 4 resource(s) failed to reconcile
```

Furthermore, when setting the environment variables and progressing, the drift status for the externally managed resource indicates False, which is illogical in this scenario where it was not provisioned, and the connector to it failed.

```text
Apply these changes? Only 'yes' is accepted: yes
ok   SecretReference/ext-orders-creds (create) in 1ms
ok   SecretReference/lake-minio-root (create) in 1ms
ok   SecretReference/lake-mysql-root (create) in 1ms
ok   SecretReference/lake-pg-admin (create) in 1ms
ok   Connection/orders-db (create) in 204ms
ok   Provider/lake-minio (create) in 2.666s
ok   Provider/lake-mysql (create) in 6.671s
ok   Provider/lake-postgres (create) in 2.734s
ok   Provider/orders-cdc (create) in 6.726s
ok   Dataset/warehouse (create) in 9ms
ok   Source/app-db (create) in 50ms
ok   Source/events-db (create) in 10ms
ok   Source/orders (configure) in 1ms
fail Binding/orders-to-events (create) after 1m31.087s: register connector "orders-to-events": HTTP 400: {"error_code":400,"message":"Connector configuration is invalid and contains the following 1 error(s):\nError while validating connector config: The connection attempt failed.\nYou can also find the above list of errors at the endpoint `/connector-plugins/{connectorType}/config/validate`"}
error: 1 resource(s) failed to reconcile

~/git/platformctl main* 1m 51s
❯ ./bin/platformctl status examples/lakehouse/
RESOURCE                          READY    DRIFT  REASON                        LIFECYCLE
Catalog/lakehouse-catalog         True     -      CatalogProvisioned            Managed
Connection/orders-db              True     -      Forwarding                    Managed
Provider/lake-minio               True     -      InstanceHealthy               Managed
Provider/catalog-svc              True     -      InstanceHealthy               Managed
Provider/lake-lineage             True     -      LineageBackendHealthy         Managed
Provider/lake-postgres            True     -      InstanceHealthy               Managed
Provider/lake-mysql               True     -      InstanceHealthy               Managed
Provider/edge                     True     -      EntrypointSurfaceReady        Managed
Provider/lake-redpanda            True     -      BrokerHealthy                 Managed
Provider/orders-cdc               True     -      ConnectWorkerHealthy          Managed
SecretReference/lake-minio-root   True     -      SecretResolvable              Managed
SecretReference/lake-pg-admin     True     -      SecretResolvable              Managed
SecretReference/lake-mysql-root   True     -      SecretResolvable              Managed
SecretReference/ext-orders-creds  True     -      SecretResolvable              Managed
Source/app-db                     True     -      SourceProvisioned             Managed
Source/events-db                  True     -      SourceProvisioned             Managed
Source/orders                     True     False  ExternalConnectionResolvable  External
Dataset/warehouse                 True     -      DatasetProvisioned            Managed
EventStream/orders-events         True     -      TopicReconciled               Managed
Binding/orders-to-events          Unknown  -      NotApplied                    Managed
```

## Postgres invalidity

**Status: resolved.** Confirmed the root cause against the images: postgres:16
mounts its data volume at `/var/lib/postgresql/data`; postgres:18 moved it to
`/var/lib/postgresql` (PGDATA → `/var/lib/postgresql/18/docker`). A free-form
`image` paired with a hard-coded mount path silently breaks persistence.

**Inventory:** only providers whose internals are coupled to the technology's
major version qualify for versioned definitions — `postgres` and
`mysql`/`mariadb` (data mount / datadir). The others (redpanda, s3/minio,
nessie, openlineage, debezium, s3sink, proxy) have no version-coupled
internals and remain single-profile (image only).

**Mechanism** (`internal/domain/versionprofile`, the Helm/Terraform
discipline): each versioned provider ships an immutable `Catalog` mapping a
version identifier to a pinned `Profile` (image **and** its data mount,
travelling together). The manifest references `configuration.version`, not a
raw image:

```yaml
configuration: { version: "18" }   # image + mount pinned & tested together
```

- `version` defaults to a current release when omitted.
- An `image` override is allowed *only* with a `version` (a private mirror of
  that version), so internals always come from a validated profile.
- Unknown version, or `image` without `version`, fails at `validate`
  (`reconciler.VersionedProvider` checked in `application/compatibility`).

Verified: postgres:18 applies and persists at the version-correct mount; all
examples/testdata switched from `image: postgres:16` to `version: "16"`.
Tests: `versionprofile_test.go`, `TestVersionedProviderValidation`.

## Graph does not render the logical/expected architecture

**Status: resolved.** Two problems, both fixed:

1. **`-o` was ignored** — `graph` had its own `--format dot|mermaid` flag, so
   the documented `platformctl graph -o dot|mermaid` (spec §5) silently did
   nothing (the inherited `-o` stayed `table`). `graph` now honours the
   persistent `-o`: `tree` (default), `dot`, `mermaid`, `json`.

2. **Raw reverse-dependency edges didn't read as the architecture** — the old
   output was the reconcile DAG (X depends on Y), so a Binding rendered as a
   hub pointing at three nodes. The new `internal/application/archview`
   derives the *architecture*: Bindings collapse into labelled data-flow
   edges (`Source ──[cdc · provider]──▶ EventStream`), Providers connect to
   the assets they realize, Connections show the external system they
   forward to, and observers show lineage. The default `tree` view groups it
   into DATA FLOW / TECHNOLOGY LAYER / EXTERNAL ACCESS / STANDALONE PROVIDERS
   — the picture you actually configure orchestrators against. Example:

   ```
   DATA FLOW
     Source/student-database ──[cdc · postgres-cdc]──▶ EventStream/attendance-events   (lineage → test-lineage-fake)
     EventStream/attendance-events ──[sink · s3-sink]──▶ Dataset/attendance-raw
   TECHNOLOGY LAYER  (provider ─realizes→ asset)
     Provider/local-postgres  (type: postgres)
       └─ Source/student-database  (engine: postgres)
     ...
   ```

Enforced by `internal/application/archview/archview_test.go` (pipeline
direction, realization edges, all four render formats, external targets).

## Docs generation does not output clean HTML

**Status: resolved.** `docs serve` previously sent raw markdown with a
`text/markdown` content-type, so browsers showed plain text. It now serves a
proper, searchable HTML site:

- **goldmark** (the CommonMark/GFM parser Hugo is built on — the popular Go
  choice) renders the schema-generated markdown to HTML, including tables.
- `internal/application/docsgen.Site()` assembles a **single self-contained
  page**: a sticky sidebar listing every Kind, the rendered content, and a
  **client-side full-text search box** that filters sections, shows a match
  count, and highlights hits (`<mark>`). Light/dark aware, responsive, and
  fully offline — no external assets, no CDN.
- `platformctl docs serve` serves it (`text/html`); `platformctl docs build
  --html` writes the same as a portable `index.html`. Plain-markdown
  `docs build` (used to keep `docs/reference/` in the repo) is unchanged.

Enforced by `internal/application/docsgen/site_test.go` (all Kinds present,
tables rendered, search UI present, no leaked markdown).

## CI Fails on Kubernetes

```text
Run go test -tags integration -timeout 3600s ./...
ok  	github.com/rezarajan/platformctl/cmd/platformctl	277.336s
?   	github.com/rezarajan/platformctl/internal/adapters/kafkaconnect	[no test files]
?   	github.com/rezarajan/platformctl/internal/adapters/providers/debezium	[no test files]
?   	github.com/rezarajan/platformctl/internal/adapters/providers/mysql	[no test files]
?   	github.com/rezarajan/platformctl/internal/adapters/providers/nessie	[no test files]
?   	github.com/rezarajan/platformctl/internal/adapters/providers/noop	[no test files]
?   	github.com/rezarajan/platformctl/internal/adapters/providers/openlineage	[no test files]
?   	github.com/rezarajan/platformctl/internal/adapters/providers/placeholder	[no test files]
?   	github.com/rezarajan/platformctl/internal/adapters/providers/postgres	[no test files]
?   	github.com/rezarajan/platformctl/internal/adapters/providers/proxy	[no test files]
?   	github.com/rezarajan/platformctl/internal/adapters/providers/redpanda	[no test files]
?   	github.com/rezarajan/platformctl/internal/adapters/providers/s3	[no test files]
?   	github.com/rezarajan/platformctl/internal/adapters/providers/s3sink	[no test files]
ok  	github.com/rezarajan/platformctl/internal/adapters/runtime/docker	3.012s
ok  	github.com/rezarajan/platformctl/internal/adapters/runtime/fake	0.004s
--- FAIL: TestConformance (0.00s)
    kubernetes_integration_test.go:14: connect to kubernetes: load kubeconfig: invalid configuration: no configuration has been provided, try setting KUBERNETES_MASTER environment variable
FAIL
FAIL	github.com/rezarajan/platformctl/internal/adapters/runtime/kubernetes	0.016s
?   	github.com/rezarajan/platformctl/internal/adapters/secrets/env	[no test files]
?   	github.com/rezarajan/platformctl/internal/adapters/secrets/file	[no test files]
ok  	github.com/rezarajan/platformctl/internal/adapters/secrets/router	0.003s
ok  	github.com/rezarajan/platformctl/internal/adapters/secrets/vault	1.642s
ok  	github.com/rezarajan/platformctl/internal/adapters/state/localfile	0.033s
ok  	github.com/rezarajan/platformctl/internal/application/archview	0.004s
ok  	github.com/rezarajan/platformctl/internal/application/compatibility	0.006s
ok  	github.com/rezarajan/platformctl/internal/application/docsgen	0.006s
ok  	github.com/rezarajan/platformctl/internal/application/engine	0.900s
?   	github.com/rezarajan/platformctl/internal/application/featuregate	[no test files]
ok  	github.com/rezarajan/platformctl/internal/application/manifest	0.019s
ok  	github.com/rezarajan/platformctl/internal/application/plan	0.003s
?   	github.com/rezarajan/platformctl/internal/application/registry	[no test files]
ok  	github.com/rezarajan/platformctl/internal/cliutil	0.002s
?   	github.com/rezarajan/platformctl/internal/domain/binding	[no test files]
?   	github.com/rezarajan/platformctl/internal/domain/catalog	[no test files]
?   	github.com/rezarajan/platformctl/internal/domain/connection	[no test files]
?   	github.com/rezarajan/platformctl/internal/domain/dataset	[no test files]
ok  	github.com/rezarajan/platformctl/internal/domain/endpoint	0.002s
?   	github.com/rezarajan/platformctl/internal/domain/eventstream	[no test files]
ok  	github.com/rezarajan/platformctl/internal/domain/graph	0.002s
ok  	github.com/rezarajan/platformctl/internal/domain/hostport	0.002s
?   	github.com/rezarajan/platformctl/internal/domain/lineage	[no test files]
?   	github.com/rezarajan/platformctl/internal/domain/provider	[no test files]
ok  	github.com/rezarajan/platformctl/internal/domain/resource	0.003s
?   	github.com/rezarajan/platformctl/internal/domain/secret	[no test files]
?   	github.com/rezarajan/platformctl/internal/domain/source	[no test files]
?   	github.com/rezarajan/platformctl/internal/domain/status	[no test files]
ok  	github.com/rezarajan/platformctl/internal/domain/versionprofile	0.002s
?   	github.com/rezarajan/platformctl/internal/ports/clock	[no test files]
?   	github.com/rezarajan/platformctl/internal/ports/reconciler	[no test files]
?   	github.com/rezarajan/platformctl/internal/ports/runtime	[no test files]
?   	github.com/rezarajan/platformctl/internal/ports/runtime/conformance	[no test files]
?   	github.com/rezarajan/platformctl/internal/ports/secretstore	[no test files]
ok  	github.com/rezarajan/platformctl/internal/ports/state	0.003s
?   	github.com/rezarajan/platformctl/internal/ports/state/conformance	[no test files]
?   	github.com/rezarajan/platformctl/schemas	[no test files]
FAIL
Error: Process completed with exit code 1.
```

**Resolution (2026-07-16):** the Kubernetes conformance test now skips
self-descriptively when no reachable cluster is configured — the same
policy docs/planning/07 §3.2 prescribes for `TestProbeTCPReachable` in
restricted runners: an environment limitation must not read as a code
failure. `New(nil)` failing or a `Discovery().ServerVersion()` probe
failing → `t.Skipf` with instructions; runners that are *supposed* to
provide a cluster set `PLATFORMCTL_REQUIRE_K8S=1` to turn the skip back
into a failure. Verified both ways: skips under `KUBECONFIG=/nonexistent`,
runs the full suite (56s) against a live minikube.
