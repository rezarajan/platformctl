# Datascape — Resource Model Reference

## 1. Versioning conventions

- `apiVersion` follows Kubernetes-style maturity staging: `datascape.io/v1alpha1` →
  `datascape.io/v1beta1` → `datascape.io/v1`.
- **Alpha** (`v1alpha1`): may change shape between minor releases without a deprecation period.
  Gated by a feature gate defaulting to disabled outside of explicit opt-in.
- **Beta** (`v1beta1`): shape is stable; behavior may still change. Enabled by default but
  overridable.
- **GA** (`v1`): shape and behavior are stable; changes follow a deprecation window.
- A Kind may exist at multiple `apiVersion`s simultaneously during a graduation window; the CLI
  accepts both and warns on the older one.

## 2. Common envelope

Every manifest shares this shape:

```yaml
apiVersion: datascape.io/v1alpha1
kind: <Kind>
metadata:
  name: <string, required, unique per namespace/Kind; DNS-label pattern, max 63 chars>
  namespace: default   # optional, DNS-label; defaults to "default". Part of every
                       # resource's identity (resource.Key = Namespace/Kind/Name);
                       # nameRef fields accept an optional namespace for
                       # cross-namespace references.
  labels: {}       # optional, free-form
  annotations: {}  # optional, free-form
  observers:       # optional, any data-plane Kind may declare this
    - name: local-marquez   # a Provider this resource's own provider may forward a LineageEndpoint to
  protect: false   # optional, default false — see below
spec:
  ...              # kind-specific, see below
status:            # populated by Datascape, never hand-authored
  conditions: []
  observedGeneration: 0
  providerState: {}
```

`observers` is a list of `Provider` names, resolved to `LineageEndpoint`s at reconcile time. It
does not change what the resource *does* — it only optionally hands its provider a connection
fact, if that provider knows what to do with one. See §9.

`metadata.protect: true` refuses any `plan`/`apply`/`destroy` action that would delete this
resource, regardless of lifecycle (Managed, External, or Imported) or of `destroy`'s
`--include-external`/`--include-imported` flags. `plan` reports the would-be delete as its own
`refused` action instead of `delete`; `apply`/`destroy` fail the run, naming the resource and the
remedy (remove `metadata.protect`, or set it to `false`, and re-apply before the resource can be
deleted). There is no separate opt-out flag — the only way to delete a protected resource is to
first apply a manifest for it with `protect` removed. This applies engine-wide, not per-provider;
data-bearing kinds (Dataset, Source, Catalog) are the primary intended use, but the field is
available on every Kind.

## 3. Lifecycle taxonomy — how it's expressed per kind

| Lifecycle | How it's declared | Behavior |
|---|---|---|
| **Managed** (default) | No `external`/`import` marker present. | Datascape creates it, updates it on spec change, deletes it on `destroy` (no extra flag required). |
| **External** | `spec.external: true` (+ a `connectionRef`/equivalent describing how to reach it) | Datascape never creates or deletes it. It may still be *configured* (e.g., a CDC binding registers a connector against an externally-running Kafka Connect) if the provider defines a configure-only path. `destroy` never touches it without `--include-external` **and** the resource-specific destructive-action flag. |
| **Imported** | Not declared in the manifest directly — produced by `platformctl import <kind>/<name> --from ...`, which writes `status.imported: true` into state. | Behaves like Managed for update/reconcile purposes going forward, but its initial creation is never re-attempted; the first reconcile after import is a Probe + reconcile-in-place, not a create. |

### 3.1 Imported vs External — which one do I want?

The two non-Managed lifecycles answer different questions and are easy to
conflate. The test: **who should own the resource going forward?**

| | **Imported** | **External** |
|---|---|---|
| The resource was created… | out-of-band, but it *should* be platform-owned | and is operated by someone else, permanently |
| Declared how | normal manifest + one-time `platformctl import <Kind>/<name> --from <name>` | `spec.external: true` + `connectionRef` in the manifest itself |
| Reconcile | probe on adoption; updates/heals like Managed afterwards | never creates or mutates the real system; verifies its `Connection` resolves |
| Drift | full probe/heal like Managed | observed (`drift` reports reachability), never healed by mutation |
| Destroy | skipped unless `--include-imported` | refused without `--include-external` **and** `--yes-i-understand-this-is-destructive`; even then only *forgotten from state* when nothing realizes it |
| Typical example | a Postgres you `docker run` last month and now want platformctl to manage | the production database another team operates |

### 3.2 How External resources integrate into an active deployment

An External resource is not a dead entry — the platform actively *configures
against* it. The moving parts:

1. The external resource (say a `Source`) declares `connectionRef`, which
   resolves to a **`Connection`** (§8.2) — address and port here, credentials
   in the `SecretReference` the Connection's `secretRef` names. (A
   `connectionRef` may also point straight at a `SecretReference`; that is
   the v1.0.0 shorthand, still supported.)
2. A managed `Connection` gives the external system a **stable
   platform-owned entrypoint**: a forwarder on the shared network (and the
   host) whose `target` is the one place that knows where the system
   actually lives. When the external endpoint moves, one manifest line
   changes; every consumer keeps its address.
3. Providers that *do work against* the external system consume the
   Connection automatically — e.g. a `Binding(mode: cdc)` on an external
   `Source` registers its Debezium connector at the Connection's endpoint
   with the Connection's credentials. The provider carrying the work must
   list the Connection's `secretRef` in its own `spec.secretRefs` (secrets
   only ever flow through the engine's SecretStore resolution).
4. **Health means reachable, not merely configured.** An external
   resource whose `connectionRef` names a `Connection` with an address is
   *reachability-probed*: the engine opens a TCP connection to the
   Connection's host-reachable address (`DialAddress` — the published
   forwarder port for a managed Connection, `host:port` for an external
   one). A live endpoint that holds the connection reports
   `Ready=True, ExternalEndpointReachable`; a forwarder whose upstream is
   down closes the probe immediately and reports
   `Ready=False, Drift=True, ExternalEndpointUnreachable`. An external
   resource can never claim health while the system behind it is
   unreachable, and its dependent Bindings are blocked rather than left to
   fail slowly. (The bare-`SecretReference` shorthand has no address, so it
   can only report `ExternalConnectionResolvable`.)
5. `drift` takes a single fast snapshot; `apply` retries the reachability
   probe for up to 30s (absorbing startup races) and heals the *managed*
   pieces (the forwarder, the connector) but never mutates the external
   system itself.

### 3.3 External-lifecycle support, audited kind by kind

`spec.external: true` is only schema-legal on the five kinds below — every
other kind's schema sets `additionalProperties: false` on `spec` without an
`external` property, so declaring it on `EventStream` or `Binding` fails at
schema validation, not silently. Within the five, the engine takes one of two
paths, chosen solely by whether `providerRef` is also set (`isExternalNoProvider`,
`internal/application/engine/engine.go`):

| Kind | `external` schema-legal? | With `providerRef` | Without `providerRef` |
|---|---|---|---|
| `Provider` | yes | N/A — a Provider has no `providerRef` field (it cannot reference itself); always takes the no-provider path | connection-resolvable-only: `connectionRef` reachability verified, nothing created |
| `Source` | yes | requires the resolved Provider to implement `ExternalConfigurer`; refused at **validate** time otherwise (`compatibility.Check`, not merely at apply) | connection-resolvable-only via `connectionRef` (Connection or SecretReference) |
| `Dataset` | yes | same — validate-time `ExternalConfigurer` requirement | connection-resolvable-only |
| `Catalog` | yes | same — validate-time `ExternalConfigurer` requirement | connection-resolvable-only |
| `Connection` | yes | same — validate-time `ExternalConfigurer` requirement | plain address record (`host`/`port`); nothing created, nothing to reach through a forwarder |
| `EventStream` | no | schema-rejected | schema-rejected |
| `Binding` | no | schema-rejected | schema-rejected |

As of this writing **no shipped provider** (redpanda, postgres, mysql/mariadb,
debezium, s3/minio, s3sink, nessie, openlineage, proxy) implements
`ExternalConfigurer`. That means every `external: true` + `providerRef`
combination above is refused today — this is a documented, validate-time
capability gap (the same shape as an unsupported CDC engine or sink format),
not an unaudited or silently-broken path. A future provider that implements
`ExternalConfigurer` (e.g. registering a connector against an
already-running, externally-operated Kafka Connect) makes that combination
work with no core-model change. See
`internal/application/compatibility/compatibility_test.go`'s
`TestExternalProviderRefRequiresConfigurerPerKind` for the per-kind negative
coverage, and `docs/planning/07-production-grade-docker-runtime-gap-analysis.md`
§0.3 for the original open item this closes.

## 4. Kind: `Provider`

Declares a technology (`type`) and where it runs (`runtime`). This is the resource that replaces
`DatabaseClass`/`DatabaseInstance`/`ConnectorClass`/`CDCClass`/`CDCInstance` from the
experimental phase.

```yaml
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: local-redpanda
spec:
  type: redpanda                 # redpanda | postgres | mysql | mariadb | debezium | s3 | minio | s3sink | nessie | openlineage | proxy
  runtime:
    type: docker                 # docker | kubernetes (Beta, KubernetesRuntime gate, enabled by default) | external (future) | terraform (future)
    network: datascape           # docker: the shared network name. kubernetes: the Namespace name (EnsureNetwork creates it).
    networkPolicy: ""            # kubernetes only (docs/planning/08 B7); "" (default) provisions a default-deny +
                                  # allow-same-namespace NetworkPolicy pair so the Namespace isn't DNS-parity-only —
                                  # without it any pod anywhere in the cluster could reach it. "none" opts out (prints
                                  # a stderr warning); docker ignores this entirely, a Docker network is always isolated.
                                  # Exception (docs/history/errors.md, 2026-07-20): a container using access node-port/load-balancer
                                  # additionally gets a per-container `datascape-allow-external-<name>` NetworkPolicy that
                                  # opens this wall to exactly its declared ports. External traffic arrives SNAT'd to a
                                  # non-pod source that allow-same-namespace never matches, so without the hole the very
                                  # node-port/load-balancer traffic those modes exist to admit would time out. The hole
                                  # is created only when the wall exists (never restricts a networkPolicy:none namespace).
    access: ""                   # kubernetes only (docs/planning/08 B1); "" (default) is port-forward | node-port |
                                  # load-balancer | in-cluster. Selects how platformctl itself (running outside the
                                  # cluster) reaches this Provider's admin/control-plane port to reconcile child
                                  # resources (e.g. redpanda's EventStream needs a live Kafka admin connection).
                                  # port-forward opens an ephemeral client-go tunnel per operation (needs
                                  # pods/portforward RBAC). node-port/load-balancer change the Service type and use
                                  # its externally-observed address (also what `platformctl inventory` reports).
                                  # in-cluster refuses CLI-side admin connections outright, naming the mode. docker
                                  # ignores this entirely — a published host port is already reachable.
  configuration:                 # provider-specific, schema keyed by `type`
    image: docker.redpanda.com/redpandadata/redpanda:v24.2.1
  secretRefs: []                 # optional list of SecretReference names, resolved and passed to the provider
```

Additional examples for the v1.0.0 provider set:

```yaml
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
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: postgres-cdc
spec:
  type: debezium
  runtime: {type: docker, network: datascape}
  configuration: {image: debezium/connect:2.7}
---
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: local-minio
spec:
  type: minio
  runtime: {type: docker, network: datascape}
  configuration: {image: minio/minio:RELEASE.2026-06-01}
---
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: s3-sink
spec:
  type: s3sink                       # Kafka-Connect-based S3 sink connector
  runtime: {type: docker, network: datascape}
  configuration: {image: debezium/connect:2.7}
```

Volume-creating providers (postgres, mysql/mariadb, redpanda, s3/minio, openlineage) accept an
optional `configuration.storage` stanza (docs/planning/08 B3) sizing and classing their managed
volume — currently wired end-to-end for `postgres` as the reference implementation; the same
2-line pattern (`storage()` resolving `configuration.storage` into `runtime.VolumeSpec.SizeBytes`/
`.StorageClass` via `internal/domain/storagesize`) is a mechanical follow-up for the rest:

```yaml
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: local-postgres
spec:
  type: postgres
  runtime: {type: kubernetes, network: datascape}
  configuration:
    version: "16"
    superuserSecretRef: postgres-admin
    storage:
      size: 50Gi        # Ki|Mi|Gi|Ti (binary) or K|M|G|T (decimal) suffix, or a bare byte count.
                         # Docker ignores this (volumes are unsized). Kubernetes sets it as the
                         # PersistentVolumeClaim's storage request; omitted defaults to 10Gi.
                         # Increasing an existing volume's size live-expands the PVC (only when
                         # the StorageClass allows it); decreasing is refused — Kubernetes does
                         # not support shrinking a bound PVC.
      class: fast-ssd    # Kubernetes StorageClass name; omitted uses the cluster default.
                         # Immutable once the volume is first created, like the PVC field itself.
  secretRefs: [postgres-admin]
```

A `redpanda` Provider additionally accepts `configuration.brokers` (integer ≥ 1, docs/adr/017): declaring
it opts the broker into the multi-broker, stable-identity ordinal shape (`ContainerSpec.Replicas` +
`StableIdentity: true` — brokers `<name>-0..<name>-(N-1)`, per-ordinal volumes, seed list from ordinal
hostnames). `brokers > 1` requires the `HighAvailability` gate, enforced at validate. Declaring `brokers`
cannot be combined with host-port pins (`kafkaPort`/`adminPort`/`schemaRegistryPort` — each broker's host
port is auto-assigned) or `schemaRegistry: enabled`. Omitting `brokers` keeps the pre-C2 single-container
shape byte-for-byte; enabling it on an existing single-broker deployment is a shape transition and
requires destroy-and-recreate. Scaling `brokers` up applies in place; scaling down is refused
(data-loss risk — docs/adr/017 §a.5):

```yaml
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: kafka-cluster
spec:
  type: redpanda
  runtime: {type: docker, network: datascape}
  configuration:
    brokers: 3          # requires --feature-gates=HighAvailability=true
```

Optional, not required for v1.0.0:

```yaml
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: local-marquez
spec:
  type: openlineage                  # e.g. stands up Marquez; NOT a required v1.0.0 deliverable
  runtime: {type: docker, network: datascape}
  configuration: {image: marquezproject/marquez:0.51.0}
```

Also optional, not required for v1.0.0 (docs/planning/08 C9, gate `MonitoringStackProvider`, Alpha/disabled):

```yaml
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: local-prometheus
spec:
  type: prometheus
  runtime: {type: docker, network: datascape}
  configuration:
    image: prom/prometheus:v2.55.1
    scrapeInterval: 15s               # optional; default "15s"
```

Field notes:
- `spec.type` selects which `Provider` (reconciler) implementation and JSON Schema for
  `spec.configuration` apply.
- **Versioned providers** (those whose internals are coupled to the technology's major
  version — `postgres`, `mysql`/`mariadb`) require `configuration.version` rather than a raw
  `image`. Each supported version is an immutable, tested profile that pins the image *and* its
  version-specific internals together (e.g. postgres:16 stores data at `/var/lib/postgresql/data`,
  postgres:18 at `/var/lib/postgresql`). `version` defaults to a current release when omitted; an
  `image` override is permitted only alongside a `version` (a private mirror of that version) so an
  image can never run with a mismatched data mount. An unknown version, or an `image` with no
  `version`, fails at `validate`. Providers without version-coupled internals stay single-profile
  (`image` only).
- **Host ports are optional.** Omit a provider's host port and platformctl auto-allocates a stable
  one, derived deterministically from the component's (unique) name — different components never
  collide, the same component gets the same port on every reconcile, and no one hand-picks a port
  to clash. The **in-network address** (`<container>:<fixed-port>`) is the stable access identifier;
  the host port is a convenience surfaced by `platformctl inventory`. Pin a port explicitly only
  when an external tool needs a fixed one. The Docker runtime publishes the port; another runtime
  (Kubernetes) would realise the same intent as a Service — the provider states the desire, the
  runtime materialises it.
- `spec.runtime.type` selects which `ContainerRuntime` (or future non-container runtime port) is
  constructed and injected. `kubernetes` is a real (Beta, `KubernetesRuntime` gate, enabled by
  default as of docs/planning/08 Stage B close, `internal/adapters/runtime/kubernetes`) second
  adapter — see docs/planning/07-production-grade-docker-runtime-gap-analysis.md's "Cross-Runtime
  Portability" section for its mapping decisions, `spec.runtime.access` for how CLI-side admin
  calls reach it from outside the cluster, and `deploy/kubernetes/rbac/README.md` for the minimal
  RBAC posture.
- `spec.runtime` fields beyond `type` are runtime-specific and validated by the runtime adapter's
  own schema fragment.
- **`redpanda`'s built-in schema registry** (docs/planning/08 D1, gate `SchemaRegistrySupport`,
  Alpha/disabled): `configuration.schemaRegistry: enabled | disabled` (default disabled) turns on
  Redpanda's Confluent-compatible registry (pandaproxy's sibling listener, port 8081) alongside the
  broker; `configuration.schemaRegistryPort` optionally pins its host-side port (0/omitted =
  auto-allocated, same convention as every other host port). The endpoint is published as
  `providerState.endpoints["schema-registry"]` (an observed `Host` binding plus the deterministic
  `Internal` address other containers on the shared network dial) and surfaced by `platformctl
  inventory` like every other endpoint. See §7.3 for how a `Binding`'s `spec.options.format`
  consumes it.
- **`prometheus` monitoring provider** (docs/planning/08 C9, gate `MonitoringStackProvider`,
  Alpha/disabled): reconciles a managed Prometheus container whose scrape config is *generated*
  from every other Provider's published `"metrics"`-named endpoint fact in state (never
  hand-authored, never constructed by the provider itself — ADR 015). `configuration.scrapeInterval`
  optionally overrides the default `"15s"`. Metrics endpoint facts ship for `redpanda`
  (`/public_metrics` on its admin API port) and `s3`/`minio` (`/minio/v2/metrics/cluster` on its API
  port) — zero extra containers. Ready requires `/-/ready` to answer *and* every configured target to
  appear in Prometheus's own `/api/v1/targets` (`activeTargets` count matches the configured target
  count); per-target up-ness is Prometheus's own concern, not part of this Ready gate. `platformctl
  inventory --for prometheus` renders the equivalent scrape config for a bring-your-own Prometheus,
  from the same facts. **Deferred** (explicit, not silently missing): postgres/mysql sidecar exporter
  containers (no native metrics endpoint to publish yet), a standalone `grafana` provider, and live
  Kubernetes-runtime verification.

## 5. Kind: `Source`

Represents a data origin. The `spec.engine` discriminator pairs with an engine-named nested
block, so a provider introducing a new engine can bring its own fields without any change to the
core `Source` schema:

```yaml
apiVersion: datascape.io/v1alpha1
kind: Source
metadata:
  name: student-database
spec:
  engine: postgres                 # required discriminator, open-ended (not a closed enum)
  providerRef:
    name: local-postgres
  postgres:                        # engine-specific block, validated by a schema fragment
    database: studentdb
    schema: public
  # deletionPolicy: retain | delete — what `destroy` does to the database
  # itself. Default retain: destroying the platform's record of a source
  # never destroys the data unless explicitly opted into (delete drops the
  # database). Ignored for external sources, which are never touched.
```

A hypothetical MySQL source, to make the extensibility concrete:

```yaml
apiVersion: datascape.io/v1alpha1
kind: Source
metadata:
  name: legacy-orders-db
spec:
  engine: mysql
  providerRef:
    name: local-mysql
  mysql:                           # a different engine, a different block, no core schema change
    database: orders
    serverId: 184054
```

External example:

```yaml
apiVersion: datascape.io/v1alpha1
kind: Source
metadata:
  name: student-database
spec:
  engine: postgres
  external: true
  connectionRef:
    name: production-student-db     # a Connection (§8.2) — or, shorthand, a SecretReference — never inline creds
```

### 5.1 HA posture (managed vs. external)

Managed `postgres`/`mysql` Sources are explicitly **single-node**, positioned
for dev, staging, and small production, hardened by backup/restore
(docs/planning/08 C6) and drift-heal rather than by in-place replication.
Production HA databases (Patroni, Galera, cloud RDS/Aurora) are not a
managed capability — they integrate as the `external: true` Source shown
above, through the Connection seam, with CDC already working against that
path unchanged (`internal/adapters/providers/debezium` resolves the
Source's `connectionRef`; see `examples/lakehouse/sources-and-datasets.yaml`'s
`orders` Source for the shipped example). See
`docs/adr/005-database-ha-posture.md` for the full decision and what
would change if a replication-capable managed mode is ever added.

## 6. Kind: `EventStream`

```yaml
apiVersion: datascape.io/v1alpha1
kind: EventStream
metadata:
  name: attendance-events
spec:
  providerRef:
    name: local-redpanda
  partitions: 6
  replication: 3          # optional (default 1, docs/adr/017 §a.7): topic replication factor. Must not
                           # exceed the realizing Provider's configuration.brokers — refused at validate
                           # (reconciler.StreamReplicationValidator). Kafka cannot change an existing
                           # topic's replication factor in place, so changing it is refused with a
                           # recreate-the-EventStream remedy; drift-probe reports an out-of-band factor
                           # mismatch as ReplicationFactorMismatch.
                           # redpanda additionally requires an ODD factor (Raft quorum: its brokers
                           # refuse "replication factor must be odd") — even values above 1 are
                           # refused at validate.
  retention:
    duration: 7d
  # keySchemaRef / valueSchemaRef: reserved for a future schema-registry integration, not in v1
```

## 7. Kind: `Binding`

Declares a relationship/data-movement contract, realized by a `Provider`. `spec.mode` determines
which Kinds `sourceRef`/`targetRef` may resolve to, and which provider capability is checked.

```yaml
apiVersion: datascape.io/v1alpha1
kind: Binding
metadata:
  name: student-db-to-events
  observers:
    - name: local-marquez            # optional; forwarded to the provider below if it's LineageAware
spec:
  mode: cdc                          # sourceRef -> Source, targetRef -> EventStream
  sourceRef:
    name: student-database
  targetRef:
    name: attendance-events
  providerRef:
    name: postgres-cdc               # a debezium-typed Provider; must declare SupportedSourceEngines() including "postgres"
  options:
    tables: ["students", "attendance"]
    snapshotMode: initial
```

A sink-mode `Binding`, carrying stream data into durable storage:

```yaml
apiVersion: datascape.io/v1alpha1
kind: Binding
metadata:
  name: attendance-events-to-lake
spec:
  mode: sink                         # sourceRef -> EventStream, targetRef -> Dataset
  sourceRef:
    name: attendance-events
  targetRef:
    name: attendance-raw
  providerRef:
    name: s3-sink                    # an s3sink-typed Provider; must declare SupportedSinkFormats() including "parquet"
  options:
    format: parquet
```

### 7.1 Mode → Kind pairing (structural rule, enforced regardless of provider)

The pairing is a **relation, not a function**: a `mode` names the movement
mechanism, and several endpoint pairings can realize it. The asset kinds are
role-neutral — a `Source` (an engine-backed database) is a legitimate
*target* of a sink-mode Binding, and a `Dataset` (an object-store location)
a legitimate *origin* of an ingest-mode one. Direction lives in
`sourceRef`/`targetRef`, never in the noun. (Revised pre-v1.0.0: the
original one-pair-per-mode table would have made database-as-sink and
object-store-as-source breaking changes after GA; as a relation they are
additive provider work.)

| `mode` | `sourceRef` resolves to | `targetRef` resolves to | Status in v1.0.0 |
|---|---|---|---|
| `cdc` | `Source` | `EventStream` | Implemented |
| `sink` | `EventStream` | `Dataset` | Implemented |
| `sink` | `EventStream` | `Source` | Schema/pairing accepted; no shipped provider declares the capability yet |
| `ingest` | `Dataset` | `EventStream` | Schema/pairing accepted; no shipped provider declares the capability yet |
| `batch` | `Source` | `Dataset` | Reserved, not implemented |

### 7.2 Provider capability (checked per matched pairing, in addition to the structural rule above)

| `mode` / pairing | Capability interface | Declares | Checked against |
|---|---|---|---|
| `cdc` | `CDCCapableProvider` | `SupportedSourceEngines() []string` | `Source.spec.engine` |
| `sink` → `Dataset` | `SinkCapableProvider` | `SupportedSinkFormats() []string` | `Dataset.spec.format` |
| `sink` → `Source` | `DatabaseSinkCapableProvider` | `SupportedSinkEngines() []string` | `Source.spec.engine` (of the target) |
| `ingest` | `IngestCapableProvider` | `SupportedIngestFormats() []string` | `Dataset.spec.format` (of the origin) |

**Provider availability (v1.x):** `cdc` (debezium) and `sink` → `Dataset`
(s3sink) ship with real providers. **No shipped provider implements
`DatabaseSinkCapableProvider` or `IngestCapableProvider`** — a `sink` →
`Source` or `ingest` Binding validates structurally, then fails at
`validate` with the standard capability error naming the missing
capability. These pairings are model-complete seams for future providers
(e.g. a Debezium JDBC sink over the existing Connect-worker pattern), not
usable features today.

A `Binding` that fails either check is rejected at `validate`/`plan` time with a message naming
the `Binding`, the `Provider`, its type, and what it actually supports — never discovered only
once `apply` starts touching real infrastructure. Example:

```
error: Binding "student-db-to-events": Provider "postgres-cdc" (type: debezium)
does not support source engine "sqlite" (supported: postgres, mysql, mongodb)
```

### 7.3 `spec.options.format`/`converter` — schema-carrying serialization (docs/planning/08 D1)

Gate: `SchemaRegistrySupport` (Alpha, disabled by default).

Any `Binding` may declare, alongside its mode-specific options:

```yaml
spec:
  options:
    format: avro          # json (default) | avro | protobuf
    converter: ""          # optional: an explicit converter class override,
                           # advanced escape hatch — wins over the
                           # format-derived default for both key and value
                           # converters (e.g. a non-Confluent-compatible
                           # Avro/Protobuf converter implementation).
```

`json` (the default when `format` is unset) needs no schema registry — the
pre-D1 behavior (schemaless JSON converters) is unchanged. `avro` and
`protobuf` are schema-carrying: they require a Confluent-compatible schema
registry reachable from the realizing connector, resolved automatically
from the **EventStream endpoint's own realizing Provider** — never
user-typed for the managed case. Today the only registry-capable provider
is `redpanda`'s built-in one:

```yaml
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: kafka-cluster
spec:
  type: redpanda
  runtime: {type: docker, network: datascape}
  configuration:
    schemaRegistry: enabled    # enabled | disabled (default disabled) —
                               # enables/exposes the built-in Confluent-
                               # compatible registry (pandaproxy's sibling
                               # listener). Endpoint published as
                               # providerState.endpoints["schema-registry"]
                               # and surfaced by `platformctl inventory`.
    schemaRegistryPort: 0      # optional host-port pin; 0 = auto-allocate
                               # (deterministic, distinct from the Kafka
                               # host port)
```

A new capability interface backs the check:

| `mode` / pairing | Capability interface | Declares | Checked against |
|---|---|---|---|
| any, when `options.format` is schema-carrying | `SchemaRegistryCapableProvider` | `SupportedSchemaFormats(cfg provider.Provider) []string` | the EventStream endpoint's own realizing Provider (not necessarily the Binding's `providerRef`) |

Unlike the other capability methods, `SupportedSchemaFormats` takes the
resolved Provider's own config (mirroring `VersionedProvider.VersionCatalog`)
because the answer is configuration-dependent
(`configuration.schemaRegistry: enabled`), not a static fact of the provider
type. A `Binding` declaring `avro`/`protobuf` against a provider chain with
no registry endpoint fails at `validate` with the standard capability-error
shape, naming the EventStream's Provider — the resource whose configuration
actually decides registry availability:

```
error: Binding "student-db-to-events": Provider "kafka-cluster" (type: redpanda)
does not support format "avro" (supported: json)
```

Today only `debezium` (cdc mode) wires the resolved registry URL into real
connector config (Avro/Protobuf key/value converters,
`*.converter.schema.registry.url`); a sink-mode Binding may declare
`options.format` too (the compatibility check is mode-agnostic), but no
shipped sink provider consumes it yet (D2, Parquet sink format end-to-end,
is the follow-up task) — `Dataset.spec.format` remains the field governing
a sink Binding's *object-store output* format (json/parquet/csv/jsonl),
a separate concept from this section's stream-serialization format. The
illustrative sink example near the top of §7 shows `options: {format:
parquet}` predating this section; that shape is not read by any shipped
provider and predates the `json|avro|protobuf` enum introduced here — noted
for a future cleanup pass, not corrected in place (additive-only doc
policy).

**Worker-image requirement (avro/protobuf):** the schema-carrying
converters must be present in the Connect worker image. The stock Debezium
image ships only Apicurio converter jars; Redpanda's built-in registry
speaks the Confluent API, so the provider wires
`io.confluent.connect.avro.AvroConverter` — the Provider's
`configuration.image` must therefore include the Confluent Avro converter
plugin (reference build:
`cmd/platformctl/testdata/avro-connect-image/Dockerfile`, the same
stock-image-lacks-the-plugin pattern as s3sink's required image). A
`Binding` declaring `format: avro|protobuf` against a worker image without
the jars fails at connector registration with Connect's
"Class ... could not be found" error — this is an image-content property
platformctl cannot verify at validate time.

**Parquet sink Datasets (docs/planning/08 D2):** `Dataset.spec.format:
parquet` behind a sink-mode `Binding` is the one *Dataset* format with a
schema-registry requirement: the Aiven S3 connector's parquet writer needs
schema-carrying Connect records, which this platform produces via the
registry-backed Avro converters above. At validate, a parquet Dataset's
sink Binding is checked against the EventStream endpoint's realizing
Provider exactly like an explicit `options.format: avro` — a registry-less
chain fails with the same standard capability-error shape (`does not
support format "parquet" (supported: json)`), naming the EventStream's
Provider. At apply, the `s3sink` provider derives the stream serialization:
`spec.options.format` if declared, else `avro` when the Dataset is parquet,
else the schemaless JSON converters (json/jsonl/csv Datasets are unchanged
— no registry involved). The worker-image requirement above applies to the
sink worker too: the Aiven release tar bundles the parquet writer's jars
and `AvroData`, but not the `AvroConverter` class itself — the reference
builds (`cmd/platformctl/testdata/s3sink-image/Dockerfile`,
`examples/cdc-attendance/s3sink-image/Dockerfile`) add the version-pinned
Confluent Avro converter plugin alongside the S3 connector.

## 8. Kind: `Dataset`

```yaml
apiVersion: datascape.io/v1alpha1
kind: Dataset
metadata:
  name: attendance-raw
spec:
  providerRef:
    name: local-minio
  bucket: raw-events
  prefix: attendance/
  format: parquet
  # deletionPolicy: retain | delete — what `destroy` does to the stored
  # objects. Default retain: destroying the platform's record of a dataset
  # keeps every object; only an explicit delete wipes bucket/prefix.
  # (Instance teardown — destroying the object-store Provider — removes the
  # backing container and volume regardless.)
```

`Dataset` reconciliation is a required v1.0.0 deliverable: `platformctl apply` creates the
bucket/prefix via the `s3`/`minio` provider, and a `sink`-mode `Binding` populates it.

External example (a bucket operated elsewhere — schema aligned with §3.3's
contract on 2026-07-20; previously `dataset.json` accepted `external: true`
with no `connectionRef` property at all, so the connection-resolvable path
§3.3 documents was unreachable for Datasets):

```yaml
spec:
  external: true
  connectionRef:
    name: prod-lake            # a Connection (preferred) or SecretReference; required when external
  bucket: raw-events
  format: parquet
```

## 8.1 Kind: `Catalog`

A table/metadata catalog (Iceberg REST, Hive Metastore, Glue, ...) as a
provider-agnostic noun. Exactly like `Source`, `spec.engine` is an open
discriminator pairing with an engine-named nested block — Nessie is one
engine *behind* the Catalog abstraction, never a shape of its own. The
realizing provider must declare the engine in `SupportedCatalogEngines()`
(checked at `validate`, same mechanism and error shape as Binding
capability).

```yaml
apiVersion: datascape.io/v1alpha1
kind: Catalog
metadata:
  name: lakehouse-catalog
spec:
  engine: nessie                   # open-ended: nessie | hive | glue | ...
  providerRef:
    name: catalog-svc              # a catalog-capable Provider (type: nessie today)
  nessie:                          # engine-specific block, validated per engine
    defaultBranch: main
```

External example (a catalog operated elsewhere):

```yaml
spec:
  engine: glue
  external: true
  connectionRef:
    name: prod-glue                # a Connection; Datascape never creates/deletes the catalog
```

## 8.2 Kind: `Connection`

A first-class, non-secret description of **how to reach a system** —
address here, credentials in the `SecretReference` named by
`spec.secretRef`. This is the "Connection/SecretReference pair" §5's
external example always promised, promoted to a real kind. One shape, two
lifecycles:

```yaml
# Managed: a stable platform-owned entrypoint, realized by a
# connection-capable Provider (type: proxy today) as a forwarder listening
# on spec.port — on the shared network at <name>:<port> and on the host at
# 127.0.0.1:<port>. spec.target is the only place that knows where the
# system actually lives.
apiVersion: datascape.io/v1alpha1
kind: Connection
metadata:
  name: orders-db
spec:
  providerRef:
    name: edge                     # must declare "tcp" in SupportedConnectionSchemes()
  scheme: tcp                      # default
  port: 15999
  target: db.corp.internal:5432
  secretRef:
    name: orders-db-creds
---
# External: a plain address record; nothing is created for it.
apiVersion: datascape.io/v1alpha1
kind: Connection
metadata:
  name: prod-warehouse
spec:
  external: true
  host: warehouse.corp.internal
  port: 9000
  secretRef:
    name: warehouse-creds
```

Field notes:
- Consumers never address `spec.target` — they address the Connection
  (managed: its own name / `127.0.0.1`; external: `spec.host`). Moving the
  real system is a one-line manifest change.
- `connectionRef` fields elsewhere (`Source`, `Catalog`, ...) resolve to a
  `Connection` first, falling back to a bare `SecretReference` (the v1.0.0
  shorthand).
- Providers doing work against an external resource consume its Connection
  automatically (see §3.2); the Connection's `secretRef` must appear in the
  working provider's `spec.secretRefs` for the engine to resolve its values.
- Tunnel chaining for VPC reach (a Connection egressing through another
  provider) is deliberately deferred; the seam is the `Connection` kind
  itself — additive when a tunnel-typed provider lands.

### 8.2.1 HTTP routing (the `ingress` provider, docs/planning/08 C7, docs/adr/018)

A second `ConnectionCapableProvider` realization on the same `Connection`
shape, declaring `scheme: http` (the `proxy` provider above declares `tcp`;
a Connection picks whichever scheme its `providerRef` supports):

```yaml
apiVersion: datascape.io/v1alpha1
kind: Connection
metadata:
  name: nessie
spec:
  providerRef:
    name: edge-http                  # must declare "http" in SupportedConnectionSchemes()
  scheme: http
  port: 80                           # required by the base Connection schema; not separately used by ingress (routing is by Host header, not by port)
  target: nessie:19120               # host:port the entrypoint forwards to, passed through as-is
```

- Docker: one shared Caddy container per `Provider(type: ingress)`, routing
  `Host(<connection-name>.<domain>)` to `spec.target`. `domain` is
  `Provider.spec.configuration.domain` (default `"localhost"` — see
  docs/adr/018 Decision 4). Reachable at
  `http://<connection-name>.<domain>:<published-http-port>`, surfaced by
  `platformctl inventory`.
- Kubernetes: one `networking.k8s.io/v1 Ingress` object per Connection,
  routing the same `Host(...)` rule to `spec.target`'s host as an existing
  Service name (`spec.target`'s host segment must name a Service already in
  the same namespace — e.g. another Provider's own runtime object name) and
  its port. No shared container; the cluster's own ingress controller does
  the proxying.
- TLS is out of scope for this scheme: `SupportedConnectionSchemes()`
  returns only `"http"`, not `"https"` — a Connection declaring
  `scheme: https` fails the standard capability error until docs/planning/08
  C8 adds `Connection.spec.tls` to this same provider.

## 9. Lineage / observability schema

```yaml
metadata:
  observers:
    - name: local-marquez     # must resolve to a Provider; that Provider's connection details
                                # become a LineageEndpoint, forwarded only if this resource's own
                                # provider implements LineageAware
```

```yaml
# what gets forwarded — not a manifest field, this is the in-memory value
# passed from the engine to a LineageAware provider's ConfigureLineage call
LineageEndpoint:
  url: http://local-marquez:5000
  namespace: datascape          # optional
  authRef: null                 # optional SecretReference, if the backend needs a token
```

Important scoping notes:

- Datascape never constructs a lineage "fact," "job," "run," or "dataset" record. It resolves a
  connection endpoint and hands it to a provider; what that provider's underlying tool does with
  it is that tool's own, real integration.
- An `observers` entry pointing at a `Provider` whose owning resource's provider does *not*
  implement `LineageAware` is not an error — it's a no-op, surfaced as an informational status
  annotation only.
- In v1.0.0, `debezium` is the one provider that implements `LineageAware` (Debezium ships its
  own native OpenLineage integration; Datascape's job is limited to setting its
  `openlineage.integration.enabled` and endpoint configuration when registering the connector). A
  concrete `openlineage`-typed `Provider` (one that stands up something like Marquez) is optional
  in v1.0.0 — the schema accepts it, but shipping one is not required for v1.0.0 sign-off.

## 10. Kind: `SecretReference`

```yaml
apiVersion: datascape.io/v1alpha1
kind: SecretReference
metadata:
  name: postgres-replication-creds
spec:
  backend: env                     # env | file (both implemented) | vault (implemented, VaultSecretBackend gate, Alpha/disabled) | kubernetes (implemented, KubernetesSecretBackend gate, Beta/enabled)
  keys:
    - username
    - password
```

Resolution: `SecretStore.Resolve` returns a `map[string]string` keyed by the logical names in
`spec.keys`; how those map to actual storage (env var names, file paths) is backend-specific
configuration, never present in the manifest's `spec` as a plaintext value.

`backend: kubernetes` (docs/planning/08 B4) resolves `spec.keys` from a native Kubernetes Secret's
data keys — the ambient kubeconfig (`KUBECONFIG` env, then `~/.kube/config`, or in-cluster config)
resolves the cluster, the same rules the `kubernetes` runtime uses when a Provider's
`spec.runtime` doesn't override them. The Secret object defaults to `metadata.name` in
`metadata.namespace` (the Datascape namespace doubles as the Kubernetes namespace, matching the
runtime adapter's Provider convention) — both overridable via an optional `spec.kubernetes` block:

```yaml
apiVersion: datascape.io/v1alpha1
kind: SecretReference
metadata:
  name: postgres-replication-creds
spec:
  backend: kubernetes
  keys: [username, password]
  kubernetes:               # optional; both fields default as described above
    name: pg-repl-secret     # the Kubernetes Secret's own object name
    namespace: data-platform # the Kubernetes namespace it lives in
```

Apply records a one-way fingerprint of the resolved material in state, never
the values themselves. Drift/status compares the current resolved fingerprint
to the last applied one and reports `DriftDetected=True, reason=SecretChanged`
when an operator rotates a secret out-of-band. A later apply updates the
`SecretReference` baseline and, because secret references are dependency
edges, re-reconciles dependents that consume the changed secret. Each provider
is responsible for making that credential rotation real in its backing system;
the Docker MySQL/MariaDB and Postgres providers rotate their admin accounts by
authenticating with the previous managed-container bootstrap credentials and
then applying the new resolved SecretReference value.

Credential rotation has an intentional recovery boundary. Datascape does not
persist plaintext old secrets, so automatic rotation requires either the new
secret to already authenticate or the managed runtime to still expose the
previous bootstrap environment from the existing container. If a runtime loses
those environment values, an operator rewrites them to bad values, or the
database is manually changed to a third password, platformctl cannot safely
guess a credential and reconciliation fails with a manual-recovery message.
The available trade-offs are:

- Store plaintext or reversibly encrypted old secrets in state: rejected for
  the current contract because state may be checked in or shared.
- Use the managed Docker container environment as a transient fallback:
  implemented for MySQL/MariaDB and Postgres; no values are written to state.
- Add an operator-supplied previous-secret override or runtime exec rescue
  workflow: viable future work for break-glass recovery, but it needs explicit
  UX and audit semantics.
- Destroy/recreate with the data volume removed: works only when data loss is
  acceptable and should remain an explicit destructive action, not automatic
  reconciliation.

MinIO/S3 root credential rotation is not equivalent to SQL `ALTER USER`.
Changing `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD` changes the server process
bootstrap credentials after restart; platformctl can restart the managed
container with the new SecretReference, but if the store is unreachable because
credentials were manually corrupted, recovery is operator-owned.

External system credentials are not rotated by platformctl. For example, a
Debezium Binding that reads an external Postgres database through a managed
Connection uses the Connection's `secretRef` when registering the connector,
but the external database must already accept those credentials. Platformctl
preflights the database login through the Connection's host endpoint before
registering the connector and fails with a credential/reachability error if
the external system still has a different password.

## 11. Status & Conditions — common shape

```yaml
status:
  observedGeneration: 3
  conditions:
    - type: Ready
      status: "True"
      reason: HealthCheckPassed
      lastTransitionTime: "2026-07-13T10:15:00Z"
    - type: Progressing
      status: "False"
      reason: ReconcileComplete
      lastTransitionTime: "2026-07-13T10:15:00Z"
    - type: DriftDetected
      status: "False"
      reason: NoDrift
      lastTransitionTime: "2026-07-13T10:15:00Z"
    - type: Ready
      status: "True"
      reason: LineageEndpointDeclaredNotConsumed   # informational only, never blocks Ready
      message: "Provider type 'redpanda' does not implement LineageAware; observer 'local-marquez' was not forwarded."
  providerState:
    containerId: "a1b2c3..."       # opaque, provider/runtime-owned, not part of the public contract
```

`Ready=True` is the single condition a user should need to check for "did this work." The others
exist for diagnosis, not routine polling.

## 12. Deferred / retired kinds from the experimental phase

| Experimental-phase kind | Disposition | Notes |
|---|---|---|
| `RelationalSource` | Folded into `Source` (`spec.engine: postgres`) | See §5. |
| `ObjectStore` | Folded into `Provider` (`spec.type: s3\|minio`) + `Dataset` | Implemented in v1.0.0. |
| `DatabaseClass`, `DatabaseInstance` | Folded into `Provider` | See `00-README.md` rationale table. |
| `ConnectorClass`, `CDCClass`, `CDCInstance` | Folded into `Provider` (`spec.type: debezium`) + `Binding` | A CDC worker is a Provider that a Binding references; no separate class/instance split. |
| `StorageClass`, `PersistentVolume`, `PersistentVolumeClaim`, `VolumeMountBinding` | Deferred past v1 | Docker volumes are managed internally by the Docker runtime adapter via `ContainerSpec.Volumes`. Revisit if/when a second runtime needs a shared storage vocabulary. |
| `Warehouse`, `Table`, `Pipeline`, `LineageSink`, `AuditStore` | Out of scope, not modeled | Downstream of "infrastructure exists and is configured" — orchestration/transformation territory. |
| `ResourceDefinition`, `ProviderInstance`, `BindingDefinition`, generic `Binding` | Retained conceptually, narrowed | The typed `Binding` kind above replaces the generic one for v1 use cases; a generic extension mechanism for custom bindings is a candidate for a later phase alongside out-of-process provider plugins, not v1. |
