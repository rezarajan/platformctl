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
  domain: default  # optional, DNS-label; defaults to "default" — see below
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

`metadata.domain` (docs/adr/022-identity-aware-mediation.md, 08 H5) is an additive, optional
DNS-label field on every Kind, defaulting to `default` when omitted. It names the resource's
governance/segmentation domain: the policy vocabulary's `matchEdge.crossDomain: {from, to}`
selector (docs/adr/021, docs/domain/policy) evaluates over graph *edges* derived from it — a
`Binding`'s `sourceRef` domain → `targetRef` domain, and a `connectionRef` consumer's own domain →
the `Connection` it references — and denies at `validate` (Ring 0) when a rule matches, naming
both domains and the edge. Declaring two or more distinct domains on `Provider`-realized resources
additionally compiles Ring 1 network segmentation at apply time: Docker gets one network per
domain instead of the single shared network (`<network>-<domain>`, identity default omitted), and
Kubernetes inherits it for free since a network name already **is** the namespace name (docs/planning/08
B7); a managed `Connection` realizing an allowed cross-domain path joins exactly its own domain's
network plus each distinct consumer domain's network — nothing else. A manifest set that never
declares a non-`default` domain produces byte-identical runtime objects to before this field
existed — segmentation is opt-in per domain, never retroactive.

`metadata.annotations["lint.datascape.io/waive"]` (docs/adr/020-design-lints.md, 08 H1) waives one
or more `platformctl lint` findings against this resource: `"DL010: <reason>"`, one entry per line
for more than one code on the same resource (newline-separated, not comma — a reason is prose and
commonly contains commas). A reason is mandatory; an empty one does not suppress the finding it
names and is itself flagged (`DL000`). A waived finding still appears in `platformctl lint -o
json` with `waived: true` and the recorded reason — this is an auditability mechanism, not a
silencing one. Run `platformctl explain <code>` for any specific code's meaning and remedies.

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

A `debezium` or `s3sink` Provider additionally accepts `configuration.workers` (integer ≥ 1,
docs/planning/08 C3): declaring it fans the Connect worker out to N ordinals
(`ContainerSpec.Replicas` + `StableIdentity: false` — Connect is natively distributed
(`group.id` + internal topics) and holds no per-worker durable state, so unlike redpanda's
`brokers` no per-ordinal storage/hostname identity is needed; on Docker the ordinals join the
shared network under an additional network alias carrying the collective name, mirroring a
Kubernetes Deployment's ClusterIP round robin). `workers > 1` requires the `HighAvailability`
gate, enforced at validate (the same `checkHighAvailabilityGate` mechanism as `brokers`, naming
whichever field triggered it). Connector REST calls (register, status, restart, delete) try each
currently-reachable worker in turn (`internal/adapters/kafkaconnect`'s multi-address failover) —
killing one of several workers does not interrupt a Binding realized by this Provider; Probe
reports per-ordinal presence as drift (`ConnectWorkerMissing(<ordinals>)`) when the count doesn't
match. Omitting `workers` keeps the pre-C3 single-container shape byte-for-byte:

```yaml
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: postgres-cdc
spec:
  type: debezium
  runtime: {type: docker, network: datascape}
  configuration:
    image: debezium/connect:2.7
    workers: 2          # requires --feature-gates=HighAvailability=true
```

**Object-store production posture** (docs/planning/08 C4, 2026-07-21): the
recommendation is external for production (a real S3/GCS/R2-compatible
endpoint, verified reachable but never created/deleted by platformctl) and
distributed MinIO for self-hosted production. Two independent knobs, the
same `s3` provider `type`:

An `s3` Provider additionally accepts `configuration.nodes` (integer ≥ 1),
the same `ContainerSpec.Replicas` + `StableIdentity: true` ordinal-set
pattern `brokers` uses above: `nodes: 1` still opts into the ordinal-set
shape (a single node, `<name>-0`); `nodes: 4` or more starts a genuine
distributed, erasure-coded MinIO cluster (every node started with the full
peer URL list, unlike redpanda's seed-and-join protocol — no per-ordinal
script needed). `nodes: 2` or `nodes: 3` is refused at validate (no
supported MinIO topology exists between "1 node, no erasure coding" and "4+
nodes, erasure coding"). `nodes > 1` requires the `HighAvailability` gate,
enforced at validate exactly like `brokers > 1`. Declaring `nodes` cannot be
combined with a pinned `port` (auto-assigned per node, like every other
ordinal-set shape). Omitting `nodes` keeps the pre-C4 single-container shape
byte-for-byte. Scaling `nodes` up applies in place (a new erasure-coded
pool); scaling down is refused (data-loss risk, same reasoning as `brokers`):

```yaml
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: minio-cluster
spec:
  type: s3
  runtime: {type: docker, network: datascape}
  configuration:
    nodes: 4              # requires --feature-gates=HighAvailability=true
  secretRefs: [minio-admin]
```

An `s3` Provider may instead declare `external: true` (§3.3's Provider
row): Datascape verifies `connectionRef` reachability and creates nothing —
zero managed containers. A Dataset naming this Provider in its own
`providerRef` (§8's external-posture example) still reconciles normally
against the real endpoint (bucket creation, `spec.lifecycle`, `s3sink`
Bindings via `spec.options.endpoint`) — see §8 for the full contract:

```yaml
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: prod-s3
spec:
  type: s3
  runtime: {type: docker, network: datascape}   # still required by schema; nothing is created
  external: true
  connectionRef:
    name: prod-lake        # a Connection (preferred) or SecretReference; required when external
  secretRefs: [minio-admin] # must list the Connection's own secretRef, resolved and handed to this Provider
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

Also optional, not required for v1.0.0 (docs/planning/08 D10, gate `TrinoProvider`, Alpha/disabled;
design note docs/adr/006-compute-engines.md):

```yaml
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: lake-trino
spec:
  type: trino
  runtime: {type: docker, network: datascape}
  configuration:
    workers: 3                        # optional; default 1. workers > 1 requires
                                       # --feature-gates=HighAvailability=true
    catalogRef: {name: lakehouse-catalog}   # optional; a Catalog, graph-ordered before this Provider
    warehouseProviderRef: {name: lake-minio} # optional; disambiguates the warehouse-backing
                                              # S3/MinIO Provider when more than one exists —
                                              # inferred when the manifest declares exactly one
  secretRefs: [minio-creds]           # must include the warehouse Provider's own credential
                                       # SecretReference for the engine to resolve it
```

Also optional, not required for v1.0.0 (docs/planning/08 C9 completion, gate `MonitoringStackProvider`,
Alpha/disabled — the postgres/mysql exporter sidecar and the `grafana` provider deferred by C9's
original core slice):

```yaml
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: lake-postgres
spec:
  type: postgres
  runtime: {type: docker, network: datascape}
  configuration:
    version: "16"
    metrics: enabled                  # optional; default disabled. Adds a postgres_exporter
                                       # sidecar (Audience: internal-only) authenticated as a
                                       # dedicated least-privilege monitoring role this
                                       # provider creates itself — never the superuser
                                       # credential. Requires --feature-gates=MonitoringStackProvider=true
  secretRefs: [pg-admin]
---
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: lake-grafana
spec:
  type: grafana
  runtime: {type: docker, network: datascape}
  configuration:
    prometheusRef: {name: local-prometheus}  # optional; inferred when exactly one
                                              # prometheus Provider exists in the manifest
  secretRefs: [grafana-admin]           # required: admin username/password
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
- **`nessie`'s default warehouse location** (docs/planning/08 D10, found while wiring the `trino`
  provider against a real Nessie instance — not previously needed by anything in this codebase):
  `configuration.defaultWarehouseLocation` (an S3-shaped URI, e.g. `s3://bucket/prefix/`), when set,
  configures Nessie's Iceberg REST Catalog personality with a default warehouse
  (`NESSIE_CATALOG_DEFAULT_WAREHOUSE`/`NESSIE_CATALOG_WAREHOUSES_WAREHOUSE_LOCATION` env vars).
  Without it, Nessie's `/iceberg/v1/config` answers `500 "No default-warehouse configured"` for
  every request, which blocks any Iceberg REST client — Trino included — from initializing the
  catalog at all. Omitted (the pre-D10 default), Nessie's behavior is unchanged for every
  deployment that never pairs it with a compute engine.
- **`nessie`'s warehouse S3 credentials** (docs/planning/08 D10, the same finding as the bullet
  above): a configured warehouse location alone is not enough. `configuration.warehouseS3Endpoint`
  (e.g. `http://minio:9000`) and `configuration.warehouseS3SecretRef` (a `SecretReference` name,
  must also be listed in `spec.secretRefs`) give Nessie itself the S3 endpoint and credentials it
  needs to associate that location with an object store
  (`NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_*`/`NESSIE_CATALOG_SECRETS_WAREHOUSE_CREDS_*` env
  vars) — without them, creating a namespace or table under the warehouse fails with `"Missing
  access key and secret for STATIC authentication mode"` even though `/iceberg/v1/config` itself
  already answers correctly.
- **Automatic derivation from `Catalog.spec.warehouseRef`** (docs/planning/08
  D8, additive — the two bullets above's explicit
  `configuration.defaultWarehouseLocation`/`warehouseS3*` fields are unchanged
  and always win when set): when a `Catalog(engine: nessie)` declares
  `warehouseRef` (§8.1) and the `nessie` Provider sets no explicit
  `defaultWarehouseLocation`, the referenced `Dataset`'s `bucket`/`prefix`
  plus its realizing Provider's published `"s3"` endpoint fact and credential
  `SecretReference` name (which must also be listed in the `nessie`
  Provider's own `spec.secretRefs`, same convention as the explicit fields
  above) drive the identical env vars. Resolved by the engine's
  `resolveWarehouseFacts` (`reconciler.Request.WarehouseFacts`, Catalog-kind
  requests only — published-facts-only, ADR 015) and applied from the
  `Catalog`-kind reconcile step, not the `Provider`-kind one: nessie's own
  Provider reconcile necessarily runs *before* any `Catalog` referencing it,
  so the derived config can only take effect once the `Catalog` (and,
  transitively, the `Dataset` it names) have reconciled — `reconcileCatalog`
  re-`EnsureInstance`s the container with the corrected env in that case,
  relying on `EnsureContainer`'s existing spec-hash idempotency rather than
  new drift-tracking (a one-time recreate the first time `warehouseRef`'s
  facts are introduced or change; a no-op on every later reconcile with
  unchanged facts).
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
- **C9 completion — exporter sidecars and the `grafana` provider** (docs/planning/08 C9 completion,
  same `MonitoringStackProvider` gate — it gates the monitoring stack as a class, not a new gate per
  provider): `postgres`/`mysql`/`mariadb`'s `configuration.metrics: enabled | disabled` (default
  disabled) reconciles a second, independent `postgres_exporter`/`mysqld_exporter` container
  alongside the instance (docs/adr/004 — a sidecar, not a replica of the instance's own
  `ContainerSpec`, which stays byte-for-byte unchanged when `metrics` is unset). The exporter's port
  is `Audience: internal` — never published to the host, reachable only by other containers on the
  shared network (`prometheus`, most immediately) — and authenticates as a dedicated
  least-privilege monitoring role/user this provider creates and password-manages itself at
  reconcile (`pg_monitor` for postgres; `PROCESS, REPLICATION CLIENT, SELECT` for mysql/mariadb) —
  never the admin/root credential, never a user-declared `SecretReference`. Its own `"metrics"`
  endpoint fact is published exactly like redpanda's/s3's, so `prometheus` scrapes it with **zero**
  changes to the `prometheus` provider itself. The new `grafana` provider (nessie-shaped: one
  container, no dependent kind) is provisioned entirely via `ContainerSpec.Files` — Grafana's own
  file-based provisioning mechanism, not an API call this provider makes — with a Prometheus
  datasource resolved from a `prometheus` `Provider`'s own published `"prometheus"` endpoint fact
  (`configuration.prometheusRef`, optional — inferred when the manifest declares exactly one
  `prometheus` `Provider`, left unresolved on ambiguity — never constructed, ADR 015) and a minimal
  starter broker+database overview dashboard. `spec.secretRefs` (or `configuration.adminSecretRef`)
  is required for the admin username/password, mounted via `GF_SECURITY_ADMIN_USER` (env) +
  `GF_SECURITY_ADMIN_PASSWORD__FILE` (file — the password itself never touches env); anonymous
  access is always explicitly off. **Known limitation, recorded not solved**: Grafana only applies
  its admin-credential env vars the first time it creates the admin user in its own on-disk
  database — unlike postgres/mysql's rotation state machine, a `SecretReference` value changed
  after the first apply is not rotated into a live Grafana container.
- **`trino` compute-engine provider** (docs/planning/08 D10, gate `TrinoProvider`, Alpha/disabled;
  design note docs/adr/006-compute-engines.md): reconciles one coordinator container plus
  `configuration.workers` (integer ≥ 1, default 1) worker containers via the C1/C2 replica
  primitive (`ContainerSpec.Replicas`, `StableIdentity: false` — workers are pure compute, no
  per-replica storage or stable hostname beyond the ordinal name). `workers > 1` requires the
  `HighAvailability` gate, enforced at validate (`checkHighAvailabilityGate`'s field list extends
  to `configuration.workers` alongside redpanda's `configuration.brokers`). `configuration.catalogRef`
  (optional, a `nameRef` to a `Catalog`) is kind-checked and graph-ordered — the referenced `Catalog`
  reconciles before this `Provider` — via a new nested-ref extraction rule in
  `internal/domain/graph/graph.go` (`configRefFields`, scoped to `spec.configuration`, alongside the
  optional `configuration.warehouseProviderRef` disambiguator described below); a `catalogRef` naming
  a resource that is not a `Catalog` is rejected at validate with graph's standard
  "does not resolve to any resource" shape (the same structural kind-check `providerRef`/
  `connectionRef` already use, not a capability-interface check — there is no "can this provider do
  X" question here, only "does this name resolve to the right Kind"). When `catalogRef` is set, the
  provider writes `etc/catalog/lakehouse.properties` on every coordinator and worker node from the
  referenced `Catalog`'s published `"iceberg-rest"` endpoint fact and a resolved warehouse-backing
  S3/MinIO `Provider`'s published `"s3"` endpoint fact plus credentials (never constructed — ADR
  015): `configuration.warehouseProviderRef` names that `Provider` explicitly when a manifest
  declares more than one S3/MinIO `Provider`; omitted, the sole S3/MinIO-typed `Provider` in the
  manifest's namespace is inferred (0 or >1 candidates leave the catalog unconfigured until
  disambiguated — no silent guess). The warehouse `Provider`'s own credential `SecretReference` name
  is a graph/state fact, but its *values* only resolve when that same name also appears in this
  `Provider`'s own `spec.secretRefs` (the engine's one existing secret-resolution mechanism —
  mirrors `s3`'s `configuration.rootSecretRef` "must also be listed in spec.secretRefs" rule).
  The generated config always additionally sets `s3.region: us-east-1` (found live: Trino's S3
  filesystem factory falls back to the AWS SDK's default region-provider chain when unset, which
  takes minutes to exhaust — env var, profile, EC2 metadata, all absent in a container — before
  failing catalog init outright, indistinguishable from the outside from a hung coordinator; MinIO
  ignores the value). Nessie itself additionally needs `configuration.defaultWarehouseLocation` set
  (see the `nessie` bullet above) for its Iceberg REST personality to answer at all.
  Drift-checked: `Probe` regenerates the desired file from current facts and diffs it against the
  live file (read via `ContainerRuntime.ReadFile`) by key, the same bar as debezium/s3sink's
  connector-config drift and prometheus's scrape-config drift; a detected drift is healed on the
  next `Reconcile` by forcing the coordinator (and worker set) to recreate with corrected content —
  `ContainerRuntime` has no primitive to rewrite a file inside an already-running container, and
  Trino only reads catalog config at process start, so healing a file-mounted config requires a
  restart, unlike debezium/s3sink's REST-reconfigurable connectors. `Ready` requires the
  coordinator's `/v1/info` to answer 200 with `"starting": false` and the worker set's
  `ContainerState.ReadyReplicas` to equal `configuration.workers`. `platformctl inventory --for
  trino` renders the coordinator's live JDBC URL/UI address once a `trino` `Provider` exists in
  applied state, alongside the existing paste-ready snippet for the bring-your-own case.
  **Deviation from the D8-context language** ("`spec.nessie` already carries warehouse config
  today"): D8 (`Catalog.spec.warehouseRef`) is not implemented as of D10; this provider does not
  depend on it and does not add a `Catalog`-side warehouse field, to avoid taking on D8's scope —
  `configuration.warehouseProviderRef` above is D10's own, narrower mechanism, additive and
  non-conflicting with a future D8 (which could supersede it as the preferred path). **Deviation
  from the literal accept-list wording** "a query against a table written by the... D2 parquet
  scenario": D1/D2's `s3sink` writes plain partitioned Parquet files (the Aiven S3 sink connector's
  own layout), not an Iceberg table (no manifest/snapshot metadata is generated by any component in
  this codebase) — Trino's `iceberg` connector cannot query that data directly. D10's integration
  test instead proves the full stack (coordinator/worker reconciliation, catalog auto-configuration,
  Nessie + MinIO wiring) by having Trino itself `CREATE TABLE`/`INSERT` the same row content produced
  by the D2 scenario as a genuine Iceberg table through the coordinator, then `SELECT` it back —
  recorded here as a finding for a maintainer decision, not a silent scope change.

**Update (docs/planning/08 D8, 2026-07-21):** `Catalog.spec.warehouseRef`
(§8.1) has landed — the "D8-context language" deviation two paragraphs above
described the state before this task and is left as historical record
(additive doc policy). `resolveCatalogFacts`'s warehouse-backing S3/MinIO
`Provider` resolution now tries, in order: (1) the referenced `Catalog`'s own
`warehouseRef` chain (`Catalog` -> `Dataset` -> the `Dataset`'s realizing
`Provider`) when the `Catalog` declares it; (2) this `trino` `Provider`'s own
`configuration.warehouseProviderRef`, unchanged from D10; (3) the sole
S3/MinIO-typed `Provider` in the namespace, auto-inferred, unchanged from
D10. `warehouseProviderRef` is not removed and remains fully functional on
its own — it simply becomes redundant once the `Catalog` in question declares
`warehouseRef`.

### 4.1 `spec.configuration` schema fragments (docs/planning/08 E5)

Each shipped provider type ships a JSON-Schema fragment for its own
`spec.configuration` shape: `schemas/v1alpha1/fragments/provider/<type>.json`
(mysql/mariadb and s3/minio — one adapter, two provider types each — share
one file). `internal/application/manifest` composes the right fragment in
by `spec.type` during `Validate`, in addition to the core `provider.json`
schema (which stays open, `additionalProperties: true`, so a new provider
type never needs a core schema change). The fragment enforces shape only
(field types, enums, ranges, `additionalProperties: false` for typo
protection) — cross-field rules that need a value from elsewhere in the
spec (a `*SecretRef` that must also appear in `spec.secretRefs`,
`bootstrapServers`'s graph-inferred fallback) remain the provider's
`SpecValidator` Go code. Generated reference tables: `docs/reference/
provider.md`'s "Provider configuration reference" section
(`platformctl docs build`). noop/container (test-only) have no fragment.

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

### 5.2 `spec.<engine>` schema fragments (docs/planning/08 E5)

Each engine's nested block ships a JSON-Schema fragment:
`schemas/v1alpha1/fragments/source/<engine>.json` (`postgres`, `mysql`,
`mariadb` today). `manifest.Validate` composes it in by `spec.engine`.
`database` is required in all three — previously checked only at reconcile
time (`"Source %q: spec.<engine>.database is required"`), an apply-time-only
gap this closes (ADR 011). `postgres` additionally allows an optional
`schema`; `mysql`'s documented `serverId` field (§5's hypothetical example
above) is accepted for forward compatibility but not read by any provider —
Debezium derives its own stable replication server id from the connector
name. Generated reference: `docs/reference/source.md`'s "Source engine
reference" section.

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

**Update (docs/planning/08 D3/D4, 2026-07-21):** both seams now have
shipped providers, behind Alpha/disabled feature gates (`JDBCSinkProvider`,
`IngestProvider`) — the "no shipped provider" text above described the
state before these tasks landed and is left as historical record (additive
doc policy). `jdbcsink` implements `DatabaseSinkCapableProvider`
(`SupportedSinkEngines`: `postgres`, `mysql`) over Confluent's
kafka-connect-jdbc `JdbcSinkConnector`, realizing `sink` → `Source`.
`s3source` implements `IngestCapableProvider` (`SupportedIngestFormats`:
`jsonl`, `avro`, `parquet` — deliberately not the literal `json` value; the
chosen connector, Aiven's s3-source-connector-for-apache-kafka, has no
whole-file-JSON-array reader, only a line-delimited `jsonl` one) over
`io.aiven.kafka.connect.s3.source.S3SourceConnector`, realizing `ingest`.
Both follow the identical Kafka-Connect-worker pattern debezium/s3sink use
(`spec.configuration.image` required, no stock image ships the plugin;
`spec.configuration.workers` for the C3 replica-set shape).

**jdbcsink schema-carrying-format requirement (stronger than every other
provider in this codebase):** `jdbcsink`'s sink Binding `spec.options.format`
MUST be `avro` or `protobuf` — unset/`json` is rejected at validate time by
`ValidateBindingOptions`. This is not a stylistic preference: verified
against kafka-connect-jdbc's own `FieldsMetadata.extract` (v10.9.6), the
connector derives value columns, and satisfies `pk.mode: record_key`, only
from a Struct-typed (schema-carrying) record; a fully schemaless
(`json`-format) record contributes zero columns and `record_key` throws
outright. Since Debezium's own `json`-format CDC path
(`debezium.applyConverterConfig`) hardcodes `schemas.enable=false`
unconditionally, a realistic CDC → jdbcsink pipeline needs the upstream cdc
Binding to also declare `options.format: avro` (or `protobuf`), wired
through the same `SchemaRegistrySupport`-gated registry both connectors
share. `jdbcsink`'s sink `spec.options` additionally supports: `mode`
(`insert`, default, or `upsert` → the connector's `insert.mode`); `table`
(overrides the target table name — the connector's own default,
`table.name.format: "${topic}"`, names the table after the source
EventStream/topic, which may need overriding since a topic name may contain
hyphens, illegal in most unquoted SQL identifiers); `pkFields` (`upsert`
only — explicit primary-key column names; omitted, the connector's
`pk.mode: record_key` uses every field of the Kafka record key, the natural
fit for a CDC-sourced topic whose key already is the source table's primary
key); `unwrap` (boolean, default false — applies Debezium's own
`io.debezium.transforms.ExtractNewRecordState` SMT, bundled in every
debezium/connect-based worker image including this provider's required one,
so a CDC envelope's `after` state becomes the sink's flat row instead of the
full `before`/`after`/`op`/`source` envelope); `autoCreate`/`autoEvolve`
(booleans, default false, pass through to the connector's own DDL
automation — off by default since the schemaless-vs-schema-carrying
distinction above already constrains when it is even meaningful).
`deadLetter` (docs/planning/08 D6) works identically on `jdbcsink`'s sink
Binding as it does on `s3sink`'s.

**s3source ordering/offset semantics (docs/planning/03, additive per
docs/planning/08 D4's Accept item):** the connector lists objects under the
Dataset's bucket/prefix via S3's `ListObjectsV2` API in lexicographical key
order and tracks progress with a `startAfter`-style cursor persisted to its
own internal offsets topic (Kafka Connect's standard source-connector offset
mechanism) — an object already processed by a given connector instance is
never reprocessed, even across a worker restart. Object naming that is not
lexicographically monotonic with upload order can be read out of arrival
order (the connector's own README documents `aws.s3.fetch.buffer.size` as
the mitigation for slow-to-upload objects arriving after
lexicographically-later ones; this provider leaves it at the connector's own
default). `s3source` sets `distribution.type: object_hash` and
`file.name.template: ".*"` explicitly (both drift-diff-visible, mirroring
`s3sink`'s explicit `file.compression.type: none`) so it replays every
object under the Dataset's bucket/prefix regardless of which process wrote
them, rather than requiring `{{topic}}`/`{{partition}}`/`{{start_offset}}`
filename placeholders — the natural "replay everything under this
bucket/prefix" semantics an ingest/backfill Binding wants. The target
EventStream/topic is set directly via the connector's own `topic` config
key (an EventStream's resource name IS its Kafka topic name), not inferred
from a filename placeholder.

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

### 7.4 `spec.options.deadLetter` — dead-letter queues (docs/planning/08 D6)

Any sink-mode `Binding` may declare, alongside its mode-specific options:

```yaml
spec:
  options:
    deadLetter:
      stream: attendance-events-dlq   # an EventStream name; must exist in the manifest set
      tolerance: all                  # all|none — default "all" when omitted
```

`stream` names an `EventStream` — an EventStream's resource name *is* its Kafka topic name (the
same convention `redpanda`'s topic reconcile uses), so no separate topic field exists. `tolerance`
is Kafka Connect's own `errors.tolerance` value verbatim (`all`/`none`); omitting it defaults to
`all`, the only value that makes declaring a DLQ meaningful on its own (`none` still fails the
task on error, but still routes a copy to the DLQ topic — an advanced, rare combination, still
reachable by setting `tolerance: none` explicitly). `deadLetter` is refused at validate on any
`Binding` whose `mode` is not `sink`, and `deadLetter.stream` must resolve to an `EventStream` in
the manifest set — an unresolvable name fails at `validate` with the same error family as an
unresolvable `sourceRef`/`targetRef`.

**Ordering note:** `deadLetter.stream` is *not* a dependency-graph edge (unlike
`sourceRef`/`targetRef`/`providerRef`/`connectionRef`) — `internal/domain/graph.Build`'s edge
fields are a fixed, generic, top-level list shared by every Kind, and `deadLetter.stream` is
nested inside `spec.options`, sink-mode-scoped, and provider-consumed. `compatibility.Check` only
verifies the named `EventStream` *exists* in the manifest set; it does not guarantee that
EventStream reconciles before the sink Binding referencing it. This is safe in practice: Kafka
Connect's own framework creates the DLQ topic itself (via the worker's internal AdminClient) the
first time a poison record needs it, using `errors.deadletterqueue.topic.replication.factor` (the
`s3sink` provider resolves this from the named EventStream's own `spec.replication` when already
resolved in the engine's resource set, else defaults to `1`); the platform-managed EventStream's
own partition/retention configuration "wins" once it reconciles, in the same or a later apply.

**Provider translation:** `s3sink` (the only shipped sink provider as of v1.x — see §7.2)
translates a declared `deadLetter` into the Aiven S3 sink connector's error-handling config:
`errors.tolerance` (pass-through), `errors.deadletterqueue.topic.name` (= `stream`),
`errors.deadletterqueue.topic.replication.factor` (see above), and
`errors.deadletterqueue.context.headers.enable: true` (the landed DLQ record carries the original
topic/partition/offset/exception as headers, the only way to diagnose a poison record after the
fact). `debezium` never sees `deadLetter` — it is CDC-only (`Source`→`EventStream`), and
`deadLetter` is refused outside `mode: sink` at validate.

### 7.1 `spec.options` schema fragments (docs/planning/08 E5)

A Binding's `spec.options` fragment is keyed by
`"<spec.mode>-<providerRef's resolved spec.type>"` — the shape a given
mode/provider pairing actually accepts, since the same mode makes different
demands of different providers (mirroring §7's own capability-check split).
Shipped: `schemas/v1alpha1/fragments/binding/{cdc-debezium,sink-s3sink,
sink-jdbcsink,ingest-s3source}.json`. `manifest.Validate` resolves
`providerRef` to a `Provider` in the same manifest set first (an
unresolvable ref is left to `application/compatibility`'s own graph-aware
error, never duplicated here) and only then composes the matching fragment
in, if one is registered — a pairing with no fragment yet is checked solely
by that provider's `BindingOptionsValidator` Go code. `deadLetter` is
accepted in every sink-mode fragment (its own `{stream, tolerance}` shape is
already enforced unconditionally by `binding.FromEnvelope` regardless of
provider) purely so `additionalProperties: false` doesn't reject it.
Generated reference: `docs/reference/binding.md`'s "Binding options
reference" section.

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

`spec.lifecycle` (docs/planning/08 D7, additive as of 2026-07-21): optional
object-store lifecycle management, reconciled via the S3 API by the
realizing `s3`/`minio` provider (one managed lifecycle rule keyed by a
deterministic per-Dataset ID, plus bucket versioning); omitting it entirely
leaves the bucket's lifecycle/versioning config unmanaged, including any
out-of-band configuration already there. `probe` diffs the live rule/
versioning state (names and values, never secrets — a lifecycle rule holds
none) and reports drift; a subsequent `apply` heals it, including an
out-of-band change to either.

```yaml
spec:
  providerRef:
    name: local-minio
  bucket: raw-events
  prefix: attendance/
  format: parquet
  lifecycle:
    expireAfterDays: 90        # manage one lifecycle rule expiring objects under this Dataset's prefix
    versioning: enabled        # enabled | suspended
```

External **object-store production posture** (docs/planning/08 C4,
2026-07-21): a Dataset's own `providerRef` may point at an object-store
`Provider` that is itself declared `external: true` (a real S3-compatible
endpoint — AWS S3, R2, or a MinIO container standing in for one — reached
through a `Connection`, docs/planning/08 §4's Provider external example).
The Dataset itself stays a normal, non-external resource — it is not "the
external system", it is platformctl's own bucket/prefix record, realized
against a store it doesn't manage the lifecycle of — so it reconciles
through the ordinary (non-`external`) path, exactly like a Dataset against
a managed store, and needs no `ExternalConfigurer` capability: only a
Provider's own `external: true` declaration takes the connection-verified,
nothing-created path (§3.3's Provider row). `spec.lifecycle` above works
identically against this external store. `s3sink` Bindings reach the same
external store via the connector's existing `spec.options.endpoint` +
the s3sink Provider's `configuration.credentialsSecretRef` — no Dataset/
Provider-specific wiring needed on the sink side.

```yaml
spec:
  providerRef:
    name: prod-s3               # a Provider(type: s3, external: true) — see §4
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

**`warehouseRef`** (docs/planning/08 D8, top-level — deliberately *not* nested
inside the engine block, so `graph.Build`'s plain `refFields` pass, kind-checked
to `Dataset`, orders it with no engine-block introspection):

```yaml
spec:
  engine: nessie
  providerRef:
    name: catalog-svc
  warehouseRef:
    name: warehouse                # a Dataset; graph-ordered before this Catalog
```

Optional. Names a `Dataset` holding this Catalog's Iceberg warehouse location.
`nessie` is the first engine to consume it: its realizing `Provider`'s own
Provider-kind reconcile necessarily runs *before* any `Catalog` referencing
it (the reverse of `warehouseRef`'s own direction), so the derived warehouse
config can only take effect from the *Catalog*-kind reconcile step — which,
thanks to the `Dataset`'s own `providerRef` edge, always runs after both the
`Dataset` and its realizing (s3/minio) `Provider` have already reconciled and
published their `"s3"` endpoint fact in the same apply. `nessie`'s
`reconcileCatalog` computes `s3://<bucket>/<prefix>` plus the resolved S3
endpoint/credentials from `reconciler.Request.WarehouseFacts` (an
engine-resolved, published-facts-only field — ADR 015 — mirroring
`CatalogFacts`) and re-`EnsureInstance`s the Nessie container with the
corrected env; `EnsureContainer`'s existing spec-hash idempotency means this
only ever recreates the container once, the first time the derived facts
differ from what's running. A realizing `Provider`'s own explicit override
(nessie's `configuration.defaultWarehouseLocation`/`warehouseS3Endpoint`/
`warehouseS3SecretRef`, pre-D8) always wins outright when also set — additive
coexistence, `warehouseRef` is not required and does not remove the explicit
path. A `warehouseRef` naming a resource that is not a `Dataset` is rejected
at validate with the standard "does not resolve to any resource" shape (the
same structural kind-check `providerRef`/D10's `catalogRef` already use).
`trino`'s `configuration.warehouseProviderRef`/auto-inference (see the
`trino` bullet below) still works unmodified but becomes unnecessary once
the referenced `Catalog` itself declares `warehouseRef`: `resolveCatalogFacts`
now prefers the referenced `Catalog`'s own `warehouseRef` chain first,
falling back to `warehouseProviderRef`, then to auto-inferring the sole
S3/MinIO Provider in the namespace, in that order.

### 8.1.1 `spec.<engine>` schema fragments (docs/planning/08 E5)

Exactly like `Source` (§5.2): each catalog engine's nested block ships a
fragment, `schemas/v1alpha1/fragments/catalog/<engine>.json` (`nessie`
today — `defaultBranch`, optional, defaults to `"main"`). `manifest.Validate`
composes it in by `spec.engine`. Generated reference: `docs/reference/
catalog.md`'s "Catalog engine reference" section.

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

### 8.2.2 TLS termination (docs/planning/08 C8, docs/adr/018 addendum)

`SupportedConnectionSchemes()` grows to `["http", "https"]`; a `scheme:
https` Connection requires `spec.tls`, exactly one of:

```yaml
apiVersion: datascape.io/v1alpha1
kind: Connection
metadata:
  name: nessie
spec:
  providerRef:
    name: edge-http
  scheme: https
  port: 443
  target: nessie:19120
  tls:
    secretRef:                       # option 1: operator-provided cert+key
      name: nessie-tls
    # selfSigned: true                # option 2: provider-managed local CA + leaf cert
    # secretName: nessie-cert-mgr     # option 3: Kubernetes only, cert-manager-managed Secret by name
```

- `tls.secretRef` names a `SecretReference` whose `spec.keys` include `cert`
  and `key` (PEM values). It resolves through the engine's existing
  secret-resolution mechanism — the same one `Connection.spec.secretRef`
  already uses — **and only when the realizing `Provider`'s own
  `spec.secretRefs` also lists this same name** (mirrors how a
  Source/Binding's Connection credentials only resolve when the *working*
  provider — e.g. `debezium` — lists them, `internal/adapters/providers/
  debezium/debezium.go`). Declaring `tls.secretRef` without adding it to
  the `ingress` Provider's `spec.secretRefs` fails at apply with a message
  naming exactly that.
- `tls.selfSigned: true` — the `ingress` provider provisions one local CA
  per `Provider(type: ingress)` instance (lazily, on first self-signed
  Connection) plus a per-host leaf certificate signed by it. The CA's
  **public certificate only** is published in `providerState.tls.caCert`
  (`platformctl inventory` names where to find it; the CA private key
  never appears in state, logs, or `docker/kubectl inspect` output).
  Persistence (so the CA doesn't rotate on every `apply`, which would
  force every trusting tool to re-trust it):
  - **Docker**: the CA keypair is placed via `ContainerSpec.Files` on the
    shared Caddy container at a fixed path, using the same
    read-existing-before-regenerate pattern `postgres`'s superuser
    password rotation already uses (`rt.ReadFile` the prior container's
    file before deciding to keep vs. rotate) — the CA only changes if that
    file is genuinely missing (a fresh Provider, or the container was
    destroyed), never on an unrelated Connection's add/change/remove. Leaf
    certificates themselves are never placed via `ContainerSpec.Files` —
    they're loaded into Caddy's live config via its admin API
    (`/config/apps/tls/certificates/load_pem`), exactly like C7's route
    reconciliation, so a per-Connection cert never restarts the shared
    proxy.
  - **Kubernetes**: the CA keypair is stored as a `kubernetes.io/tls`
    Secret the `ingress` provider owns (not referenced by any `Ingress`
    object), read back before regenerating via the same
    `IngressCapableRuntime.GetTLSSecret` capability used for provided/
    self-signed leaf certs.
- `tls.secretName` (Kubernetes only) references an existing
  `kubernetes.io/tls` Secret by name — typically cert-manager-managed.
  platformctl only ever *reads* this Secret (`Ingress.spec.tls[].secretName`
  points at it); it is never created, updated, or deleted by platformctl,
  matching the "integration = referencing, not operating cert-manager"
  scope line. Declaring `secretName` on a Docker-runtime Provider fails at
  apply — Docker has no cert-manager equivalent; use `secretRef` or
  `selfSigned` there.
- Docker: a second Caddy HTTP server (`srv1`, container-internal port 443,
  `tls_connection_policies: [{}]` so it actually terminates TLS — found
  live: `automatic_https.disable: true` alone leaves the listener speaking
  plain HTTP) hosts every `https` Connection's route, addressed the same
  `Host(...)` way `srv0` (plain HTTP, unchanged) addresses `http`
  Connections.
- Kubernetes: the `Ingress` object gains `spec.tls: [{hosts: [<host>],
  secretName: <resolved secret name>}]`.
- A `https` endpoint reports `Insecure: false`; `platformctl inventory`
  renders its URL with the `https://` scheme (same `Endpoint.Scheme`/
  `Insecure` fields every other endpoint already uses — no separate
  rendering path).
- Gate `TLSTermination` (Alpha, disabled) — doc 04 §12.
**Fragment note (2026-07-22):** the ingress provider fragment gained
`httpsPort` (integer, 1–65535 — the TLS listener's host-published port,
C8), closing an E5 fragment gap found live: the field is exercised only
by the ingress-tls integration scenario, which the examples/blueprints
validation sweep could not see. The systematic used-keys-vs-fragment
sweep that found it also confirmed no other gap exists (doc 11).

### 8.2.3 Tunnel-mediated connections (the `wireguard` provider, docs/planning/08 D5, docs/adr/023)

A third realization of the `Connection` shape: a `wireguard`-typed
`Provider` is itself a `ConnectionCapableProvider` (scheme `tcp`) whose
Connection's upstream (`spec.target`) is only reachable through a
WireGuard tunnel the Provider maintains into another network (a VPC, a
corporate peer network, ...):

```yaml
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: vpc-tunnel
spec:
  type: wireguard
  secretRefs: [vpc-tunnel-key]
  configuration:
    privateKeySecretRef: vpc-tunnel-key   # SecretReference key "privateKey"; file-mounted only, never env/state/inspect
    peerNetwork: vpc-transit               # the Docker network the WireGuard peer's UDP endpoint is reachable on
    peerPublicKey: <base64>
    peerEndpoint: vpc-gateway:51820         # host:port; consumed as-is, like an external Connection's host (not a constructed address)
    address: 10.13.13.2/32                  # this tunnel's own address on the WireGuard point-to-point subnet
    allowedIPs: [10.13.13.0/24]             # the private subnet(s) routed through the tunnel
---
apiVersion: datascape.io/v1alpha1
kind: Connection
metadata:
  name: orders-db-vpc
spec:
  providerRef:
    name: vpc-tunnel
  scheme: tcp
  port: 25432
  target: 10.13.13.10:5432                  # host:port reachable via the tunnel's AllowedIPs route, not the shared network
  secretRef:
    name: orders-db-creds
```

- `NET_ADMIN` is required (the container creates and manages the `wg0`
  interface and `iptables` NAT/forward rules) — documented plainly, not a
  narrower capability, because Linux exposes none narrower for "manage a
  WireGuard interface" (docs/adr/023 Decision 2).
- The forwarder is `iptables` DNAT inside the tunnel container (one rule
  per Connection naming this Provider), not a second forwarder tool —
  docs/adr/023 Decision 4.
- Each Connection realized by this Provider gets its own tunnel container
  (named after the Connection itself, mirroring `proxy`'s own "one
  forwarder container per route" shape) — not one container shared across
  every Connection naming the same Provider. Every existing consumer of a
  managed Connection (e.g. a CDC Binding's provider resolving its Source's
  address) dials the runtime object named after the *Connection*, so this
  is the only shape that keeps that resolution working.
- Key rotation (a new `SecretReference` value) recreates the tunnel
  container (the same spec-hash mechanism every `ContainerSpec.Files`
  change already triggers) rather than a live in-place key swap —
  docs/adr/023 Decision 3.
- `Connection.spec.via` (optional `nameRef`, managed connections only)
  names a tunnel-capable Provider a *different* Connection's own realizing
  provider (e.g. `proxy`) could chain its egress through — schema-accepted
  and validate-time capability-checked (the named Provider must implement
  `TunnelCapableProvider`) but not yet consumed by any realizing provider's
  own reconciliation; see docs/adr/023's "Scope" section for the full
  reasoning. The example above does not use `via` — a Connection realized
  directly by a `wireguard` Provider (as shown) needs no `via`, since the
  `providerRef` already names the tunnel.

  **Update (docs/planning/08 I1, 2026-07-22):** `via` is now consumed —
  `proxy` (`reconciler.ViaConsumingProvider`) realizes a Connection whose
  `spec.via` names a tunnel-capable Provider by joining its forwarder ONLY
  to that Provider's transit network (never the platform network's other
  workloads) and dialing the tunnel's own per-Connection published address
  instead of `spec.target` directly. Validate now pairs the two
  capabilities: `via` set on a Connection whose realizing provider does
  NOT implement `ViaConsumingProvider` is refused (the same completeness
  bar the scheme check already enforces), rather than the prior blanket
  refusal of every `via`. See §8.2.4 item 3 below and docs/adr/023's
  closure note.

### 8.2.4 Reaching cloud-managed databases (worked topologies, docs/adr/025)

Three production topologies for a database Datascape does not run, in
increasing order of network isolation. In every case the database enters
the model as an **External Connection** (`spec.external: true`) plus a
`SecretReference` for its credentials — nothing is created, the address
is consumed as-is (§3).

1. **Directly reachable, TLS required** (public/peered RDS, Cloud SQL,
   Azure Database): declare the External Connection at the cloud
   endpoint. Transport TLS from consumers (Debezium, exporters, the
   admin connection) is doc 08 **I2** — until it lands, providers dial
   plaintext (`sslmode=disable`) and cannot reach a TLS-requiring
   endpoint; this is a known, tracked gap, not a configuration problem.

   **I2 shipped:** `spec.tls.mode` on the External Connection declares the
   outbound posture (gate `ExternalDatabaseTLS`, Alpha/enabled):

   ```yaml
   apiVersion: datascape.io/v1alpha1
   kind: Connection
   metadata:
     name: prod-rds
   spec:
     external: true
     host: prod-db.abcdefg.us-east-1.rds.amazonaws.com
     port: 5432
     secretRef:
       name: prod-rds-creds
     tls:
       mode: verify-full             # require | verify-ca | verify-full
       caSecretRef:
         name: prod-rds-ca            # SecretReference, key "ca" — the CA bundle PEM
   ```

   `spec.tls` absent entirely preserves the pre-I2 plaintext behavior
   (`sslmode=disable`-equivalent) — fully back-compat. Declared, `mode` is
   required (one of `require`, `verify-ca`, `verify-full` — libpq's own
   vocabulary, reused as-is rather than inventing a parallel one):
   `require` encrypts the transport with no certificate verification at
   all; `verify-ca` additionally verifies the server certificate chains to
   a trusted CA (`caSecretRef`, or the consuming process's system trust
   store when omitted — sufficient for a public CA-issued cert, e.g. most
   Cloud SQL/Azure Database endpoints) but does not check the hostname;
   `verify-full` additionally verifies the certificate's hostname matches
   the address actually dialed — the strongest posture, and the one every
   cloud vendor's own connection-string documentation recommends.
   `caSecretRef` names a `SecretReference` whose `spec.keys` include `ca`
   (a PEM-encoded CA bundle, e.g. an RDS regional bundle or a private
   CA's root) — like every other `secretRef` a Connection carries, it
   only resolves when the **consuming** Provider (the one actually
   dialing the database: `debezium`, `jdbcsink`, `postgres`, `mysql`)
   lists it in its own `spec.secretRefs`. The posture threads through
   every consumer that dials the database: the CDC preflight and the
   registered connector's own `database.sslmode`/`database.ssl.mode`
   properties (`debezium`), the sink's JDBC URL (`jdbcsink`), and the
   admin/replication connection a self-hosted engine's own provider makes
   (`postgres`, `mysql`/`mariadb`) — see those providers' own doc entries
   for the exact parameter each one sets. A wrong or unparseable CA bundle
   fails at the CDC preflight (before a connector is ever registered,
   ADR 011 — never mid-apply) with the real TLS error surfaced, not a
   generic timeout.
2. **Behind a cloud auth proxy** (Cloud SQL Auth Proxy, RDS IAM
   sidecars — the IAM/token-auth pattern): run the cloud's own proxy
   (your process or a container you declare); it handles IAM token
   refresh + TLS outbound and presents a plain local socket. Declare the
   External Connection at the **proxy's** address. This is the supported
   IAM-auth topology — Datascape deliberately does not mint or refresh
   cloud tokens itself (docs/adr/025: a resident refresher contradicts
   the one-shot posture, and the clouds ship the refresher as a proxy).
3. **VPN-only VPC** (no public endpoint, no peering): the `wireguard`
   tunnel provider (§8.2.3) realizes a managed Connection whose upstream
   is only reachable through the tunnel; `spec.via` (chaining another
   provider's Connection through the tunnel, blast-minimized to the
   forwarder alone) is doc 08 **I1** — until it lands, `validate`
   refuses `via` rather than silently realizing an untunneled forwarder.

   **Update (I1, 2026-07-22):** landed. `spec.via` on a `proxy`-realized
   Connection now chains that forwarder's egress through the named
   `wireguard` Provider's tunnel, blast-minimized exactly as designed:
   only the forwarder container joins the tunnel's transit network, never
   the consumer workloads. `validate` no longer refuses `via` outright —
   it refuses only when the Connection's realizing provider does not
   consume it (see §8.2.3's "Update" note above).

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

## 13. Policy — a sibling reference, not a governed-set kind (docs/adr/021)

`Policy` (`apiVersion: policy.datascape.io/v1alpha1`, `kind: Policy`) is
**not** one of the kinds in §1's `datascape.io/v1alpha1` set documented
above — it is a distinct governance input, loaded from its own channel
(`--policies <dir>` or the conventional `.datascape/policies/` directory),
never from the manifest set it governs (docs/adr/021-policy-engine-zero-
trust.md §1: "putting them inside the governed set would let the set amend
its own guardrails"). Its schema lives at `schemas/policy/v1alpha1/policy.json`
— a parallel embed (`schemas.PolicyFS`/`PolicyKindFiles`), deliberately kept
out of `schemas.KindFiles` (§1's resource-kind schema set) for the same
reason.

```yaml
apiVersion: policy.datascape.io/v1alpha1
kind: Policy
metadata:
  name: prod-zero-trust
spec:
  rules:
    - id: no-plaintext-connections        # globally unique across every loaded policy
      match: {kind: Connection}           # kind (string or list) / label / name selectors
      assert: {field: spec.scheme, in: [https]}  # field/equals/notEquals/in/absent/matches
      effect: deny                        # deny | warn
      exemptible: true                    # honor metadata.annotations["policy.datascape.io/exempt"]
      message: "prod requires TLS-terminated Connections"
    - id: escalate-duplicate-capture
      matchFinding: {code: DL001}         # promotes an ADR 020 lint code to enforcement
      effect: deny
    - id: no-dataset-deletes-in-ci
      matchPlan: {action: delete, kind: Dataset}  # evaluated at plan/apply/destroy only
      effect: deny
```

A rule sets exactly one of `(match + assert)`, `matchFinding`, or
`matchPlan` — never more than one. `assert`'s field selector reads the raw,
undecoded envelope (the same "raw spec map, not the FromEnvelope-defaulted
value" convention `internal/application/lint`'s DL020/DL021 checks use); a
field the resource never declared is `nil`, and each operator's own
ordinary comparison semantics decide the outcome (see
`internal/domain/policy.Assert`'s doc comment for the full worked-example
table) — there is no separate "field is absent" special case except the
`absent` operator itself. Deny-wins: `assert.Equals`/`In`/`Matches`'s
absent-fails-by-default posture means an *unset* field is exactly as
non-compliant as an explicit non-conforming value; `NotEquals`'s
absent-passes posture means an unset field has opted out of nothing.

`platformctl policy init zero-trust` writes the built-in starter pack
(docs/adr/021-policy-engine-zero-trust.md §4) for local tailoring;
`platformctl policy test [path]` evaluates a `--policies` set against a
manifest set without the rest of `validate`. Gated by `PolicyEngine`
(Alpha, disabled by default — docs/planning/04-roadmap-and-feature-gates.md
§12).
