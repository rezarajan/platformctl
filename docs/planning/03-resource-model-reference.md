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
  name: <string, required, unique per Kind>
  labels: {}       # optional, free-form
  annotations: {}  # optional, free-form
  observers:       # optional, any data-plane Kind may declare this
    - name: local-marquez   # a Provider this resource's own provider may forward a LineageEndpoint to
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

## 3. Lifecycle taxonomy — how it's expressed per kind

| Lifecycle | How it's declared | Behavior |
|---|---|---|
| **Managed** (default) | No `external`/`import` marker present. | Datascape creates it, updates it on spec change, deletes it on `destroy` (no extra flag required). |
| **External** | `spec.external: true` (+ a `connectionRef`/equivalent describing how to reach it) | Datascape never creates or deletes it. It may still be *configured* (e.g., a CDC binding registers a connector against an externally-running Kafka Connect) if the provider defines a configure-only path. `destroy` never touches it without `--include-external` **and** the resource-specific destructive-action flag. |
| **Imported** | Not declared in the manifest directly — produced by `platformctl import <kind>/<name> --from ...`, which writes `status.imported: true` into state. | Behaves like Managed for update/reconcile purposes going forward, but its initial creation is never re-attempted; the first reconcile after import is a Probe + reconcile-in-place, not a create. |

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
  type: redpanda                 # redpanda | postgres | debezium | s3 | minio | s3sink | openlineage(optional)
  runtime:
    type: docker                 # docker | kubernetes (future) | external (future)
    network: datascape           # docker-specific; ignored/validated per runtime.type
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

Field notes:
- `spec.type` selects which `Provider` (reconciler) implementation and JSON Schema for
  `spec.configuration` apply.
- `spec.runtime.type` selects which `ContainerRuntime` (or future non-container runtime port) is
  constructed and injected.
- `spec.runtime` fields beyond `type` are runtime-specific and validated by the runtime adapter's
  own schema fragment.

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
    name: production-student-db     # resolved via a Connection/SecretReference pair, not inline creds
```

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

A `Binding` that fails either check is rejected at `validate`/`plan` time with a message naming
the `Binding`, the `Provider`, its type, and what it actually supports — never discovered only
once `apply` starts touching real infrastructure. Example:

```
error: Binding "student-db-to-events": Provider "postgres-cdc" (type: debezium)
does not support source engine "sqlite" (supported: postgres, mysql, mongodb)
```

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
```

`Dataset` reconciliation is a required v1.0.0 deliverable: `platformctl apply` creates the
bucket/prefix via the `s3`/`minio` provider, and a `sink`-mode `Binding` populates it.

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
  backend: env                     # env | file | kubernetes (future) | vault (future)
  keys:
    - username
    - password
```

Resolution: `SecretStore.Resolve` returns a `map[string]string` keyed by the logical names in
`spec.keys`; how those map to actual storage (env var names, file paths) is backend-specific
configuration, never present in the manifest's `spec` as a plaintext value.

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
