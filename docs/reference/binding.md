# Binding

`datascape.io/v1alpha1`

A directed data-movement contract realized by a Provider. spec.mode names the movement mechanism; each mode admits a set of endpoint Kind pairings (cdc: Source→EventStream; sink: EventStream→Dataset or EventStream→Source; ingest: Dataset→EventStream), with the matching provider capability checked at validate time. Direction lives in sourceRef/targetRef — asset kinds are role-neutral.

| Field | Type | Required | Description |
|---|---|---|---|
| `metadata.name` | string | yes | Unique per Kind within a manifest set. |
| `metadata.observers[].name` | string | no | Provider names resolved to LineageEndpoints and forwarded when this resource's provider is LineageAware. |
| `spec.mode` | `cdc` \| `sink` \| `ingest` \| `batch` | yes | Movement mechanism: cdc (log-based capture out of a database), sink (continuous delivery from a stream into a durable target — object store or database), ingest (continuous pickup from an object store into a stream). batch is schema-reserved and rejected by validation in v1. |
| `spec.options` | object | no | Mode/provider-specific options (e.g. cdc: tables, snapshotMode, databaseHostname/databasePort for external sources). format (json|avro|protobuf, default json) and converter select the record serialization; avro/protobuf are schema-carrying and require a schema registry auto-wired from the EventStream's Provider (SchemaRegistrySupport gate, docs/planning/08 D1). |
| `spec.providerRef` | object `{name}` | yes | Must implement the capability interface matching spec.mode (CDCCapableProvider / SinkCapableProvider). |
| `spec.sourceRef` | object `{name}` | yes |  |
| `spec.targetRef` | object `{name}` | yes |  |
