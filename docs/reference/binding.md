# Binding

`datascape.io/v1alpha1`

A relationship/data-movement contract realized by a Provider. spec.mode fixes which Kinds sourceRef/targetRef may resolve to (cdc: Source→EventStream, sink: EventStream→Dataset) and which provider capability is checked at validate time.

| Field | Type | Required | Description |
|---|---|---|---|
| `metadata.name` | string | yes | Unique per Kind within a manifest set. |
| `metadata.observers[].name` | string | no | Provider names resolved to LineageEndpoints and forwarded when this resource's provider is LineageAware. |
| `spec.mode` | `cdc` \| `sink` \| `batch` | yes | batch is schema-reserved and rejected by validation in v1. |
| `spec.options` | object | no | Mode/provider-specific options (e.g. cdc: tables, snapshotMode, databaseHostname/databasePort for external sources). |
| `spec.providerRef` | object `{name}` | yes | Must implement the capability interface matching spec.mode (CDCCapableProvider / SinkCapableProvider). |
| `spec.sourceRef` | object `{name}` | yes |  |
| `spec.targetRef` | object `{name}` | yes |  |
