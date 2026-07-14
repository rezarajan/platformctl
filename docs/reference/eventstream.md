# EventStream

`datascape.io/v1alpha1`

A durable, partitioned event log (a topic), realized by a streaming Provider such as redpanda.

| Field | Type | Required | Description |
|---|---|---|---|
| `metadata.name` | string | yes | Unique per Kind within a manifest set. |
| `metadata.observers[].name` | string | no | Provider names resolved to LineageEndpoints and forwarded when this resource's provider is LineageAware. |
| `spec.partitions` | integer | no | Partition count; increases apply in place, decreases are rejected by the broker. |
| `spec.providerRef` | object `{name}` | yes |  |
| `spec.retention` | object | no |  |
| `spec.retention.duration` | string | no | Retention window, e.g. 7d, 12h, 30m, 45s. |
