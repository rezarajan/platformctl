# Dataset

`datascape.io/v1alpha1`

A durable landing zone (bucket/prefix + format) realized by an object-store Provider (s3/minio); populated by sink-mode Bindings.

| Field | Type | Required | Description |
|---|---|---|---|
| `metadata.name` | string | yes | Unique per Kind within a manifest set. |
| `metadata.observers[].name` | string | no | Provider names resolved to LineageEndpoints and forwarded when this resource's provider is LineageAware. |
| `spec.bucket` | string | yes |  |
| `spec.connectionRef` | object `{name}` | no | A Connection (preferred) or SecretReference describing how to reach an external object store. Required when external. |
| `spec.deletionPolicy` | `retain` \| `delete` | no | What Dataset destroy does to the stored objects: retain (default) keeps bucket contents — destroying the platform's record of a dataset must not destroy the data; delete removes every object under bucket/prefix. Instance teardown (Provider destroy) removes the backing store regardless. |
| `spec.external` | boolean | no |  |
| `spec.format` | string | yes | Validated against the sink provider's SupportedSinkFormats() (e.g. json, jsonl, csv, parquet). |
| `spec.prefix` | string | no |  |
| `spec.providerRef` | object `{name}` | no |  |
