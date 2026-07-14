# Dataset

`datascape.io/v1alpha1`

A durable landing zone (bucket/prefix + format) realized by an object-store Provider (s3/minio); populated by sink-mode Bindings.

| Field | Type | Required | Description |
|---|---|---|---|
| `metadata.name` | string | yes | Unique per Kind within a manifest set. |
| `metadata.observers[].name` | string | no | Provider names resolved to LineageEndpoints and forwarded when this resource's provider is LineageAware. |
| `spec.bucket` | string | yes |  |
| `spec.external` | boolean | no |  |
| `spec.format` | string | yes | Validated against the sink provider's SupportedSinkFormats() (e.g. json, jsonl, csv, parquet). |
| `spec.prefix` | string | no |  |
| `spec.providerRef` | object `{name}` | no |  |
