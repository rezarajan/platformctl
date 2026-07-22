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
| `spec.lifecycle` | object | no | Object-store lifecycle management (docs/planning/08 D7), reconciled via the S3 API by the realizing s3/minio provider; omit entirely to leave the bucket's lifecycle/versioning config unmanaged (including any out-of-band config already there). Works identically for an external Dataset's Provider (configure-only, docs/planning/08 C4) as for a managed one. |
| `spec.lifecycle.expireAfterDays` | integer | no | Manage exactly one lifecycle rule expiring objects under this Dataset's prefix after this many days. Omit to leave expiration unmanaged. |
| `spec.lifecycle.versioning` | `enabled` \| `suspended` | no | Manage the bucket's versioning state. Omit to leave versioning unmanaged. |
| `spec.prefix` | string | no |  |
| `spec.providerRef` | object `{name}` | no |  |
