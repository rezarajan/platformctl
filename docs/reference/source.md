# Source

`datascape.io/v1alpha1`

A data origin. spec.engine is an open discriminator pairing with an engine-named nested block (e.g. spec.postgres), so new engines bring their own fields without a core schema change.

| Field | Type | Required | Description |
|---|---|---|---|
| `metadata.name` | string | yes | Unique per Kind within a manifest set. |
| `metadata.observers[].name` | string | no | Provider names resolved to LineageEndpoints and forwarded when this resource's provider is LineageAware. |
| `spec.connectionRef` | object `{name}` | no | SecretReference describing how to reach an external source. Required when external. |
| `spec.engine` | string | yes | Open-ended engine discriminator (postgres, mysql, ...). Names the sibling engine-specific block. |
| `spec.external` | boolean | no | The database lives outside the platform; Datascape never creates or deletes it. |
| `spec.providerRef` | object `{name}` | no | The Provider realizing this source. Required unless external. |
| `spec.<other>` | object | no | Engine-specific block named after spec.engine (e.g. postgres: {database, schema}). |
