# Source

`datascape.io/v1alpha1`

An engine-backed database asset. spec.engine is an open discriminator pairing with an engine-named nested block (e.g. spec.postgres), so new engines bring their own fields without a core schema change. Role-neutral despite the historical name: a Source is the origin of cdc-mode Bindings and a legitimate target of sink-mode ones.

| Field | Type | Required | Description |
|---|---|---|---|
| `metadata.name` | string | yes | Unique per Kind within a manifest set. |
| `metadata.observers[].name` | string | no | Provider names resolved to LineageEndpoints and forwarded when this resource's provider is LineageAware. |
| `spec.connectionRef` | object `{name}` | no | SecretReference describing how to reach an external source. Required when external. |
| `spec.deletionPolicy` | `retain` \| `delete` | no | What Source destroy does to the database: retain (default) keeps it — destroying the platform's record of a source must not destroy the data; delete drops the database. Instance teardown (Provider destroy) removes the backing store regardless. Ignored for external sources (never touched). |
| `spec.engine` | string | yes | Open-ended engine discriminator (postgres, mysql, ...). Names the sibling engine-specific block. |
| `spec.external` | boolean | no | The database lives outside the platform; Datascape never creates or deletes it. |
| `spec.providerRef` | object `{name}` | no | The Provider realizing this source. Required unless external. |
| `spec.<other>` | object | no | Engine-specific block named after spec.engine (e.g. postgres: {database, schema}). |
