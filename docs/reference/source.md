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

## Source engine reference (by `spec.engine`)

This table is generated from each engine's own JSON-Schema fragment (`schemas/v1alpha1/fragments/source/`) — the shape of the `spec.<engine>` block (docs/planning/08 E5).

### mariadb

Shape-only fragment (docs/planning/08 E5, docs/planning/08 A9's mariadb-cdc-scenario): database is required — identical shape to the mysql engine block (same adapter, distinct engine discriminator since a Source names its own nested block after spec.engine verbatim).

| Field | Type | Required | Description |
|---|---|---|---|
| `spec.<engine>.database` | string | yes |  |

### mysql

Shape-only fragment (docs/planning/08 E5, docs/planning/03 §5): database is required — omitting it previously failed only at reconcile time, a validate-time-completeness gap this fragment closes (ADR 011). serverId is documentation-only (docs/planning/03 §5's hypothetical example); Debezium's own connector derives a stable server id from the connector name (debezium.serverID), never read from this block.

| Field | Type | Required | Description |
|---|---|---|---|
| `spec.<engine>.database` | string | yes |  |
| `spec.<engine>.serverId` | integer | no |  |

### postgres

Shape-only fragment (docs/planning/08 E5, docs/planning/03 §5): database is required — omitting it previously failed only at reconcile time ("Source %q: spec.postgres.database is required"), a validate-time-completeness gap this fragment closes (ADR 011).

| Field | Type | Required | Description |
|---|---|---|---|
| `spec.<engine>.database` | string | yes |  |
| `spec.<engine>.schema` | string | no |  |

