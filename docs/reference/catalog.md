# Catalog

`datascape.io/v1alpha1`

A table/metadata catalog (Iceberg REST, Hive Metastore, Glue, ...) as a provider-agnostic noun. spec.engine is an open discriminator pairing with an engine-named nested block, exactly like Source — Nessie is one engine behind the Catalog abstraction, never a shape of its own.

| Field | Type | Required | Description |
|---|---|---|---|
| `metadata.name` | string | yes | Unique per Kind within a manifest set. |
| `metadata.observers[].name` | string | no | Provider names resolved to LineageEndpoints and forwarded when this resource's provider is LineageAware. |
| `spec.connectionRef` | object `{name}` | no | A Connection (preferred) or SecretReference describing how to reach an external catalog. Required when external. |
| `spec.engine` | string | yes | Open-ended catalog engine discriminator (nessie, hive, glue, ...). Names the sibling engine-specific block; the realizing provider must declare it in SupportedCatalogEngines(). |
| `spec.external` | boolean | no | The catalog service lives outside the platform; Datascape never creates or deletes it. |
| `spec.providerRef` | object `{name}` | no | The catalog-capable Provider realizing this catalog. Required unless external. |
| `spec.warehouseRef` | object `{name}` | no | A Dataset holding this Catalog's warehouse location (docs/planning/08 D8). Kind-checked to Dataset and graph-ordered before this Catalog. Optional; a realizing provider that knows how to derive warehouse config from a Dataset (nessie) uses it automatically. An engine-specific explicit override on the realizing Provider (e.g. nessie's Provider(type: nessie).spec.configuration.defaultWarehouseLocation) always wins when also set. |
| `spec.<other>` | object | no | Engine-specific block named after spec.engine (e.g. nessie: {defaultBranch}). |

## Catalog engine reference (by `spec.engine`)

This table is generated from each engine's own JSON-Schema fragment (`schemas/v1alpha1/fragments/catalog/`) — the shape of the `spec.<engine>` block (docs/planning/08 E5).

### nessie

Shape-only fragment (docs/planning/08 E5, docs/planning/03 §8.1): defaultBranch is optional (defaults to "main", docs/planning/08 E2 configuration-minimization).

| Field | Type | Required | Description |
|---|---|---|---|
| `spec.<engine>.defaultBranch` | string | no |  |

