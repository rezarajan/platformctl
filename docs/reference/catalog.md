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
| `spec.<other>` | object | no | Engine-specific block named after spec.engine (e.g. nessie: {defaultBranch}). |
