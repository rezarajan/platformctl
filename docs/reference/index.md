# Datascape resource reference

Generated from `schemas/` by `platformctl docs build` — do not edit by hand.

| Kind | apiVersion | Description |
|---|---|---|
| [Binding](binding.md) | `datascape.io/v1alpha1` | A directed data-movement contract realized by a Provider. spec.mode names the movement mechanism; each mode admits a set of endpoint Kind pairings (cdc: Source→EventStream; sink: EventStream→Dataset or EventStream→Source; ingest: Dataset→EventStream), with the matching provider capability checked at validate time. Direction lives in sourceRef/targetRef — asset kinds are role-neutral. |
| [Catalog](catalog.md) | `datascape.io/v1alpha1` | A table/metadata catalog (Iceberg REST, Hive Metastore, Glue, ...) as a provider-agnostic noun. spec.engine is an open discriminator pairing with an engine-named nested block, exactly like Source — Nessie is one engine behind the Catalog abstraction, never a shape of its own. |
| [Connection](connection.md) | `datascape.io/v1alpha1` | A first-class, non-secret description of how to reach a system: address here, credentials in the SecretReference named by spec.secretRef. Managed connections are realized by a connection-capable Provider as a stable platform-owned entrypoint (a forwarder on the shared network and the host) whose target is where the system actually lives; external connections are plain address records consumed as-is. External resources' connectionRef resolves to a Connection (preferred) or directly to a SecretReference (the v1.0.0 shorthand). |
| [Dataset](dataset.md) | `datascape.io/v1alpha1` | A durable landing zone (bucket/prefix + format) realized by an object-store Provider (s3/minio); populated by sink-mode Bindings. |
| [EventStream](eventstream.md) | `datascape.io/v1alpha1` | A durable, partitioned event log (a topic), realized by a streaming Provider such as redpanda. |
| [Provider](provider.md) | `datascape.io/v1alpha1` | Declares a technology (spec.type) and where it runs (spec.runtime). The provider implementation selected by spec.type defines the shape of spec.configuration. |
| [SecretReference](secretreference.md) | `datascape.io/v1alpha1` | A named reference to secret material resolved through a backend at reconcile time. The schema has no field that could carry a secret value: manifests declare names and keys only (FR-9). |
| [Source](source.md) | `datascape.io/v1alpha1` | An engine-backed database asset. spec.engine is an open discriminator pairing with an engine-named nested block (e.g. spec.postgres), so new engines bring their own fields without a core schema change. Role-neutral despite the historical name: a Source is the origin of cdc-mode Bindings and a legitimate target of sink-mode ones. |

## Provider types

Provider implementation to construct. Shipped: redpanda, postgres, mysql, mariadb, debezium, s3, minio, s3sink, nessie (realizes Catalog engine nessie), openlineage (Marquez lineage backend), proxy (realizes managed Connections), prometheus (managed monitoring stack, gate MonitoringStackProvider, docs/planning/08 C9) — plus noop/container for testing. Open-ended: unknown types fail at registry construction, not schema validation. Technology providers realize the provider-agnostic kinds; the model speaks Catalog/Connection, never a technology's name.

- `redpanda`
- `postgres`
- `mysql`
- `mariadb`
- `debezium`
- `s3`
- `minio`
- `s3sink`
- `nessie`
- `openlineage`
- `proxy`
- `prometheus`
- `noop`
- `container`
