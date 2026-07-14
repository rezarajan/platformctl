# Datascape resource reference

Generated from `schemas/` by `platformctl docs build` — do not edit by hand.

| Kind | apiVersion | Description |
|---|---|---|
| [Binding](binding.md) | `datascape.io/v1alpha1` | A relationship/data-movement contract realized by a Provider. spec.mode fixes which Kinds sourceRef/targetRef may resolve to (cdc: Source→EventStream, sink: EventStream→Dataset) and which provider capability is checked at validate time. |
| [Dataset](dataset.md) | `datascape.io/v1alpha1` | A durable landing zone (bucket/prefix + format) realized by an object-store Provider (s3/minio); populated by sink-mode Bindings. |
| [EventStream](eventstream.md) | `datascape.io/v1alpha1` | A durable, partitioned event log (a topic), realized by a streaming Provider such as redpanda. |
| [Provider](provider.md) | `datascape.io/v1alpha1` | Declares a technology (spec.type) and where it runs (spec.runtime). The provider implementation selected by spec.type defines the shape of spec.configuration. |
| [SecretReference](secretreference.md) | `datascape.io/v1alpha1` | A named reference to secret material resolved through a backend at reconcile time. The schema has no field that could carry a secret value: manifests declare names and keys only (FR-9). |
| [Source](source.md) | `datascape.io/v1alpha1` | A data origin. spec.engine is an open discriminator pairing with an engine-named nested block (e.g. spec.postgres), so new engines bring their own fields without a core schema change. |

## Provider types

Provider implementation to construct. Shipped in v1.0.0: redpanda, postgres, debezium, s3, minio, s3sink (plus noop/container for testing). Open-ended: unknown types fail at registry construction, not schema validation.

- `redpanda`
- `postgres`
- `debezium`
- `s3`
- `minio`
- `s3sink`
- `openlineage`
- `noop`
- `container`
