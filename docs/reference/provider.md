# Provider

`datascape.io/v1alpha1`

Declares a technology (spec.type) and where it runs (spec.runtime). The provider implementation selected by spec.type defines the shape of spec.configuration.

| Field | Type | Required | Description |
|---|---|---|---|
| `metadata.name` | string | yes | Unique per Kind within a manifest set. |
| `metadata.observers[].name` | string | no | Provider names resolved to LineageEndpoints and forwarded when this resource's provider is LineageAware. |
| `spec.configuration` | object | no | Provider-specific configuration, keyed by spec.type. Host ports are OPTIONAL: omit a port and platformctl auto-allocates a stable, per-component one (surfaced by `platformctl inventory`), so components never collide by hand-picked ports; pin a port only when an external tool needs a fixed one. Versioned providers (postgres, mysql/mariadb) take `version` (an immutable, tested profile pinning image+internals) rather than a raw image. Never contains secret values. |
| `spec.connectionRef` | object `{name}` | no |  |
| `spec.external` | boolean | no | External lifecycle: Datascape never creates or deletes the backing system. |
| `spec.runtime` | object | yes | Where the provider's backing objects run. Fields beyond type are runtime-specific (e.g. network for docker). |
| `spec.runtime.network` | string | no | The shared addressing/isolation domain the provider's objects join. docker: the network name (default: datascape). kubernetes: the Namespace name (EnsureNetwork creates it; must not collide with an existing unmanaged namespace — see the runtime adapter's ownership policy). |
| `spec.runtime.type` | `docker` \| `fake` \| `kubernetes` \| `external` \| `terraform` | yes | docker and fake (testing) are implemented. kubernetes is a real, Alpha adapter behind the KubernetesRuntime feature gate (disabled by default) as of Phase 7. external/terraform are accepted for forward compatibility and rejected at registry construction as planned-but-unavailable. |
| `spec.secretRefs` | array of string | no | Names of SecretReference resources resolved by the engine and passed to the provider. |
| `spec.type` | string | yes | Provider implementation to construct. Shipped: redpanda, postgres, mysql, mariadb, debezium, s3, minio, s3sink, nessie (realizes Catalog engine nessie), openlineage (Marquez lineage backend), proxy (realizes managed Connections) — plus noop/container for testing. Open-ended: unknown types fail at registry construction, not schema validation. Technology providers realize the provider-agnostic kinds; the model speaks Catalog/Connection, never a technology's name. |
