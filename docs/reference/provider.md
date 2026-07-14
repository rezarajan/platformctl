# Provider

`datascape.io/v1alpha1`

Declares a technology (spec.type) and where it runs (spec.runtime). The provider implementation selected by spec.type defines the shape of spec.configuration.

| Field | Type | Required | Description |
|---|---|---|---|
| `metadata.name` | string | yes | Unique per Kind within a manifest set. |
| `metadata.observers[].name` | string | no | Provider names resolved to LineageEndpoints and forwarded when this resource's provider is LineageAware. |
| `spec.configuration` | object | no | Provider-specific configuration, keyed by spec.type (e.g. image, ports, *SecretRef names). Never contains secret values. |
| `spec.connectionRef` | object `{name}` | no |  |
| `spec.external` | boolean | no | External lifecycle: Datascape never creates or deletes the backing system. |
| `spec.runtime` | object | yes | Where the provider's backing objects run. Fields beyond type are runtime-specific (e.g. network for docker). |
| `spec.runtime.network` | string | no | docker-specific: the shared network name (default: datascape). |
| `spec.runtime.type` | `docker` \| `fake` \| `kubernetes` \| `external` \| `terraform` | yes | docker and fake (testing) are implemented; kubernetes/external/terraform are accepted for forward compatibility and rejected at registry construction as planned-but-unavailable. |
| `spec.secretRefs` | array of string | no | Names of SecretReference resources resolved by the engine and passed to the provider. |
| `spec.type` | string | yes | Provider implementation to construct. Shipped in v1.0.0: redpanda, postgres, debezium, s3, minio, s3sink (plus noop/container for testing). Open-ended: unknown types fail at registry construction, not schema validation. |
