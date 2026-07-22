# Connection

`datascape.io/v1alpha1`

A first-class, non-secret description of how to reach a system: address here, credentials in the SecretReference named by spec.secretRef. Managed connections are realized by a connection-capable Provider as a stable platform-owned entrypoint (a forwarder on the shared network and the host) whose target is where the system actually lives; external connections are plain address records consumed as-is. External resources' connectionRef resolves to a Connection (preferred) or directly to a SecretReference (the v1.0.0 shorthand).

| Field | Type | Required | Description |
|---|---|---|---|
| `metadata.name` | string | yes | Unique per Kind within a manifest set. |
| `metadata.observers[].name` | string | no | Provider names resolved to LineageEndpoints and forwarded when this resource's provider is LineageAware. |
| `spec.external` | boolean | no | A plain address record; nothing is created for it. |
| `spec.host` | string | no | External only: where the system answers. |
| `spec.port` | integer | yes | The port consumers use. Managed: the entrypoint's listen port on the shared network and the host. |
| `spec.providerRef` | object `{name}` | no | The connection-capable Provider realizing the entrypoint. Required unless external. |
| `spec.scheme` | string | no | Transport scheme; the realizing provider must declare it in SupportedConnectionSchemes(). Default: tcp. |
| `spec.secretRef` | object `{name}` | no | Optional SecretReference carrying credentials for whatever answers at this connection. |
| `spec.target` | string | no | Managed only: host:port the entrypoint forwards to — the one place that knows where the system actually lives. |
| `spec.tls` | object | no | Terminates TLS at the entrypoint (docs/planning/08 C8). Only meaningful together with scheme: https on a managed (non-external) Connection. Exactly one of secretRef, selfSigned, or secretName. |
| `spec.tls.secretName` | string | no | Kubernetes only: references an existing kubernetes.io/tls Secret by name (e.g. cert-manager-managed). platformctl only ever reads this Secret. |
| `spec.tls.secretRef` | object `{name}` | no | SecretReference carrying the cert+key PEM material (keys: cert, key). Must also appear in the realizing Provider's spec.secretRefs for the engine to resolve it (mirrors spec.secretRef's own plumbing). |
| `spec.tls.selfSigned` | boolean | no | The provider provisions a local CA plus a per-host leaf certificate for dev use. The CA's public certificate is published in providerState so tools can trust it. |
| `spec.via` | object `{name}` | no | Managed only: optional reference to a tunnel-capable Provider (docs/adr/023) this Connection's egress additionally routes through. Schema-accepted and validate-time capability-checked (the named Provider must implement TunnelCapableProvider); no realizing provider consumes it yet — a tunnel-mediated Connection is realized directly by the tunnel provider itself today (see docs/adr/002's addendum, docs/adr/023's Scope section). |
