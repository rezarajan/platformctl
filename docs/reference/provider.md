# Provider

`datascape.io/v1alpha1`

Declares a technology (spec.type) and where it runs (spec.runtime). The provider implementation selected by spec.type defines the shape of spec.configuration.

| Field | Type | Required | Description |
|---|---|---|---|
| `metadata.name` | string | yes | Unique per Kind within a manifest set. |
| `metadata.observers[].name` | string | no | Provider names resolved to LineageEndpoints and forwarded when this resource's provider is LineageAware. |
| `spec.configuration` | object | no | Provider-specific configuration, keyed by spec.type. Host ports are OPTIONAL: omit a port and platformctl auto-allocates a stable, per-component one (surfaced by `platformctl inventory`), so components never collide by hand-picked ports; pin a port only when an external tool needs a fixed one. Versioned providers (postgres, mysql/mariadb) take `version` (an immutable, tested profile pinning image+internals) rather than a raw image. redpanda takes an optional schemaRegistry (enabled|disabled, default disabled) enabling its built-in Confluent-compatible registry, whose endpoint is published in providerState and inventory (SchemaRegistrySupport gate, docs/planning/08 D1), and an optional brokers (integer >= 1) opting into a multi-broker cluster of stable-identity ordinals (docs/adr/017): brokers > 1 requires the HighAvailability gate at validate; declaring brokers cannot be combined with host-port pins (kafkaPort/adminPort/schemaRegistryPort — each broker's host port is auto-assigned) or schemaRegistry: enabled; omitting brokers keeps the single-container shape, and enabling brokers on an existing single-broker deployment requires destroy-and-recreate; scaling brokers up applies in place, scaling down is refused (data-loss risk). prometheus takes an optional scrapeInterval (a Prometheus duration literal, e.g. "15s"; default "15s") — its scrape config is generated from every other Provider's published "metrics" endpoint fact in state, never hand-authored (MonitoringStackProvider gate, docs/planning/08 C9). s3 takes an optional nodes (integer >= 1) opting into a distributed, erasure-coded MinIO cluster of stable-identity ordinals, the same ordinal-set pattern as redpanda's brokers (docs/planning/08 C4): nodes: 1 opts into the single-ordinal StableIdentity shape (still one node, but the set shape rather than the legacy bare container); nodes: 2 or 3 is refused at validate (MinIO erasure coding requires 4+, or exactly 1 for standalone — 2-3 has no supported topology); nodes >= 4 requires the HighAvailability gate at validate, exactly like brokers > 1; omitting nodes keeps the legacy single-container shape byte-for-byte; scaling nodes up applies in place (a new erasure-coded pool), scaling down is refused (data-loss risk). debezium and s3sink take an optional workers (integer >= 1) fanning the Kafka Connect worker out to N interchangeable ordinals (ContainerSpec.Replicas, StableIdentity: false — docs/planning/08 C3): workers > 1 requires the HighAvailability gate at validate; connector REST calls try each currently-reachable worker; omitting workers keeps the single-container shape. ingress takes an optional domain (the Host(...) suffix every Connection's route is built from, default "localhost" — docs/adr/018 Decision 4: *.localhost resolves to loopback on modern resolvers with no manual setup) and, Docker-runtime only, optional port/adminPort host-port pins for the shared reverse-proxy container's HTTP and admin-API listeners (auto-allocated like every other component's host port when omitted); Kubernetes-runtime ingress Providers ignore port/adminPort (no shared container — one native Ingress object per Connection instead, docs/planning/08 C7). trino takes an optional workers (integer >= 1, default 1) sizing the worker replica set (StableIdentity: false — pure compute, no per-replica storage; workers > 1 requires the HighAvailability gate at validate, docs/planning/08 D10); an optional catalogRef (a nameRef to a Catalog, graph-ordered before this Provider) auto-configures etc/catalog/lakehouse.properties from the Catalog's published REST endpoint and its resolved warehouse-backing S3/MinIO Provider's endpoint + credentials (drift-checked; out-of-band edits are detected and healed); an optional warehouseProviderRef (a nameRef to a Provider) disambiguates which S3/MinIO Provider backs the warehouse when more than one exists — omit it when the manifest declares exactly one, in which case it is inferred. The warehouse Provider's credential SecretReference must also be listed in this Provider's own secretRefs for the engine to resolve it (mirroring s3's own configuration.rootSecretRef convention). nessie takes an optional defaultWarehouseLocation (an S3-shaped URI, e.g. "s3://bucket/prefix/") configuring its Iceberg REST Catalog personality with a default warehouse — required for any Iceberg REST client (trino included) to initialize the catalog at all; alongside it, warehouseS3Endpoint (e.g. "http://minio:9000") and warehouseS3SecretRef (a SecretReference name, must also be listed in spec.secretRefs) give Nessie itself the S3 endpoint and credentials it needs to associate that warehouse location with an object store — without them, creating a namespace/table under the warehouse fails even though the warehouse location itself is configured; omitted, Nessie's behavior is unchanged (docs/planning/08 D10). configuration itself never contains secret values.jdbcsink and s3source (docs/planning/08 D3/D4, gates JDBCSinkProvider/IngestProvider) take an optional workers (integer >= 1) identically to debezium/s3sink (workers > 1 requires the HighAvailability gate at validate; connectPort cannot combine with workers). jdbcsink takes an optional credentialsSecretRef (the fallback credential source when the sink Binding's target Source has no Connection secretRef of its own — mirrors debezium's replicationSecretRef fallback, so it is NOT unconditionally required). s3source takes a required credentialsSecretRef (object-store credentials for the origin Dataset's bucket — a Dataset has no Connection of its own, so this provider is the only possible credential source, mirroring s3sink's identical unconditional requirement). postgres, mysql, and mariadb take an optional metrics (enabled|disabled, default disabled, MonitoringStackProvider gate, docs/planning/08 C9 completion) opting into a postgres_exporter/mysqld_exporter sidecar container — a second, independent container alongside the instance (docs/adr/004), never a replica — reachable only in-network (Audience: internal) and authenticated as a dedicated least-privilege monitoring role/user this provider creates itself at reconcile (never the admin/root credential, never a user-declared SecretReference); its "metrics" endpoint fact is published exactly like redpanda/s3's, so the prometheus provider scrapes it with zero prometheus-side changes. grafana takes a required secretRefs entry (or configuration.adminSecretRef selecting one explicitly) for its admin username/password, an optional prometheusRef (a nameRef to a Provider) selecting which prometheus Provider's published endpoint to provision as its datasource — omit it when the manifest declares exactly one prometheus Provider, in which case it is inferred (ambiguous when more than one exists and prometheusRef is unset: the datasource is left unprovisioned until resolved) — and provisions a starter broker+database overview dashboard; anonymous access is always off. Never contains secret values. |
| `spec.connectionRef` | object `{name}` | no | A Connection (preferred) or SecretReference describing how to reach an externally-operated instance of this technology. Required when external. Verified reachable at apply; Datascape creates nothing for it (docs/planning/03 §3.3) — a Dataset/Source/Catalog naming this Provider in its own providerRef still reconciles normally, resolving this connectionRef itself (docs/planning/08 C4). |
| `spec.external` | boolean | no | External lifecycle: Datascape never creates or deletes the backing system. |
| `spec.runtime` | object | yes | Where the provider's backing objects run. Fields beyond type are runtime-specific (e.g. network for docker). |
| `spec.runtime.access` | `port-forward` \| `node-port` \| `load-balancer` \| `in-cluster` | no | kubernetes only; docker ignores it (a published host port is already reachable). Selects how platformctl itself, running outside the cluster, reaches this Provider's admin/control-plane ports (e.g. redpanda's Kafka admin API) to reconcile child resources like EventStream. Default (unset): port-forward, an ephemeral client-go tunnel opened per operation — zero cluster config but requires pods/portforward RBAC and a live apply process. node-port/load-balancer change the backing Service's type and use its externally-observed address — the same address `platformctl inventory` reports. in-cluster refuses CLI-side admin connections outright, naming the mode, for providers whose admin operations are expected to run from inside the cluster instead. |
| `spec.runtime.network` | string | no | The shared addressing/isolation domain the provider's objects join. docker: the network name (default: datascape). kubernetes: the Namespace name (EnsureNetwork creates it; must not collide with an existing unmanaged namespace — see the runtime adapter's ownership policy). |
| `spec.runtime.networkPolicy` | `none` | no | kubernetes only; docker ignores it (a Docker network is always isolated). By default EnsureNetwork also provisions a default-deny + allow-same-namespace NetworkPolicy pair so the Namespace mapping isn't DNS-parity-only — without it, any pod anywhere in the cluster could reach a Service. Set to "none" to opt out (e.g. a CNI that doesn't enforce NetworkPolicy, or an operator with their own policy story); a stderr warning is printed when opted out. |
| `spec.runtime.type` | `docker` \| `fake` \| `kubernetes` \| `external` \| `terraform` | yes | docker and fake (testing) are implemented. kubernetes is a real, Beta adapter behind the KubernetesRuntime feature gate (enabled by default as of docs/planning/08 Stage B close) — see runtime.access and the deploy/kubernetes/rbac/ manifests for what running against a real cluster needs. external/terraform are accepted for forward compatibility and rejected at registry construction as planned-but-unavailable. |
| `spec.secretRefs` | array of string | no | Names of SecretReference resources resolved by the engine and passed to the provider. |
| `spec.type` | string | yes | Provider implementation to construct. Shipped: redpanda, postgres, mysql, mariadb, debezium, s3, minio, s3sink, nessie (realizes Catalog engine nessie), openlineage (Marquez lineage backend), proxy (realizes managed Connections, scheme tcp), prometheus (managed monitoring stack, gate MonitoringStackProvider, docs/planning/08 C9), ingress (realizes managed Connections, scheme http — HTTP hostname routing; gate IngressProvider, docs/planning/08 C7, docs/adr/018) — plus noop/container for testing. Open-ended: unknown types fail at registry construction, not schema validation. Technology providers realize the provider-agnostic kinds; the model speaks Catalog/Connection, never a technology's name. Shipped: redpanda, postgres, mysql, mariadb, debezium, s3, minio, s3sink, nessie (realizes Catalog engine nessie), openlineage (Marquez lineage backend), proxy (realizes managed Connections), prometheus (managed monitoring stack, gate MonitoringStackProvider, docs/planning/08 C9), trino (compute-engine coordinator + workers, gate TrinoProvider, docs/planning/08 D10), grafana (managed Grafana provisioned with a Prometheus datasource + starter dashboard, gate MonitoringStackProvider, docs/planning/08 C9 completion) — plus noop/container for testing. jdbcsink (realizes Binding(mode: sink, targetRef: Source) — a JDBC database sink over Confluent's kafka-connect-jdbc, gate JDBCSinkProvider, docs/planning/08 D3) and s3source (realizes Binding(mode: ingest) — an S3 object-store source over Aiven's s3-source-connector, gate IngestProvider, docs/planning/08 D4) are the ADR 001/009 sink-into-Source and ingest capability seams made real; both Alpha/disabled by default. wireguard (realizes managed Connections, scheme tcp — a tunnel initiator dialing an externally-operated WireGuard peer; NET_ADMIN required; gate TunnelProvider, docs/planning/08 D5, docs/adr/023) takes required configuration.peerNetwork (the Docker network the peer's UDP endpoint is reachable on), configuration.peerPublicKey, configuration.peerEndpoint (host:port), configuration.address (this tunnel's own CIDR on the WireGuard point-to-point subnet), configuration.allowedIPs (the private subnet(s) routed through the tunnel), an optional configuration.keepalive (seconds, default 25), and configuration.privateKeySecretRef (a SecretReference key "privateKey", must also be listed in spec.secretRefs — file-mounted only, never env/state/inspect). A Connection realized by a wireguard Provider must set spec.target to an IP:port pair (not a hostname — iptables --to-destination does not resolve DNS names). |

## Provider configuration reference (by `spec.type`)

This table is generated from each provider's own JSON-Schema fragment (`schemas/v1alpha1/fragments/provider/`) — the shape-only rules enforced on `spec.configuration` at `validate` time, in addition to the cross-field rules a provider's `SpecValidator` still checks (docs/planning/08 E5).

### debezium

Shape-only fragment (docs/planning/08 E5): bootstrapServers is intentionally NOT required here — it is graph-inferable from an in-manifest redpanda Provider (docs/planning/08 E2) and that fallback, plus replicationSecretRef's spec.secretRefs membership, remain SpecValidator cross-field rules. The connectPort/workers mutual exclusion also remains a SpecValidator rule.

| Field | Type | Required | Description |
|---|---|---|---|
| `spec.configuration.bootstrapServers` | string | no |  |
| `spec.configuration.connectPort` | integer | no |  |
| `spec.configuration.image` | string | no |  |
| `spec.configuration.replicationSecretRef` | string | no |  |
| `spec.configuration.workers` | integer | no |  |

### grafana

Shape-only fragment (docs/planning/08 E5): the adminSecretRef-or-nonempty-secretRefs fallback and its spec.secretRefs membership remain SpecValidator cross-field rules; prometheusRef's graph resolution (ambiguous-when-unset-and-multiple) is a compatibility/engine concern, not schema shape.

| Field | Type | Required | Description |
|---|---|---|---|
| `spec.configuration.adminSecretRef` | string | no |  |
| `spec.configuration.image` | string | no |  |
| `spec.configuration.port` | integer | no |  |
| `spec.configuration.prometheusRef` | object `{name}` | no |  |

### ingress

Shape-only fragment (docs/planning/08 E5). port/adminPort are Docker-runtime-only (ignored on Kubernetes, docs/planning/08 C7) — a runtime-conditional refusal, if ever added, would be a SpecValidator/engine concern, not schema shape. httpsPort is the TLS listener's host-published port (docs/planning/08 C8) — missed by E5's fragment (found live at the day's closing gate, doc 11: the field is exercised only by the ingress-tls integration scenario, which examples/blueprints validation could not catch).

| Field | Type | Required | Description |
|---|---|---|---|
| `spec.configuration.adminPort` | integer | no |  |
| `spec.configuration.domain` | string | no |  |
| `spec.configuration.httpsPort` | integer | no |  |
| `spec.configuration.image` | string | no |  |
| `spec.configuration.port` | integer | no |  |

### jdbcsink

Shape-only fragment (docs/planning/08 E5). image is unconditionally required (no stock image ships the JDBC sink plugin). bootstrapServers is intentionally NOT required here — it is graph-inferable from an in-manifest redpanda Provider (docs/planning/08 E2). credentialsSecretRef is optional (falls back to the sink Binding's target Source's own Connection secretRef) but, when set, its spec.secretRefs membership and the connectPort/workers mutual exclusion remain SpecValidator cross-field rules.

| Field | Type | Required | Description |
|---|---|---|---|
| `spec.configuration.bootstrapServers` | string | no |  |
| `spec.configuration.connectPort` | integer | no |  |
| `spec.configuration.credentialsSecretRef` | string | no |  |
| `spec.configuration.image` | string | yes |  |
| `spec.configuration.workers` | integer | no |  |

### mariadb, mysql

Shape-only fragment (docs/planning/08 E5): shared by both the mysql and mariadb provider types (same adapter, per-type image/profile catalog). The rootSecretRef-or-nonempty-secretRefs fallback and *SecretRef spec.secretRefs-membership checks remain SpecValidator cross-field rules.

| Field | Type | Required | Description |
|---|---|---|---|
| `spec.configuration.image` | string | no |  |
| `spec.configuration.metrics` | `enabled` \| `disabled` | no |  |
| `spec.configuration.port` | integer | no |  |
| `spec.configuration.replicationSecretRef` | string | no |  |
| `spec.configuration.rootSecretRef` | string | no |  |
| `spec.configuration.version` | string | no |  |

### minio, s3

Shape-only fragment (docs/planning/08 E5): shared by both the s3 and minio provider types (same adapter). The rootSecretRef-or-nonempty-secretRefs fallback, *SecretRef spec.secretRefs-membership checks, and the nodes/port mutual exclusion remain SpecValidator cross-field rules; nodes 2-3 is refused there too (no supported MinIO topology between standalone and erasure-coded).

| Field | Type | Required | Description |
|---|---|---|---|
| `spec.configuration.image` | string | no |  |
| `spec.configuration.imagePullSecretRef` | string | no |  |
| `spec.configuration.nodes` | integer | no |  |
| `spec.configuration.port` | integer | no |  |
| `spec.configuration.rootSecretRef` | string | no |  |

### nessie

Shape-only fragment (docs/planning/08 E5). All fields optional: a Catalog's warehouseRef (docs/planning/08 D8) can derive warehouse config without any of these being set. warehouseS3SecretRef's spec.secretRefs membership remains a cross-field rule (no SpecValidator implemented yet for this provider — first coverage from this fragment).

| Field | Type | Required | Description |
|---|---|---|---|
| `spec.configuration.defaultWarehouseLocation` | string | no |  |
| `spec.configuration.image` | string | no |  |
| `spec.configuration.port` | integer | no |  |
| `spec.configuration.warehouseS3Endpoint` | string | no |  |
| `spec.configuration.warehouseS3SecretRef` | string | no |  |

### openlineage

Shape-only fragment (docs/planning/08 E5). No SpecValidator implemented for this provider today — first coverage from this fragment.

| Field | Type | Required | Description |
|---|---|---|---|
| `spec.configuration.apiPort` | integer | no |  |
| `spec.configuration.image` | string | no |  |

### openziti

Shape-only fragment (docs/planning/08 E5, docs/adr/022, docs/adr/027). adminSecretRef names the SecretReference carrying the controller's bootstrap admin credentials (keys: username, password) — falls back to the first entry in spec.secretRefs when unset, matching every other provider's ResolveCredential convention. targetNetworks are additional Docker networks the router container joins so it can reach a mediated Connection's target while staying off the shared/platform network consumers are on (the dark-service posture, docs/adr/022) — the explicit-declaration precedent docs/adr/023's peerNetwork sets.

| Field | Type | Required | Description |
|---|---|---|---|
| `spec.configuration.adminSecretRef` | string | no |  |
| `spec.configuration.controllerPort` | integer | no |  |
| `spec.configuration.routerPort` | integer | no |  |
| `spec.configuration.targetNetworks` | array of string | no |  |

### postgres

Shape-only fragment (docs/planning/08 E5): the superuserSecretRef-or-nonempty-secretRefs fallback and *SecretRef spec.secretRefs-membership checks remain SpecValidator cross-field rules.

| Field | Type | Required | Description |
|---|---|---|---|
| `spec.configuration.image` | string | no |  |
| `spec.configuration.metrics` | `enabled` \| `disabled` | no |  |
| `spec.configuration.port` | integer | no |  |
| `spec.configuration.replicationSecretRef` | string | no |  |
| `spec.configuration.storage` | object | no |  |
| `spec.configuration.storage.class` | string | no |  |
| `spec.configuration.storage.size` | string | no |  |
| `spec.configuration.superuserSecretRef` | string | no |  |
| `spec.configuration.version` | string | no |  |

### prometheus

Shape-only fragment (docs/planning/08 E5).

| Field | Type | Required | Description |
|---|---|---|---|
| `spec.configuration.image` | string | no |  |
| `spec.configuration.port` | integer | no |  |
| `spec.configuration.scrapeInterval` | string | no |  |

### proxy

Shape-only fragment (docs/planning/08 E5). No SpecValidator implemented for this provider today — first coverage from this fragment.

| Field | Type | Required | Description |
|---|---|---|---|
| `spec.configuration.image` | string | no |  |

### redpanda

Shape-only fragment (docs/planning/08 E5): mutual-exclusion rules between brokers and the host-port pins, and between brokers and schemaRegistry, remain SpecValidator checks (cross-field, not expressed here).

| Field | Type | Required | Description |
|---|---|---|---|
| `spec.configuration.adminPort` | integer | no |  |
| `spec.configuration.brokers` | integer | no |  |
| `spec.configuration.image` | string | no |  |
| `spec.configuration.kafkaPort` | integer | no |  |
| `spec.configuration.schemaRegistry` | `enabled` \| `disabled` | no |  |
| `spec.configuration.schemaRegistryPort` | integer | no |  |

### s3sink

Shape-only fragment (docs/planning/08 E5). image and credentialsSecretRef are unconditionally required (no fallback, no stock image ships the S3 sink plugin). bootstrapServers is intentionally NOT required here — it is graph-inferable from an in-manifest redpanda Provider (docs/planning/08 E2). credentialsSecretRef's spec.secretRefs membership and the connectPort/workers mutual exclusion remain SpecValidator cross-field rules.

| Field | Type | Required | Description |
|---|---|---|---|
| `spec.configuration.bootstrapServers` | string | no |  |
| `spec.configuration.connectPort` | integer | no |  |
| `spec.configuration.credentialsSecretRef` | string | yes |  |
| `spec.configuration.image` | string | yes |  |
| `spec.configuration.workers` | integer | no |  |

### s3source

Shape-only fragment (docs/planning/08 E5). image and credentialsSecretRef are unconditionally required (no fallback — a Dataset has no Connection of its own, so this provider is the only possible credential source). bootstrapServers is intentionally NOT required here — it is graph-inferable from an in-manifest redpanda Provider (docs/planning/08 E2). credentialsSecretRef's spec.secretRefs membership and the connectPort/workers mutual exclusion remain SpecValidator cross-field rules.

| Field | Type | Required | Description |
|---|---|---|---|
| `spec.configuration.bootstrapServers` | string | no |  |
| `spec.configuration.connectPort` | integer | no |  |
| `spec.configuration.credentialsSecretRef` | string | yes |  |
| `spec.configuration.image` | string | yes |  |
| `spec.configuration.workers` | integer | no |  |

### trino

Shape-only fragment (docs/planning/08 E5): catalogRef/warehouseProviderRef graph resolution remains an engine/SpecValidator concern (not required, since a manifest declaring exactly one Catalog/S3-or-MinIO Provider infers them).

| Field | Type | Required | Description |
|---|---|---|---|
| `spec.configuration.catalogRef` | object `{name}` | no |  |
| `spec.configuration.image` | string | no |  |
| `spec.configuration.port` | integer | no |  |
| `spec.configuration.warehouseProviderRef` | object `{name}` | no |  |
| `spec.configuration.workers` | integer | no |  |

### wireguard

Shape-only fragment (docs/planning/08 E5, docs/adr/023). peerNetwork/peerPublicKey/peerEndpoint/address/allowedIPs are unconditionally required (no fallback — parseConfig's identical check moves here). privateKeySecretRef's spec.secretRefs-membership-or-nonempty-secretRefs fallback remains a SpecValidator cross-field rule.

| Field | Type | Required | Description |
|---|---|---|---|
| `spec.configuration.address` | string | yes |  |
| `spec.configuration.allowedIPs` | array of string | yes |  |
| `spec.configuration.image` | string | no |  |
| `spec.configuration.keepalive` | integer | no |  |
| `spec.configuration.peerEndpoint` | string | yes |  |
| `spec.configuration.peerNetwork` | string | yes |  |
| `spec.configuration.peerPublicKey` | string | yes |  |
| `spec.configuration.privateKeySecretRef` | string | no |  |

