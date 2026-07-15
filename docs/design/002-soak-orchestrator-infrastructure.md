# Design note 002 — Soak stage: orchestrator-ready infrastructure

**Status:** accepted; implemented in the post-v1.0.0 soak release (Phase 6.5).
**Prompted by:** project-owner direction — platformctl should stand up the
core infrastructure a Dagster (or similar) deployment runs against; users
run Dagster/Metabase themselves and connect to what the platform built.

## Requirements evaluated

A representative Dagster run touches: relational stores (Postgres, MySQL,
MariaDB), object storage (S3), an Iceberg catalog (Nessie), and an
OpenLineage backend (Marquez). Users also want a **stable entrypoint** to
*external* resources (an external Postgres or S3) rather than wiring every
tool to remote addresses — and external systems may sit behind a VPC.

What that requires of platformctl, and its status:

| Requirement | Verdict | Delivered as |
|---|---|---|
| Postgres, S3/MinIO provisioning | Already shipped (v1.0.0) | `postgres`, `s3`/`minio` providers |
| MySQL and MariaDB provisioning | **Feasible now** | `mysql` provider (`mariadb` is the same adapter: same protocol, binlog flags added explicitly); Source reconciliation creates the database + a replication-capable user; Debezium's connector class is now looked up per engine instead of hardcoded to Postgres |
| Iceberg catalog | **Feasible now** | `nessie` provider — single container, in-memory version store by default, Ready = REST config endpoint answers |
| OpenLineage backend | **Feasible now** (was the optional Phase 6 item) | `openlineage` provider standing up Marquez + its dedicated Postgres; publishes its endpoint in provider state so `metadata.observers` resolves against it; graduates `LineageObservability` to Beta |
| External sources / imported resources usable | Shipped in v1.0.0 but gated | `ImportedResources` graduates to Beta/enabled (its Phase 6 graduation was due) |
| Stable entrypoint to external resources | **Feasible now** | `proxy` provider: one TCP forwarder container per declared route, on the shared network *and* published to the host — in-network consumers use `<provider>-<route>:<port>`, host tools (Dagster, psql, Metabase) use `127.0.0.1:<port>`. Credentials still flow through `SecretReference`s; the proxy is transport only |
| VPC / VPN / tunnel reach | **Design only, deliberately** | See below |
| Dagster/Metabase themselves | Out of scope this release, by direction | README documents the endpoints they connect to |

## The proxy surface

`Provider(type: proxy)` declares `configuration.routes`, each `{name,
listenPort, target}`. Reconciliation runs one `alpine/socat` container per
route (`<provider>-<route>`), listening on `listenPort` inside the network
and on the host. This gives every external dependency a **platform-owned
address that never changes** even when the external endpoint moves — the
manifest changes, tools don't. An external `Source`'s Binding points its
`options.databaseHostname` at the route container; a user's `psql` or a
Dagster resource points at `127.0.0.1:<listenPort>`.

socat-per-route was chosen over one haproxy/nginx container because it
needs no config file (the runtime port mounts named volumes, not files),
restarts independently per route, and each route is separately probeable
and separately drift-healable.

## VPC / VPN reach — considered and deferred

The honest requirement analysis: reaching a VPC needs an *egress leg*
(WireGuard peer, SSH tunnel, or cloud-specific connector) that carries
material platformctl shouldn't own yet (long-lived private keys, cloud
session tokens) and failure modes (handshake, MTU, rekey) that deserve
their own provider with its own drift semantics. Bolting a `wireguard`
container into the proxy provider now would ship an untestable-in-CI,
security-sensitive feature days into a soak period.

Decision: the **schema carries the seam, the implementation waits.** Each
proxy route accepts an optional `via` field naming another Provider
(schema-accepted, validation rejects it as not-yet-supported with a clear
message). When a `tunnel`-typed provider lands (WireGuard first, most
likely), `via` chains a route's egress through it — additive, no schema
change, same discipline as the Binding pairing relation (design note 001).
Until then: run the tunnel out-of-band and point a plain route's `target`
at it — the proxy still gives tools a stable platform-owned address.

## Roadmap placement

These land as **Phase 6.5 — Soak: orchestrator-ready infrastructure**,
explicitly before Phase 7 (Kubernetes), added to
04-roadmap-and-feature-gates.md. Kubernetes gains from the soak: by the
time a second runtime exists, the provider set it must support is the one
real orchestrators already exercised.

## Addendum (2026-07-15) — remodeled before release; terminology retired

Project-owner review redirected this design before it shipped, on two
grounds; this note is retained as the historical record of the first cut.

1. **Model first, providers second.** Nessie landed here as a bare provider
   type and proxy routes as provider *configuration* — both provider-specific
   surfaces. The shipped design instead extends the resource model with two
   provider-agnostic kinds: **`Catalog`** (engine-discriminated, mirroring
   `Source`; Nessie is `engine: nessie` behind it) and **`Connection`**
   (the stable-entrypoint noun; one forwarder per Connection, realized by
   the proxy provider via `ConnectionCapableProvider`). External resources'
   `connectionRef` now resolves to a `Connection` first, and Bindings
   against external Sources consume the Connection's endpoint and
   credentials automatically. See docs/planning/03 §§3.1–3.2, 8.1–8.2.
2. **"Soak" is not a product term.** The stage name leaked into manifests,
   examples, tests, and the roadmap. The feature set is baseline GA-track
   functionality: the example is `examples/lakehouse/`, the roadmap phase
   is "Orchestrator-ready infrastructure", and the word survives only in
   historical development documents such as this one.
