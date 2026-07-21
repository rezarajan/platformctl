# Design note 005 — Database HA posture: single-node managed, HA via external

**Status:** accepted; decision note only (no code — implementation tasks,
if any, spawn from this note per docs/planning/08 §C5).
**Prompted by:** docs/planning/08-production-readiness-plan.md §C5 —
managed `postgres`/`mysql` are single-container; real HA databases
(Patroni, Galera, cloud RDS) are operationally deep, and the audit asked
whether platformctl should grow that operational depth as a managed
capability or route production HA around it.

## The question

Should the `postgres`/`mysql` providers gain multi-node replication
(leader election, automatic failover, replica promotion) as a managed
capability, or should production-grade database HA be positioned outside
what platformctl provisions, reachable through the already-existing
`external: true` + Connection seam?

## What "HA database" actually requires operationally

- **Patroni**: a distributed consensus store (etcd/Consul/ZooKeeper) for
  leader election, continuous WAL streaming to replicas, automatic
  failover with fencing of the demoted primary, and connection routing
  (pgbouncer/HAProxy, or a virtual IP) that always finds the current
  leader.
- **Galera**: synchronous multi-master replication, quorum/split-brain
  handling, and state-transfer (SST/IST) protocols when a node (re)joins.
- **Cloud RDS/Aurora**: the "HA" is bought, not run — an entirely
  provider-managed control plane (cloud-operated failover, replicated
  storage, automated backups) with no equivalent open surface to
  reimplement.

None of these is "add a `replicas` field." Each is a distinct operational
subsystem with its own consensus/quorum layer and its own failure modes —
and, for Patroni/Galera, a *second* piece of infrastructure (a consensus
store, or a synchronous-replication protocol) that itself needs lifecycle
management. This is qualitatively different from docs/planning/08 C1/C2's
"N replicas with stable identity" for Redpanda or MinIO: those use a
**shared-nothing, provider-native clustering protocol the provider adapter
already talks to** (Kafka's own admin API for broker membership, MinIO's
own erasure coding for node membership). Postgres/MySQL have no equivalent
"just add nodes" primitive — replication topology and failover policy
*are* the product being asked for, not a config knob on top of an existing
one.

## The decision

- Managed `postgres`/`mysql` Sources remain explicitly **single-node**,
  hardened along the two axes docs/planning/08 already commits to:
  **backup/restore** (C6 — data recoverability, since drift-healing
  recreates infrastructure but never data) and **fast drift-heal** (a
  killed or stopped database container is detected and recreated quickly
  by the existing Docker/Kubernetes reconciliation + drift probes,
  already shipped). This positions managed databases for **dev, staging,
  and small production** — the same tier the rest of the v1.0.0 product
  boundary targets (docs/planning/01 §1: "locally today, elsewhere
  later"; §9: "v1 targets single-machine/single-node scenarios").
- **Production HA databases are out of scope as a managed capability, and
  enter the platform as `external: true` Sources through the Connection
  seam — already fully supported today, CDC included, not a future
  design.** Concretely, and verified against the current code rather than
  asserted:
  - `internal/adapters/providers/debezium/debezium.go` (`buildDesiredConnector`,
    lines ~233–264) resolves an external Source's `connectionRef` to a
    `Connection` — managed (proxy-forwarded) or external (a plain address
    record) — and registers the Debezium connector against that resolved
    endpoint. No `ExternalConfigurer` capability is required on the
    Source's own (absent) provider for this path: the work is done by the
    *Binding's* provider (debezium), consuming the Source's connection
    facts, exactly as docs/planning/03 §3.3 documents for the
    external-without-`providerRef` row.
  - `examples/lakehouse/sources-and-datasets.yaml`'s `orders` Source is
    this pattern in the shipped, CI-exercised example: `external: true`
    with a `connectionRef` naming a Connection
    (`examples/lakehouse/catalog-and-connections.yaml`), CDC'd by the
    Binding in `examples/lakehouse/streams-and-bindings.yaml`.
  - A team running Patroni, Galera, or RDS in production declares the
    cluster's write endpoint as one `external: true` Source with a
    `connectionRef`; platformctl's CDC, inventory, and status machinery
    work against it unchanged. Platformctl never creates, deletes, or
    attempts to manage its replication topology — it only configures a
    connector against whatever address the Connection names, matching
    NFR-3's "external is never mutated, only configured" contract.
- This is consistent with NG2 (not a general-purpose orchestrator) and
  with the guiding principle that a provider should own technology
  semantics it is actually equipped to own (docs/planning/01 §5, guiding
  principle 2). Patroni/Galera/RDS's semantics are consensus and failover
  policy; that is not a container-plus-health-check problem, and
  reimplementing it would put platformctl in competition with tools whose
  entire job is being that operationally deep.

## Why this doesn't box anything out

A replication-capable managed mode remains additive later, following the
same discipline design notes 001–003 already established for this
codebase (relation over function, additive schema, swap-the-backend). If
ever built, here is what concretely changes — enumerated so the current
decision is legible as "not yet," not "never possible":

- **Schema.** The `postgres`/`mysql` engine-discriminated block on
  `Source` (docs/planning/03 §5) gains a `configuration.replicas`/
  `configuration.topology` field, mirroring the pattern docs/planning/08
  C2 already uses for `Provider(type: redpanda).spec.configuration.brokers:
  N` — additive to the existing engine block, no core `Source` schema
  change, same "engine brings its own fields" discipline the schema
  already documents.
- **Port/runtime surface.** This needs docs/planning/08 C1's
  `ContainerSpec.Replicas` + `StableIdentity` substrate (ordinal-suffixed
  containers/StatefulSet + per-ordinal volumes) — the same seam C2
  (Redpanda) already spawns from. Unlike Redpanda/MinIO, a Patroni-style
  mode additionally needs a **new dependency class**: a consensus store
  (etcd/Consul) the provider would have to either provision itself or
  require as another `external: true` resource — its own version of the
  new-dependency-class tradeoff design note 003 already worked through
  for shared state (there, object storage was chosen specifically to
  avoid introducing that class; a managed-HA-database provider would face
  the same fork and would need its own explicit answer).
- **Provider (`reconciler.Provider`) shape.** `Probe` needs failover-aware
  semantics — one replica down is not automatically `Ready=false` for the
  Source as a whole if a new leader was elected, matching C1's stated
  principle that "the *provider* decides meaning" for quorum-relevant
  health, not the generic replica-count check. Provider state needs a
  leader-tracking field so CDC/Binding resolution can find the *current*
  write endpoint, parallel to how `Connection.Endpoint()` already resolves
  a reachable address today (docs/planning/03 §8.2).
- **A new capability interface**, e.g. `ReplicationCapableProvider`
  (`SupportedTopologies() []string`), following the exact shape of
  `CDCCapableProvider`/`SinkCapableProvider` (docs/planning/02 §4.2) —
  gating which Bindings are structurally legal against a replicated
  Source. This matters because CDC against a replica set has a different
  replication-slot story per node (Patroni needs
  `synchronous_standby_names`/slot-failover extensions such as
  `pg_failover_slots`; vanilla logical replication slots don't survive
  failover) — `debezium.buildDesiredConnector` would need topology-aware
  address resolution instead of the single `dbHost` it resolves today.
- **A new feature gate**, e.g. `DatabaseHighAvailability` (Alpha,
  disabled), independent of the `HighAvailability` gate C1 introduces for
  stateless replica counts — database failover carries correctness stakes
  (split-brain data loss) that a stateless broker or object-store replica
  does not, and deserves its own graduation bar (docs/planning/02 §11).

Nothing in the current schema, port, or provider interface forecloses any
of the above; they are all additive extensions, not revisions of a
published contract — the same property design note 001 identified as the
reason the relation-shaped `AllowedKindPairs` mattered more than the kind
names themselves.

## Cross-references

- docs/planning/03 §3.1–3.3 (External vs Imported; external-lifecycle
  support per kind), §5 (`Source`), §8.2 (`Connection`) — the mechanics
  this decision relies on already being fully specified and shipped.
- docs/planning/08 §C6 — backup/restore, the other half of the managed-
  database posture (data recoverability without reimplementing HA).
- docs/planning/09 §4.1 — this same posture was independently recorded as
  a confirmed finding ("HA databases stay external") during the
  segregation-readiness audit; this note is the decision record that
  finding refers to.

## Follow-ups (non-blocking)

- If real usage produces demand for a managed replication mode (rather
  than "external Patroni/RDS is sufficient in practice"), the enumeration
  above is the starting checklist for that design note.
- `docs/planning/08` C6 (backup/restore) should ship regardless of this
  decision — it is the actual data-recoverability story for the single-
  node managed path, and is unaffected by whether a replication-capable
  mode is ever added.
