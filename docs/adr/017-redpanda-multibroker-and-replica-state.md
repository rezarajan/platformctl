# ADR 017 — Redpanda multi-broker clusters and the state-level replica representation

**Status:** accepted (C2, 2026-07-21); implemented in this note's own commit.
**Prompted by:** `docs/planning/08-production-readiness-plan.md` §C2 — single-broker
only today; `EventStream` has no replication factor; a broker restart pauses the
pipeline. Builds directly on ADR 004's `Replicas`/`StableIdentity` primitive (C1,
merged), ADR 015's connectivity plane, and B1's kgo.Dialer advertised-address
redirect in `internal/adapters/providers/redpanda/kafka.go`.

This note answers the two design questions doc 08 assigns to C2:
(a) the multi-broker mechanics on ADR 004's primitive, and (b) whether per-ordinal
facts belong in state (`state.ResourceState` is one entry per resource with
aggregate status today) or stay runtime-observed.

---

## Question (a): multi-broker mechanics

### a.1 Which runtime shape realizes `configuration.brokers`?

`Provider(type: redpanda).spec.configuration.brokers: N` must map onto
ADR 004's primitive so that **changing `brokers: 1 → 3` is an in-place scale**
(doc 08 §C2's own words) and `3 → 1` can be refused. That single requirement
decides the shape question, because C1 deliberately shipped *shape-transition
refusals*: an existing single container/Deployment is never converted to a
replica set/StatefulSet in place, and vice versa (a review finding fixed in C1's
own commit, pinned by `ReplicaSet_ShapeTransition_Refused`). If `brokers: 1`
were realized as today's bare single container, then `1 → 3` would be a
cross-shape transition — refused by C1's own guards on Kubernetes
(Deployment → StatefulSet), silently stranding the old broker's volume on
Docker, and losing the broker's log either way. "In-place scale" is only
possible when both counts live in the *same* shape.

**Options considered:**

1. **`brokers: 1` = today's single container; `N > 1` = ordinal set; the
   provider auto-migrates on `1 → 3`** (remove the old container, form a fresh
   cluster). Rejected: the migration silently discards the single broker's log
   data (per-ordinal volumes are fresh; Docker volumes cannot be renamed into
   the ordinal naming scheme, and Kubernetes PVC names are structurally
   different), and "in-place scale" that wipes data is worse than a refusal.
2. **Always the ordinal shape, even with `brokers` unset.** Rejected: every
   existing redpanda deployment (container `<name>`, volume `<name>-data`)
   would break on upgrade — the exact "zero behavior change for manifests that
   don't opt in" breach ADR 014 forbids.
3. **Chosen: `brokers` *declared* (any N ≥ 1) selects the ordinal-set shape
   (`ContainerSpec{Replicas: N, StableIdentity: true}`); `brokers` *unset*
   keeps the pre-C2 single-container shape, byte-for-byte.** Declaring
   `brokers` is the opt-in; scaling `1 ↔ 3` then never crosses a shape
   boundary (a 1-member StatefulSet scales to 3 in place; Docker adds
   ordinals `-1`/`-2` beside the existing `-0`, whose volume — and therefore
   the cluster's log — survives). Enabling `brokers` on an *existing* pre-C2
   deployment is a shape transition and is refused by C1's guards with their
   standard destroy-and-recreate remedy; this is recorded here as the honest
   reading of doc 08's "1→3 is an in-place scale" line — in-place *within* the
   declared-`brokers` shape, not across the grandfathered legacy shape.

### a.2 The port amendment this requires (ADR 004, amended additively)

ADR 004 declared `StableIdentity` "meaningful only when Replicas > 1". Option 3
gives it a meaning at `ReplicaCount() == 1` too: **`StableIdentity: true`
selects the ordinal-set shape at any replica count** — a 1-member set is
ordinal `<name>-0` with volume `<VolumeName>-0` (Docker) / a 1-replica
StatefulSet (Kubernetes), not a bare single container. This is additive: no
spec before C2 sets `StableIdentity` at all, so every existing caller's
behavior is unchanged (verified by the unmodified pre-C1 conformance suite).

Two consequences, both implemented in this commit:

- **Scale-down *within* the set shape is runtime-mechanics, not runtime
  policy.** `3 → 2` was already allowed (Docker's `pruneStaleOrdinals`,
  Kubernetes' StatefulSet replica update); `2 → 1` now is too, because
  `Replicas: 1, StableIdentity: true` is a valid set. Whether a scale-down is
  *safe* is provider domain knowledge (ADR 004's "provider decides meaning"
  rule) — see a.5. The conformance pin `ReplicaSet_ShapeTransition_Refused`
  is amended to collapse to `StableIdentity: false, Replicas: 1` (the
  set → bare-single transition), which remains refused on every adapter; a new
  subtest pins the 1-member-set shape and the `1 → 2 → 1` in-place scale.
- **The set path gains the missing symmetric guard on Docker/fake:** a literal
  container named `<name>` (the legacy single shape) refuses conversion to a
  replica set in place, mirroring the guard Kubernetes always had
  (StatefulSet ensure refuses when a Deployment of the name exists).

### a.3 Per-broker identity, seeds, and listeners

Every ordinal receives an *identical* `ContainerSpec` (ADR 004's contract);
per-broker differentiation uses the `HOSTNAME` convention ADR 004 reserved for
exactly this decision — on both runtimes an ordinal's in-container hostname is
its ordinal name (StatefulSet natively; Docker adapter sets `Config.Hostname`):

- **Node ID:** `--node-id ${HOSTNAME##*-}` — the ordinal index, unique and
  stable across restarts, no controller-assignment complexity.
- **Seed list:** ordinal 0 is the founding seed; ordinals ≥ 1 pass
  `--seeds <name>-0:33145` (the pattern Redpanda's own multi-node compose
  uses, expressed as a shell conditional on the ordinal so the command stays
  identical across ordinals). Membership persists in each ordinal's data
  volume after first join, so a healed broker rejoins without its seed being
  special. Limitation, recorded: *first* formation and a *wiped* ordinal's
  rejoin require ordinal 0 reachable.
- **Listeners:** unchanged ports — INTERNAL `29092`, EXTERNAL `9092`, admin
  `9644` — plus the RPC listener `33145`, declared `Audience: internal`
  (ADR 015 F2: every listener a dependent dials is declared; brokers dial each
  other's RPC).

### a.4 Advertised addresses and the kgo.Dialer redirect, per ordinal

- **INTERNAL advertised:** `INTERNAL://${HOSTNAME}:29092` — a *real* address:
  ordinal names resolve on both runtimes (Docker container names; StatefulSet
  headless-Service pod DNS). In-network clients (Connect workers) bootstrap
  against the comma-joined ordinal list and follow real redirects; no
  interception needed. `KafkaBootstrapAddress` returns that list, still
  computed from manifest facts alone (name + declared count — the naming
  authority plus declared ports, ADR 015 F4; nothing constructed).
- **EXTERNAL advertised:** `EXTERNAL://${HOSTNAME}:9092` — a deliberately
  *undialable, per-broker-unique, stable token*, generalizing B1's insight: on
  Kubernetes no host-audience address is knowable at container-start time, and
  on Docker a `Replicas > 1` set cannot pin host ports at all (every ordinal
  would inherit the same one — C1's known limitation), so host ports are
  auto-allocated and equally unknowable at start. The CLI-side admin client
  resolves, per ordinal, a currently-dialable address via
  `EnsureReachable(OrdinalName(name, i), 9092)` (both adapters support ordinal
  names), and `kafka.go`'s dialer — now a token→address *map* rather than a
  single pair — redirects every dial of a broker's token to that broker's
  resolved address. A broker whose `EnsureReachable` fails (killed, mid-heal)
  is simply left out of the map and the seed list: admin operations proceed
  against the survivors, which is what lets drift-probe and re-apply work
  while a broker is down. The legacy single-broker path keeps its existing
  `127.0.0.1:<hostPort>` advertised sentinel unchanged.
- **Host-side third-party clients** (anything not going through
  `EnsureReachable`) cannot follow the token redirects — the same NAT reality
  any multi-broker Kafka behind port-mapping has. The per-ordinal host
  addresses are published as endpoint facts (see Question b) for tools that
  can do their own mapping; this is documented, not papered over.
- **Host port pins** (`kafkaPort`, `adminPort`, `schemaRegistryPort`) are
  refused at validate when `brokers` is declared — closing C1's known
  limitation ("a validate-time refusal is C2's concern").

### a.5 Scale-down `3 → 1`: refused, with the remedy named

Scaling down discards the removed brokers' partition replicas; below the
topic's replication factor that is data loss. The provider refuses the
scale-down at reconcile time by *observing* the current ordinal count (an
`Inspect` of the ordinal at index `N` — runtime observation, not state; see
Question b) and returning an error naming both counts and the remedy.

**Why refusal rather than the NFR-3 destructive flag pair:** the engine's
`AllowDestructive` exists (`Engine.AllowDestructive`) but is wired to
`destroy`/`apply`-delete of External resources via
`--include-external --yes-i-understand-this-is-destructive`; it does not flow
into `reconciler.Request`, and plumbing a new flag pair through
apply → engine → Request for this one transition is exactly the
disproportionate-plumbing case doc 08 §C2 anticipated ("refusing 3→1 outright
with a destroy-and-recreate remedy is acceptable — record the choice in ADR
017"). Recorded: `brokers: 3 → 1` is refused unconditionally; the remedy is
destroy-and-recreate (or restoring the previous count). If a future task
plumbs destructive intent into reconcile, the refusal site is a single
function in the redpanda provider.

### a.6 Health: container-level vs cluster-level

The container healthcheck stays `rpk cluster health --exit-when-healthy` —
which is *cluster-scoped*: a broker's Docker health flips unhealthy when any
peer is down. It is **not**, however, a formation barrier: ordinal 0 alone is
a perfectly healthy 1-node cluster before its peers join (caught live — the
first integration run created an RF-3 topic against a 1-member cluster and
got `INVALID_REPLICATION_FACTOR`), and `WaitHealthy` deliberately returns at
one-member-ready (ADR 004). Reconcile therefore polls cluster membership
explicitly (`waitClusterFormed`: per-ordinal `EnsureReachable` re-resolved
per attempt, admin broker list == N) before declaring Ready — which is also
what makes Ready mean "all brokers joined" rather than "a process is up".
The cluster-scoped healthcheck also makes healthcheck-derived
`ReadyReplicas` useless for "which broker is missing". The provider's `Probe`
therefore computes cluster facts itself (ADR 004's provider-decides rule):

- **Broker presence:** per-ordinal `Inspect` — a missing/stopped ordinal is
  drift, reason `BrokerMissing(<ordinals>)` (G4 pattern: stable constant
  prefix, dynamic detail).
- **Membership:** when all ordinals are present, the admin client's broker
  list must have N entries — otherwise drift, `BrokerNotJoined(got!=want)`.
- **Per-topic replication factor:** `probeTopic` also compares each topic's
  observed RF against `EventStream.spec.replication` —
  `ReplicationFactorMismatch(got!=want)`.

Re-apply heals a missing ordinal (Docker: `EnsureContainer` recreates it and
its retained volume lets it rejoin; Kubernetes: the StatefulSet controller
typically heals before drift is even observed — a documented per-runtime
difference, not a bug).

### a.7 `EventStream.spec.replication` and its validation

`spec.replication: N` (schema-additive, default 1 — existing topics are RF 1,
so the default changes nothing). Two checks:

- **Validate-time:** `replication > brokers` is refused with an error naming
  both numbers, and — caught live on the Kubernetes leg — an *even* factor
  above 1 is refused too: Redpanda's brokers reject it outright
  ("replication factor must be odd", a Raft-quorum constraint Kafka proper
  does not share), which would otherwise be an apply-time-only failure
  (ADR 011's regression class). Implemented per ADR 009's recipe as a new optional capability
  `reconciler.StreamReplicationValidator` —
  `ValidateStreamReplication(cfg provider.Provider, replication int) error` —
  checked by `internal/application/compatibility` for every EventStream whose
  realizing provider implements it. A capability interface (not a hardcoded
  `brokers` read in the compatibility layer) because "how many replicas can
  this stream backend host" is provider knowledge, exactly like
  `SupportedSchemaFormats`.
- **Apply-time:** `ensureTopic` creates topics with the declared RF; an
  existing topic whose RF differs is refused with a recreate remedy (Kafka
  cannot change a topic's RF short of a partition reassignment, which is out
  of scope), mirroring the existing partition-shrink refusal.

### a.8 The HighAvailability gate at validate

`brokers > 1` requires the `HighAvailability` gate (Alpha/disabled, registered
since C1) **at validate time** — closing ADR 004's deferred accept line.
Deviation from doc 08's "via the provider's SpecValidator" wording, recorded
explicitly: `SpecValidator.ValidateSpec(cfg provider.Provider)` deliberately
has no feature-gate access (gates live in `internal/application/featuregate`;
widening the port signature would break all seven implementors — the exact
breakage pattern F5/ADR 016 exists to prevent), and no provider in this
codebase consults gates. The established validate-time gate mechanism is a
`loadAndValidate` scan in `cmd/platformctl/root.go` (`checkExternalGate`,
D1's `checkSchemaRegistryGate`); C2 adds `checkHighAvailabilityGate` in that
exact pattern: any Provider declaring `configuration.brokers > 1` fails
validate with the gate's standard disabled message. The substance of the
accept line — failing at `validate`, not at `apply` — is met; the registry's
`haGuardRuntime` decorator remains the apply-time backstop. The redpanda
`ValidateSpec` still owns everything gate-independent: `brokers` must be an
integer ≥ 1, and host-port pins are refused with `brokers` declared (a.4).

---

## Question (b): the state-level replica representation

**Decision: per-ordinal facts do not enter the state format. `ResourceState`
stays one entry per resource with aggregate status; per-broker *published
endpoint facts* ride in `providerState` (the channel that already exists for
published facts); per-broker *liveness* stays runtime-observed at Probe time
and is never persisted. No state version bump.**

Concretely, the broker Provider's `providerState` gains additive fields:
`brokers` (the declared count, echoed for operators and `state doctor`),
`internalAddr` (the comma-joined ordinal bootstrap list), and per-ordinal
entries in the existing `endpoints` list (`kafka-0` … `kafka-(N-1)`, each with
its runtime name, container port, audience, observed host address, and
in-network address) alongside the aggregate `kafka` endpoint. All of this is
`map[string]any` content inside the existing `ResourceState.ProviderState` —
the state format's version and migration chain are untouched.

**Options considered:**

1. **Per-ordinal child entries in `state.ResourceState`** (one row per
   broker). Rejected: state is keyed by *resource identity*, and an ordinal is
   not a resource — it is runtime cardinality that ADR 004 deliberately kept
   below the port's `Name` abstraction (one `ContainerSpec`, one aggregate
   `ContainerState`). Ordinal names are deterministic
   (`runtime.OrdinalName` + the naming authority), so persisted per-ordinal
   rows carry no information a consumer cannot re-derive — they would only add
   a scale-time sync hazard (every `brokers` change churns row count) and a
   state-format migration for zero read value.
2. **Persisting per-broker liveness/health in state.** Rejected: state records
   intent and published facts; observations rot the moment they are written
   (ADR 012's determinism line). "Which broker is missing" is exactly what
   `Probe` answers live — persisting it would let `status` lie about a broker
   that died after the last apply.
3. **Chosen: aggregate entry + additive `providerState` facts.** Endpoint
   facts are legitimately *published* (they cannot be re-derived by consumers:
   observed host addresses are runtime-allocated — ADR 015's "publish, don't
   construct"), so they belong in `providerState`; everything re-derivable or
   observational stays out.

---

## Why this doesn't box anything out

- Manifests without `configuration.brokers` are byte-for-byte unchanged
  (shape, command, ports, endpoints, state) — the ADR 014 bar.
- The `StableIdentity`-at-1 amendment is additive at the port: no existing
  spec sets the field; the amended conformance pin covers the transition that
  remains refused, and new pins cover the new shape.
- `StreamReplicationValidator` is an optional capability interface — the
  ADR 009 pattern; providers that never implement it validate exactly as
  before.
- The dialer map degrades to today's single-pair behavior for one broker; the
  legacy path doesn't even construct it.
- C4 (MinIO) inherits the whole pattern: declared-count opt-in shape, ordinal
  seeds/identity via `HOSTNAME`, per-ordinal endpoint facts, aggregate state.

## Follow-ups (non-blocking)

- `schemaRegistry: enabled` is refused alongside `brokers` for now (scope
  control; the registry is cluster-wide in Redpanda and can be exposed through
  any ordinal once a consumer needs it against a cluster).
- Only ordinal 0's metrics endpoint is published as the `metrics` fact; C9's
  prometheus provider scrapes one broker of a cluster until per-ordinal scrape
  targets are modeled.
- Per-ordinal `node-port`/`load-balancer` access on Kubernetes remains
  unimplemented (ADR 004's known limitation); multi-broker on Kubernetes uses
  the per-ordinal port-forward path regardless of the declared access mode.
- D10 (Trino workers) should extend `checkHighAvailabilityGate` (or its own
  scan) for `configuration.workers > 1`, per a.8's mechanism.
- Plumbing destructive intent into `reconciler.Request` would upgrade a.5's
  refusal into a flag-gated scale-down; the refusal site is a single function.

## References

docs/planning/08 §C2 (task), §C1 status (merge record); ADR 004 (primitive,
amended here); ADR 009 (capability recipe); ADR 012 (state determinism);
ADR 014 (gate contract); ADR 015 (connectivity plane); ADR 016/F5 (request
contract, why interfaces don't widen); `internal/adapters/providers/redpanda`
(B1's dialer redirect, generalized here).
