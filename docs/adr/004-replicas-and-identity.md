# Design note 004 — Replicas and stable identity in the runtime port

**Status:** accepted; implemented in this note's own commit (port + all three
adapters + conformance + gate).
**Prompted by:** `docs/planning/08-production-readiness-plan.md` §C1 —
`ContainerSpec` models exactly one container; the Kubernetes adapter pins
`Replicas: 1` (`convert.go:117`, now `buildDeployment`'s `replicas := one`).
HA data services (Redpanda — C2, MinIO — C4, Trino — D10) need N replicas,
some with stable per-replica identity and storage (broker IDs, seed lists),
others as pure interchangeable compute (query workers). Doc 07's Cross-Runtime
section explicitly called this "deliberately not more" for the first
Kubernetes adapter; this note is the honest reversal of that scope line, now
that Stage B has closed and a second runtime exists to make replication real.

## The question

What is the smallest port-level vocabulary that lets a provider ask for N
replicas of a `ContainerSpec`, covers both "N interchangeable compute units"
(Trino workers: D10) and "N members of a stateful, individually-addressed
cluster" (Redpanda brokers: C2; MinIO nodes: C4), and translates faithfully
to Docker (N separate containers — Docker has no native replica-set concept),
Kubernetes (Deployment vs. StatefulSet — two genuinely different native
objects), and the fake (in-memory, engine-test-faithful)?

## Options considered

1. **A single `Replicas int` field, always stable-identity.** Rejected: it
   forces per-replica storage and ordinal DNS onto Trino workers (D10), which
   hold no durable state and want a plain load-balanced Service — modeling
   them as a degenerate StatefulSet wastes the simpler, already-correct
   Deployment-scaling path Kubernetes offers, and gives Docker N needlessly
   distinct per-ordinal volumes for workers that declare no volumes at all
   anyway. It also cannot express "I want horizontal scale but explicitly no
   per-replica storage semantics," which D10's own design note (006) already
   commits to (`StableIdentity: false` for Trino workers).
2. **Two orthogonal fields as the roadmap doc's own recommendation:
   `ContainerSpec.Replicas int` + `ContainerSpec.StableIdentity bool`
   (chosen).** `Replicas` alone drives horizontal count on every runtime;
   `StableIdentity` is the only field that changes *shape* (Deployment vs.
   StatefulSet on Kubernetes; shared vs. per-ordinal volumes on Docker).
   `Replicas > 1, StableIdentity: false` is C1's simpler case (D10, Trino);
   `Replicas > 1, StableIdentity: true` is the harder case (C2, Redpanda) —
   exactly the sequencing note 006 anticipated ("a reasonable second adapter
   of the primitive after C2").
3. **A separate `ReplicaSetSpec` port method distinct from
   `EnsureContainer`.** Rejected: every provider, every conformance subtest,
   and every drift/GC code path already keys off `ContainerSpec` and
   `ContainerState`; a parallel type doubles the surface for zero behavioral
   gain, and (worse) reopens the "N replicas of a stateful set" question as
   a second port shape a future contributor has to learn instead of one
   `Replicas`/`StableIdentity` pair. `Ensure*` idempotency, drift, and the
   ownership-label GC story all keep working unmodified because the
   *shape* of the port doesn't change, only the cardinality one spec can
   produce.

## The decision

### Port shape

```go
type ContainerSpec struct {
    ...
    // Replicas is the desired size of this ContainerSpec's replica set. 0
    // and 1 are equivalent to today's single-container behavior,
    // byte-for-byte (ContainerSpec.ReplicaCount() normalizes this). N > 1
    // requires the HighAvailability feature gate (checked by
    // application/registry's runtime decorator on every EnsureContainer
    // call, not by a per-provider check — see "Feature gate enforcement"
    // below) and fans out to N distinct runtime-managed units, addressed
    // collectively by Name and individually by ordinal name
    // ("<Name>-0".."<Name>-(N-1)", runtime.OrdinalName).
    Replicas int
    // StableIdentity, meaningful only when Replicas > 1, additionally gives
    // each ordinal its own persistent volume set and a stable per-ordinal
    // hostname reachable independent of which other ordinals are up.
    // false means replicas are interchangeable pure-compute units with no
    // per-ordinal storage (Replicas > 1 alone is enough for horizontal
    // scaling of, e.g., Trino workers — D10).
    StableIdentity bool
}

type ContainerState struct {
    ...
    // ReadyReplicas is the number of replicas currently observed ready, out
    // of ContainerSpec.Replicas requested (1 for a non-replicated
    // container). Healthy reports "at least one replica ready" — it never
    // flips false merely because ReadyReplicas < Replicas. A provider that
    // cares about full-quorum health (C2's Redpanda, comparing
    // ReadyReplicas against EventStream.spec.replication) computes that
    // itself; the port makes the raw number available and asserts nothing
    // about its meaning. This is the "provider decides" rule the roadmap
    // doc's Accept criterion names explicitly.
    ReadyReplicas int
}

func (s ContainerSpec) ReplicaCount() int  // normalizes 0/1
func OrdinalName(name string, i int) string // "<name>-<i>", every adapter's exact format
```

Two new ownership-label constants, `LabelReplicaBase` and
`LabelReplicaOrdinal`, let the Docker adapter (which has no native
"replica set" object — see below) group N separately-named containers back
into one logical set via a label query, mirroring how Kubernetes gets the
same grouping for free from object ownership.

### Naming and addressing

- **Always** (`Replicas > 1`, either `StableIdentity` value): each replica
  gets an ordinal-suffixed identity, `"<Name>-0".."<Name>-(N-1)"`. This is
  forced on Docker regardless of `StableIdentity` — Docker containers must
  be uniquely named per host, unlike Kubernetes Pods, which get random
  suffixes under a Deployment. Making the *naming* convention uniform
  across both values of `StableIdentity` means a caller (or conformance
  test) can always predict an ordinal's name without needing to know which
  branch produced it.
- **`StableIdentity: false`, `Replicas > 1`** (D10, Trino workers): on
  Kubernetes this stays a single Deployment with `Replicas: N` — no
  structural change, ordinary ClusterIP Service load-balancing across all N
  pod endpoints (a strictly *better* "any of them" address than Docker gets
  here, a genuine, documented per-runtime asymmetry). On Docker, N
  containers still exist (forced by unique-naming), each joining the shared
  network with the collective name (`Name`) as an *additional* network
  alias — Docker's embedded DNS round-robins across containers sharing an
  alias, the closest analog to a Kubernetes Service's virtual-IP
  round robin it has. Volumes, if declared at all on such a spec, are
  **not** ordinal-suffixed — every replica shares the same named
  volume/PVC. Providers should not declare `Volumes` on a
  `StableIdentity: false` multi-replica spec (D10 already commits to this:
  "Trino workers need no per-replica storage"); doing so anyway is a
  configuration hazard this port does not prevent (a Kubernetes RWO PVC
  cannot attach to pods on different nodes; Docker permits concurrent
  writers to one volume with no coordination) — documented here, not solved
  here, matching the "genuine per-runtime difference, not a bug" standard
  doc 07 already applies to `RestartPolicy`/`LogConfig`/`SecurityOpt`.
- **`StableIdentity: true`** (C2, Redpanda; C4, MinIO): on Kubernetes this
  becomes a StatefulSet named `Name`, governed by a **headless Service**
  (`ClusterIP: None`) also named `Name` — StatefulSet pods are *natively*
  named `"<Name>-<i>"` by Kubernetes itself, so no custom naming code is
  needed there; the headless Service gives each pod DNS
  `"<Name>-<i>.<Name>.<namespace>.svc.cluster.local"`, and the namespace's
  default search domain makes the short form `"<Name>-<i>"` resolve too —
  matching Docker's per-ordinal container-name resolution exactly. There is
  deliberately **no** plain `"<Name>"` ClusterIP address for a
  `StableIdentity` set — that address never had one coherent meaning for a
  cluster whose members are not interchangeable (a client needs the
  specific broker holding a partition leader, not "any one"), the same
  reason headless services exist in the first place. `EnsureReachable`/
  `ReadFile`/`Logs` against the bare `Name` of a `StableIdentity` set
  resolve to ordinal 0 as a documented best-effort default (see
  "Known limitations" below) — real per-replica admin access should always
  address an ordinal name directly.

### Volumes: who owns the per-ordinal lifecycle

When `StableIdentity`, the **runtime owns the entire volume lifecycle for
ordinal storage** — a provider must not call `EnsureVolume`/`RemoveVolume`
for the per-ordinal names itself:

- **Kubernetes**: `VolumeClaimTemplates` on the StatefulSet, one per
  distinct `VolumeMount.VolumeName` the spec declares, sized from
  `defaultVolumeSizeBytes` (10Gi, the adapter's existing `EnsureVolume`
  default) since `ContainerSpec`/`VolumeMount` carries no per-volume
  size/StorageClass metadata today (`VolumeSpec` does, but a
  `StableIdentity` caller never calls `EnsureVolume` — see "Follow-ups").
  Kubernetes creates and owns the per-ordinal PVCs
  (`"<claim>-<Name>-<i>"`) automatically; the pod template's
  `VolumeMounts` reference the claim template name unchanged, so no
  adapter code manufactures per-ordinal PVC objects by hand.
- **Docker**: the adapter creates one Docker volume per
  `"<VolumeMount.VolumeName>-<i>"` directly (`VolumeCreate`, same
  unsized/unlabeled-beyond-ownership call `EnsureVolume` already makes) the
  first time each ordinal container is created, and mounts it into that
  ordinal only.
- **Removal is deliberately conservative**: `Remove(Name)` on a
  `StableIdentity` set tears down every ordinal *container/pod* but leaves
  per-ordinal volumes/PVCs in place — mirroring how `Remove` has never
  touched volumes for a single container either (`RemoveVolume` is always a
  separate, explicit call). On Kubernetes this means leaving the
  StatefulSet's `persistentVolumeClaimRetentionPolicy` at its default
  (retain-on-delete) rather than opting into `WhenDeleted: Delete` — the
  safer default given `docs/planning/08` A5's "data-bearing protection"
  concern and C6's not-yet-shipped backup/restore story. A caller that
  wants to actually reclaim ordinal storage calls `RemoveVolume` (Docker)
  or deletes the labeled PVCs directly (Kubernetes) — both are already
  individually visible and GC-able via `ListManagedVolumes`, since every
  per-ordinal volume/PVC still carries the standard ownership labels.

### PodDisruptionBudget and anti-affinity (Kubernetes only)

Applied automatically whenever `Replicas > 1` on Kubernetes, **independent
of `StableIdentity`** — both the Deployment path (D10-shaped) and the
StatefulSet path (C2-shaped) get:

- A `PodDisruptionBudget` (`maxUnavailable: 1`) selecting `app: Name` —
  guards against voluntary disruption (node drain, cluster upgrade) taking
  down more than one replica at a time, regardless of whether the replicas
  are stateful.
- Soft (`preferredDuringSchedulingIgnoredDuringExecution`, weight 100)
  pod anti-affinity on the same `app: Name` label and
  `topologyKey: kubernetes.io/hostname` — spreads replicas across nodes
  when the scheduler can, without making single-node clusters (minikube,
  kind, CI) fail to schedule the way a *hard*
  (`requiredDuringScheduling...`) rule would have.

Docker has no disruption-budget or scheduler-anti-affinity concept (a
single daemon *is* the placement domain); this section is Kubernetes-only
by construction, the same "genuine per-runtime difference" pattern as
`Resources.CPUReservation`.

### `ContainerState.Healthy`/`ReadyReplicas`: the "provider decides" rule

The port-level rule chosen: **`Healthy` reports "at least one replica
ready," exactly the same rule the pre-existing single-container Deployment
code already used (`d.Status.ReadyReplicas > 0`)** — extended unchanged to
N replicas rather than redefined. This means killing one of N replicas
never flips the aggregate `Healthy` to `false` as long as at least one
remains — satisfying the roadmap's Accept criterion directly, and
requiring zero new "how healthy is healthy enough" policy at the port
layer. `ReadyReplicas` is exposed raw so a provider (C2's Redpanda probe,
comparing `ReadyReplicas` against `EventStream.spec.replication`, or a
future MinIO probe comparing against its erasure-coding minimum) computes
whatever quorum-relevant meaning its own technology needs. The port
deliberately does not attempt to guess "is 2 of 3 enough" — that is
provider domain knowledge, not a generic runtime concept, matching the
same division of responsibility `RestartPolicy.Mode`/`HealthCheck` already
draw between "the runtime enforces a mechanism" and "the provider defines
what healthy means for its technology."

### Feature gate enforcement

`HighAvailability` (Alpha, disabled) is registered in `cmd/platformctl/main.go`
and enforced by a **runtime-decorator** in `internal/application/registry`
(`Registry.Runtime` wraps every constructed adapter in a thin
`haGuardRuntime` that checks the gate on `EnsureContainer` whenever
`spec.ReplicaCount() > 1`), rather than a provider-level `SpecValidator`
check. This is the deliberate choice for C1 specifically: **no provider yet
exposes a schema field that sets `Replicas` at all** — that first happens
in C2 (`Provider(type: redpanda).spec.configuration.brokers`) and D10
(Trino's worker count). A `SpecValidator` hook only fires for a provider
that implements one, and would need re-adding, correctly, once per new
replica-capable provider; a decorator at the single choke point every
provider's `Request.Runtime` already passes through enforces the invariant
once, for every current and future provider, with no per-provider code to
forget. **C2 and D10 should still add their own `SpecValidator`-level
check** (`spec.configuration.brokers > 1` / worker count `> 1` requires
`HighAvailability`) for the better DX of failing at `platformctl validate`
rather than at `apply` — the decorator is the correctness backstop, not a
replacement for that earlier, friendlier failure point. This is recorded
explicitly as a non-blocking follow-up below so it isn't lost.

## Known limitations (honest, not silently punted)

- `EnsureReachable`/`ReadFile`/`Logs` against the **aggregate** name of a
  `StableIdentity` multi-replica set resolve to ordinal 0 as a
  best-effort default on Docker and the fake; the Kubernetes adapter
  requires an explicit ordinal name for these three methods against a
  StatefulSet-backed set (it has no bare-`Name` Service/pod to fall back
  to the way Docker's shared-alias round robin does). Callers that need a
  *specific* replica (which is the only sensible ask for a
  `StableIdentity` set) should always address it by ordinal name; this
  limitation only affects the "give me any one" convenience path, which is
  undefined territory for a stable-identity set by the same logic that
  motivated a headless Service having no round-robin ClusterIP in the
  first place.
- `EnsureReachable(ordinalName, port)` on Kubernetes only implements the
  `port-forward` access mode against a specific ordinal Pod; `node-port`/
  `load-balancer` addressing of one specific ordinal is not implemented in
  this change (the per-container external-NetworkPolicy/Service machinery
  from B1/B7 was built for single-container Deployments and would need
  its own ordinal-aware extension). No shipped provider needs per-ordinal
  external access yet; flagged here for whoever adds one.
- Per-ordinal environment differentiation (e.g. a Redpanda broker's own
  `--node-id`, or a seed list naming its peers) is **not** a runtime-port
  concern in this change — every ordinal receives an *identical* `Env`
  from one `ContainerSpec`. C2's provider is expected to either template
  `Env` per call (if `EnsureContainer` gains a per-ordinal-call shape,
  which this change does not add) or have its container image/entrypoint
  derive its own identity from `HOSTNAME`, which for a `StableIdentity`
  ordinal equals the ordinal name on every adapter — but *not* for free on
  Docker: Kubernetes gets it from the StatefulSet's headless-Service pod
  DNS, while the Docker adapter must explicitly set the container's
  `Config.Hostname` to the ordinal name (it otherwise defaults to Docker's
  random short container ID, diverging from Kubernetes — a real defect
  caught by running the Docker conformance suite, now fixed and pinned by
  `ReplicaSet_PerOrdinalVolumePersistence`, whose written content embeds
  `$(hostname)`). The exact identity mechanism is still left to C2 to
  decide against a real image, not guessed here.
- `VolumeMount`/`ContainerSpec` carry no per-volume size/StorageClass
  metadata, so `StableIdentity` volumes always use the adapter's existing
  10Gi/default-StorageClass default. A provider needing a different size
  for ordinal storage has no way to request it yet; the fix is additive
  (a `Size`/`StorageClass` pair on `VolumeMount` itself) and deferred until
  a concrete provider (C2/C4) actually needs it, rather than speculatively
  widened here.
- **A fixed host-audience `HostPort` cannot be combined with
  `Replicas > 1` on Docker** — every ordinal inherits the same published
  host port, so ordinal 1's create fails with the daemon's raw
  port-already-allocated error. Replicated sets should leave host ports
  auto-allocated (or internal-audience); a validate-time refusal is C2's
  concern once a provider can actually declare a replicated spec.
- **Collective-name addressing on Docker is health-unaware** — a real
  hazard, not just an implementation detail: Docker's embedded DNS
  round-robins the shared alias across *every* container carrying it,
  including members that are still starting or currently unhealthy,
  whereas the Kubernetes Deployment path's ClusterIP Service only
  load-balances across pods whose readiness probe passes. Combined with
  `WaitHealthy`'s deliberate at-least-one-ready return, a Docker client
  that dials the collective `Name` immediately after `WaitHealthy` can be
  handed a replica that is not ready yet. Callers needing a
  guaranteed-live endpoint should retry at the connection level
  (`runtime.WithReachable` already absorbs this class) or address a
  specific ordinal name; the port does not paper over the difference.
- **Cross-adapter observable-surface asymmetries** for a `Replicas > 1`
  set — each individually harmless, all worth knowing before building
  tooling on top: (i) `ListManaged` reports N per-ordinal entries on
  Docker and the fake (each member is a real, individually named managed
  unit) but one aggregate entry per set on Kubernetes (the
  Deployment/StatefulSet is the managed object); (ii) per-ordinal storage
  names differ — Docker volumes are `"<VolumeName>-<i>"` while Kubernetes
  StatefulSet PVCs are `"<VolumeName>-<Name>-<i>"` (StatefulSet claim
  naming), so storage cleanup/GC must be adapter-aware (the conformance
  suite's per-ordinal-volume cleanup deletes both name shapes
  best-effort); (iii) the aggregate `ContainerState.ID` is ordinal 0's
  own container ID on Docker/the fake but the StatefulSet's/Deployment's
  own UID on Kubernetes — never treat it as a stable cross-runtime
  identity; (iv) scale-down mechanics differ — Docker force-removes stale
  ordinals synchronously inside `EnsureContainer`, while Kubernetes'
  controllers terminate pods gracefully and asynchronously after the
  replica-count update returns.

## Why this doesn't box anything out

Every addition here is a new, optional field or a new label constant;
`Replicas: 0` (or unset) and `StableIdentity: false` reproduce today's
exact single-container Deployment/single-container path, unchanged —
verified by running the full existing conformance suite (which never sets
either field) unmodified against the fake, and by code inspection that the
`Replicas <= 1` branch of `EnsureContainer` in both Docker and Kubernetes
adapters is the pre-existing code, untouched. This is the same "additive,
never a revision of a published contract" discipline design notes 001–003
already established.

## Cross-references

- `docs/planning/08-production-readiness-plan.md` §C1 (this task), C2
  (Redpanda, the first `StableIdentity: true` consumer), C4 (MinIO,
  the second), §8 (gate table entry).
- `docs/adr/005-database-ha-posture.md` — explicitly named this note as
  the prerequisite substrate for a hypothetical future managed-HA-database
  mode, and drew the line that Postgres/MySQL replication topology is *not*
  "add a `replicas` field" the way Redpanda/MinIO's shared-nothing
  clustering is; this note's scope (stateless-of-topology replica fan-out)
  is consistent with that boundary.
- `docs/adr/006-compute-engines.md` — the Trino provider (D10) is this
  note's first designed (not yet implemented) `StableIdentity: false`
  consumer.
- `docs/planning/07-production-grade-docker-runtime-gap-analysis.md`
  Cross-Runtime Portability, "No coverage of multi-replica scenarios,
  PodDisruptionBudgets, or anti-affinity ... deliberately not more" — the
  item this note reverses, now that Stage B has closed.

## Follow-ups (non-blocking)

- C2/D10 should each add a `SpecValidator`-level `HighAvailability` check
  for validate-time failure, per "Feature gate enforcement" above.
- A `VolumeMount`-level size/StorageClass override, once a concrete
  provider needs ordinal storage sized differently from the 10Gi default.
- Per-ordinal environment templating (or a documented `HOSTNAME`-derived
  identity convention) once C2 is built against a real Redpanda image and
  can prove which mechanism its entrypoint actually needs.
- Ordinal-aware `node-port`/`load-balancer` `EnsureReachable` on
  Kubernetes, if a future provider needs external access to one specific
  stateful replica rather than the set's port-forward default.

## Addendum (2026-07-22) — I7: ordinal-free addressing for Deployment-shaped worker sets

**Found live by I6** (docs/planning/08 §7.8, doc 07's per-runtime-differences
dated finding): `providerkit.ReachableURLs`/`ProbeConnectWorkerSet` address a
`Replicas > 1, StableIdentity: false` set's members via
`runtime.OrdinalName(name, i)` — correct on Docker/the fake, where every
ordinal is forced onto a literal, separately-named object regardless of
`StableIdentity` (see "Naming and addressing" above), but wrong on
Kubernetes: a Deployment is exactly *one* object, and its pods get
Kubernetes-assigned random name suffixes, never `"<name>-<i>"` (only
StatefulSet ordinals get that treatment, per `findOrdinalPod`'s own doc
comment). Every ordinal lookup therefore fails outright on Kubernetes for
this shape — `no member of "<name>" (N ordinals) is currently reachable` —
even when the set itself is perfectly healthy. debezium/s3sink's
`spec.configuration.workers > 1` is this shape's first real consumer
(docs/planning/08 C3), so a `workers: 2` Binding on Kubernetes failed hard
at apply.

### The question this addendum answers

docs/planning/08 I7 posed two options: (a) ordinal-free, any-member
addressing for Deployment-shaped sets on Kubernetes — resolve the set's own
Service/pod-label-selector instead of synthetic ordinal names; or (b) switch
Connect worker sets to the `StableIdentity: true` (StatefulSet) shape, which
already has working ordinal addressing.

### The decision: (a), any-member addressing

Connect workers are interchangeable members of one Kafka-consumer-group
rebalancing set — they hold no per-worker durable state and no per-worker
identity a caller could ever need specifically (unlike Redpanda brokers,
where a client needs the *specific* broker holding a partition leader).
"Any one live member can serve the group's REST API" is not a compromise for
this shape, it is the semantically correct address — precisely the same
reasoning that already made Kubernetes' plain `ClusterIP` Service (rather
than the `StableIdentity` branch's headless Service) the right choice for
`StableIdentity: false` sets in this note's original decision. Option (b)
was rejected for the reason the original decision already gives for keeping
Connect workers off `StableIdentity`: it would force per-ordinal volumes and
hostnames onto a workload that needs neither, trading a real simplification
(Deployment + Service scaling) for a heavier shape solely to work around an
addressing bug — the same "wastes the simpler, already-correct path" logic
Option 1 was rejected for above.

### The mechanism: no new Kubernetes reachability code

The fix needed **zero changes** to `internal/adapters/runtime/kubernetes`'s
actual reachability logic: `EnsureReachable`/`Inspect`, called with a
Deployment-shaped set's own bare `Name` (not an ordinal name), already
resolve correctly — the Deployment's `Service` load-balances across every
ready pod (or, for the default port-forward access mode, a label-selector
pick of the newest ready pod), and `Inspect(name)` already reports the
Deployment's own aggregate `ReadyReplicas`. The only thing missing was a way
for `providerkit` (which must stay runtime-agnostic — it imports only
`internal/ports/runtime`, never a concrete adapter) to know *when* to prefer
the set's bare name over the ordinal loop. That seam is a new optional
`ContainerRuntime` capability, `runtime.MemberSetRuntime`
(`AddressesMembersCollectively() bool`), following `IngressCapableRuntime`'s
exact type-assert-an-optional-capability pattern: Kubernetes implements it
(`return true`); Docker and the fake do not implement it at all, so the
type assertion fails there and `providerkit`'s original per-ordinal loop
runs unchanged — the Docker `connect-ha-dlq` suite's behavior and semantics
are untouched by this change.

**The registry-wrapper gotcha, caught before it repeated:** the 2026-07-21
addendum to docs/adr/018 recorded that `application/registry.haGuardRuntime`
embeds `runtime.ContainerRuntime` as an *interface* field, so it only
promotes that interface's own declared method set — a provider's
`req.Runtime.(runtime.IngressCapableRuntime)` assertion silently failed for
every registry-obtained runtime until `haGuardRuntime` grew explicit
delegating methods. Read before writing any code this time, so
`AddressesMembersCollectively` got its own explicit delegating method on
`haGuardRuntime` in the same commit, pinned by
`TestRuntime_PromotesMemberSetRuntime` (`internal/application/registry/registry_test.go`)
— never reproduced live.

### Consequence

`providerkit.ReachableURLs`/`ProbeConnectWorkerSet` now resolve/probe a
Deployment-shaped set once, by its own `Name`, on any runtime implementing
`MemberSetRuntime`; `ProbeConnectWorkerSet`'s missing-member reason on such a
runtime names a ready/expected count (`ConnectWorkerMissing(1/2 ready)`)
rather than ordinal names, since there is nothing per-ordinal to name there
— documented on `status.ReasonConnectWorkerMissing` itself. `workers: 2` on
Kubernetes now applies, drifts, and self-heals (native Deployment-controller
pod replacement) the same way it always has on Docker, closing docs/planning/07's
dated multi-replica finding and doc 08 I6's "unconditional GA" carve-out for
Connect-worker HA specifically.

### Cross-references

- docs/planning/08-production-readiness-plan.md §7.8 I7 (this task), I6 (the
  live finding this closes).
- docs/planning/07-production-grade-docker-runtime-gap-analysis.md's dated
  multi-replica finding (closed by this addendum).
- docs/adr/018-ingress-routing.md's 2026-07-21 addendum (the registry-wrapper
  pitfall this addendum's mechanism section deliberately avoided repeating).
