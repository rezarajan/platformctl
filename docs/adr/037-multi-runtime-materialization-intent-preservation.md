# ADR 037 — Multi-runtime provider materialization: intent as a set, materializers as verified functions

**Status:** proposed (2026-07-24). **Prompted by:** bringing the M7 example
up on Kubernetes and the owner's follow-on direction. Two forces meet here.
(1) Production Kubernetes wants production *mechanisms* — official Helm
charts, tiered PVCs, StatefulSet nuances — not one hand-rolled
container-shaped path for every runtime; a Provider legitimately needs
different *routes* for different runtimes because the runtimes have real,
irreducible nuances. (2) Whatever route is taken, **the user's original
intent must not be misrepresented**: a fast storage tier may be
undeliverable on Docker, but the *size* must match on both; Docker has no
StatefulSets, but if HA is asked for and the Provider realizes it with
ordinals, then Docker must synthesize matching ordinals. The governing
philosophy the owner set: **at scale you stop relying on assumptions and
let the math do the heavy lifting — the engine's translation must undergo
formal verification of correctness.** This ADR proposes the design; it is
not yet implemented.

## The shape of the problem

Today a Provider has one runtime-neutral reconciler emitting
`ContainerSpec`/`VolumeSpec` to the `ContainerRuntime` port, whose Docker
and Kubernetes adapters realize it. That is *already* a materialization
seam and it works — but it is a single mechanism, it cannot express
"use the official Redpanda Helm chart on Kubernetes," and its cross-runtime
agreement is a matter of careful coding, not proof. We want to (a) let a
Provider offer runtime-specific routes, (b) keep runtime specifics out of
the domain (the one layering invariant), and (c) *prove* every route
preserves intent.

## Move 1 — Intent: a set-theoretic minimal capture (runtime-neutral, in the domain)

For each Provider type `p`, define its **Intent** `I_p` as the *minimal*
set of user-facing fields that fully determine the desired platform
behaviour, independent of any runtime. In user terms: **what the operator
wants, never how a runtime delivers it** — `brokers: 3`,
`storage: {size: 200Gi, tier: fast}`, never `kind: StatefulSet` or a
`storageClassName`. `I_p` is a point in a product space,
`I_p ⊆ Π_i (field_i : Domain_i)`, and *minimal* means no field is
derivable from the others and dropping any loses a distinct, user-meaningful
degree of freedom.

Partition the fields by their **cross-runtime obligation** into two
disjoint classes:

    I_p  =  C_p  ⊎  B_p

- **`C_p` — the invariant core.** What every runtime MUST realize *exactly*,
  or the materialization is refused. For Redpanda: replica count (`brokers`),
  per-replica durable-storage **size**, stable per-ordinal identity when
  replicated, credential bindings, and the **reachability relation** (which
  peers may connect — the ADR 035 zero-trust graph). These are *equalities*
  the materializer must preserve.
- **`B_p` — the best-effort hints.** What a runtime MAY be unable to honour
  fully — storage performance **tier**, a specific StorageClass, node
  affinity / topology spread. These are governed by a *partial order*, and
  any shortfall must be surfaced as an explicit fact — never silently
  dropped, and **never** honoured at the cost of a `C_p` property.

The `C_p ⊎ B_p` split is the whole game: it is exactly the encoding of
"size must match on both, tier may degrade." Size ∈ `C_p` (equality); tier
∈ `B_p` (ordered, explicit degradation).

## Move 2 — Materializers: a family of functions selected by the project runtime (adapters)

For a runtime `r`, a **materializer** is a total function on the invariant
core, `M_r : I_p → R_r`, where `R_r` is the space of runtime-`r`
realizations — Docker: containers/volumes/networks; Kubernetes: a set of API
objects *or a Helm release*; Terraform: a plan. A Provider **registers the
set of materializers it offers**, `Mat_p ⊆ {M_docker, M_k8s, M_tf, …}`. The
engine, given the project runtime `r` (ADR 035), selects `M_r ∈ Mat_p`, or
refuses at validate with a precise message when `r ∉ dom(Mat_p)`
("Provider `redpanda` does not materialize on kubernetes").

Hexagonal placement (the invariant is non-negotiable):

- **domain** owns `I_p` — a value type that imports nothing runtime.
- **ports** own the `Materializer` interface (and the existing
  `ContainerRuntime` port, which is the *default materializer's* substrate).
- **adapters** own the concrete materializers. A Kubernetes materializer MAY
  be **Helm-backed** — it maps `I_p → chart values`, pins the chart version,
  and drives the Helm SDK — with no Helm type ever visible to the domain. A
  Docker materializer emits containers; a Terraform materializer emits HCL.
- **registry** wires `Mat_p` per Provider and selects by project runtime.

This is a **superset** of today, not a rewrite. Most Providers keep one
runtime-neutral reconciler emitting to the `ContainerRuntime` port — which
IS the shared default materializer whose Docker/K8s adapters already exist.
A Provider only registers a bespoke per-runtime materializer when the nuance
earns it (Helm on K8s, tiered PVCs). Default path = the container port;
escape hatch = a Provider-specific materializer.

## Move 3 — Intent preservation as a machine-checked invariant, not a convention

State the preservation obligations as predicates over `(I, M_r(I))`:

**Core equalities — hold for every runtime `r`:**

- **CAP (capacity):** realized durable-storage size `=` `I.storage.size`, on
  every runtime. Docker's volume may be unsized by its driver, but the size
  it records/annotates must equal `I`'s — an operator reads the *same number*
  on both.
- **REP (replication):** `|replicas realized| = I.replicas`. If the Provider
  realizes replicas with ordinals (a StatefulSet on K8s), Docker MUST
  synthesize `I.replicas` ordinal containers+volumes with the same
  per-ordinal identity and per-ordinal durable storage — *matching
  functionality* (ADR 004's existing synthesis, promoted from happenstance
  to obligation).
- **IDN (identity):** stable per-ordinal identity (hostname / mount /
  credential per ordinal) is preserved across runtimes.
- **REACH (reachability):** the who-may-reach-whom relation (ADR 035
  zero-trust / graph-scoped access) is realized as the *same relation* even
  where the enforcement substrate differs (K8s NetworkPolicy vs a Docker
  network). Where a runtime *cannot enforce* it — e.g. a CNI without policy
  support (found live: minikube's Calico did not enforce NetworkPolicy) —
  that gap is a **surfaced diagnostic** (ADR 031), because a silently
  unenforced restriction is itself an intent misrepresentation.

**Best-effort orderings — hold for every `r`, degradation explicit:**

- **TIER:** `tier(M_r(I)) ⊑ tier(I)`; if strict, emit a declared "degraded"
  fact naming requested-vs-delivered. Never resolve a `B_p` shortfall by
  violating a `C_p` equality (never shrink the volume to fit a tier).

**Formal verification of the translation — two mechanized layers:**

1. **A formal model (Alloy, or TLA+ where reconcile/heal timing matters).**
   Model the intent algebra and the materialization relation; state CAP /
   REP / IDN / REACH as theorems and **model-check** them over a finite
   scope. Alloy fits the set-theoretic core (the `C ⊎ B` partition, ordinal
   sets, the reachability relation); it proves the *design* is internally
   consistent and that no materializer satisfying the interface can meet the
   type yet violate a core equality. This is "let the math do the heavy
   lifting": the properties are checked, not assumed.
2. **An executable conformance suite (property-based, e.g. gopter/rapid).**
   The same predicates as tests: for every Provider × registered runtime,
   generate random Intents, materialize, and assert every core equality and
   best-effort ordering. This generalizes the existing runtime
   contract/conformance suite (architecture §9) from "the adapter is
   idempotent" to "the materializer preserves intent," and it is the CI gate
   proving the *implementation* refines the model.

Add the sharpest cross-runtime check of all — **differential (metamorphic)
verification:** materialize the *same* Intent under two runtimes and assert
the `C_p` projections are equal (same replica count, same declared size,
same reachability relation). This directly enforces "the same intent means
the same thing on Docker and Kubernetes," which is the property the owner is
actually asking for. The Alloy model is the specification; the property +
differential suites are the mechanized refinement check. A new Provider or a
new runtime route is not "done" until it passes them; changing a core
equality starts by changing the model.

## Consequences

- Providers get a principled path to production-grade, runtime-specific
  materialization (Helm, tiered storage) **without** leaking runtime
  specifics into the domain — the `Materializer` port holds the line.
- Intent is *provably* preserved: "200Gi, 3 brokers, these peers may
  connect" means the same on Docker, Kubernetes, and Terraform — or the
  divergence is an explicit, ordered, surfaced fact.
- **Costs, stated up front.** Authoring the Alloy/TLA+ model and the
  property + differential suites is real work. An external Helm chart lives
  *outside* our verification boundary, so it must be version-pinned, its
  value mapping verified, AND followed by a post-materialization assertion
  (the realized StatefulSet actually has `I.replicas` and `I.storage.size`)
  — the chart is trusted only as far as that assertion checks.
- **Risk: over-formalization.** Mitigation: the model covers the *core*
  (`C_p` equalities + reachability); best-effort hints are covered by the
  property suite alone. Pilot on Redpanda + MinIO (the STS/storage-heavy
  Providers) before generalizing.

## Relationship to existing decisions

- **ADR 004** (replicas/identity): the ordinal-synthesis-on-Docker vs
  StatefulSet-on-K8s split *is* REP/IDN — implemented already, here promoted
  to a verified invariant.
- **ADR 035** (just-works DX, project runtime): the project runtime is the
  materializer *selector*; Intent minimality is that ADR's "declare
  resources, it just works" made formal.
- **ADR 036** (storage size/tier): CAP (size ∈ `C_p`) and TIER (tier ∈
  `B_p`) are exactly this ADR's storage slice — 036 becomes the concrete
  first instance of 037's partition.
- **`ContainerRuntime` port + Docker/K8s adapters:** the default
  materializer; bespoke per-runtime materializers are the opt-in superset.

## Follow-up

Sequence a plan: (1) extract the Intent value types and the `C ⊎ B`
partition for redpanda/minio/postgres; (2) add the `Materializer` port +
registry selection by project runtime, keeping the container port as the
default; (3) author the Alloy model of CAP/REP/IDN/REACH; (4) build the
property-based + differential conformance suites and gate CI on them;
(5) pilot a Helm-backed Kubernetes materializer for redpanda behind a
feature gate; (6) wire the degraded-tier and unenforced-reachability
diagnostics through the ADR 031 channel.
