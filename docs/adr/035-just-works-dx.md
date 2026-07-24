# ADR 035 — The just-works DX: one runtime per project, auto-provisioned everything, zero-trust by default

**Status:** accepted (2026-07-24). **Prompted by:** the owner's review of
the `examples/zero-trust-lakehouse` capstone, which — while functionally
complete — exposed how far the developer experience had drifted from the
Datascape premise: *"define the resources you want, wire them up over
Connections, and everything JUST WORKS."* Anything a developer must reason
about beyond that is a product failure. This ADR is the corrective.

## The one principle

A developer declares **resources** (Providers, Sources, Datasets,
Catalogs), wires them with **Connections** and **Bindings**, and runs
`apply`. They never think about: which zero-trust mechanism is in play,
what labels make a connection work, what port a connection listens on,
what runtime each provider targets, or how much memory a provider needs.
Every one of those is either inferred, auto-provisioned, or defaulted —
with an explicit escape hatch when (and only when) the developer wants
control.

## Decisions

### 1. One runtime per project (the Go-module shape)

A **project** is a directory tree of manifests that targets exactly one
runtime. The runtime is declared **once**, at the project level — not
per-Provider — via a project config (`datascape.yaml` at the project
root, a new lightweight document read before the manifest set):

```yaml
# datascape.yaml
apiVersion: datascape.io/v1alpha1
kind: Project
metadata: { name: orders-platform }
spec:
  runtime:
    type: docker            # the ONE runtime for every Provider here
    # network/access/etc. are runtime-level, not per-provider
  zeroTrust: true           # default; see decision 3
```

- Providers **drop** their `spec.runtime` block. If a Provider still
  carries one it is an explicit per-provider override (validated to match
  the project runtime family, or refused with a clear message) — but the
  common, documented path is: no runtime on any Provider.
- **Multiple runtimes = multiple project folders**, each with its own
  `datascape.yaml`. Datascape does not wire across runtimes; that is
  Terraform/OpenTofu's job in a future phase, consuming Datascape's
  published endpoint facts (ADR 015) — the boundary this decision keeps
  clean.
- The engine resolves every Provider's runtime from the project config,
  so the per-Provider `RuntimeType` plumbing (ADR 007 amendment, ADR 030)
  continues to work unchanged internally — it is just *populated from one
  place* instead of repeated in every manifest.

### 2. Auto-provisioned Connection ports

`Connection.spec.port` becomes **optional**. Omitted, the entrypoint's
listen port is auto-allocated deterministically (the existing
`internal/domain/hostport` allocator that already gives every Provider a
stable host port). Consumers never learn or need the port — they resolve
the connection's address from its published endpoint fact (ADR 015), as
they already do. A developer pins a port only when an outside system
requires a fixed one; the default is silence.

### 3. Zero-trust by default — one concept, auto-compiled, invisible

The four gates `MediatedConnections`, `GraphScopedAccess`,
`LabelScopedAccess`, `PolicyEngine` collapse into **one**: `ZeroTrust`,
**enabled by default**. A developer flips zero-trust off with a single
`spec.zeroTrust: false` (project level) or the CLI `--no-zero-trust`
flag — never anything finer-grained.

With zero-trust on (the default):

- **Every Connection is identity-mediated.** Declaring a Connection makes
  its target dark and reachable only through an identity-attested,
  per-edge-authorized path. No `type: openziti` Provider ceremony beyond
  declaring *a* mediation provider for the project (or the engine standing
  up the platform-owned fabric, ADR 034 L2, when none is declared); no
  labels; no ports.
- **Access is graph-scoped automatically.** Least-privilege networking is
  derived from the declared graph. The developer declares the edges (via
  Connections/Bindings/refs) they want; nothing else is reachable.
- **Policies are auto-compiled from the graph.** Every Connection and
  Binding auto-generates the zero-trust *allowance* it needs — e.g. "this
  CDC Binding may reach this dark database through this Connection." This
  is the baseline the developer never writes.

**User policies intersect the auto-generated set; they never widen it.**
A hand-written policy may *narrow* (deny a subset) or *annotate* (attach
an additional label/constraint to an already-declared edge), and it is
accepted. A policy that would grant access to a resource for which **no
Connection or Binding is declared** is **refused** — you cannot policy
your way to reachability that the graph does not already justify. Access
requires BOTH: a declared edge (the graph) AND admission (the intersected
policy). This is the zero-trust invariant made structural.

**The graph×mediation composition is fixed as part of this.** Zero-trust
being default-on requires the very thing the capstone review found
missing: graph-scoped derivation must follow a consumer's `connectionRef`
to the realizing mediation tunneler, so a mediated consumer (a CDC
worker) is automatically on a network that reaches its Connection's
tunneler. Without this, "every Connection is mediated by default" would
break every consumer — so it is a hard prerequisite, not a follow-up.

### 4. Sensible per-provider resource defaults

Every provider ships a **default resource profile** (memory/CPU) sized to
its technology — a Postgres default, a JVM default for Nessie/Marquez/
Trino/Connect, a small default for proxies/tunnelers. `spec.runtime.
resources` (ADR J5) stays available as an explicit, environment-dependent
override — the owner's stated exception — but is **never required**. An
undecorated manifest gets bounded containers that fit a documented
footprint; a developer tunes only when their environment demands it.

## What the developer writes, after this ADR

```yaml
# datascape.yaml — one runtime, zero-trust on (both are the defaults shown for clarity)
kind: Project
spec: { runtime: { type: docker } }
---
# a source, a dark database, and a CDC binding — no ports, no labels, no gates
kind: Provider
metadata: { name: orders-db }
spec: { type: postgres }
---
kind: Connection
metadata: { name: orders }
spec: { target: orders-db, secretRef: orders-creds }   # port auto; mediated automatically
---
kind: Binding
metadata: { name: orders-cdc }
spec: { mode: cdc, sourceRef: orders, ... }             # allowance auto-compiled
```

It just works, and it is zero-trust — without the developer ever typing
"zero-trust", a port, a label, or a policy.

## Consequences & migration

- The four zero-trust gates are retired in favor of `ZeroTrust`
  (default-on). Existing manifests that set the old gates get a
  deprecation shim mapping them onto `ZeroTrust` for one release.
- `spec.runtime` on a Provider becomes an override; the primary path is
  the project `datascape.yaml`. A migration note ships.
- `Connection.port` optional is purely additive (existing pinned ports
  unchanged).
- Resource defaults are additive (an explicit `resources` still wins).
- This is a DX/behavior change of the first order: it gets its own
  migration guide and the example is rebuilt to demonstrate it (doc 08
  Stage M).

## References

Doc 08 §7.12 Stage M (M1–M7 sequencing). Builds on ADR 015 (facts —
the cross-runtime boundary), ADR 026 (graph-scoped access), ADR 027/033
(mediation + policy), ADR 034 (platform fabric), J5 (resource bounds),
`internal/domain/hostport` (the port allocator). Supersedes the
developer-facing surface of ADRs 026/033's gate-by-gate opt-in.
