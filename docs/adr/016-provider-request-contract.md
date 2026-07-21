# ADR 016 — Provider invocation via a Request struct

**Status:** accepted (Stage F5, shipped `ba68b26`; this record consolidates
docs/planning/09 §3-F5).

## Context

Provider inputs originally arrived through an accretion of optional setter
interfaces (`ProviderResourceAware`, `SecretsAware`, `ResourceSetAware`)
plus method parameters that widened when a need appeared
(`LineageAware.ConfigureLineage` grew a runtime parameter in `81025c9`,
breaking every implementor). Three structural costs: interface widening is
closed-world; `Set*`-before-`Reconcile` is temporal coupling the compiler
cannot check (stateful providers); and the surface is unserializable — a
gRPC plugin protocol (Phase 8) cannot express "call these setters in this
order".

## Options considered

1. **Keep adding `*Aware` interfaces** — rejected: every new cross-cutting
   input adds an engine special case, and providers stay stateful.
2. **Widen method signatures per need** — rejected: breaks every
   implementor each time (it already had, once).
3. **A single request-scoped struct with additive fields** — chosen.

## Decision

`reconciler.Request{Resource, Runtime, Provider, Secrets, Resources}` is
the **single input** to `Reconcile`/`Destroy`/`Probe` and every capability
method that needs more than static config
(`internal/ports/reconciler/reconciler.go`). Rules:

- **Adding a field is the only way to add an input.** Non-breaking for
  every implementor; a zero field means "not resolved/applicable for this
  call" and providers must not assume population beyond what their own
  resource declares. (The D1 branch's `SchemaRegistryURL` field is the
  first post-F5 exercise of this rule.)
- **Providers are stateless per call**: constructors take nothing but
  static config; no cross-call state. This is what parallel reconciliation,
  testability, and out-of-process plugins all require.
- **Capability marker interfaces stay** (ADR 009) — declaring *what a
  provider can do* by interface is the discovery mechanism; only
  *data-passing* through setters was the defect.
- The setter interfaces and their engine special cases are deleted, not
  deprecated.

## Consequences

- Phase 8's plugin protocol becomes a serialization exercise over an
  already-closed request/response surface (doc 09 §5.5).
- E6 (provider-author contract) documents a stable shape — this ADR was
  deliberately sequenced before it.
- Review rule: any PR adding a `Set*` method or an `*Aware` data-passing
  interface to `ports/reconciler` is wrong by construction; the answer is
  a Request field.

## References

docs/planning/09 §3-F5; doc 08 F5; docs/planning/02 §4.2 (the current
contract text); ADR 009; ADR 015.
