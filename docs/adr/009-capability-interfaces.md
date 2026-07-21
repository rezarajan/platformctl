# ADR 009 — Compatibility as provider capability, not type system

**Status:** accepted (retroactive record, 2026-07-21, of the founding
decision in docs/planning/00-README.md's table and 02-architecture.md
§4.2/§5.2).

## Context

A `Binding(mode: cdc)` naming a provider that cannot speak the Source's
engine is a configuration mistake. Where should "can this provider do
that?" live — in the type system (a Kind per pairing), in providers'
runtime errors, or in a declared capability checked early?

## Options considered

1. **Kind-per-capability** (`CDCBinding`, `SinkBinding` as distinct kinds) —
   rejected: multiplies schema surface and re-couples the model to
   technologies (see ADR 001's rejection of role-named kinds).
2. **Fail at apply** when the provider rejects the work — rejected: the
   platform half-applies before the user learns of the mistake.
3. **Optional capability interfaces on the provider implementation,
   checked at `validate`** — chosen.

## Decision

- A provider declares each capability by implementing a small optional
  interface: `CDCCapableProvider`, `SinkCapableProvider`,
  `DatabaseSinkCapableProvider`, `IngestCapableProvider`,
  `CatalogCapableProvider`, `ConnectionCapableProvider`,
  `ExternalConfigurer`, plus the validation hooks `SpecValidator`,
  `BindingOptionsValidator`, `VersionedProvider` (all in
  `internal/ports/reconciler`).
- `internal/application/compatibility` type-asserts the mode/pairing-
  appropriate interface at `validate`/`plan` — a pure check, no side
  effects, never deferred to apply.
- **The error shape is part of the contract**, matched on the character:
  `Binding %q: Provider %q (type: %s)\ndoes not support <thing> %q
  (supported: %s)` (doc 02 §5.2). New capability checks reuse this exact
  family.
- A missing capability is a *seam*, not a bug: `sink → Source` and
  `ingest` validate structurally and fail with the standard error until a
  provider ships (ADR 001) — features arrive as one adapter plus one
  interface declaration, never a schema change.

## Consequences

- Capability discovery is compile-time-checkable and engine-agnostic; the
  registry stays free of provider-type switches.
- Marker interfaces are the *discovery* mechanism only — data flows through
  the Request struct (ADR 016), a distinction doc 09 §3-F5 drew after the
  `*Aware` setter pattern conflated the two.
- Every new pairing/capability must add: the interface, the compatibility
  check, the standard error, and a negative validate test (the D1 branch's
  schema-format check followed this recipe verbatim).

## References

docs/planning/02 §4.2/§5.2; docs/planning/03 §7.1–7.2; ADR 001; ADR 016.
