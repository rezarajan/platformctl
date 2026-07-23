# ADR 032 — Provider distribution: compiled-in until the plugin protocol earns its keep

**Status:** accepted (2026-07-23). **Prompted by:** docs/planning/08
E6's Do item 3 (recorded as follow-up at E6's merge): decide the
compiled-in vs out-of-process seam for providers ahead of Phase 8
(doc 04 §11), so third parties reading the provider-authoring guide
know what they are building against.

## Decision

Providers remain **compiled into the platformctl binary**, registered in
`internal/application/registry`, for all of v1 and until at least one
trigger criterion below is met. No plugin loader, no RPC protocol, no
side-binary discovery is built speculatively.

## Why compiled-in is the right default today

1. **The seams are already plugin-shaped without paying plugin costs.**
   A provider touches the core only through `reconciler.Provider`, the
   optional capability interfaces, and `reconciler.Request` — whose
   field list is archtest-frozen (I9) and whose one non-serializable
   member (`Runtime`) is exactly the boundary a future protocol would
   have to marshal. Keeping providers in-process preserves full
   type-checking across that surface while it is still evolving —
   three capability interfaces and two Request changes landed in the
   last month alone; a wire protocol would have needed a version bump
   for each.
2. **The conformance suite, not process isolation, is the quality
   boundary** (E6, ADR 028): any `reconciler.Provider` is driven
   through lifecycle/idempotency/drift semantics against the fake
   runtime in milliseconds. Isolation adds operational surface
   (discovery, versioning, crash handling) without adding any check the
   suite doesn't already impose.
3. **Distribution reality:** platformctl ships as one static
   CGO-disabled binary; every acceptance scenario and the release
   process (docs/releasing.md) assume it. There is no third-party
   provider today, so the only cost of compiled-in — forks must rebuild
   — has no bearer yet.

## Criteria that would trigger building the plugin protocol

Reopen this decision (new ADR, Phase 8) when the **first** of these is
true:

- A third party wants to ship a provider they cannot or will not
  upstream (proprietary SDK, incompatible license, private system).
- A provider needs a dependency that materially degrades the core
  binary (CGO, GPL-incompatible, or a SDK tree that dominates build
  time/size).
- A provider's failure domain must be isolated — e.g. a driver with a
  history of panics or unbounded memory that must not take down an
  apply.
- The Terraform runtime adapter (Phase 8) lands and its own
  out-of-process needs subsidize the protocol anyway.

What the protocol must then solve, recorded now while the shape is
fresh: marshaling the `Runtime` port across the boundary (the fake-
runtime conformance suite becomes the protocol's own contract test),
capability discovery replacing type assertions (the ADR 018/030 rule —
dispatch on declared facts — already points the way), and version-
skewing the frozen Request surface.

## References

Doc 04 §11 (Phase 8), doc 08 E6 (the follow-up this closes), ADR 016
(provider contract), ADR 028 (conformance as the fast-tier boundary),
`internal/archtest/request_facts_frozen_test.go` (the frozen surface a
protocol would version).
