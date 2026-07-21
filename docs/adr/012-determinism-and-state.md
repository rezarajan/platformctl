# ADR 012 — Determinism, state, and authoritative apply

**Status:** accepted (retroactive record, 2026-07-21, of decisions in
docs/planning/01 NFR-1/2/9, 02 §5.4/§7, and doc 07 §0.4's close-out).

## Context

The core value proposition is a reviewable, deterministic plan. Live
infrastructure is inherently non-deterministic; state files corrupt; users
remove resources from manifests and expect them gone.

## Decision

- **Plan is computed from manifests + recorded state only.** Live probes
  contribute *annotations* (drift), never actions; identical inputs yield
  byte-identical plans (NFR-1, golden-file-tested). Non-determinism is
  confined to `status`.
- **Change detection is a canonicalized spec hash** stored per resource;
  runtime adapters use the same idea (a spec-hash label/annotation) for
  container-level idempotency. Set-level fields must not leak into
  member-level hashes (the C1 review's ordinal-hash rule is this ADR
  applied).
- **Apply is authoritative** (Terraform-style): a resource present in
  state but absent from manifests plans as `delete` in normal apply — no
  separate prune command. Pre-rename legacy state surfaces as
  `ActionOrphanUnknown` and is **refused**, never guessed at
  (doc 07 §0.4).
- **State is versioned with a formal migration chain** (ordered, named,
  gap-checked — `internal/ports/state`), written atomically
  (temp-file+rename+dir-fsync locally; conditional-PUT lease remotely,
  ADR 003), persisted **after each resource** so a crash mid-apply leaves
  state truthful (NFR-9).
- **Secrets appear in state only as one-way fingerprints** — rotation is
  detected (`SecretChanged` drift) without ever persisting a value.

## Consequences

- CI can diff plans; drift is a report, not a surprise mutation.
- Every new field that feeds a hash must be canonicalized (sorted keys, no
  timestamps) — reviewed per provider PR (doc 04 §14's risk register).
- Removal/rename/provider-type-change all have dedicated plan tests; new
  action types must extend them.

## References

docs/planning/01 NFR-1/2/3/9; 02 §5.4/§7; 07 §0.4/§1.4; ADR 003.
