# ADR 008 — Hexagonal layering and the one invariant

**Status:** accepted (retroactive record, 2026-07-21, of the founding
decision in docs/planning/00-README.md and 02-architecture.md §1–2,
committed at `847a5f5`).

## Context

The experimental phase blurred resource semantics, technology drivers, and
Docker mechanics in one tree. The production rebuild needed a structure
where (a) the resource model can never become Docker-specific (doc 01,
guiding principle 1), and (b) a second runtime could be added without
touching any provider (goal G6).

## Options considered

1. **Flat packages with discipline** — rejected: discipline decays; nothing
   stops an import.
2. **Plugin architecture from day one** — rejected as premature (revisit at
   Phase 8; see ADR 016 for the contract that now makes it a serialization
   exercise).
3. **Hexagonal (ports & adapters) with a mechanically-enforced invariant**
   — chosen.

## Decision

Three layers with dependencies pointing inward:

- `internal/domain` imports **nothing else in this repo**.
- `internal/ports` imports **only `domain`**.
- `internal/adapters` implement ports and may import third-party SDKs.
- Concrete adapters are imported **only** by `cmd/platformctl` and
  `internal/application/registry`.
- Test exception: `_test.go` in `internal/application` may import the four
  sanctioned doubles (fake runtime, localfile state, env secrets, noop
  provider) — never technology adapters (docs/remediation/F-004).

Enforcement is mechanical, not conventional: `internal/archtest` greps the
import graph in CI; CLAUDE.md states the invariant so every agent session
loads it.

## Consequences

- The Kubernetes adapter (Phase 7) ran unmodified providers end-to-end —
  the bet this structure existed to enable, verified live
  (docs/planning/07 Cross-Runtime).
- Cost: shared behavior must live in `domain`/`ports`/`application` or an
  intra-adapter helper package (`internal/adapters/kafkaconnect`), never in
  a provider that others import.
- Every audit since (remediation 2026-07-17, doc 09, the 2026-07-20
  review) re-verified the invariant; it has never been violated in
  production code.

## References

docs/planning/02 §1–2; CLAUDE.md; `internal/archtest/`; ADR 015/016 for the
Stage-F tightenings layered on top.
