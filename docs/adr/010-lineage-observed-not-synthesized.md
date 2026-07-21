# ADR 010 — Lineage is observed, never synthesized

**Status:** accepted (retroactive record, 2026-07-21, of the founding
decision in docs/planning/00-README.md's table, 01 NG6/NG7/G8, and 02 §5.5).

## Context

Real lineage tools (OpenLineage/Marquez) model *job execution* — Job, Run,
Dataset facts produced by the tool doing the work. Datascape reconciles
infrastructure; it does not execute jobs. How much lineage machinery should
it own?

## Options considered

1. **Emit lineage events for reconciliation actions** — rejected: a
   reconcile is not a job run; fabricating facts on another tool's behalf
   violates "observation is not participation" (doc 01, principle 7).
2. **A required lineage backend in v1.0.0** — rejected (NG6): mechanism
   correctness must not depend on a real backend existing first (NFR-10).
3. **Forward a connection fact to providers whose tools have native
   lineage support** — chosen.

## Decision

- `metadata.observers` on any data-plane resource names Provider(s). The
  engine resolves each to a `lineage.LineageEndpoint` — URL, optional
  namespace, optional auth ref; **a connection fact, nothing more**.
- If the resource's own provider implements `LineageAware`, the engine
  hands it the endpoint (now via the Request struct, ADR 016); the
  provider wires it into its tool's *native* integration (debezium sets
  Debezium's own `openlineage.*` connector config).
- A non-`LineageAware` provider is a **no-op, never an error**: the
  informational `LineageEndpointDeclaredNotConsumed` condition, `Ready`
  unaffected (FR-20).
- Datascape never constructs a Job/Run/Dataset record. Datascape's own
  reconciliation telemetry is a separate, deferred concern — the two uses
  of "observability" must not blur (doc 02 §8's standing note).

## Consequences

- The mechanism was provable with a fake provider before any backend
  existed (NFR-10), and the real backend (`openlineage`/Marquez, Phase
  6.5) slotted in without engine changes.
- Any provider whose tool gains native lineage support opts in by
  implementing one interface — no engine registry of "lineage-capable
  types".

## References

docs/planning/01 §3 G8, §4 NG6/NG7; docs/planning/02 §3.6/§5.5/§8;
docs/planning/03 §9.
