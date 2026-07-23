# ADR 014 — Feature-gate strategy: gate, don't branch

**Status:** accepted (retroactive record, 2026-07-21, of docs/planning/04
§1/§12 and FR-15).

## Context

New providers and behaviors need a shipping path that keeps `main` always
releasable without long-lived feature branches, and users need an honest
signal of maturity.

## Decision

- Every provider, runtime, and behavior class ships **behind a named gate
  in the same release it's built**. Registration is one line in
  `cmd/platformctl/main.go` (`gates.Register(name, stage, enabled)`); the
  registry refuses construction of gated types with an error naming the
  gate and the enabling flag.
- **The Alpha/disabled convention is a behavioral contract**: with a gate
  off, there is *zero behavior change* for manifests that don't opt in —
  not "mostly none". (The D1 branch's one review finding was exactly a
  breach of this; its fix is the worked example.) Default-enabled Alpha is
  reserved for hardening periods explicitly declared in the plan
  (Phase 6.5's providers), with the graduation point named up front.
- Maturity follows Alpha → Beta → GA with the graduation *trigger* stated
  when the gate is introduced (doc 08 §8's table pattern: "Beta once used
  by CI itself", "Beta after soak").
- **doc 04 §12 is the master table and states the current registration**;
  it and `main.go` must agree — a sync the standing review checks. Planned
  graduations are executed when their trigger fires, in the same commit as
  the table update (the 2026-07-20 review found two lapsed graduations;
  don't accumulate more).

## Consequences

- `main` stays releasable; risky work is opt-in, not branch-isolated.
- Cost: gate hygiene is real work — introduction row, graduation
  execution, retirement (the test-only `ContainerProvider` gate was
  retired in E7, 2026-07-23: registered ungated like `noop` once evidence
  showed it load-bearing for integration tests rather than a user-facing
  maturity surface — the gate is gone, the placeholder provider stays as
  a test fixture; docs/planning/04 §12's retirement note has the evidence).
- A gate name is API: users script `--feature-gates=Name=...`; renames are
  breaking.

## References

docs/planning/01 FR-15; 04 §1/§12; 08 §8; docs/planning/10 §5 (the lapsed-
graduation lesson).
