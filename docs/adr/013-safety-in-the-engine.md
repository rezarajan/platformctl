# ADR 013 — Safety enforced in the engine, not by convention

**Status:** accepted (retroactive record, 2026-07-21, of docs/planning/01
NFR-3 / guiding principle 4 and its extensions through doc 07 §0.4 and
doc 08 A5).

## Context

The single worst failure this product can have is destroying data or
someone else's infrastructure. Safety rules enforced per-provider are N
places to audit and one forgotten place away from disaster (doc 04 §14's
risk register named exactly this).

## Decision

Every destructive-action guard lives at **one engine-level enforcement
point**, never in provider code:

- `destroy` touches managed resources only; external requires
  `--include-external` **and** `--yes-i-understand-this-is-destructive`;
  imported requires `--include-imported`. External resources are only ever
  *de-configured* or forgotten from state — never issued a destructive
  delete (doc 01 open-question sign-off).
- `metadata.protect: true` refuses deletion regardless of lifecycle or
  flags; the only path is applying a manifest without `protect` first
  (doc 08 A5).
- Data-bearing kinds carry `spec.deletionPolicy: retain|delete` with
  retain as default — destroying the platform's *record* never destroys
  *data* without explicit opt-in (doc 07 §2.2).
- Runtime adapters touch **only objects carrying the ownership labels**
  (`io.datascape.*`); unlabeled same-name objects are refused, not
  adopted. `gc apply` and Kubernetes' namespace removal inherit the same
  refusal posture (the `RemoveNetwork` conformance rule).
- The same double-flag pattern is the template for every new destructive
  surface (`gc apply`, restore-over-existing in the C6 branch).

## Consequences

- One place to review per rule; providers cannot forget a guard they never
  owned.
- New destructive capabilities must name their engine-level guard and its
  flags in the task's accept criteria before implementation (doc 08 §2).
- Open edge recorded by the C6 review: `protect` vs restore-over-existing
  interplay is undecided — ADR 007 (reserved) must settle it; the safe
  default is refusal.

## References

docs/planning/01 NFR-3, §5 principle 4; 02 §5.5/§10; 07 §0.4/§0.7/§1.3;
doc 08 A5.
