# ADR 011 — Validate-time completeness (the DX contract)

**Status:** accepted (retroactive record, 2026-07-21; crystallized during
the post-6.5 hardening sweep — `5189edd` "validate-time DX contract" — and
recorded in docs/history/checkpoint.md "Validate-time completeness").

## Context

Early sessions produced failures where a manifest set validated cleanly and
then half-applied into a mis-wired platform (missing secrets discovered
mid-apply, provider options rejected by the technology's API after
infrastructure existed). Each is a worse experience than any validate
error.

## Decision

`platformctl validate` is a **gate with a completeness guarantee**: a
manifest set that validates must not be able to half-apply into a mis-wired
platform. The check order, all inside the shared `loadAndValidate` path
every command uses:

1. **JSON Schema** per kind (`schemas/` — shapes, required fields, no
   representable secret values).
2. **Kind-specific Go validation** (`FromEnvelope` per kind).
3. **Graph**: every ref (providerRef/sourceRef/targetRef/connectionRef/
   secretRef/observers) resolves in-set, unambiguously; cycles rejected.
4. **Compatibility**: mode↔Kind pairing relation + per-pairing capability
   (ADR 009), Catalog engine / Connection scheme capability, plus the
   provider's own `SpecValidator`/`BindingOptionsValidator`.
5. **Feature gates**: every Provider type resolves through the gated
   registry; a disabled gate names itself and the enabling flag.

Corollaries:

- Secrets are additionally **preflighted** at apply (`Preflight` aggregates
  every missing key before any infrastructure is touched) — the one check
  that inherently needs the live environment.
- An apply-time-only configuration error is a **regression**: the fix is a
  new `SpecValidator`/`BindingOptionsValidator` rule (or schema fragment,
  doc 08 E5), never documentation telling users to be careful.

## Consequences

- New providers with required configuration must implement
  `SpecValidator`; new Binding options must implement
  `BindingOptionsValidator` — reviewers check this per doc 08 §2.
- E5 (provider-owned schema fragments) is the planned completion: moving
  the remaining open-ended blocks under generated validation and docs.

## References

docs/planning/02 §5.1–5.2; docs/history/checkpoint.md; doc 08 E5; the D1
branch review (its gate-boundary fix, `2a05bd4`, is a worked example of
keeping this contract while adding a check).
