# F-004: Application-layer test files import concrete adapters (layering-invariant exception without recorded waiver)

**Severity:** Low (production code is clean; the invariant's wording admits
no test exception, so either the tests or the documented rule is wrong).
**Status:** Confirmed at `ae99505`.

## Claim audited

CLAUDE.md / `docs/planning/02-architecture.md` §1-2: "Only `cmd/platformctl`
and `internal/application/registry` import concrete adapters." Architecture
audit verified production `internal/domain`, `internal/ports`, and
`internal/application` (excluding `registry`) import no adapter packages.
Two **test** files do:

## Evidence

```
internal/application/compatibility/compatibility_test.go:
    imports internal/adapters/providers/postgres   (TestVersionedProviderValidation uses postgres.New())
internal/application/engine/engine_test.go:
    imports internal/adapters/providers/noop, runtime/fake, secrets/env, state/localfile
```

## Root cause

- `compatibility_test.go` uses the real postgres adapter only to obtain a
  `reconciler.VersionedProvider` with a populated catalog — the test's
  subject is compatibility's *mechanism*, not postgres's catalog contents.
- `engine_test.go` needs working runtime/state/secret implementations; the
  fake runtime and localfile store are the intended test doubles, but the
  documented invariant doesn't say so.

## Required behavior

Two bounded changes, no architectural decision required:

1. **compatibility_test.go**: replace the postgres import with a local stub
   implementing `reconciler.VersionedProvider` (embed the existing
   `stubProvider`, return a two-entry `versionprofile.Catalog` with a
   `Default`). The three assertions in `TestVersionedProviderValidation`
   (valid version accepted, unknown version rejected, image-without-version
   rejected) must keep passing — they exercise compatibility's use of
   `VersionCatalog()`, which the stub provides. The real postgres catalog
   remains covered by the CDC/lakehouse integration suites.
2. **engine_test.go**: keep the imports, and record the waiver where the
   invariant is stated: amend the layering note in CLAUDE.md (and the
   `.claude/rules/layering.md` file) with one sentence: *"Exception:
   `_test.go` files in `internal/application` may import the `fake` runtime,
   `localfile` state, `env` secrets, and `noop` provider adapters as test
   doubles; importing technology adapters (postgres, redpanda, ...) from
   application tests is not allowed."*

## Exact files and symbols

- `internal/application/compatibility/compatibility_test.go`:
  `TestVersionedProviderValidation`, `resolvePG`.
- `internal/ports/reconciler/reconciler.go`: `VersionedProvider` (reference
  only).
- `internal/domain/versionprofile`: `Catalog`, `Profile` (reference only).
- `CLAUDE.md`, `.claude/rules/layering.md`: waiver sentence.

## Implementation constraints

- Do not weaken the invariant for production code.
- Do not move the fake runtime out of `internal/adapters` (that *would* be
  an architectural decision — out of scope).
- The stub catalog must include a profile named "18" and reject "99" so the
  existing test bodies keep their inputs.

## Tests / validation commands

```
go test ./internal/application/compatibility/
grep -rn "internal/adapters/providers/postgres" internal/application/ && exit 1 || true
```

## Dependencies / ordering

None.

## Risk

Minimal. The only coverage change: compatibility tests no longer notice a
*postgres catalog* regression — which belongs to postgres's own tests and
the integration suites anyway.

## Escalation conditions

Escalate if `TestVersionedProviderValidation` turns out to assert on
postgres-specific version strings that a stub cannot satisfy without
duplicating the real catalog (then the test's intent needs a decision:
mechanism test vs. catalog regression test).
