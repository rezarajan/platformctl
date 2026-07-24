# M1: Project-level runtime — one runtime per project

Ref: docs/adr/035-just-works-dx.md decision 1; docs/planning/08-production-readiness-plan.md §7.12 M1.

## Design (locked in, see commit for full rationale)

- New `internal/domain/project` package: `Project{Name, Runtime{Type,
  Config}, ZeroTrust}`, `FromEnvelope`.
- New schema `schemas/v1alpha1/project.json`, registered in
  `schemas.KindFiles["datascape.io/v1alpha1"]["Project"]` (reuses the
  existing compiler/meta.json $ref machinery — Project is NOT added to
  `manifest.KnownKinds`, so it never enters the governed envelope set or
  the graph).
- `internal/application/manifest/project.go`:
  - `LoadProject(path)` reads `datascape.yaml` at path's root (path
    itself if a dir, else its parent dir). Absent file => (nil, nil).
  - `ResolveProjectRuntime(envelopes, proj)`: proj == nil is a total
    no-op (backward compat). proj != nil: every Provider with no
    spec.runtime inherits a CLONE of the project's runtime map; a
    Provider that declares its own spec.runtime is an explicit override,
    refused unless its type matches the project's (exact message in the
    task prompt). This single per-Provider check is definitionally the
    single-runtime-per-inventory enforcement too (once every Provider is
    proven to match the one project type, no divergence is possible) —
    see the code comment for why no separate whole-inventory scan is
    needed.
- `collectFiles` skips `datascape.yaml` so it never lands in the ordinary
  manifest document stream.
- `manifest.Load`: LoadProject -> collectFiles/decode (unchanged) ->
  ResolveProjectRuntime (mutates envelope Spec maps in place) -> Validate
  (unchanged). Every existing caller of `manifest.Load` (root.go,
  policy.go, compose.go) gets project resolution for free with NO
  signature change.
- `provider.go`/`provider.json`: `spec.runtime` becomes schema-optional
  (dropped from `required`); `provider.Provider.validate()`'s existing
  "spec.runtime.type is required" Go-level check is UNCHANGED and now
  fires only when, after resolution, no runtime exists at all — i.e. the
  exact backward-compat case (no project, no per-Provider runtime).

## Critical backward-compat finding

`examples/cdc-attendance/provider-lineage-fake.yaml` sets `runtime.type:
fake` alongside sibling Providers on `docker`, with NO datascape.yaml —
exercised live by `cmd/platformctl/acceptance_integration_test.go`. This
proves mixed-runtime inventories WITHOUT a project file must keep working
(today's free-for-all). Confirms single-runtime enforcement (M1 item 3)
must be scoped to "a project file exists" — never applied when
datascape.yaml is absent.

## Status: DONE

All verification passed: gofmt clean, go build ok, go vet (plain +
`-tags integration`) clean, `go test ./...` exit 0 with zero FAIL,
`golangci-lint run ./...` 0 issues, `docs/reference` regenerated and
docsgen sync test green. Live-smoke-tested via the real CLI (`validate`/
`plan`/`apply`) against a hand-built `datascape.yaml` + Providers on the
`fake` runtime: inheritance, matching override, mismatched-override
refusal, and a full create-succeeds apply all behaved as designed.
Committed — see commit message for the full design writeup, hash, and
open items.
