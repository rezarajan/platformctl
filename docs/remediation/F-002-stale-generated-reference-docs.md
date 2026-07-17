# F-002: Committed `docs/reference/` not regenerated after schema changes

**Severity:** Medium (the generated reference is the user-facing contract;
it currently contradicts the schemas on a data-loss-relevant field).
**Status:** RESOLVED (2026-07-17). Regenerated; a sync-drift test
(`TestGeneratedReferenceInSync`) now guards against recurrence. One
wrinkle found during implementation: `secretreference.md` carried
hand-added rotation-behavior prose with no home in the schema
description — moved into `schemas/v1alpha1/secretreference.json`'s
description (multi-paragraph, `\n\n`-separated) so it survives
regeneration; `docsgen.go` gained a `description()` helper (Kind-page
header, preserves paragraph breaks) distinct from `str()` (table cells,
collapses newlines) and a `firstParagraph()` helper for the index page's
one-line summary column.

## Claim audited

- Repo rule (`.claude/rules/schema-changes.md` + CLAUDE.md): schema changes
  require a matching update to `docs/planning/03-resource-model-reference.md`
  in the same commit — that was honored. But `docs/reference/*.md` is the
  *generated* reference ("Generated from `schemas/` by `platformctl docs
  build` — do not edit by hand", `docs/reference/index.md`) and is committed;
  nothing regenerates it on schema change.
- `docs/planning/07` §3.3 lists "[ ] Regenerate reference docs after schema
  changes" as open — so this is not an unsupported claim, but the drift is
  now concrete and user-visible.

## Evidence

At `ae99505`:

- `grep deletionPolicy docs/reference/dataset.md docs/reference/source.md`
  → no matches, though `schemas/v1alpha1/dataset.json` and `source.json`
  both define `deletionPolicy` (enum `retain|delete`) — the field governing
  whether destroy deletes data.
- `docs/reference/provider.md` still says kubernetes is "rejected at
  registry construction as planned-but-unavailable", though
  `schemas/v1alpha1/provider.json` was updated (kubernetes is a real Alpha
  adapter behind the `KubernetesRuntime` gate).
- Last regeneration commit touching `docs/reference/`: `a2c1484`, which
  predates both schema changes.

## Root cause

Regeneration is manual (`platformctl docs build`) and unenforced: no CI
step or test compares committed `docs/reference/` with freshly generated
output, so schema commits silently strand it.

## Required behavior

1. Regenerate `docs/reference/` from the current schemas and commit it.
2. Add a drift guard so this class of staleness fails CI: a unit test in
   `internal/application/docsgen` (or `cmd/platformctl`) that renders the
   reference in-memory and diffs it against the committed files, failing
   with "run `platformctl docs build` and commit the result" on mismatch.

## Exact files and symbols

- Generator: `internal/application/docsgen` (`docs build` writes the
  markdown; find the exact entry point via `newDocsCmd` in
  `cmd/platformctl/root.go`).
- Committed output: `docs/reference/*.md`.
- New test: e.g. `internal/application/docsgen/generated_sync_test.go`.

## Implementation constraints

- Do not edit `docs/reference/*.md` by hand — only commit generator output.
- The sync test must not write into `docs/reference/` (render to memory or
  a temp dir and compare).
- If the generator's output is nondeterministic (map iteration), fix
  ordering in the generator first — deterministic output is a precondition
  for the sync test and for reviewable diffs.

## Tests to add

- `TestGeneratedReferenceInSync`: renders all reference pages and compares
  byte-for-byte with `docs/reference/`; failure message names the command
  to run.

## Validation commands

```
go run ./cmd/platformctl docs build --out docs/reference   # or the repo's actual flag; check `docs build -h`
git diff --exit-code docs/reference/
go test ./internal/application/docsgen/
grep -n deletionPolicy docs/reference/dataset.md docs/reference/source.md
```

## Dependencies / ordering

Do this after (or together with) F-005 (`provider.json` network
description), so the regeneration picks that correction up in one pass.

## Risk

Low — docs-only plus one test. Watch for the docsgen site test
(`site_test.go`) assumptions if generator ordering changes.

## Escalation conditions

Escalate if `docs build` writes to a hardcoded path that differs from
`docs/reference/` (then the task needs a decision on the canonical output
location), or if regeneration produces diffs in pages whose schemas did not
change (indicates nondeterminism that must be fixed first).
