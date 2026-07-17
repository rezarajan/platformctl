# F-010: `inventory --for dbt/psql/...` silently picks one endpoint when several of a kind exist

**Severity:** Low (single-database platforms — every shipped example —
are unaffected; multi-database platforms get an arbitrary-looking pick with
no warning).
**Status:** RESOLVED (2026-07-17). toolFacts fields are now slices,
gathered in two passes so Source-to-Provider pairing is by providerRef, not
envelope order (the original bug's exact mechanism). Single-instance output
verified byte-identical; multi-instance output verified correctly paired
(not just present) via index-position assertions in the new test.

## Claim audited

§2.3 resolved item: `inventory --for` "renders paste-ready snippets from
the recorded (observed) endpoints". True for the shipped examples; the
gathering logic does not handle plurality.

## Evidence

`cmd/platformctl/toolconfig.go`, `gatherToolFacts`: single-valued fields
assigned in envelope-iteration order —

```go
case "s3":       f.s3Host = ep.Host          // last s3 provider wins
case "postgres": f.postgresHost = ep.Host    // last postgres provider wins
...
case "Source":   ... f.postgresDB = db       // last postgres Source wins
```

Two postgres Providers (or two postgres Sources) → the rendered `psql`/
`dbt` snippet points at whichever the manifest loader yielded last, with no
indication a choice was made. Envelope order is deterministic
(`manifest.Load` file order) but semantically meaningless.

## Root cause

`toolFacts` models the single-instance lakehouse example, not the resource
model (which allows N providers per type).

## Required behavior

Bounded fix (no schema/flag additions):

1. Collect plural facts: change the postgres/mysql/s3/kafka fields to
   slices of `{component resource.Key, host, db, credsRef}` entries,
   preserving envelope order.
2. Renderers: when exactly one entry exists, output is unchanged
   (byte-for-byte — existing tests must keep passing). When several exist,
   render one clearly-labeled section per component
   (`# --- default/Provider/local-pg ---`) in order.
3. `iceberg-rest`/catalog facts get the same treatment.

## Exact files and symbols

- `cmd/platformctl/toolconfig.go`: `toolFacts`, `gatherToolFacts`, all
  `render*` functions.
- `cmd/platformctl/toolconfig_test.go`: extend, don't rewrite.

## Implementation constraints

- No new flags (`--for <tool>@<component>` selection is a UX decision —
  out of scope; sections cover the need).
- Single-instance output must remain byte-identical (guarded by the
  existing `TestToolConfigViews`).
- Secret values still never rendered.

## Tests to add

- `TestToolConfigMultipleDatabases`: two postgres providers + two Sources
  in synthetic state → both sections present, each pairing the right
  host/db/credsRef; order matches envelope order.

## Validation commands

```
go test ./cmd/platformctl/ -run TestToolConfig -v
```

## Dependencies / ordering

After F-001 if both touch the `--for` branch (F-001 wraps the renderer for
structured output; rebase whichever lands second).

## Risk

Low — additive; the only behavior change is for multi-instance platforms
that currently get silently wrong output.

## Escalation conditions

Escalate if reviewers want per-component selection flags instead of
sections (UX decision).
