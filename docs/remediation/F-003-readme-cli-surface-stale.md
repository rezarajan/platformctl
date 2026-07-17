# F-003: README CLI surface stale — graph flag/description wrong, inventory missing

**Severity:** Low (documentation; but the documented `graph -o dot|mermaid`
invocation no longer selects a format at all).
**Status:** Confirmed at `ae99505`. Concretizes the open §3.3 item "Update
README command descriptions".

## Evidence

`README.md` "CLI surface" table:

- `graph <dir> -o dot|mermaid` — "Render the dependency DAG". Both halves
  are wrong: graph renders the *architecture* view (data-flow pipelines +
  technology layer, `internal/application/archview`), and format selection
  moved to `--format tree|dot|mermaid|json`; `-o` is ignored by graph today
  (see F-001, which changes `-o json|yaml` to emit structured output — the
  README must describe the post-F-001 contract).
- `inventory` (aliases `services`, `endpoints`) is absent from the table
  entirely, including its `--for spark|trino|dbt|psql|s3|kafka` config
  views.
- `drift`/`status`/`import` rows spot-checked accurate.

## Root cause

README command table is hand-maintained; the graph rewrite (`b6700ca`) and
inventory addition (`b4a1633`) updated code and planning docs but not it.

## Required behavior

Update the README CLI table:

- `graph <dir> [--format tree|dot|mermaid|json]` — "Render the platform
  architecture (data-flow pipelines + technology layer)." Mention `-o
  json|yaml` per the post-F-001 contract.
- Add `inventory <dir>` (aliases `services`, `endpoints`) — endpoints +
  credentials refs + SECURITY column; `--for <tool>` renders paste-ready
  config for spark|trino|dbt|psql|s3|kafka.

## Exact files and symbols

- `README.md` — the "CLI surface" table only.

## Implementation constraints

- Text-only change; command behavior descriptions must match `--help`
  output verbatim in spirit (verify against `go run ./cmd/platformctl
  <cmd> -h` before writing).
- Coordinate with F-001: land after it, describing the fixed behavior.

## Tests / validation commands

```
go run ./cmd/platformctl graph -h
go run ./cmd/platformctl inventory -h
grep -n "dependency DAG" README.md   # must return nothing afterwards
```

## Dependencies / ordering

After F-001 (describes its outcome).

## Risk

Minimal.

## Escalation conditions

None.
