# F-008: `just check` cannot fail on unformatted files

**Severity:** Low (local/CI hygiene gate silently ineffective for
formatting; vet still runs).
**Status:** Confirmed and reproduced at `ae99505`.

## Claim audited

`docs/planning/07` §3.2 open item: "Fix `just check`; `gofmt -l .` alone
does not fail when files need formatting." Open in the doc — consistent —
now verified with a live repro.

## Evidence

```
$ printf 'func  bad(){}\n' >> internal/domain/hostport/hostport.go   # deliberately unformatted
$ just check; echo $?
internal/domain/hostport/hostport.go     # gofmt -l lists it...
0                                        # ...but the recipe exits 0
```

`justfile` recipe:

```
check:
    gofmt -l . && go vet ./... && go vet -tags integration ./...
```

`gofmt -l` exits 0 whether or not it prints filenames; the `&&` chain
therefore never breaks on formatting.

## Root cause

`gofmt -l` communicates through stdout, not exit code.

## Required behavior

Make the recipe fail when `gofmt -l` produces output:

```
check:
    @test -z "$(gofmt -l .)" || (echo "gofmt needed:" && gofmt -l . && exit 1)
    go vet ./...
    go vet -tags integration ./...
```

(Any equivalent construction is fine; the acceptance criterion is the
validation below.)

## Exact files and symbols

- `justfile`: the `check` recipe only.

## Implementation constraints

- Keep the three checks (fmt, vet, vet-integration); do not add linters
  here (golangci-lint availability differs per machine — out of scope).
- Recipe must work under the repo's `just` settings (check for
  `set shell` directives before using bashisms).

## Tests / validation commands

```
# clean tree:
just check; echo $?          # → 0
# with a deliberately unformatted file appended to any .go file:
just check; echo $?          # → non-zero, names the file
```

## Dependencies / ordering

None.

## Risk

Minimal. CI's own gofmt gate (ci.yml unit job) should be checked for the
same pattern while here — if it uses `gofmt -l` bare, fix identically.

## Escalation conditions

None.

## Doc correction required

Tick the §3.2 checkbox when landed.
