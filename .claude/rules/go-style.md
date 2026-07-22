---
name: go-formatting-lint
description: Format and lint Go files per project conventions.
paths:
  - "**/*.go"
---

# Go Formatting & Linting

All Go files must pass `gofmt`, `go vet`, and `golangci-lint run` without errors.

**Conventions enforced:**
- `gofmt` (no custom formatting flags)
- `go vet ./...` (both plain and `-tags integration`)
- `golangci-lint run` against the committed `.golangci.yml` (v2 config,
  repo root) — pinned in `.github/workflows/ci.yml`'s `lint` step; install
  locally with `go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@<pinned-version> run`
- Package-level tests run automatically after edits (see CLAUDE.md)

`.golangci.yml` is committed (docs/planning/11 follow-up, adopted): govet,
staticcheck, unused, ineffassign, errcheck enabled. Two tunings exist and
each is justified in the config file itself — don't re-litigate them
without re-reading the comment first:
- `staticcheck.checks` drops ST1005 ("error strings should not be
  capitalized") because this repo's domain validation errors deliberately
  start with the resource Kind capitalized (`fmt.Errorf("Binding %q: ...",
  name)`, `internal/domain/*/*.go`) — settled convention, not an oversight.
- `errcheck` excludes a narrow, type-specific list (database/sql,
  io.ReadCloser, net.Conn, pgx.Conn, minio.Object Close; fmt.Fprint* CLI
  output) plus a `source`-regex for the `close<X>()`/`closeFn()`
  reachability-teardown-closure convention used across every provider
  adapter, and relaxes errcheck (only) in `_test.go` files. Write-path
  closes (e.g. `internal/adapters/state/localfile`,
  `internal/application/compose/patch.go`) are deliberately NOT excluded
  and must stay checked — do not broaden the exclude list to "any Close"
  or "any unnamed return" without reading the config's own comments first.

If a file has existing lint issues unrelated to your change, fix them in a separate commit with a clear message ("fix: remove unused variable in adapters/runtime/docker") — don't bundle style cleanup with logical changes.
