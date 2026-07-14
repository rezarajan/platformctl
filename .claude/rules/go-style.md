---
name: go-formatting-lint
description: Format and lint Go files per project conventions.
paths:
  - "**/*.go"
---

# Go Formatting & Linting

All Go files must pass `gofmt` and `golangci-lint` without errors. These are enforced by a `PostToolUse` hook on every `Edit`/`Write` to `.go` files, so manual runs are unnecessary but welcome for local development.

**Conventions enforced:**
- `gofmt` (no custom formatting flags)
- `golangci-lint run --fix` (repo's `.golangci.yml` applies)
- Package-level tests run automatically after edits (see CLAUDE.md)

If a file has existing lint issues unrelated to your change, fix them in a separate commit with a clear message ("fix: remove unused variable in adapters/runtime/docker") — don't bundle style cleanup with logical changes.
