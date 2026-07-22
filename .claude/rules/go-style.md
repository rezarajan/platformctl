---
name: go-formatting-lint
description: Format and lint Go files per project conventions.
paths:
  - "**/*.go"
---

# Go Formatting & Linting

All Go files must pass `gofmt` and `go vet` without errors.

**Conventions enforced:**
- `gofmt` (no custom formatting flags)
- `go vet ./...`
- Package-level tests run automatically after edits (see CLAUDE.md)

No `.golangci.yml` is committed (the 2026-07 production review found this
file referenced a config that never existed). Adopting golangci-lint with
a tuned config is a recorded follow-up in docs/planning/11 — until then,
gofmt + vet + the repo's archtests are the enforced bar; don't assume a
lint hook ran.

If a file has existing lint issues unrelated to your change, fix them in a separate commit with a clear message ("fix: remove unused variable in adapters/runtime/docker") — don't bundle style cleanup with logical changes.
