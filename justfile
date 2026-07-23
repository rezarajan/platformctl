# Datascape task runner. `just --list` for an overview.

# Build the platformctl binary.
build:
    CGO_ENABLED=0 go build -trimpath -buildvcs=false -o bin/platformctl ./cmd/platformctl

# Fast tier (ADR 028): unit + contract tests against fakes only, no Docker,
# no timing — t.Parallel() throughout. This is the TDD default: the ONLY
# thing a developer waits for on every save. Budget-guarded in CI
# (internal/tools/testbudget): any single test over 60s or the tier over
# 90s fails the build.
test:
    go test ./...

# Deep tier (ADR 028): the existing integration suites, impact-mapped and
# ledger-deduped per content-state (docs/planning/06 §10) — pre-push
# confidence on what your diff touches, not the everyday loop. Wraps
# scripts/test-impact.sh BARE: the script self-serializes on its own flock
# (/tmp/platformctl-itest.lock, see the script's own header) — wrapping it
# in another flock here would deadlock a nested invocation against itself
# (docs/planning/11's flock note).
#
#   just test-deep                # impact-mapped set for your diff (--base main)
#   just test-deep postgres,kafka # only the named suites (comma-separated ids)
test-deep suites="":
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -n "{{suites}}" ]; then
        scripts/test-impact.sh --only "{{suites}}"
    else
        scripts/test-impact.sh --base main
    fi

# Back-compat alias for the pre-ADR-028 name (docs/CLAUDE.md, doc 06 §10).
alias test-affected := test-deep

# Integration tests against a live Docker daemon. The suite stands up the
# full provider set several times over (acceptance, chaos, lakehouse);
# budget an hour so a slow machine or cold image cache never aborts it
# mid-run.
test-integration:
    go test -tags integration -timeout 3600s ./...

# Format and vet. gofmt -l only lists unformatted files on stdout — it
# exits 0 either way, so `gofmt -l . && ...` never actually gated on
# formatting; check the output explicitly instead (mirrors the CI gofmt step).
check:
    #!/usr/bin/env bash
    set -euo pipefail
    out=$(gofmt -l .)
    if [ -n "$out" ]; then
        echo "files need gofmt:" && echo "$out" && exit 1
    fi
    go vet ./...
    go vet -tags integration ./...
