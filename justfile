# Datascape task runner. `just --list` for an overview.

# Build the platformctl binary.
build:
    CGO_ENABLED=0 go build -trimpath -buildvcs=false -o bin/platformctl ./cmd/platformctl

# Unit + contract tests (no Docker required).
test:
    go test ./...

# Minimal affected integration suites for your diff (docs/planning/06 §10):
# impact-mapped, ledger-deduped per content-state, daemon-serialized.
# `just test-affected` before reaching for the full sweep below.
test-affected base="main":
    scripts/test-impact.sh --base {{base}}

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
