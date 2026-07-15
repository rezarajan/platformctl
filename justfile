# Datascape task runner. `just --list` for an overview.

# Build the platformctl binary.
build:
    CGO_ENABLED=0 go build -trimpath -buildvcs=false -o bin/platformctl ./cmd/platformctl

# Unit + contract tests (no Docker required).
test:
    go test ./...

# Integration tests against a live Docker daemon. The suite stands up the
# full provider set several times over (acceptance, chaos, lakehouse);
# budget an hour so a slow machine or cold image cache never aborts it
# mid-run.
test-integration:
    go test -tags integration -timeout 3600s ./...

# Format and vet.
check:
    gofmt -l . && go vet ./... && go vet -tags integration ./...
