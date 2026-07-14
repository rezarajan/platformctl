# Datascape (platformctl)

Go 1.22+. Build: `CGO_ENABLED=0 go build -trimpath -buildvcs=false ./cmd/platformctl`. Test: `go test ./...`. Integration: `just test-integration` (requires Docker; tagged `integration`, skipped by default).

## Layering (see docs/planning/02-architecture.md §1-2)

- `internal/domain` imports nothing else in this repo. `internal/ports` imports only `domain`.
  `internal/adapters` implement ports and may import third-party SDKs.
- Only `cmd/platformctl` and `internal/application/registry` import concrete adapters.
- **The one invariant:** if you're about to import an adapter package from `domain` or `ports`, stop — that's the architecture this whole design depends on.

## Before implementing anything

1. **Phase & exit criteria** (docs/planning/04-roadmap-and-feature-gates.md): Which phase and which line is this task?
2. **Kind/interface shapes** (docs/planning/02-architecture.md + 03-resource-model-reference.md): What's the final shape?
3. **Capability interfaces** (docs/planning/02-architecture.md §4.2, §5.2): Does this touch `CDCCapableProvider`, `SinkCapableProvider`, or `LineageAware`? If so, re-read the exact error-message format.
4. **Acceptance scenario** (docs/planning/05-v1-first-version-spec.md): Is this resource/provider used in the example?
5. **Contract test suite** (docs/planning/02-architecture.md §9): Does the port have one? New adapters must pass it.

## Conventions

- New provider → implement `reconciler.Provider`, register in `application/registry`, add a JSON Schema, add a feature gate entry defaulting to Alpha/disabled (docs/planning/02-architecture.md §11).
- Every `Ensure*` runtime method must be idempotent — a second call with the same spec makes zero API calls to Docker. Tested by conformance suite.
- A schema change under `schemas/` requires a matching update to docs/planning/03-resource-model-reference.md in the same commit.

## Compact instructions

When compacting, preserve: which phase/exit-criteria item is in progress, test output, and any open design question raised during this session. Discard exploratory file-reading history.
