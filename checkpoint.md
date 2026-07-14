# Datascape v1.0.0 build — checkpoint

Written for whichever agent resumes this session (expected: Fable 5 under
`/goal`). Goal in force: complete v1.0.0 per
`docs/planning/05-v1-first-version-spec.md` — all five providers working
end-to-end against Docker, drift detection, safety guarantees, and
`examples/cdc-attendance/` passing.

**Read `docs/planning/06-agentic-execution-guide.md` first if you haven't** —
it defines the layering rules, hook behavior, and model-selection guidance
this build follows. `CLAUDE.md` is the always-loaded summary of the same.

## Where things stand against the roadmap (`docs/planning/04-roadmap-and-feature-gates.md`)

| Phase | Status | Notes |
|---|---|---|
| 0 — Foundations | **Done, verified** | All exit criteria pass; see `cmd/platformctl/e2e_test.go`. |
| 1 — Docker Runtime Adapter | **Done, verified** | Conformance suite + out-of-band-kill test pass against a live daemon. |
| 2 — Redpanda Provider | **Done, verified** | Full exit criteria pass against a live daemon (`cmd/platformctl/redpanda_integration_test.go`). |
| 3 — Postgres + Debezium CDC + Lineage | **Code written, partially verified** | Postgres and Debezium providers compile and pass unit tests. Lineage forwarding mechanism verified with fakes. **Not yet run against real Postgres/Kafka Connect containers together as one scenario** — no integration test exists for this yet. This is the next task. |
| 4 — Object Storage Sink | **Not started** | No `s3`/`s3sink` provider code exists. |
| 5 — External/Imported + Drift | **Not started** | `import`/`drift` commands don't exist; only the `Lifecycle` enum and `plan.ActionConfigure` groundwork are in place. |
| 6+ | Not in scope for v1.0.0 | Ignore. |

**Also not yet done, needed before v1.0.0 sign-off regardless of phase:**
- `schemas/*.json` — no JSON Schema files exist yet. `manifest.Load` currently
  does its own ad-hoc field extraction/validation in Go, not schema-driven.
  Phase 0's deliverable list called for this; it was skipped to get the
  engine working first. `application/manifest` and each provider's
  `spec.configuration` need schemas before `validate` truly satisfies FR-9
  (no plaintext secrets caught at schema time) and the extensibility model
  in `02-architecture.md` §11.
- `examples/cdc-attendance/` — the acceptance-scenario manifest directory
  named directly in the v1.0.0 goal. Does not exist yet. This should be
  built alongside/after Phase 3's integration test, then reused as the
  literal fixture for the Phase 5 "v1.0.0 declared" checkpoint.
- `justfile` has `build`, `test`, `test-integration`, `check` — sufficient
  for now, extend as new providers land.

## Repo layout recap

Standard Go module, root `github.com/rezarajan/platformctl`, Go 1.22+
(toolchain reports go1.26.4 in this environment — fine, code has no
version-specific dependencies).

```
cmd/platformctl/         # cobra CLI, wiring (main.go), commands (root.go)
internal/domain/         # resource, status, source, eventstream, binding,
                          # dataset, provider, secret, lineage, graph
internal/ports/          # runtime, reconciler, state, secretstore, clock
                          # + conformance/ subpackages for runtime & state
internal/adapters/
  runtime/fake/           # in-memory ContainerRuntime, passes conformance
  runtime/docker/         # real Docker Engine API adapter, passes conformance
  state/localfile/        # JSON file + flock, passes conformance
  secrets/env/             # SecretStore backend for `env`
  providers/noop/           # Phase 0 test provider
  providers/placeholder/     # "container" type — generic single-container
                              # provider used to prove the Docker runtime in
                              # isolation (Phase 1 exit criteria)
  providers/redpanda/         # Phase 2 — broker + topic reconciliation
  providers/postgres/          # Phase 3 — instance + database/replication role
  providers/debezium/           # Phase 3 — Connect worker + connector + LineageAware
internal/application/
  manifest/    compatibility/    plan/    engine/    registry/    featuregate/
internal/cliutil/        # output formatting, exit-code contract
docs/planning/            # the six-doc planning package — read-only source of truth
.claude/                  # agents, rules, settings.json (hooks)
scripts/hooks/             # fmt-and-lint.sh, guard-planning-docs.sh
```

## Feature gates currently registered (`cmd/platformctl/main.go`, `defaultWiring`)

Defaults follow the master table in `04-roadmap-and-feature-gates.md` §12,
**except** `ContainerProvider`, which isn't in that table at all — it's a
test-only "container" provider type used to exercise the Docker runtime in
isolation during Phase 1, disabled by default, safe to leave registered or
remove once real providers cover the same ground.

```
CoreReconciler         GA      enabled
DockerRuntime           Alpha   enabled
ContainerProvider        Alpha   disabled   (test-only, not in master table)
RedpandaProvider           Alpha   enabled
PostgresProvider             Alpha   enabled
DebeziumCDCProvider            Alpha   enabled
CDCBinding                       Alpha   enabled
LineageObservability               Alpha   disabled  (correct per spec — no required backend yet)
```

Still to register as later phases land: `ObjectStoreProvider`, `SinkBinding`
(Phase 4), `ImportedResources`, `ExternalResourceConfiguration`,
`DriftDetection` (Phase 5).

## Provider implementation-revealed additions (not yet reflected in planning docs)

The architecture doc's interfaces turned out to need small additions once
providers were actually built. These are deliberate, working deviations —
flag them to a human if this session hits a design question big enough to
warrant a docs/planning edit; otherwise keep building against what's here:

- `runtime.ContainerSpec` gained a `Cmd []string` field — real containers
  need a command/args, the original spec omitted it.
- `reconciler.ProviderResourceAware` (optional interface): engine calls
  `SetProviderResource(env)` before Reconcile/Destroy/Probe so a provider
  reconciling a *dependent* resource (e.g. redpanda reconciling an
  `EventStream`) can see its own `Provider` resource's configuration
  (broker address, runtime config) without re-deriving it.
- `reconciler.SecretsAware` (optional interface): engine resolves every
  name in `Provider.spec.secretRefs` via the configured `SecretStore` and
  calls `SetSecrets(map[refName]map[key]value)` before reconciling. Postgres
  and Debezium both use this for credentials.
- `reconciler.ResourceSetAware` (optional interface): engine calls
  `SetResourceSet(byKey)` so a provider reconciling a `Binding` can resolve
  the `Source`/`EventStream` it references without a second manifest load.
  Debezium uses this to find the Postgres `Source`'s provider hostname.
- `graph.Build` now also creates edges for `Provider.spec.secretRefs` (to
  `SecretReference` resources) — needed so the topological order resolves
  secrets before the provider that needs them.

None of these break anything already-written; they're additive. If you
create a new provider needing something similar, prefer adding another small
optional interface over widening the base `Provider` interface.

## Known rough edges / things to check first

1. **No integration test exercises Postgres + Debezium + Redpanda together.**
   `internal/adapters/providers/postgres` and `.../debezium` compile and
   pass `go vet`, but have never been run against real containers. This is
   the highest-value next step — write
   `cmd/platformctl/cdc_integration_test.go` (build-tag `integration`)
   modeled on `redpanda_integration_test.go`, covering the Phase 3 exit
   criteria list in `04-roadmap-and-feature-gates.md` §6 verbatim:
   - full Provider×3 + Source + EventStream + Binding manifest set reaches
     `Ready` from empty state
   - `Binding` Ready means connector verified `RUNNING`, not just "container
     started"
   - idempotent re-apply, zero mutating calls
   - `destroy` reverse-order teardown, no orphans
   - changing `Binding.spec.options.tables` updates the connector without
     recreating Postgres/Redpanda
   - lineage forwarding to a **real** fake-lineage-provider test double
     (already covered at the engine-unit level in
     `internal/application/engine/engine_test.go` — that test can likely be
     extended rather than rewritten)
   - compatibility-check error-shape enforcement at `validate`, not `apply`
     (already covered in `internal/application/compatibility/compatibility_test.go`)

2. **Debezium provider assumes Postgres always** — `connector.class` is
   hardcoded to `io.debezium.connector.postgresql.PostgresConnector` in
   `internal/adapters/providers/debezium/debezium.go`. Fine for v1.0.0 scope
   (Postgres is the only CDC source required), but if a second engine shows
   up later this needs a lookup table keyed by `Source.spec.engine`.

3. **Port collisions on a shared dev machine.** This environment already had
   other unrelated Docker containers running (`datascape-metadata-*`,
   `datascape-lineage-*`, etc. — pre-existing, not part of this build) that
   collided with the default Redpanda Kafka port (19092) during Phase 2
   testing. Test manifests were moved to `19192`. If Phase 3/4 integration
   tests hit similar collisions (5432 for Postgres, 8083 for Kafka Connect,
   9000/9001 for MinIO), pick non-default host ports the same way rather
   than assuming a clean Docker environment.

4. **No JSON Schema validation yet** (see table above) — `validate` catches
   structural errors via Go-level `FromEnvelope` parsing in each domain
   package, not via `schemas/*.json` + a JSON Schema validator library. This
   satisfies the *behavior* users see today but not the letter of Phase 0's
   deliverable list or NFR-9/FR-9's schema-time guarantees. Worth doing
   before declaring v1.0.0, not necessarily before Phase 4/5 functionally
   land — use judgment on sequencing.

5. **`.claude/rules/schema-changes.md` will currently never fire** — there's
   no `schemas/` directory for its `paths:` glob to match. Once schemas
   exist, revisit whether the rule's guidance still reads correctly.

## How to verify state after resuming

```bash
# Full unit/contract suite (no Docker needed) — should be all green:
go build ./... && go vet ./... && go test ./...

# Integration suite (needs a running Docker daemon):
just test-integration
# or selectively:
go test -tags integration -run TestConformance ./internal/adapters/runtime/docker/
go test -tags integration -run TestDockerProviderEndToEnd ./cmd/platformctl/
go test -tags integration -run TestRedpandaEndToEnd -timeout 600s ./cmd/platformctl/

# Manual smoke test against the noop scenario (fast, no Docker):
CGO_ENABLED=0 go build -trimpath -buildvcs=false -o /tmp/platformctl ./cmd/platformctl
/tmp/platformctl validate cmd/platformctl/testdata/noop-scenario
/tmp/platformctl apply cmd/platformctl/testdata/noop-scenario --state-file /tmp/st/state.json --auto-approve
/tmp/platformctl status cmd/platformctl/testdata/noop-scenario --state-file /tmp/st/state.json
```

All of the above passed as of this checkpoint.

## Suggested next steps, in order

1. Write and pass the Phase 3 end-to-end integration test (item 1 above) —
   this is the actual remaining Phase 3 exit-criteria verification, not new
   feature code. Debug whatever the real Postgres/Debezium containers reveal
   that unit tests couldn't (connector registration timing, wal_level
   propagation, network naming, etc.).
2. Once Phase 3 is verified, build `examples/cdc-attendance/` as real
   manifests mirroring the Phase 3 integration test's resource set — this
   becomes the literal fixture the v1.0.0 spec doc names.
3. Phase 4: `adapters/providers/s3` (MinIO-compatible) and
   `adapters/providers/s3sink` (Kafka-Connect-based, implements
   `SinkCapableProvider`). Follow the same pattern as `redpanda`/`debezium`:
   provider package + kafka/http helper file + integration test + feature
   gates (`ObjectStoreProvider`, `SinkBinding`) registered in `main.go`.
4. Phase 5: `import` command, `drift` command, engine-level External-lifecycle
   destroy guard (`NFR-3` — the domain already has `resource.External` and
   `plan.ActionConfigure`; the missing piece is CLI wiring + the destroy-time
   guard + drift-as-a-condition surfaced through `probe`).
5. Backfill `schemas/*.json` — can happen in parallel with 3/4 if convenient,
   but must land before v1.0.0 sign-off per the architecture doc.
6. Re-run every phase's exit criteria against the actual binary one more
   time at the end (per `06-agentic-execution-guide.md` §7 step 5) before
   considering v1.0.0 declared.

## Model-selection note (from `06-agentic-execution-guide.md` §4)

This checkpoint was written mid-session on a lighter model than the guide
recommends for this work (Docker/adapter/integration-test implementation is
Sonnet-tier at minimum; the full-phase autonomous run this goal describes is
explicitly Fable's use case). If you are Fable resuming via `/goal`, proceed
as normal — this note is just context for why the commit history shows a
model mismatch, not an instruction to stop and ask.
