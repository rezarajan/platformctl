# Datascape v1.0.0 build — checkpoint

Written for whichever agent resumes this session. Goal in force: complete
v1.0.0 per `docs/planning/05-v1-first-version-spec.md` — all five providers
working end-to-end against Docker, drift detection, safety guarantees, and
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
| 3 — Postgres + Debezium CDC + Lineage | **Done, verified** | `cmd/platformctl/cdc_integration_test.go` covers every exit criterion against real containers (apply-to-Ready ~15s with pre-pulled images, connector verified RUNNING, idempotent re-apply, in-place `options.tables` update, clean reverse-order destroy). Lineage forwarding + not-consumed condition covered in `application/engine` tests; capability error shapes in `application/compatibility` tests. |
| 4 — Object Storage Sink | **Done, verified** | `s3` (+`minio` alias) and `s3sink` providers; `cmd/platformctl/sink_integration_test.go` runs the extended 11-resource scenario: real CDC rows land as objects in MinIO (~40s incl. traffic), `Dataset.spec.format` change updates the connector without touching broker/db/store, sink capability failures at validate, clean destroy. |
| 5 — External/Imported + Drift | **Done, verified** | Drift: `platformctl drift`, `status` DRIFT column, apply-side healing, destroy convergence (chaos_integration_test.go incl. SIGKILL-mid-apply NFR-9). Import: `platformctl import <Kind>/<name> --from <name>` (adopt-by-name, probe-never-create, gated by ImportedResources) — `phase5_integration_test.go` TestImportEndToEnd adopts an out-of-band `docker run` postgres, applies as no-op, destroy skips it. External: `Source.spec.external: true` + connectionRef reconciles via engine (connection-resolvable check), Binding registers Debezium connector against the out-of-band DB (`options.databaseHostname`/`databasePort`); NFR-3 double lock enforced in CLI **and** engine (`Engine.AllowDestructive`); external-no-provider destroy = state removal only — TestExternalSourceEndToEnd + TestDestroyExternalGuard. |
| 6+ | Not in scope for v1.0.0 | Ignore. |

**Also not yet done, needed before v1.0.0 sign-off regardless of phase:**
- `schemas/*.json` — still no JSON Schema files. `manifest.Load` does ad-hoc
  Go validation. Must land before v1.0.0 sign-off (FR-9/NFR-9, architecture
  §11, and the `docs build` DoD item all depend on it).
- `platformctl docs build|serve` and `platformctl import` CLI surface (§5 of
  the v1 spec) don't exist yet.
- The §6 acceptance-scenario *automated test* (10-resource set from
  `examples/cdc-attendance/` incl. drift/external/kill-recovery steps 7–9)
  — the example manifests now exist and validate (14 resources incl. the 3
  SecretReferences and the `test-lineage-fake` provider), and their behavior
  is equivalent to `sink_integration_test.go`, but the literal
  examples-directory run with the §6 step list belongs to Phase 5.
- `justfile` has `build`, `test`, `test-integration`, `check` — extend as
  needed.

## Repo layout recap

Standard Go module, root `github.com/rezarajan/platformctl`, Go 1.22+.
New since the last checkpoint: `internal/adapters/kafkaconnect/` (shared
Connect REST helpers), `internal/adapters/providers/s3/` and `.../s3sink/`,
`examples/cdc-attendance/` (runnable acceptance manifests + README +
s3sink-image Dockerfile), `cmd/platformctl/testdata/{cdc,sink}-scenario/`,
`cmd/platformctl/{cdc,sink}_integration_test.go`,
`cmd/platformctl/testdata/s3sink-image/` (Connect + Aiven S3 sink plugin,
built by the sink test as `datascape-s3sink-connect:test`).

## Feature gates currently registered (`cmd/platformctl/main.go`, `defaultWiring`)

```
CoreReconciler         GA      enabled
DockerRuntime          Alpha   enabled
ContainerProvider      Alpha   disabled   (test-only, not in master table)
RedpandaProvider       Alpha   enabled
PostgresProvider       Alpha   enabled
DebeziumCDCProvider    Alpha   enabled
CDCBinding             Alpha   enabled
LineageObservability   Alpha   disabled
ObjectStoreProvider    Alpha   enabled    (gates "s3", "minio", "s3sink")
SinkBinding            Alpha   enabled
DriftDetection         Alpha   enabled    (master table says disabled — deliberate deviation, see below)
```

**Deviation:** the master table introduces `DriftDetection` default-disabled;
it is registered default-enabled because real usage showed out-of-band
failures are the common case and an unobservable platform was judged worse
than an early Alpha behavior being on. Flag at v1.0.0 sign-off: either amend
the master table or flip the default.

Still to register in Phase 5: `ImportedResources`,
`ExternalResourceConfiguration` (both Alpha/disabled).

## Implementation-revealed deviations (not yet reflected in planning docs)

Carried over from the previous checkpoint (all still true): `ContainerSpec.Cmd`,
`ProviderResourceAware`, `SecretsAware`, `ResourceSetAware`, secretRef graph
edges. New this session:

- **Engine handles `SecretReference` directly** — it has no provider, so
  `reconcileOne`/`Destroy` special-case it: reconcile = validate + resolve
  through the SecretStore (nothing stored), destroy = drop from state. First
  real-container run revealed apply could never process the kind before.
- **Plan cascade** (`application/plan.Compute`): a managed resource whose
  *dependency* changed gets `ActionUpdate` even if its own spec hash is
  unchanged (reason: `dependency <key> changed`). Required by the Phase 4
  exit criterion "changing `Dataset.spec.format` updates the connector" —
  the Binding's own spec doesn't change. Single deterministic pass over
  topological levels; unchanged manifests still produce all-noop plans.
- **Debezium images come from quay.io** (`quay.io/debezium/connect:2.7`);
  the Docker Hub `debezium/connect` repo has no 2.x tags. The spec doc's
  sketch predates this.
- **Postgres provider pre-creates `dbz_publication` (FOR ALL TABLES) as
  superuser** during Source reconciliation and grants `pg_read_all_data` to
  the replication role — the least-privilege pgoutput setup; Debezium's own
  publication autocreate would need superuser/table ownership.
- **Debezium connector config sets `topic.creation.default.*`** so Connect
  creates per-table CDC topics (Redpanda doesn't auto-create).
- **s3sink worker tuning**: `OFFSET_FLUSH_INTERVAL_MS=5000` (files are cut
  on offset commit) and `CONNECT_CONSUMER_METADATA_MAX_AGE_MS=10000`
  (topics.regex only discovers late-created CDC topics on metadata refresh;
  the 5-minute default stalls pipelines).
- **`s3sink` requires `configuration.image`** — no stock Connect image
  carries an S3 sink plugin. Reference Dockerfile (Debezium Connect + Aiven
  s3-connector v2.15.0) in `testdata/s3sink-image/` and
  `examples/cdc-attendance/s3sink-image/`.
- **Sink formats**: `SupportedSinkFormats() = [json, jsonl, csv, parquet]`
  (the Aiven connector's set), but parquet needs schema-carrying records —
  this pipeline runs schemaless JSON converters, so the example Dataset uses
  `format: json`, deviating from the §6 sketch's `parquet`. Flag to a human
  at v1.0.0 sign-off: either accept json in the acceptance scenario or wire
  schema-carrying converters for parquet.

## Known rough edges / things to check first

1. **No JSON Schema validation yet** (see above) — highest-priority
   pre-sign-off backfill, can land in parallel with Phase 5.
2. **Debezium provider assumes Postgres** — `connector.class` hardcoded.
   Fine for v1.0.0 scope.
3. **Port collisions on a shared dev machine** — test scenarios use
   non-default host ports (cdc: pg 15544 / connect 18183 / kafka 19193;
   sink: pg 15545 / connect 18185+18186 / kafka 19194 / minio 19101).
   `examples/cdc-attendance/` also moved off the well-known defaults
   (pg 15432 / connect 18083+18084 / kafka 19093 / minio 19000) after a
   real user machine had Postgres/Connect/MinIO already holding
   5432/8083/9000 — see errors.md for the full post-mortem.
4. **`.claude/rules/schema-changes.md` still never fires** (no `schemas/`).
5. **minio image is `minio/minio:latest`** in the example and sink test —
   pin a release tag once one is chosen for v1.0.0.

## How to verify state after resuming

```bash
# Full unit/contract suite (no Docker needed) — all green as of this checkpoint:
go build ./... && go vet ./... && go test ./...

# Integration suite (needs a running Docker daemon) — all green as of this checkpoint:
just test-integration
# or selectively:
go test -tags integration -run TestRedpandaEndToEnd -timeout 600s ./cmd/platformctl/
go test -tags integration -run TestCDCEndToEnd -timeout 1200s ./cmd/platformctl/
go test -tags integration -run TestSinkEndToEnd -timeout 1500s ./cmd/platformctl/

# The acceptance example validates and graphs cleanly:
go run ./cmd/platformctl validate examples/cdc-attendance/
```

## Suggested next steps, in order

1. **Phase 5**: `import` command + `Imported` lifecycle path; honor
   `Source.spec.external: true` end-to-end; `drift` command with
   `DriftDetected` surfaced in `plan`/`status`; engine-level NFR-3 destroy
   guard (External resources refuse deletion without both flags). Register
   the three Phase 5 gates. The engine already has `Lifecycle`,
   `plan.ActionConfigure`, and per-provider `Probe` implementations to build
   on.
2. Backfill `schemas/*.json` + a JSON Schema validator in `manifest.Load`,
   with the matching 03-resource-model-reference.md sync check
   (schema-doc-sync agent exists for this).
3. `platformctl docs build|serve` from the schemas directory (DoD item).
4. Automate the §6 acceptance scenario against `examples/cdc-attendance/`
   literally (steps 1–10, incl. mid-apply kill recovery), then re-run every
   phase's exit criteria one final time before declaring v1.0.0
   (06-agentic-execution-guide.md §7 step 5).
