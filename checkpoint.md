# Datascape build — checkpoint (v1.0.0 declared; Phase 6 committed scope done)

Written for whichever agent resumes work. Previous goal — ship v1.0.0 and
complete Phase 6 — is **done** (see verification below). This file records
exactly where everything stands and what a next session would pick up.

**Read `docs/planning/06-agentic-execution-guide.md` first if you haven't.**
`CLAUDE.md` is the always-loaded summary of layering rules and conventions.

## Phase status vs. roadmap (`docs/planning/04-roadmap-and-feature-gates.md`)

| Phase | Status | Verified by |
|---|---|---|
| 0 — Foundations | Done | `cmd/platformctl/e2e_test.go`, plan golden file |
| 1 — Docker runtime | Done | runtime conformance + `docker_integration_test.go` |
| 2 — Redpanda | Done | `redpanda_integration_test.go` |
| 3 — Postgres + Debezium CDC + lineage | Done | `cdc_integration_test.go`; lineage + capability checks in engine/compatibility unit tests |
| 4 — Object storage sink | Done | `sink_integration_test.go` (objects land from real CDC traffic) |
| 5 — Import/External/Drift — **v1.0.0 declared** | Done | `phase5_integration_test.go`, `chaos_integration_test.go`, `drift` cmd, NFR-3 double lock (CLI + engine), `acceptance_integration_test.go` (spec §6 against `examples/cdc-attendance/`, steps 1–4 in ~22s vs 4-min NFR-8 budget) |
| 6 — Scale-out (committed scope) | Done | `TestParallelReconciliation` (race-clean), vault backend vs real dev server (`vault_integration_test.go`), file backend + router unit tests |
| 6 — optional openlineage provider | **Not built, by design** | Roadmap marks it optional ("not a gap in v1.0.0"); LineageObservability stays Alpha because its Beta graduation is contingent on this provider existing |
| 7/8 — K8s runtime, plugins | Not started (future) | — |

## v1.0.0 Definition-of-Done ledger (spec §9)

- Phase 0–5 exit criteria: all individually covered by the tests above.
- §6 acceptance scenario automated in CI: `acceptance_integration_test.go`
  (steps 8/9 covered by TestExternalSourceEndToEnd / TestChaosApplyKilledMidRun
  on equivalent sets; step 5's fake-provider half in engine unit tests, the
  real half asserted on the live connector's openlineage.* config).
- NFRs: determinism (plan golden), idempotency (re-apply asserts in every
  e2e), safety (NFR-3 tests), recoverability (SIGKILL chaos), performance
  (asserted in acceptance test), lineage mechanism (engine tests), sink
  correctness (objects land) — all automated.
- Gates at GA: DockerRuntime, RedpandaProvider, PostgresProvider,
  DebeziumCDCProvider, CDCBinding, ObjectStoreProvider, SinkBinding.
  Beta/enabled: DriftDetection, ExternalResourceConfiguration.
  Alpha/disabled: ImportedResources (roadmap says Beta in Phase 6 — see
  open items), ParallelReconciliation, VaultSecretBackend,
  LineageObservability, ContainerProvider (test-only).
- `docs build`/`docs serve` generate the reference from `schemas/`
  (committed under `docs/reference/`); schema validation live in
  `manifest.Load` with negative-path tests.
- README quickstart runs the example end-to-end; `--version` reports v1.0.0.

## The taxonomy revision (read before touching Binding/compatibility)

`docs/design/001-bindings-are-directed-edges.md` is the authoritative note.
Summary: asset kinds are role-neutral (Source = engine-backed database,
EventStream = log, Dataset = object location); `Binding.mode` names the
movement mechanism; `binding.AllowedKindPairs` is a **relation** — sink
admits EventStream→Dataset and EventStream→Source, ingest admits
Dataset→EventStream. Capability seams `DatabaseSinkCapableProvider` and
`IngestCapableProvider` exist with no shipped implementation: such Bindings
validate structurally, then fail with the standard capability error.
docs/planning/03 §7.1/§7.2 were deliberately revised to match (project-owner
mandated, pre-GA).

## Architecture facts an agent needs (beyond CLAUDE.md)

- Engine special-cases: SecretReference (resolve-only), external-no-provider
  resources (connection-resolvable check; destroy = forget-state-only under
  the NFR-3 double lock `Engine.AllowDestructive`), drift healing (plan-noop
  + Managed + DriftDetected → re-reconcile), `Engine.Import` (adopt-by-name,
  probe-never-create), `Engine.Parallelism` (per-level concurrency; state
  writes serialize behind `stateMu`).
- Plan cascades: a changed dependency marks dependents ActionUpdate.
- Destroy blocks dependencies of failed resources (mirror of apply blocking)
  and tolerates already-dead backing infra in every provider's sub-resource
  destroy (Inspect-first guards).
- Secrets: router keyed by backend — env, file (`$DATASCAPE_SECRETS_DIR/
  <name>/<key>`), vault (KV v2, VAULT_ADDR/VAULT_TOKEN, gated). kubernetes
  fails fast with "not available".
- Connect-based providers share `internal/adapters/kafkaconnect`; PUT
  retries transient 409/validation errors 90s; WaitConnectorRunning
  restarts FAILED instances (rate-limited) until deadline.
- Docker adapter: spec-hash reuse verifies network attachment; WaitHealthy
  errors carry last log lines; unlabeled objects are never touched (which
  means: destroy --include-imported on an adopted unlabeled container
  refuses — documented v1 limitation).
- `status.SetCondition` keys by Type only (k8s semantics).
- s3sink requires `configuration.image` (no stock image has an S3 plugin;
  Dockerfiles in testdata/s3sink-image and examples/.../s3sink-image).
- Debezium images come from quay.io; example uses non-default host ports
  (pg 15432 / connect 18083+18084 / kafka 19093 / minio 19000).

## Known open items (next session's natural backlog)

1. **ImportedResources → Beta** (roadmap says Beta in Phase 6): flip the
   gate default + promote import out of Alpha once exercised more broadly.
2. **Optional openlineage provider** (Marquez + its postgres as one
   provider) — unlocks LineageObservability → Beta and an end-to-end
   lineage demo. The LineageAware mechanism is already proven.
3. **Providers for the new pairings**: `jdbcsink` (sink→Source) and an
   s3-source provider (ingest) over the existing Connect-worker pattern —
   pure adapters, no schema work needed.
4. **Parquet in the acceptance example** (deviates from the §6 sketch: uses
   json because the pipeline runs schemaless converters) — either accept
   json permanently or wire schema-carrying converters.
5. minio image is `minio/minio:latest` in example + sink test — pin a tag.
6. `ContainerProvider` test-only gate could be retired.
7. Tag exists locally? — see "Release mechanics" below; if the v1.0.0 tag
   isn't on the remote, push it (`git push origin v1.0.0 && git push`).

## Release mechanics

- `main.version = "v1.0.0"`; `git tag v1.0.0` created at the final verified
  commit. CI (.github/workflows/ci.yml): unit job (gofmt/build/vet/tests/
  example validate) gating the integration+chaos job (pre-pulls images,
  runs the full -tags integration suite, which includes the acceptance
  scenario).

## How to verify state after resuming

```bash
go build ./... && go vet ./... && go test ./...      # unit/contract — green
go test -tags integration -timeout 2400s ./...       # full e2e incl. acceptance + chaos — green
go run ./cmd/platformctl validate examples/cdc-attendance/
```

All green as of the v1.0.0 tag.
