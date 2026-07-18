# Datascape build — checkpoint (v1.0.0 declared; Phases 6 and 6.5 done)

Written for whichever agent resumes work. Goals to date — ship v1.0.0,
complete Phase 6, and land Phase 6.5 (orchestrator-ready infrastructure,
remodeled architecture-first per project-owner direction) — are **done**
(see verification below). This file records exactly where everything stands
and what a next session would pick up.

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
| 6 — optional openlineage provider | Done (in 6.5) | Built as the `openlineage` provider (Marquez + dedicated Postgres); LineageObservability graduated to Beta |
| 6.5 — Orchestrator-ready infrastructure | Done | `lakehouse_integration_test.go` against the literal `examples/lakehouse/`: Catalog(nessie) + managed Connection + external Source with CDC flowing through the entrypoint + MySQL + Marquez, incl. Connection drift-heal and clean destroy |
| 7 — Kubernetes runtime adapter | Started, Alpha, early | `internal/adapters/runtime/kubernetes`; passes `internal/ports/runtime/conformance` live against a real cluster; unmodified `redpanda` provider reconciled through `platformctl apply` end-to-end |
| 8 — External/Terraform adapter, plugins | Not started (future) | — |

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
  Beta/enabled: DriftDetection, ExternalResourceConfiguration,
  ImportedResources, LineageObservability.
  Alpha/enabled (Phase 6.5 hardening): MySQLProvider, NessieProvider,
  OpenLineageProvider, ProxyProvider.
  Alpha/disabled: ParallelReconciliation, VaultSecretBackend,
  ContainerProvider (test-only).
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

## The Catalog/Connection remodel (read before touching providers)

Phase 6.5 was redirected mid-flight (project-owner direction; historical
first cut recorded in `docs/design/002-*.md` + addendum): **extend the
resource model before implementing functionality**. Two provider-agnostic
kinds landed:

- `Catalog` — engine-discriminated (`spec.engine: nessie | hive | ...`,
  engine-named block), mirroring `Source`. The nessie provider realizes it
  (instance + default-branch reconciliation) via
  `CatalogCapableProvider.SupportedCatalogEngines()`.
- `Connection` — non-secret "how to reach a system": managed (proxy
  provider runs one socat forwarder per Connection, named after it,
  network+host) or external (plain address record). `secretRef` names the
  credentials. `connectionRef` fields resolve Connection-first,
  SecretReference as v1.0.0 shorthand. Bindings on external Sources consume
  the Connection automatically (endpoint + creds — the working provider
  must list the Connection's secretRef in its `spec.secretRefs`).
  `ConnectionCapableProvider.SupportedConnectionSchemes()` gates realization.

Imported-vs-external distinction and the external-integration walkthrough
live in docs/planning/03 §3.1–3.2; kind references in §8.1–8.2. "Soak" was
removed from all product surfaces (it names nothing; retained only in the
historical design note).

## Architecture facts an agent needs (beyond CLAUDE.md)

- Engine special-cases: SecretReference (resolve-only, stores a one-way
  resolved-material fingerprint on apply, reports `SecretChanged` drift when
  the backend value differs), external-no-provider
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

## Post-6.5 hardening (errors.md + feature-requests.md sweep)

A round of reported errors and feature requests, all resolved and committed:

- **Secret pre-flight + `--env-file`**: apply/import resolve every declared
  SecretReference before touching infra (`SecretStore.Preflight`,
  `Engine.PreflightSecrets`), aggregating all missing vars; a persistent
  `--env-file` loads dotenv (shell env wins). Cannot half-apply for a
  missing credential.
- **Secret drift fingerprints**: apply stores a one-way fingerprint for each
  resolved SecretReference; drift/status reports changed backend material as
  `SecretChanged`; apply records the new baseline and re-reconciles dependents.
  Docker MySQL/MariaDB root rotation and Postgres superuser rotation are
  supported by using previous managed-container bootstrap credentials
  transiently, then applying the new resolved value in the database. Edge case
  documented: if both desired credentials and previous runtime env credentials
  fail, manual recovery is required because plaintext old secrets are not
  stored in state. External systems are not rotated; Debezium now preflights
  external database credentials through the Connection endpoint before
  registering a connector, producing a direct credential/reachability error.
- **Honest external reachability**: an external resource whose connectionRef
  names a Connection is TCP-probed (`Connection.DialAddress` +
  `probeTCPReachable`) — a dead-upstream forwarder reports
  `ExternalEndpointUnreachable`, not Ready. reconcile retries 30s.
- **`platformctl graph`** rewritten to render the *architecture*
  (`internal/application/archview`): data-flow pipelines + technology layer,
  honouring `-o tree|dot|mermaid|json` (the old `--format` flag ignored `-o`).
- **`platformctl inventory`** (aliases services/endpoints): service-endpoint
  explorer from state + the `internal/domain/endpoint` type every provider
  publishes; surfaces host + in-network address + credential SecretReference.
- **Docker-style apply progress**: `engine.Reporter` +
  `cliutil.ProgressReporter` — `[n/total]`, ◐ started / ✓✗ done + timing,
  ⟳ drift-heal, ⊘ skip; TTY-gated colour (NO_COLOR); stderr, stdout scriptable.
- **Searchable HTML docs**: `docsgen.Site()` renders the schema markdown via
  goldmark into a single self-contained page with sidebar + client-side
  search; `docs serve` serves it, `docs build --html` writes it.
- **Versioned providers** (`internal/domain/versionprofile`,
  `reconciler.VersionedProvider`): postgres & mysql/mariadb pin
  image+internals per version (postgres:16 mount /var/lib/postgresql/data,
  18 /var/lib/postgresql). Manifests use `configuration.version`; image
  without version fails validate. Other providers are single-profile.
- **Auto-allocated host ports** (`internal/domain/hostport`): a port is
  optional — omit it and a stable, per-name deterministic one (20000–29999)
  is used; pin only when a fixed one is needed. In-network address is the
  stable identifier; inventory surfaces the host port.

All unit + integration suites green. The `verify` details are below;
`errors.md` and `feature-requests.md` carry the per-item write-ups.

## Validate-time completeness (the DX contract)

`platformctl validate` is the gate: a manifest set that validates must not
be able to half-apply into a mis-wired platform. The mechanisms, in check
order (all in `loadAndValidate`):

1. JSON Schema per kind (shapes, required fields, no secret-bearing fields).
2. Kind-specific Go validation (`FromEnvelope` per kind).
3. Graph: every reference — providerRef/sourceRef/targetRef/connectionRef/
   secretRef — must resolve in-set (connectionRef's old skip-if-missing is
   gone; the engine demands resolution at apply, so validate does too).
4. Compatibility: Binding mode↔Kind pairing relation + capability per
   pairing; Catalog engine and Connection scheme capability; connectionRef
   targets must be Connection|SecretReference; Connection.secretRef must be
   a SecretReference; **`SpecValidator`** — providers validate their own
   configuration (debezium/s3sink: bootstrapServers, s3sink: image +
   credentialsSecretRef; postgres/mysql/s3: credential secretRefs declared
   and cross-listed in spec.secretRefs).
5. Feature gates: external-declaring sets need ExternalResourceConfiguration;
   every Provider's type resolves through the gated registry at validate
   (a disabled gate names itself and the enable flag).

Adding a provider with required configuration? Implement
`reconciler.SpecValidator` — apply-time-only config errors are regressions.

## Known open items (next session's natural backlog)

> **Superseded (2026-07-17):** these items (and every open item in
> docs/planning/07) are now sliced into the stage-gated, individually
> actionable backlog in `docs/planning/08-production-readiness-plan.md`
> (Stages A–E; its §9 maps each item below to a task ID). Work from that
> plan; the list below is retained as the historical record.

1. **Providers for the new pairings**: `jdbcsink` (sink→Source) and an
   s3-source provider (ingest) over the existing Connect-worker pattern —
   pure adapters, no schema work needed.
2. **Parquet in the acceptance example** (deviates from the §6 sketch: uses
   json because the pipeline runs schemaless converters) — either accept
   json permanently or wire schema-carrying converters.
3. minio image is `minio/minio:latest` in examples + sink test — pin a tag.
   Same for nessie/marquez/vault `latest` tags.
3b. **mariadb is registered but untested**: it shares the mysql adapter
   (image + binlog flags differ); no integration test applies a
   `type: mariadb` Provider yet.
4. `ContainerProvider` test-only gate could be retired.
5. **Tunnel provider** for VPC reach: the `Connection` kind is the seam
   (design note 002 addendum); a wireguard-typed provider chains a managed
   Connection's egress — additive, no schema change.
6. **Phase 6.5 gate graduations**: MySQL/Nessie/OpenLineage/Proxy providers
   are Alpha/enabled for their hardening period; promote once proven in
   real use.
7. Tag exists locally? — see "Release mechanics" below; if the v1.0.0 tag
   isn't on the remote, push it (`git push origin v1.0.0 && git push`).
8. **Kubernetes runtime adapter (Phase 7, started)**: `internal/adapters/
   runtime/kubernetes`, gated `KubernetesRuntime` (Alpha, disabled). Passes
   conformance live; unmodified `redpanda` provider reconciles through it.
   Biggest remaining gap: external reachability (Services are ClusterIP-only;
   `platformctl` run from outside the cluster can't reach them for a
   provider's own control-plane calls, e.g. redpanda's topic management) —
   needs a NodePort/port-forward design decision before this is useful
   beyond a Provider with no CLI-side follow-up calls. Full findings in
   `docs/planning/07-production-grade-docker-runtime-gap-analysis.md`'s
   "Cross-Runtime Portability" section, including a real bug (Docker's `Cmd`
   → Kubernetes `Args`, not `Command`) and a real port-boundary fix
   (`VolumeSpec.Networks`, since PVCs are namespace-scoped and Docker
   volumes are not) found by actually building the second adapter.
9. **Gate 2 (lakehouse/pipeline completeness) closed** (docs/planning/07,
   2026-07-16; §2.2's checkboxes themselves weren't ticked until the
   2026-07-17 remediation audit caught the staleness — code was correct,
   the doc wasn't): 2.2 bugs fixed (connector-name URL escaping,
   topics.regex quoting, URL-safe conn strings/DSNs with round-trip tests,
   unique database.server.id — a behavioral migration, see
   `docs/upgrade-notes.md` for the one-time drift report pre-existing
   MySQL/MariaDB CDC connectors show on the first apply after upgrading —,
   BindingOptionsValidator capability, deletionPolicy retain|delete on
   Dataset/Source — s3's silent bucket-wipe on destroy is gone);
   2.1 drift probes verify desired config (connector config diffs,
   wal_level/binlog_format/credential validity, retention.ms, prefix
   listability, upstream-through-forwarder) with a per-provider equivalence
   table in the doc; 2.5 images pinned + endpoints carry explicit
   insecure labeling; 2.3 `inventory --for spark|trino|dbt|psql|s3|kafka`
   renders paste-ready config; pairings without providers (ingest,
   sink→Source) documented as unavailable in 03 §7.2. Deferred-with-reason:
   schema registry (blocks Parquet/Avro production), tunnel/TLS providers
   on the Connection seam, image digests, out-of-band config-change tests.
   errors.md CI failure fixed (k8s conformance skips without a cluster;
   PLATFORMCTL_REQUIRE_K8S=1 enforces).
10. **Gate 1 (Docker production runtime) closed** (docs/planning/07 stage
   gate, 2026-07-16): all four acceptance criteria done across incremental
   commits — observed-port inspection (`ContainerState.Ports`/`HostAddr`),
   endpoint discovery from observed bindings (all nine providers), network
   aliases (Docker endpoint aliases / K8s alias Services), image pull
   policy + digest pinning, file-mounted secrets (`ContainerSpec.Files` +
   `ReadFile`; postgres/mysql/minio bootstrap passwords no longer in
   inspectable env — rotation recovery reads the file back, env fallback
   for pre-change containers), state-dir fsync. Explicitly deferred with
   rationale in the doc: registry auth, host-path mounts, 1.3 GC tooling,
   1.4 state doctor/repair.

## Release mechanics

- `main.version = "v1.0.0"`; `git tag v1.0.0` created at the final verified
  commit. CI (.github/workflows/ci.yml): unit job (gofmt/build/vet/tests/
  example validate) gating the integration+chaos job (pre-pulls images,
  runs the full -tags integration suite, which includes the acceptance
  scenario).
- Image pinning (docs/planning/08 A10): `scripts/pinned-images.txt` is the
  source of truth for every release-tested default image (provider
  defaults, examples, testdata); `scripts/refresh-digests.sh` resolves each
  to its current registry digest and rewrites every `repo:tag`/
  `repo:tag@sha256:...` occurrence across `*.go`/`*.yaml`/Dockerfiles
  in-place (idempotent — a second run with an unmoved upstream digest edits
  nothing). `.github/workflows/refresh-digests.yml` runs it weekly
  (`workflow_dispatch` also available) and opens a PR when a digest moved;
  it never gates `ci.yml`'s push/pull_request triggers. Support window per
  image: postgres (16/17/18), mysql (8.0/8.4), and mariadb (10.11/11) each
  track their own upstream EOL — add/drop a version by editing the
  provider's `versionprofile.Catalog` and `scripts/pinned-images.txt`
  together. The single-version providers (redpanda, debezium, minio,
  nessie, marquez, socat) are supported at exactly the pinned tag; bumping
  one is a deliberate version-bump PR (edit the `defaultImage` constant,
  `scripts/pinned-images.txt`, then run the refresh script), not something
  the scheduled job does on its own — the scheduled job only refreshes the
  digest *of the tag already pinned*, never changes which tag is pinned.

## How to verify state after resuming

```bash
go build ./... && go vet ./... && go test ./...      # unit/contract — green
just test-integration                                 # full e2e (-timeout 3600s) incl. acceptance, chaos, lakehouse — green
go run ./cmd/platformctl validate examples/cdc-attendance/
go run ./cmd/platformctl validate examples/lakehouse/
```

All green as of the Phase 6.5 (Catalog/Connection) commits.
