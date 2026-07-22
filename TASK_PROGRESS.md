# E5: Provider-owned schema fragments + typed option validation — progress

Task: docs/planning/08 §7 E5 (Size L). Final commit: ONE squashed commit
`feat(validation): provider-owned schema fragments + negative corpus (E5)`.
Branch: worktree-agent-af9e4d6fb80205dd4 (do not touch main; no push).

Design decision (recorded here + in final commit body): fragments live
under `schemas/v1alpha1/fragments/{provider,source,catalog,binding}/*.json`
(embedded via a new `schemas.FragmentFS`), NOT physically inside each
adapter package. Composition happens in Go
(`internal/application/manifest/fragment.go`), not via JSON `$ref`/if-then
chains inside the core Kind schemas (provider.json/source.json/etc. are
UNCHANGED) — this keeps the core Kind shape and each provider's fragment
independently evolvable, avoids a 17-way `if spec.type == ...` inside one
shared file, and needs no new adapter->schema-compiler dependency (schemas
remain pure data, manifest already imports `schemas`). "Provider-owned" is
satisfied by one file per provider/engine/binding-discriminator, named for
it, with the redundant-check migration recorded in that provider's own
diff.

## Steps

1. [done] Read: doc 08 E5 + doc 07 §3.1, doc 02 §4.2/§5.6/§11, doc 06
   §2.1/§10, ADR 011, schemas/ layout, manifest/schema.go, registry.go,
   docsgen.go, compatibility.go (where SpecValidator/BindingOptionsValidator
   are actually invoked today — NOT apply time, already validate-time via
   `compatibility.Check`; the gap is uneven/non-generated shape coverage +
   no typo protection, not "fails only at apply" as doc 07's older framing
   suggested for most cases — except Source/Catalog engine blocks, see
   Finding below).
2. [done] Mined every provider's `ValidateSpec`/`ValidateBindingOptions` Go
   body (redpanda, postgres, mysql, debezium, s3, s3sink, jdbcsink,
   s3source, nessie, openlineage, proxy, prometheus, grafana, ingress,
   trino, wireguard) to classify each check shape-only (migratable) vs
   cross-field/graph (stays in Go per ADR 011 — a JSON Schema fragment
   cannot express "this string must equal a value in a sibling array/another
   resource" without hard-coding it, e.g. every `*SecretRef` ∈
   `spec.secretRefs` check, and `bootstrapServers`-required-unless-
   graph-inferred for debezium/s3sink/jdbcsink/s3source).
3. [done] **Finding (real, pre-existing gap, not a manifest bug — E5 closes
   it, no manifest edited):** `Source.spec.<engine>.database` (postgres/
   mysql/mariadb) was NOT checked at validate time at all — only at
   reconcile (`"Source %q: spec.postgres.database is required"` in
   postgres.go/mysql.go/backup.go). Every shipped example/testdata manifest
   already sets it (since reconcile would otherwise fail), so making it
   `required` in the new Source-engine fragments is behavior-preserving and
   closes exactly the kind of apply-time-only regression ADR 011 names.
4. [done] Cross-checked every fragment's key list against the ACTUAL keys
   used across `cmd/platformctl/testdata/**/*.yaml` and `examples/**/*.yaml`
   (a Python/yaml scan, not just Go-source grep — caught `port`/`apiPort`
   keys my first Go-grep pass missed for postgres/mysql/nessie/openlineage/
   prometheus/grafana/trino, since those read the key through
   `providerkit.HostPort(cfg, name, "port")` inline rather than a literal
   `cfg.Configuration["port"]` index).
5. [done] Fragment files written under `schemas/v1alpha1/fragments/`:
   16 provider files (redpanda, postgres, mysql[shared mariadb],
   debezium, s3[shared minio], s3sink, jdbcsink, s3source, nessie,
   openlineage, proxy, prometheus, grafana, ingress, trino, wireguard —
   noop/container deliberately excluded, test-only per provider.json's own
   description, never "shipped"), 3 source-engine files (postgres, mysql,
   mariadb), 1 catalog-engine file (nessie), 4 binding-options files
   (cdc-debezium, sink-s3sink, sink-jdbcsink, ingest-s3source).
6. [done] `schemas/embed.go`: `FragmentFS` + `ProviderConfigFragments`/
   `SourceEngineFragments`/`CatalogEngineFragments`/`BindingOptionsFragments`
   maps.
7. [done] `internal/application/manifest/fragment.go` (new file): lazy
   `compiledFragments()` (same `sync.Once` pattern as `schema.go`'s
   `compiledSchemas()`), `validateProviderConfigurationFragment`,
   `validateEngineFragment` (Source/Catalog), `validateBindingOptionsFragment`
   (resolves providerRef -> provider type via a pre-pass map built in
   `manifest.Validate`, since a Binding may precede its Provider in file
   order; silently no-ops on an unresolved ref — `compatibility.Check`
   already gives the authoritative graph error for that).
8. [done] `internal/application/manifest/manifest.go`: `Validate` wires the
   three fragment-check call sites into the existing per-Kind switch.
9. [done] Gates so far: `gofmt -l .` empty; `go build ./...` and
   `-tags integration` both clean; `go vet ./...` and `-tags integration`
   both clean; unfiltered `go test ./... ; echo true-exit=$?` = 0 (two
   `cmd/platformctl/lint_h2_test.go` fixtures needed a `postgres: {database:
   attendance}` block added — synthetic lint-test fixtures, not a
   blueprint/example/scenario manifest, so allowed; this was itself an
   instance of the Finding in step 3).
10. [done] docsgen: `renderFragmentsFor`/`renderFragmentGroup`
    (`internal/application/docsgen/docsgen.go`) append a fragment-derived
    reference section to provider.md/source.md/catalog.md/binding.md;
    regenerated via `go run ./cmd/platformctl docs build --out
    docs/reference`; `TestGeneratedReferenceInSync` green.
11. [done] doc 03 additive sync: new §4.1, §5.2, §7.1, §8.1.1 subsections
    (fragment layout, keyed by provider.md/source.md/catalog.md/binding.md's
    new generated sections) — additive only, guard hook passed.
12. [done] Pruned redundant standalone shape-check `if` blocks (positive-int/
    enum/required-field, no longer reachable ahead of `SpecValidator` on any
    real CLI path) from redpanda, postgres, mysql, debezium, s3, s3sink,
    jdbcsink, s3source; each site left a comment naming the fragment now
    responsible and why the remaining checks stay (cross-field, or a shared
    helper also called from `Reconcile`, e.g. trino.workerCount,
    wireguard.parseConfig — NOT touched). Updated the direct-unit-tests that
    exercised the deleted checks (they call `ValidateSpec` directly,
    bypassing the manifest/fragment layer) to stop asserting the
    now-fragment-owned rejection while keeping the "still accepts every
    legal value" and cross-field assertions. **Scoping note (deliberate, not
    exhaustive):** prometheus/grafana/ingress/trino/wireguard's
    image/domain-string shape checks and postgres/mysql's version-catalog
    interplay were left untouched — postgres/mysql's `metrics` enum check
    WAS deleted (zero direct test coverage, confirmed before deleting); the
    others have direct `ValidateSpec`-calling unit tests asserting the exact
    shape rejection, and rewriting all of them was traded off against
    finishing the corpus + doc sync within this session's budget. This is a
    narrower sweep than "every provider fully pruned" — recorded here and in
    the final commit body, not hidden.
13. [done] Negative-test corpus: `cmd/platformctl/testdata/negative-corpus/`
    (19 fixtures, one per misconfiguration class) +
    `cmd/platformctl/negative_corpus_test.go`'s `TestNegativeCorpus`
    (table-driven, real `platformctl validate` invocation via the existing
    `run(t, ...)` cobra-in-process helper, one subtest per fixture,
    asserting non-zero exit + the error names both the resource and the
    offending field/value). All 19 green on first full run after fixture
    construction. Class list: redpanda {schemaRegistry enum typo, brokers
    wrong type, unknown-key typo}, postgres {metrics enum typo}, mysql
    {metrics enum typo}, debezium {workers wrong type}, s3 {nodes
    unsupported topology 2/3 — a SpecValidator cross-field rule, not a
    fragment, included anyway since the Accept bar is "fails at validate"},
    s3sink {missing required image}, jdbcsink {missing required image},
    s3source {missing required credentialsSecretRef}, wireguard {missing
    required peerNetwork/peerPublicKey/peerEndpoint/address/allowedIPs},
    nessie Catalog {unknown-key typo}, Source engine {postgres/mysql/mariadb
    missing database — the real reconcile-time-only gap this task closed},
    Binding options {cdc-debezium snapshotMode typo, cdc-debezium empty
    tables array, sink-s3sink format typo, sink-jdbcsink missing required
    format, ingest-s3source unknown-key typo}.
14. [done] `bash scripts/test-impact.sh --base main --print`: 16 suites
    selected (broad, as expected — manifest.Load is shared core). Real
    (non-print) sweep launched in the background at final-commit time;
    per task instructions, not polled — see final report for the launch
    command and log path.
15. [done] Doc 08 Stage E exit-criterion checkbox toggled `[x]` (pure toggle,
    separate edit from the additive Done-note under E5's own section, to
    satisfy the guard hook's "toggle-only or purely-additive, never both in
    one edit" rule).
16. [next→closing] Squash the 5 WIP commits into the one final commit
    `feat(validation): provider-owned schema fragments + negative corpus
    (E5)` immediately after this checkpoint.

## Verification log

- `go test ./...` full run (final, after all steps above): PASS,
  true-exit=0.
- `gofmt -l .`: empty. `go build ./...` and `go build -tags integration
  ./...`: clean. `go vet ./...` and `go vet -tags integration ./...`: clean.
- `go run ./cmd/platformctl docs build --out docs/reference` regenerated
  binding.md/catalog.md/provider.md/source.md;
  `TestGeneratedReferenceInSync` green.
- `examples/cdc-attendance` and `examples/lakehouse` re-validated directly
  (`platformctl validate <dir> --feature-gates ...`) after all fragment
  changes: both still "N resource(s) valid" with zero manifest edits.
- `bash scripts/test-impact.sh --base main --print`: 16 suites selected
  (redpanda, cdc, sink, connect-ha-dlq, acceptance, lakehouse, backup,
  prometheus, monitoring, object-store-posture, trino, jdbcsink, s3source,
  wireguard, external-import, external-db-tls) — the real sweep is
  Docker-backed and was launched in the background, not awaited.
