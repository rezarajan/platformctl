# D8: First-class Catalog↔warehouse reference — TASK_PROGRESS

Doc: docs/planning/08-production-readiness-plan.md §6, D8. Protocol: doc 08
§2.1 (this file is step 0). Supersedes the prior D3/D4 checkpoint content
below this task's own history (that work is already merged on main;
`git log -- TASK_PROGRESS.md` has the prior content if needed).

## Pre-reading done

- D8's full entry (doc 08 §6).
- ADR 006's "Implementation notes (D10, added post-implementation)" —
  D10's four recorded deviations + five live-caught fixes, in particular:
  `configRefFields` (nested-ref graph extraction under `spec.configuration`)
  and `warehouseProviderRef` (trino's own S3-provider disambiguator, added
  *because* D8 wasn't implemented yet).
- `internal/domain/graph/graph.go`: `refFields` (top-level, a slice) vs
  `configRefFields` (nested under `spec.configuration`, D10's mechanism).
  D8's `warehouseRef` is **top-level** on Catalog (task text is explicit:
  "not inside the engine block, so the graph needs no engine-block
  introspection after all") — so it belongs in `refFields`, not
  `configRefFields`. `configRefFields` stays untouched.
- `internal/adapters/providers/nessie/nessie.go` in full: `defaultWarehouseEnv`
  reads `spec.configuration.defaultWarehouseLocation`/`warehouseS3Endpoint`/
  `warehouseS3SecretRef` (Provider-level, static) at `reconcileInstance`
  (Kind==Provider) container-create time.
- `internal/adapters/providers/trino/catalogconfig.go` + `trino.go`:
  `CatalogFacts` resolution lives entirely in
  `internal/application/engine/engine.go`'s `resolveCatalogFacts`, not in
  the trino package itself — trino only *consumes* `req.CatalogFacts`.

## The reconciliation-ordering problem (why this isn't a trivial field add)

Catalog already depends on its `providerRef` (nessie Provider) — so nessie's
own Provider-kind reconcile (which boots the container) necessarily runs
**before** any Catalog that references it. But `Catalog.spec.warehouseRef`
lives on the Catalog, naming a Dataset — info only available *after* the
Catalog resolves its own dependency. Nessie's Iceberg REST personality needs
its default-warehouse + S3-credential config baked in as **container env at
create time** (Quarkus config, no REST endpoint to set it dynamically) — so
there's a real chicken-and-egg: the facts needed to configure nessie's
container aren't known until after nessie's own Provider-kind reconcile has
already run.

Resolution (recorded as the design, see ADR 006 addendum + code comments):
- Graph: add `warehouseRef` to `refFields` (top-level), kind-checked to
  `Dataset`. Catalog -> Dataset edge; Dataset already has its own
  `providerRef` edge to its realizing (s3/minio) Provider. So by the time
  Catalog reconciles, both the Dataset and its Provider have already
  reconciled and published state, in the *same* apply.
- Engine: new `reconciler.Request.WarehouseFacts` (mirrors `CatalogFacts`'s
  published-facts-only discipline, ADR 015), resolved **for Catalog-kind
  Resources only** (`resolveWarehouseFacts`), non-nil whenever warehouseRef
  is set and its chain is published — which, thanks to the graph edge above,
  is always true on a normal single apply.
- nessie's `reconcileCatalog` (Kind==Catalog step, runs strictly after the
  Provider-kind step that first booted the container) computes the desired
  warehouse env from `req.WarehouseFacts` (skipped when the Provider already
  sets an explicit `defaultWarehouseLocation` — override wins, additive
  coexistence, no removal) and calls `providerkit.EnsureInstance` **again**
  with the same Image/Network/Ports but corrected Env. `EnsureContainer`'s
  existing spec-hash idempotency (`docker.go`'s `specHash`/`specGenLabel`)
  is the *only* mechanism needed for idempotency here: first Catalog
  reconcile after warehouseRef is introduced recreates the container once
  (spec hash changes); every later reconcile with unchanged facts is a
  zero-Docker-API-call no-op — no new drift-fingerprint bookkeeping
  required. `Probe` is intentionally not extended to detect out-of-band
  drift of the *derived* warehouse env — recorded as a follow-up (out of
  D8's scope; the pre-existing explicit-config path had no such detection
  either).

Trino's own `warehouseProviderRef` resolution order (`resolveCatalogFacts`
in engine.go), recorded per the task's instruction: (1) the referenced
Catalog's own `warehouseRef` chain (Catalog -> Dataset -> Dataset's
realizing Provider) — canonical once set; (2) trino's own
`configuration.warehouseProviderRef` explicit disambiguator (pre-D8, kept,
becomes unnecessary once (1) applies); (3) auto-infer the sole S3/MinIO
Provider in the namespace. `resolveDatasetS3Facts` factors the shared tail
(Dataset -> Provider -> published "s3" fact + secretRef name) used by both
`resolveWarehouseFacts` and this fallback chain.

## Step plan

1. [done] Read CLAUDE.md, doc 08 §2.1 + D8 entry, ADR 006 Implementation
   notes, graph.go, nessie.go, catalogconfig.go/trino.go, doc 03 §8.1,
   lakehouse example, trino-scenario testdata, engine.go's
   resolveCatalogFacts/resolveMetricsTargets precedent, reconciler.go
   Request/CatalogFacts, TestResolveCatalogFactsFromCatalogRef fixture
   style, TestLakehouse integration test, gatherToolFacts (confirmed:
   already fact-driven, no changes needed for the inventory accept item).
2. [done] This file — reconciliation design recorded.
3. [in-progress] Implement:
   - `internal/domain/catalog/catalog.go`: `WarehouseRef *string`.
   - `internal/domain/graph/graph.go`: `warehouseRef` -> `Dataset` in
     `refFields`/`allowedKinds`.
   - `schemas/v1alpha1/catalog.json` + doc 03 §8.1: additive `warehouseRef`.
   - `internal/ports/reconciler/reconciler.go`: `WarehouseFacts` type +
     `Request.WarehouseFacts` field.
   - `internal/application/engine/engine.go`: `resolveWarehouseFacts`,
     `resolveDatasetS3Facts` (shared), `resolveCatalogFacts` gains the
     warehouseRef-chain-first precedence.
   - `internal/adapters/providers/nessie/nessie.go`: `instanceSpec` factor,
     `warehouseFactsEnv`, `reconcileCatalog` wiring.
   - `examples/lakehouse/catalog-and-connections.yaml`: `warehouseRef:
     {name: warehouse}` (the pre-existing, previously-unwired `warehouse`
     Dataset).
   - `cmd/platformctl/testdata/trino-scenario/manifests.yaml`: prove the
     resolution order live (warehouseRef present alongside
     warehouseProviderRef, same target — see manifest comment).
   - ADR 006: additive "Implementation notes (D8, added post-implementation)"
     section.
4. [pending] Tests: graph_test.go (ordering + negative path), catalog
   decode test, engine_test.go (WarehouseFacts resolution + trino
   precedence order), nessie unit test (new file) for `warehouseFactsEnv`.
5. [pending] Verify: gofmt, build, vet, go test ./..., scripts/test-impact.sh
   --base main, targeted integration (TestLakehouse, trino e2e).
6. [pending] Commit.

## Verification log

- gofmt -l . / go build ./... / go vet ./... — clean.
- go test ./... — all green, including new tests: graph_test.go (2 new:
  ordering + wrong-kind rejection), catalog_test.go (new file, 2 tests),
  nessie_test.go (new file, 5 tests: env derivation, secretRef error,
  skip-without-facts, skip-with-explicit-override,
  recreate-once-then-idempotent via fake runtime MutationCount),
  engine_test.go (2 new: WarehouseFacts resolves within one apply;
  warehouseRef-chain-first precedence over warehouseProviderRef).
- docs/reference regenerated (go run ./cmd/platformctl docs build --out
  docs/reference) — TestGeneratedReferenceInSync green.
- `go run ./cmd/platformctl validate examples/lakehouse` — 20 resources
  valid (warehouseRef wired, no schema/graph errors).
- `go run ./cmd/platformctl validate --feature-gates=TrinoProvider=true,...
  cmd/platformctl/testdata/trino-scenario` — 17 resources valid.
- **Live Docker: TestTrinoComputeEngineEndToEnd — PASS (117.93s), zero live
  bugs found this run.** Confirmed via docker inspect that the nessie
  container's env carries the warehouseRef-derived
  NESSIE_CATALOG_WAREHOUSES_WAREHOUSE_LOCATION=s3://raw-events/
  iceberg-warehouse/ (matching the new trn-warehouse Dataset, not the old
  hardcoded Provider-level fields, which were removed from the scenario) —
  the D8 derivation path is genuinely exercised, not just present in code.
  Full accept list (Ready, scale 2->3 in place, drift-heal, idempotent
  re-apply "no changes", validate rejection) all still green.
- **Live Docker: TestLakehouse — PASS (67.89s)**: examples/lakehouse applies
  to Ready with warehouseRef (no Provider-level defaultWarehouseLocation),
  including idempotent re-apply ("no changes"), secret rotation, CDC
  through the managed Connection, drift/heal, clean destroy. Bonus:
  **TestLakehouseExampleOnKubernetes also matched the -run pattern and
  passed (135.82s)** — the warehouseRef flow verified on the Kubernetes
  runtime too, unplanned but recorded.
- **Live inventory check — PASS** (accept: "inventory --for spark/trino
  reflect the warehouse"; scripted apply -> inventory -> probe -> destroy,
  scratchpad/inventory_check.sh, full log in the session transcript):
  - apply examples/lakehouse: 20 applied, all Ready.
  - docker inspect catalog-svc env shows the warehouseRef-DERIVED config:
    `NESSIE_CATALOG_WAREHOUSES_WAREHOUSE_LOCATION=s3://warehouse/iceberg/`
    (bucket+prefix of the `warehouse` Dataset), warehouse-creds
    name/secret from lake-minio-root — no Provider-level explicit fields
    exist in the example anymore.
  - `curl http://127.0.0.1:19121/iceberg/v1/config` answers 200 with
    defaults — pre-D8 (no warehouse config at all in this example) this
    endpoint answered 500 "No default-warehouse configured"; D8's derived
    config genuinely took effect server-side.
  - `inventory --for spark`: renders catalog uri
    (http://127.0.0.1:19121/iceberg), ref main, s3a endpoint
    (http://127.0.0.1:19010) + lake-minio-root credsRef.
  - `inventory --for trino`: paste-ready lakehouse.properties with
    iceberg.rest-catalog.uri + s3.endpoint + credsRef comment.
  - destroy: 19 succeeded, 0 failed; external Source untouched.
  - gatherToolFacts needed NO code change: it is already fact-driven
    (published endpoint facts), which is exactly why the snippets pick the
    warehouse up automatically — verified live rather than modified.
- scripts/test-impact.sh --base main: 14 suites selected (diff touches
  SHARED_CORE: internal/domain, internal/ports, internal/application/
  engine). Launched; result + per-suite timings recorded below when done.
