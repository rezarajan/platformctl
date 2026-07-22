# Design note 006 — Compute-engine infrastructure: Trino first, Flink deferred

**Status:** accepted; D10 task spec below, to be added to
`docs/planning/08-production-readiness-plan.md` Stage D as a new task.
**Prompted by:** `docs/planning/08-production-readiness-plan.md` §6 (Stage D)
task D9 — users querying the lakehouse need a compute engine; decide
whether platformctl ships one as a provider, and where the line between
"infrastructure" and "orchestration" falls.

## The question

The lakehouse example (`examples/lakehouse/`) already stands up a Catalog
(Nessie), an object store (MinIO), CDC into it via Redpanda/Debezium, and a
lineage backend (Marquez) — everything up to "queryable data exists in the
lake." Nothing runs the query. Today `inventory --for trino` and
`inventory --for spark` (`cmd/platformctl/toolconfig.go:226-243,200-224`)
render a paste-ready `etc/catalog/lakehouse.properties` /
`spark-defaults.conf` snippet naming the platform's own Catalog and S3
endpoints — correct, but it assumes the engine itself is a black box the
user brings and runs elsewhere. Should platformctl instead *provision* a
compute engine, the same way it provisions Nessie and Marquez, so that
facts flow into the engine's own configuration automatically rather than
into a snippet a human pastes?

## Options considered

1. **`trino` provider** — coordinator + worker containers, catalog
   auto-configured from the Catalog/S3 facts already computed for
   `inventory --for trino`. Read-only, stateless-of-its-own-data compute:
   Trino holds no durable state beyond its own config: it re-derives
   everything from the Iceberg REST catalog and object store on every
   query. That statelessness is exactly what makes "provision it and wire
   the catalog" a complete, honest offering — there's no partial-ownership
   problem the way there would be for a stateful engine.
2. **`flink` session-cluster provider** — a JobManager + TaskManagers
   session cluster real orchestrators submit jobs into. Unlike Trino,
   Flink's core value is running long-lived, stateful streaming jobs
   (checkpointed operator state, savepoints, job lifecycle) — the
   session cluster is infrastructure, but the thing users actually want
   from Flink (a running job) is exactly the NG1 boundary
   (`docs/planning/01-product-requirements.md:63-64`: "does not schedule or
   execute data processing jobs... provisions the infrastructure those
   tools run on top of; it does not run them"). A Flink session cluster
   with no job submitted is infrastructure with nothing to point
   `inventory --for` at that isn't already "it's running, submit your jar" —
   a much thinner UX win than Trino's, where the same shape of provider
   makes queries *work* the moment `apply` finishes.
3. **Both now.** Rejected on sequencing, not on either engine's merits:
   D9 is scoped S ("at most one follow-up provider"), and stacking two new
   providers plus C1 (replicas) risk in one task inflates a decision note
   into a double implementation. Nothing here precludes Flink later as its
   own D-numbered task once Trino has proven the pattern.
4. **Neither, keep `inventory --for` as the whole story.** Rejected: it's
   the only lakehouse-story component still asking the user to run and
   babysit infrastructure by hand (postgres, redpanda, MinIO, Nessie,
   Marquez are all managed) — the exact gap D9 was opened to close, and
   the weakest option against G7's stated intent
   (`docs/planning/01-product-requirements.md:53-56`: Dataset exists "so
   that future orchestrator-facing providers... have a stable,
   already-proven input contract to build against" — a compute-engine
   provider is the natural next consumer of that contract, not a new kind
   of thing platformctl has never done).

## The decision

**Ship `trino` first; defer `flink` as application-adjacent — no decision
against it, just not now.**

- A `trino` provider realizes a coordinator + N worker containers via
  **C1's replica primitive** (`ContainerSpec.Replicas` /
  `StableIdentity` — docs/planning/08 §5 C1, design note
  `docs/adr/004-replicas-and-identity.md` once written). Trino workers
  need no per-replica storage (`StableIdentity: false` is the right
  default — pure compute, no ordinal volumes), so this is C1's simpler
  case, a reasonable second adapter of the primitive after C2
  (Redpanda multi-broker, which does need `StableIdentity: true`).
- The catalog is **auto-configured, not user-supplied**: the provider
  resolves a referenced `Catalog` resource (new
  `Provider(type: trino).spec.configuration.catalogRef`, the same
  reference-plus-graph-ordering discipline D8 established for
  `Catalog.spec.warehouseRef` — Catalog must reconcile before the Trino
  provider that reads it, same pattern as Dataset-before-Catalog in D8)
  and writes `etc/catalog/lakehouse.properties` inside the coordinator
  container from the Catalog's REST endpoint and the S3/MinIO Provider's
  endpoint + credentials — exactly the facts `gatherToolFacts`
  (`cmd/platformctl/toolconfig.go:59-133`) already assembles for the
  human-paste path. This is the strongest UX win named in D9:
  `inventory --for trino`, once a `trino` Provider exists in the manifest,
  changes its answer from a snippet to paste into your own installation to
  a live coordinator endpoint (JDBC URL / UI address) — "it's already
  running."
- **Flink is deferred**, not rejected: revisit once a real use case
  (streaming job on the CDC EventStream) shows what a session-cluster
  provider actually needs to expose, rather than guessing the shape now.
  The rationale (option 2 above) is durable enough that this isn't a
  "revisit in a month" deferral — it's application-adjacent by nature of
  what Flink is for, and stays that way regardless of scheduling pressure.

### The scope line

Engine **infrastructure** is in scope: provisioning the coordinator and
worker containers, reconciling them to Ready, wiring the catalog and
object-store connector config, reporting drift if that config is changed
out-of-band. Engine **usage** is not: platformctl does not submit,
schedule, or manage Trino queries, does not run a query scheduler, and
does not model a "query" or "job" as a resource kind. This is a direct
application of NG1
(`docs/planning/01-product-requirements.md:63-64`: "Datascape does not
schedule or execute data processing jobs... It provisions the
infrastructure those tools run on top of; it does not run them") to a new
technology — Trino is infrastructure a query runs *through*, the same
relationship Datascape already has with Postgres, Redpanda, and Nessie. A
`trino` provider changes nothing about that boundary; it just means one
more piece of "the infrastructure" is provisioned instead of hand-run.

### Feature gate

`TrinoProvider` — Alpha, disabled by default, following the naming and
posture of the other Stage D provider gates in
`docs/planning/04-roadmap-and-feature-gates.md` §12 (`JDBCSinkProvider`,
`IngestProvider`, `TunnelProvider`: new provider, new tech surface, opt-in
until soaked), not the Phase 6.5 precedent (`NessieProvider`,
`OpenLineageProvider`: those shipped enabled-Alpha because they had no
externally-reachable new attack surface beyond a REST endpoint already
behind the platform network; a query engine accepting arbitrary SQL from
whoever can reach its coordinator port is a meaningfully different risk
profile and should default off until reviewed).

## D10 task spec (ready to drop into doc 08 Stage D, after D9)

```
### D10: Trino compute-engine provider

- **Size:** L. **Depends:** C1 (coordinator/worker replicas); D8 helpful,
  not required (warehouseRef makes catalog-to-Dataset wiring first-class,
  but `spec.nessie` already carries warehouse config today per D8's
  context, so the Trino provider can read either shape).
- **Context:** design note `docs/adr/006-compute-engines.md` decided
  Trino first: read path completes the lakehouse story, catalog
  auto-configuration is the strongest inventory UX win, and Trino's
  stateless-of-its-own-data shape avoids the storage questions a stateful
  engine would raise. `inventory --for trino`
  (`cmd/platformctl/toolconfig.go:226-243`) today assumes a user-operated
  engine and only renders a paste-ready snippet.
- **Do:** `trino` provider: one coordinator container + N worker
  containers via C1's `ContainerSpec.Replicas` (`StableIdentity: false` —
  workers hold no durable per-replica state). New
  `Provider(type: trino).spec.configuration.catalogRef` (Catalog,
  kind-checked, same graph-ordering discipline as D8's warehouseRef —
  Catalog reconciles before the Trino Provider that reads it); the
  provider resolves the referenced Catalog's REST endpoint and its
  warehouse Dataset's S3/MinIO Provider endpoint + `SecretReference`,
  and writes `etc/catalog/lakehouse.properties` into the coordinator on
  reconcile (drift-checked: out-of-band catalog config edits are detected
  and healed, matching the debezium/s3sink config-drift bar). Probe:
  coordinator `/v1/info` reachable and `starting: false`, worker count in
  `ContainerState.ReadyReplicas` matches declared replicas. `inventory
  --for trino` gains a live-endpoint branch: when a `trino` Provider
  exists in the applied state, render the coordinator's JDBC URL / UI
  address instead of (or alongside) the paste-ready snippet — "it's
  already running" per the design note. Gate `TrinoProvider` (Alpha,
  disabled).
- **Accept:** integration: `trino` Provider + `catalogRef` to the
  lakehouse Catalog reaches Ready; a query against a table written by the
  existing CDC→Parquet path (D1/D2) returns rows through the coordinator;
  scale-up (1→3 workers) is in-place, no coordinator restart;
  out-of-band catalog config change reported as drift and healed;
  idempotent re-apply makes zero Docker API calls beyond probes;
  `inventory --for trino` reflects the live coordinator once the provider
  is applied; capability/negative-path test: `catalogRef` to a
  non-Catalog kind rejected at validate with the standard error.
```

## Implementation notes (D10, added post-implementation)

The task spec above was executed as written, with four deviations recorded
here per doc 08 §2.1's "a deviation you cannot avoid is a finding, not a
judgment call" rule — none required re-litigating the decision above; all
are additive.

1. **`catalogRef` graph extraction is a new nested-ref rule, not a
   capability check.** `internal/domain/graph/graph.go`'s `refFields` only
   ever scanned top-level `spec.<field>` entries; `catalogRef` and the
   disambiguator below live one level down, inside the open
   `spec.configuration` bag. A new `configRefFields` (a slice, not a map,
   so which field's error surfaces first stays deterministic) scans
   `spec.configuration` after the top-level pass, with the identical
   resolve/validate/error-shape machinery `providerRef`/`connectionRef`
   already use. A `catalogRef` naming a resource that isn't a `Catalog`
   fails with graph's standard "does not resolve to any resource" message
   — a structural kind mismatch, not a "can this provider do X" question,
   so ADR 009's capability-interface error shape does not apply here (that
   shape is reserved for compatibility.go's provider-capability checks).
2. **Warehouse resolution does not depend on D8.** D8
   (`Catalog.spec.warehouseRef`) is not implemented as of D10, so "spec.nessie
   already carries warehouse config today" (this note's original §2 context)
   does not hold in the code as it stands. Rather than adding a `Catalog`-side
   warehouse field (D8's scope), the trino Provider gained its own optional
   `spec.configuration.warehouseProviderRef` (Provider, same nested-ref
   mechanism as `catalogRef`) as an explicit disambiguator; omitted, the
   engine auto-infers the sole S3/MinIO-typed Provider in the manifest's
   namespace (0 or >1 candidates leave the catalog config unresolved rather
   than guessing). This is additive and does not conflict with a future D8,
   which could become the preferred path without removing this one. Facts
   flow through a new `reconciler.Request.CatalogFacts` field, resolved
   engine-side in `internal/application/engine/engine.go`'s
   `resolveCatalogFacts` — the same published-facts-only pattern
   `SchemaRegistryURL`/`MetricsTargets` already established (ADR 015).
3. **Catalog-config drift healing requires a container recreate, not a
   live rewrite.** `ContainerRuntime` has no primitive to overwrite a file
   inside an already-running container (only `ReadFile`); Trino also only
   reads catalog config at process start. So unlike debezium/s3sink (which
   heal drift by re-PUTting a REST-configurable connector config on every
   Reconcile), the trino provider's Reconcile detects a live file that no
   longer matches the desired render and forces `ContainerRuntime.Remove`
   on the coordinator before `EnsureContainer`, so the create path's
   file-copy re-lays correct content. Scoped to the coordinator only for
   this first cut — worker catalog content converges on the workers' own
   next natural recreation (e.g. a scale change), not forced.
4. **The "table written by the D2 CDC->Parquet path" accept item is
   satisfied in spirit, not literally.** No component in this codebase
   (s3sink's Aiven S3 sink connector, specifically) generates genuine
   Iceberg table metadata (manifest/snapshot files) — D1/D2 write plain
   partitioned Parquet. Trino's `iceberg` connector cannot query that data
   directly (it would need the `hive` connector plus a metastore, which
   this stack does not provision, and was never in D1/D2's scope). D10's
   integration test instead proves the full reconciliation stack
   end-to-end — coordinator/worker provisioning, catalog
   auto-configuration from Nessie+MinIO facts, credential wiring — by
   having Trino itself `CREATE TABLE`/`INSERT` the same row content the
   CDC pipeline produced as a genuine Iceberg table through the
   coordinator, then `SELECT` it back. A live-caught bug fixed en route
   (found by re-reading nessie.go before wiring this, not by a unit test
   whose fixture had baked in the same wrong assumption): the Catalog's
   published `"iceberg-rest"` endpoint fact's `Internal` value is already a
   full `http://host:port/iceberg` URL (unlike the S3 fact's bare
   `host:port`) — the catalog-config renderer originally re-prepended
   `http://`, double-scheming `iceberg.rest-catalog.uri`.
5. **More missing pieces found only by running the real stack**, beyond
   the four deviations above (all fixed; recorded because none was
   knowable from reading the code or the task spec alone):
   - **Nessie needs a server-side default warehouse, and its own S3
     endpoint + credentials.** Nessie's Iceberg REST Catalog personality
     answers `/iceberg/v1/config` with `500 "No default-warehouse
     configured"` for *every* request — including a client's first
     catalog-init call — until a default warehouse exists, client-specified
     or server-configured. A configured location alone is still not
     enough: creating a namespace/table under it then fails with
     `"Malformed request: ... Missing access key and secret for STATIC
     authentication mode"` unless Nessie itself also has S3 endpoint +
     credentials to associate with that location (a location says *where*,
     not *how to reach it*). Nothing in this codebase configured either
     before D10 (nothing needed an Iceberg REST client before — Nessie's
     native branch API, all the `nessie` provider used until now, needs
     neither). Fixed with three new, optional `nessie` Provider
     configuration fields — `defaultWarehouseLocation`, `warehouseS3Endpoint`,
     `warehouseS3SecretRef` (must also be listed in `spec.secretRefs`,
     mirroring `s3`'s own `rootSecretRef` convention) — wired to
     `NESSIE_CATALOG_DEFAULT_WAREHOUSE`/`NESSIE_CATALOG_WAREHOUSES_
     WAREHOUSE_LOCATION`/`NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_*`/
     `NESSIE_CATALOG_SECRETS_WAREHOUSE_CREDS_*` — additive, byte-for-byte
     unchanged when unset.
   - **Trino's S3 filesystem needs an explicit region.** Without
     `s3.region` in the catalog config, Trino's S3 filesystem factory
     falls back to the AWS SDK's default region-provider chain (env var,
     profile, EC2 metadata), which — all three absent in a container —
     takes about three minutes to exhaust before failing catalog
     initialization outright; from the outside this looked exactly like a
     hung "starting: true" coordinator, not a fast, loud failure. Fixed by
     always setting `s3.region: us-east-1` (MinIO ignores the value; the
     SDK only requires one be present) in both the trino provider's
     generated config and `toolconfig.go`'s pre-existing paste-ready
     snippet, which carried the same latent gap.
   - **The worker set's `ContainerSpec` needs `Networks` set explicitly.**
     Unlike the coordinator (`providerkit.EnsureInstance` sets `Networks`
     from `InstanceSpec.Network` automatically), the worker set's
     `rt.EnsureContainer` call is made directly and initially omitted
     `Networks` — the workers landed on Docker's default `bridge` network
     instead of the shared one, unable to resolve the coordinator's DNS
     name at all. Discovery announcement then failed silently forever
     (retried every few seconds, logged only as a `WARN`, never surfaced
     as a hard error) and every query stayed `QUEUED` indefinitely,
     indistinguishable from the outside from the `s3.region` hang above.
     Fixed by setting `Networks: []string{network}` on the worker
     `ContainerSpec` explicitly.
   - **Nessie's Iceberg REST Catalog write path (`STATIC` S3 auth) does
     not support table creation, only namespace creation, as configured
     here** — reproduced live with a bare REST call bypassing Trino
     entirely: `POST .../namespaces` succeeds, `POST .../namespaces/
     <ns>/tables` fails with `"...Missing access key and secret for
     STATIC authentication mode"` regardless of the table's location.
     Trino's alternative (`iceberg.rest-catalog.vended-credentials-
     enabled=true`) fails differently (`"Failed to initialize the vended
     credentials from the provided fileIoProperties"`). Left unresolved —
     out of D10's scope (see the D10 integration test's own deviation
     note for how the accept item is still proven within this
     boundary) — and tracked as a follow-up below.
6. **"1->3 worker scale-up in place" as literally written is not
   achievable.** `Replicas <= 1` with `StableIdentity: false` (workers'
   shape) is byte-for-byte the single-container shape, not an ordinal set
   of one (`runtime.ContainerSpec`'s own documented contract); scaling
   from it to any `Replicas > 1` is a shape transition, refused in place —
   the identical rule redpanda's `brokers` field already obeys for its
   legacy-to-declared transition (docs/adr/017 §a.1). The D10 integration
   test instead starts at `workers: 2` (the smallest count already in the
   ordinal-set shape) and scales 2->3, proving a genuine in-place
   scale-up within that shape; the coordinator never restarts either way.

## Follow-ups (non-blocking)

- **Nessie Iceberg REST Catalog table-write credentials**: root-cause and
  fix the `STATIC`-auth `CREATE TABLE` failure (or the
  `vended-credentials-enabled` alternative's `fileIoProperties`
  initialization failure) found above, so a `trino` Provider with
  `catalogRef` can genuinely write Iceberg tables, not just read/create
  namespace-level metadata. Candidates to investigate first: a newer
  Nessie release (0.108.1 was current at D10 time; the Iceberg REST
  Catalog personality is a comparatively new Nessie feature), or Nessie's
  own `objectstoreauthorization`/credential-vending documentation for a
  config shape this note's live experimentation didn't find.

- A `flink` session-cluster provider, once a concrete streaming-job use
  case defines what its infrastructure actually needs to expose (this
  note deliberately does not pre-design it).
- Trino's own connectors beyond Iceberg (e.g. a second catalog pointing at
  a managed Postgres Source via the JDBC connector) — natural once D10
  ships and the `catalogRef` pattern is proven; out of scope for the first
  cut, which targets the one catalog the lakehouse example already has.
- Auth/TLS in front of the coordinator's HTTP port — deferred to Stage C's
  `IngressProvider`/`TLSTermination` gates for the same reason those exist
  as separate tasks rather than being reinvented per-provider.
