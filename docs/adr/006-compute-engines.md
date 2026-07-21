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

## Follow-ups (non-blocking)

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
