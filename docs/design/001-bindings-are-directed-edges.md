# Design note 001 — Bindings are directed edges; asset kinds are role-neutral

**Status:** accepted, implemented pre-v1.0.0.
**Prompted by:** project-owner review — "a database can also be a sink, and
S3 can also be a source; getting this taxonomy correct is critical for the
first major release."

## The question

The v1 draft taxonomy read as role-named: `Source` (things data comes from),
`Dataset` (places data lands), with `Binding.mode` mapping each mode to
exactly one Kind pair (`cdc`: Source→EventStream, `sink`:
EventStream→Dataset). Real platforms invert those roles routinely: a JDBC
sink connector writes a stream *into* a database; an S3 source connector
reads a bucket *into* a stream. Where should that directionality live?

## Options considered

1. **Role-named kinds** (drafted): add kinds per role (`DatabaseSink`,
   `ObjectSource`, ...). Rejected: it duplicates every asset's schema per
   role, and the same physical database used in both roles becomes two
   resources that can drift apart.
2. **Rename the kinds role-neutrally** (`Source` → `Database`): the purest
   fix for the one role-flavored noun, but a rename of a shipping kind days
   before GA churns every manifest, schema, provider, and test for a purely
   lexical gain — and `Database` is itself too narrow once non-database
   origins (APIs, files) arrive.
3. **Keep the nouns; fix the rule** (chosen): the kinds already denote
   assets, not roles — `Source` is *an engine-backed database asset*
   (historical name, redefined in its schema description), `EventStream` a
   log, `Dataset` an object-store location. Direction already lives in the
   Binding's `sourceRef`/`targetRef`. The actual defect was the pairing
   table's *shape*: `mode → pair` was a function when it must be a
   **relation** (`mode → set of pairs`).

## The decision

- `Binding.mode` names the **movement mechanism**, never the endpoint types:
  `cdc` (log-based capture), `sink` (continuous delivery from a stream into
  a durable target), `ingest` (continuous pickup from a durable origin into
  a stream), `batch` (reserved).
- `AllowedKindPairs` is a relation. v1.0.0 ships: cdc: Source→EventStream;
  sink: EventStream→Dataset **and** EventStream→Source; ingest:
  Dataset→EventStream.
- Each pairing carries its own capability interface, checked at validate:
  sink→Source requires `DatabaseSinkCapableProvider.SupportedSinkEngines()`;
  ingest requires `IngestCapableProvider.SupportedIngestFormats()`. No
  shipped v1.0.0 provider declares either — a Binding using those pairings
  validates structurally and then fails with the standard capability error
  naming exactly what's missing.

## Why this is the v1-critical part

Shapes are the GA contract; implementations are not. With the relation and
the capability seams shipped at v1.0.0, adding a JDBC-sink or S3-source
provider later is a new adapter plus one interface declaration — no schema
change, no new mode semantics, no breaking change. Had the function-shaped
table gone GA, both features would have required revising the meaning of
published manifests.

## Follow-ups (non-blocking)

- A `jdbcsink`-type provider (Kafka Connect JDBC sink) realizing
  sink→Source, and an s3-source provider realizing ingest — natural Phase 6+
  adapters over the existing Connect-worker pattern.
- `batch` remains reserved; when implemented it should follow the same
  relation discipline (plausibly Source→Dataset and Dataset→Source).
