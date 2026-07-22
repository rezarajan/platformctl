# Binding

`datascape.io/v1alpha1`

A directed data-movement contract realized by a Provider. spec.mode names the movement mechanism; each mode admits a set of endpoint Kind pairings (cdc: Source→EventStream; sink: EventStream→Dataset or EventStream→Source; ingest: Dataset→EventStream), with the matching provider capability checked at validate time. Direction lives in sourceRef/targetRef — asset kinds are role-neutral.

| Field | Type | Required | Description |
|---|---|---|---|
| `metadata.name` | string | yes | Unique per Kind within a manifest set. |
| `metadata.observers[].name` | string | no | Provider names resolved to LineageEndpoints and forwarded when this resource's provider is LineageAware. |
| `spec.mode` | `cdc` \| `sink` \| `ingest` \| `batch` | yes | Movement mechanism: cdc (log-based capture out of a database), sink (continuous delivery from a stream into a durable target — object store or database), ingest (continuous pickup from an object store into a stream). batch is schema-reserved and rejected by validation in v1. |
| `spec.options` | object | no | Mode/provider-specific options (e.g. cdc: tables, snapshotMode, databaseHostname/databasePort for external sources). format (json|avro|protobuf, default json) and converter select the record serialization; avro/protobuf are schema-carrying and require a schema registry auto-wired from the EventStream's Provider (SchemaRegistrySupport gate, docs/planning/08 D1). deadLetter (sink mode only): {stream: <EventStream name>, tolerance: all|none, default all} — a dead-letter queue for poison records; stream must resolve to an EventStream in the manifest set (docs/planning/08 D6). jdbcsink-typed sink Bindings (sink -> Source, docs/planning/08 D3): mode (insert|default insert|upsert) selects the JDBC connector's insert.mode; format is REQUIRED to be avro or protobuf for this provider (schemaless json contributes zero columns — kafka-connect-jdbc needs a Struct-typed schema); table overrides the target table name (default: the source EventStream/topic name, via the connector's own table.name.format default); pkFields (upsert only) names explicit primary-key columns, else the full Kafka record key is used; unwrap (boolean, default false) applies Debezium's envelope-unwrap SMT so a CDC-sourced topic's before/after/op envelope is flattened to a row before insert; autoCreate/autoEvolve (booleans, default false) opt into the connector's own DDL automation. s3source-typed ingest Bindings (Dataset -> EventStream, docs/planning/08 D4): endpoint overrides the object-store endpoint (mirrors s3sink's identical option); converter overrides the value converter class; the input format itself is Dataset.spec.format (jsonl|avro|parquet), not a separate options field, checked via IngestCapableProvider.SupportedIngestFormats(). |
| `spec.providerRef` | object `{name}` | yes | Must implement the capability interface matching spec.mode (CDCCapableProvider / SinkCapableProvider). |
| `spec.sourceRef` | object `{name}` | yes |  |
| `spec.targetRef` | object `{name}` | yes |  |

## Binding options reference (by `spec.mode` + provider type)

This table is generated from each mode/provider pairing's own JSON-Schema fragment (`schemas/v1alpha1/fragments/binding/`) — the shape of the `spec.options` block once the realizing provider's type is known (docs/planning/08 E5). Only pairings with a registered fragment appear; every other provider's options are checked solely by its `BindingOptionsValidator` Go code, if it implements one.

### cdc-debezium

Shape-only fragment (docs/planning/08 E5), migrated from debezium.ValidateBindingOptions. Whether options.format's avro/protobuf actually has a schema registry to talk to remains a compatibility.Check graph concern.

| Field | Type | Required | Description |
|---|---|---|---|
| `spec.options.converter` | string | no |  |
| `spec.options.databaseHostname` | string | no |  |
| `spec.options.databasePort` | integer | no |  |
| `spec.options.format` | `json` \| `avro` \| `protobuf` | no |  |
| `spec.options.snapshotMode` | `always` \| `initial` \| `initial_only` \| `no_data` \| `never` \| `when_needed` \| `schema_only` \| `schema_only_recovery` | no |  |
| `spec.options.tables` | array of string | no |  |

### ingest-s3source

Shape-only fragment (docs/planning/08 E5, D4), migrated from s3source.ValidateBindingOptions. options.endpoint's well-formed-URL check (scheme+host) remains a SpecValidator rule.

| Field | Type | Required | Description |
|---|---|---|---|
| `spec.options.converter` | string | no |  |
| `spec.options.endpoint` | string | no |  |

### sink-jdbcsink

Shape-only fragment (docs/planning/08 E5, D3), migrated from jdbcsink.ValidateBindingOptions. format is required and restricted to avro|protobuf — stricter than every other provider in this codebase, deliberate: kafka-connect-jdbc cannot derive column names/types from a schemaless json record. deadLetter is accepted only to avoid additionalProperties:false rejecting it (shape owned by binding.FromEnvelope at the Kind level).

| Field | Type | Required | Description |
|---|---|---|---|
| `spec.options.autoCreate` | boolean | no |  |
| `spec.options.autoEvolve` | boolean | no |  |
| `spec.options.converter` | string | no |  |
| `spec.options.databaseHostname` | string | no |  |
| `spec.options.databasePort` | integer | no |  |
| `spec.options.deadLetter` | object | no |  |
| `spec.options.format` | `avro` \| `protobuf` | yes |  |
| `spec.options.mode` | `insert` \| `upsert` | no |  |
| `spec.options.pkFields` | array of string | no |  |
| `spec.options.table` | string | no |  |
| `spec.options.unwrap` | boolean | no |  |

### sink-s3sink

Shape-only fragment (docs/planning/08 E5), migrated from s3sink.ValidateBindingOptions. options.endpoint's well-formed-URL check (scheme+host) remains a SpecValidator rule (more than a JSON Schema string shape can express); deadLetter's own shape ({stream, tolerance}, docs/planning/08 D6) is validated unconditionally at the Kind level by binding.FromEnvelope regardless of provider, so it is accepted here only to avoid additionalProperties:false rejecting it.

| Field | Type | Required | Description |
|---|---|---|---|
| `spec.options.converter` | string | no |  |
| `spec.options.deadLetter` | object | no |  |
| `spec.options.endpoint` | string | no |  |
| `spec.options.format` | `json` \| `avro` \| `protobuf` | no |  |

