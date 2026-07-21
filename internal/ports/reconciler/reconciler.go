// Package reconciler defines the Provider port and capability sub-interfaces.
// See docs/planning/02-architecture.md §4.2.
package reconciler

import (
	"context"

	"github.com/rezarajan/platformctl/internal/domain/backup"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/lineage"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/domain/versionprofile"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// Request is the single input to Reconcile/Destroy/Probe and every
// capability method that needs more than a provider's own static config
// (docs/planning/08 F5, docs/planning/09 §3-F5). It replaces an accretion
// of setter interfaces (ProviderResourceAware, SecretsAware,
// ResourceSetAware) that made providers stateful (a Set* call before
// Reconcile is a temporal coupling the compiler can't check) and made
// adding a cross-cutting input either a breaking signature change
// (LineageAware.ConfigureLineage grew a runtime.ContainerRuntime parameter
// in 81025c9, breaking every implementor) or another *Aware interface plus
// an engine special case.
//
// Adding a field here is non-breaking for every implementor — the open/
// closed property the setter pattern lacked — and a provider built against
// it holds no state across calls: its constructor takes nothing but static
// config, and every call is self-contained, which is what an out-of-process
// plugin (Phase 8) requires. A zero field means "not resolved/applicable
// for this call" (e.g. Secrets is empty when the Provider declares no
// secretRefs); providers must not assume any field is populated beyond what
// their own resource declares.
type Request struct {
	// Resource is the envelope actually being reconciled/destroyed/probed —
	// may be the Provider itself, or a dependent resource (EventStream,
	// Source, Binding, Connection, Catalog, ...) the Provider realizes.
	Resource resource.Envelope
	// Runtime is the constructed ContainerRuntime for Resource's realizing
	// Provider's spec.runtime.
	Runtime runtime.ContainerRuntime
	// Provider is the realizing Provider resource's own envelope — Resource
	// itself when Resource.Kind == "Provider".
	Provider resource.Envelope
	// Secrets holds every SecretReference the Provider declared in
	// spec.secretRefs, resolved and keyed by reference name, then by key.
	// Empty when the Provider declares none.
	Secrets map[string]map[string]string
	// Resources is the full validated resource set for this operation,
	// keyed by resource.Key — used to resolve related resources (a
	// Binding's sourceRef/targetRef, a Source's connectionRef, ...).
	Resources map[resource.Key]resource.Envelope
	// SchemaRegistryURL is the resolved schema registry endpoint for a
	// Binding declaring a schema-carrying spec.options.format (avro,
	// protobuf) — docs/planning/08 D1. The engine resolves it from the
	// EventStream endpoint's own realizing Provider's already-published
	// "schema-registry" endpoint fact (internal address, reachable from
	// other containers on the shared network) — never constructed here by
	// string convention (docs/planning/09 F4): a provider consuming this
	// field reads exactly what the registry-hosting provider published,
	// the same way every other cross-provider address is resolved. Empty
	// when options.format is unset/"json", or the upstream Provider has
	// not published the endpoint yet in this state (not yet reconciled).
	SchemaRegistryURL string
	// KafkaBootstrapServers is the resolved in-network Kafka address a
	// Connect-worker Provider (debezium, s3sink) should join, when
	// spec.configuration.bootstrapServers is omitted — docs/planning/08 E2.
	// Unlike SchemaRegistryURL, this is resolved from the manifest graph
	// alone (compatibility.ResolveKafkaBootstrapAddress), not from
	// published state: a Connect worker's own reconcile has no dependency
	// edge guaranteeing the EventStream's broker Provider reconciled
	// first (nothing in the worker's own spec references it — only a
	// Binding using the worker does), so waiting on published state would
	// be an ordering hazard under ParallelReconciliation. The address is
	// instead a graph-resolved manifest fact: the broker Provider's own
	// name plus its fixed/declared Kafka port, which a
	// KafkaBootstrapAddressProvider can compute without having reconciled.
	// Empty when configuration.bootstrapServers is already set, or when
	// zero or more than one distinct address would result (ambiguous —
	// the provider must then require an explicit value, same as before
	// this field existed).
	KafkaBootstrapServers string
	// MetricsTargets is every currently-published Prometheus-compatible
	// "metrics" endpoint fact in state (docs/planning/08 C9) — the
	// prometheus provider's scrape-config-generation input. The engine
	// resolves it by scanning state for every Provider resource's published
	// endpoint list, filtering for Name == "metrics"; the prometheus
	// provider itself never constructs a scrape target (ADR 015). Populated
	// only when Resource.Kind == "Provider" (mirroring SchemaRegistryURL's
	// Binding-only scoping above) — empty for every other provider, which
	// simply never reads it.
	MetricsTargets []MetricsTarget
}

// MetricsTarget names one already-published metrics endpoint fact: JobName
// is the owning Provider resource's own name (the scrape job's stable
// identity), Endpoint is the published fact itself — Internal carries the
// full in-network scrape URL (scheme + host:port + metrics path, e.g.
// "http://redpanda:9644/public_metrics"), since a metrics endpoint is
// inherently path-scoped the same way nessie's iceberg-rest endpoint is
// (see internal/domain/endpoint.Endpoint's doc comment on Internal).
type MetricsTarget struct {
	JobName  string
	Endpoint endpoint.Endpoint
}

type Provider interface {
	Type() string // "redpanda", "postgres", "debezium", "s3", "s3sink"
	Reconcile(ctx context.Context, req Request) (status.Status, error)
	Destroy(ctx context.Context, req Request) error
	Probe(ctx context.Context, req Request) (status.Status, error)
}

// ExternalConfigurer is the only provider capability allowed to mutate or
// configure resources declaring spec.external: true with a providerRef.
// External resources without providerRef remain connection/probe-only.
type ExternalConfigurer interface {
	Provider
	ConfigureExternal(ctx context.Context, req Request) (status.Status, error)
}

// CDCCapableProvider is declared by a provider that can sit behind a
// `mode: cdc` Binding.
type CDCCapableProvider interface {
	Provider
	SupportedSourceEngines() []string
}

// SinkCapableProvider is declared by a provider that can sit behind a
// `mode: sink` Binding targeting a Dataset (object-store location).
type SinkCapableProvider interface {
	Provider
	SupportedSinkFormats() []string
}

// DatabaseSinkCapableProvider is declared by a provider that can sit behind
// a `mode: sink` Binding targeting a Source (an engine-backed database used
// in its sink role — e.g. JDBC-style connectors). No v1.0.0 provider
// implements it; the seam exists so database sinks land additively.
type DatabaseSinkCapableProvider interface {
	Provider
	SupportedSinkEngines() []string
}

// IngestCapableProvider is declared by a provider that can sit behind a
// `mode: ingest` Binding reading a Dataset (object-store location used in
// its origin role — e.g. S3 source connectors) into an EventStream. No
// v1.0.0 provider implements it; the seam exists so object-store ingest
// lands additively.
type IngestCapableProvider interface {
	Provider
	SupportedIngestFormats() []string
}

// CatalogCapableProvider is declared by a provider that can realize a
// Catalog resource. Checked against Catalog.spec.engine at validate time,
// exactly as CDC capability is checked against Source.spec.engine — the
// Catalog kind stays provider-agnostic; engines (nessie, hive, glue, ...)
// are provider capability declarations.
type CatalogCapableProvider interface {
	Provider
	SupportedCatalogEngines() []string
}

// ConnectionCapableProvider is declared by a provider that can realize a
// managed Connection (a stable platform-owned entrypoint forwarding to
// where a system actually lives). Checked against Connection.spec.scheme at
// validate time.
type ConnectionCapableProvider interface {
	Provider
	SupportedConnectionSchemes() []string
}

// VersionedProvider is implemented by providers whose internals are coupled
// to the technology's major version (a data mount path, a data directory) —
// the ones where pairing a free-form image with hard-coded internals would
// silently break a deployment. The manifest references
// configuration.version; the provider resolves the pinned Profile (image +
// internals together) from the catalog, so an image can never be run with a
// mismatched mount. Providers without version-coupled internals do not
// implement this and remain single-profile (image-only). cfg is the
// resource's own parsed config — mirroring SpecValidator.ValidateSpec — so a
// provider whose catalog depends on its own type (mysql vs. mariadb) needs
// no stored state to answer this at validate time, before any Request exists.
type VersionedProvider interface {
	Provider
	VersionCatalog(cfg provider.Provider) versionprofile.Catalog
}

// SpecValidator is optionally implemented by providers that can check their
// own Provider resource's spec at validate time — before anything is
// scheduled. The canonical checks: required configuration keys (an image, a
// bootstrap address) and configuration.*SecretRef entries that must also
// appear in spec.secretRefs for the engine to resolve them. A failure here
// surfaces at `validate`, never as a half-applied platform.
type SpecValidator interface {
	Provider
	ValidateSpec(cfg provider.Provider) error
}

// BindingOptionsValidator is optionally implemented by providers that can
// check a Binding's provider-specific spec.options block at validate time —
// the same DX contract as SpecValidator: any misconfiguration a provider
// would reject at apply time (an unparsable table list, an unknown snapshot
// mode, a malformed sink endpoint) is a validate-time regression if it only
// surfaces after `platformctl validate` passes (docs/planning/07 §2.2, §3.1).
type BindingOptionsValidator interface {
	Provider
	ValidateBindingOptions(mode string, options map[string]any) error
}

// SchemaRegistryCapableProvider is declared by a provider that can expose a
// Confluent-compatible schema registry — Redpanda's built-in registry today
// (docs/planning/08 D1) — enabling a Binding's schema-carrying
// spec.options.format (avro, protobuf) in addition to json. Checked against
// the EventStream endpoint's own realizing Provider, not necessarily the
// Binding's own providerRef: registry availability is a fact of the stream
// backend, not of the CDC/sink connector realizing the Binding. cfg mirrors
// VersionedProvider.VersionCatalog's pattern — the answer is config-dependent
// (configuration.schemaRegistry: enabled), not a static capability of the
// provider type, so it cannot be a no-argument method like
// SupportedSourceEngines.
type SchemaRegistryCapableProvider interface {
	Provider
	SupportedSchemaFormats(cfg provider.Provider) []string
}

// KafkaBootstrapAddressProvider is declared by an EventStream-realizing
// provider (redpanda) whose in-network Kafka listener address is fully
// determined by its own manifest facts — the realizing Provider's runtime
// object name and its fixed/declared Kafka port — with no live reconcile
// required (docs/planning/08 E2). This lets a Kafka Connect worker
// (debezium, s3sink) omit spec.configuration.bootstrapServers and have the
// engine infer it from the manifest graph (compatibility.
// ResolveKafkaBootstrapAddress) even though nothing guarantees this
// Provider reconciles before the Connect worker's own reconcile — unlike
// SchemaRegistryCapableProvider/SchemaRegistryURL (D1), which reads a
// *published* endpoint fact because the registry's presence is
// config-gated and only knowable post-reconcile. name is the broker
// Provider's own runtime object name (naming.RuntimeObjectName); cfg
// mirrors SupportedSchemaFormats's pattern for a config-dependent answer.
type KafkaBootstrapAddressProvider interface {
	Provider
	KafkaBootstrapAddress(name string, cfg provider.Provider) string
}

// StreamReplicationValidator is declared by an EventStream-realizing
// provider that can bound a stream's replication factor from its own
// Provider configuration — redpanda's configuration.brokers today
// (docs/adr/017 §a.7). Checked at validate for every EventStream declaring
// spec.replication, with the realizing Provider's parsed config: "how many
// replicas can this stream backend host" is provider knowledge, exactly
// like SupportedSchemaFormats's config-dependent answer, so the
// compatibility layer never reads a provider-specific configuration key
// itself. The returned error names both numbers (the declared replication
// and the configured capacity); the compatibility check prefixes the
// resource identity, mirroring SpecValidator's error handling.
type StreamReplicationValidator interface {
	Provider
	ValidateStreamReplication(cfg provider.Provider, replication int) error
}

// LineageAware is declared by a provider that knows how to consume a lineage
// backend's connection details and wire them into its own, real integration.
// Implemented by `debezium` in v1.0.0. Takes Request like every other
// capability method: a future cross-cutting need lands as an additive
// Request field, not another widened signature (docs/planning/08 F5) — the
// exact breakage this method caused once already (81025c9 added
// runtime.ContainerRuntime as a bare parameter).
type LineageAware interface {
	Provider
	ConfigureLineage(ctx context.Context, req Request, endpoint lineage.LineageEndpoint) error
}

// BackupCapableProvider is declared by a provider whose realized resource
// carries data that can be dumped to, and restored from, an object-store
// location — the data-recoverability half of docs/design/005's single-node
// managed-database posture (docs/planning/08 C6): drift-healing rebuilds
// infrastructure, never data. Implemented by `postgres` and `mysql` (a
// pg_dump/mysqldump streamed to dest via a short-lived job container on the
// realizing Provider's own network) and `s3` (a bucket/prefix sync using its
// existing S3-API client — no job container needed).
//
// dest/src are already-resolved object-store locations (endpoint, bucket,
// prefix, credentials) — never a bare providerRef/secretRef the method
// itself would have to resolve, mirroring how every other capability method
// takes only already-resolved inputs via Request (docs/planning/08 F5); the
// caller (the engine) resolves a Dataset or a raw URL + SecretReference into
// one before calling either method.
type BackupCapableProvider interface {
	Provider
	// Backup streams req.Resource's data to dest and returns a Manifest
	// recording where it landed — the Manifest and every error this method
	// returns must never carry dest's or req.Resource's credentials in any
	// field or message (Accept: "backups never embed plaintext
	// credentials").
	Backup(ctx context.Context, req Request, dest backup.Location) (backup.Manifest, error)
	// Restore streams src back into req.Resource's backing store,
	// unconditionally overwriting whatever data is already there. The
	// restore-over-existing-data safety gate (NFR-3-style: refuse without an
	// explicit flag) is the engine's responsibility, enforced before this is
	// ever called — Restore itself performs no such check.
	Restore(ctx context.Context, req Request, src backup.Location) error
}
