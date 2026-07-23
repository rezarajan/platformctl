// Package reconciler defines the Provider port and capability sub-interfaces.
// See docs/planning/02-architecture.md §4.2.
package reconciler

import (
	"context"
	"sort"

	"github.com/rezarajan/platformctl/internal/domain/backup"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/graph"
	"github.com/rezarajan/platformctl/internal/domain/lineage"
	"github.com/rezarajan/platformctl/internal/domain/lint"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/domain/versionprofile"
	"github.com/rezarajan/platformctl/internal/ports/mediation"
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
	// Facts is the generic, read-only query surface over every published
	// endpoint fact in state, engine-backed and populated for every
	// Request regardless of Resource.Kind (docs/planning/08 I9) — see the
	// Facts interface's own doc comment for the full pattern and an
	// example. It supersedes the one-bespoke-field-per-need pattern the
	// fields below established: SchemaRegistryURL/MetricsTargets/
	// CatalogFacts/PrometheusURL/WarehouseFacts are now deprecated thin
	// wrappers over exactly this query (see each field's own doc comment);
	// TunnelFacts, the narrowest and newest of them, was migrated and
	// deleted outright rather than kept as a wrapper — it shipped days
	// before I9 with no external consumers to preserve compatibility for.
	// A new cross-provider published-fact need must be consumed through
	// Facts directly; internal/archtest freezes the field list below so a
	// new bespoke field cannot be added without a documented decision.
	Facts Facts
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
	//
	// Deprecated: this is now a thin wrapper over
	// Facts.Endpoint(esProvEnv.Key(), "schema-registry") — a new consumer
	// should read Facts directly (docs/planning/08 I9). Retained
	// byte-identical; not removed by this task.
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
	//
	// Deliberately NOT a Facts wrapper (docs/planning/08 I9): Facts only
	// covers published state (ADR 015); this is a graph-resolved manifest
	// fact by design (see above) and stays a bespoke field for that reason
	// — it is not part of the accretion I9 addresses.
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
	//
	// Deprecated: this is now a thin wrapper over Facts.ByName("metrics")
	// filtered to the requesting env's own namespace — a new consumer
	// should read Facts directly (docs/planning/08 I9). Retained
	// byte-identical; not removed by this task.
	MetricsTargets []MetricsTarget
	// CatalogFacts resolves Provider(type: trino).spec.configuration.
	// catalogRef (+ the optional warehouseProviderRef disambiguator) into
	// the published facts the trino provider needs to write
	// etc/catalog/lakehouse.properties (docs/planning/08 D10) — the
	// referenced Catalog's "iceberg-rest" endpoint fact and the resolved
	// S3/MinIO Provider's "s3" endpoint fact plus its credential
	// SecretReference *name* (not its values — those still only resolve
	// when that same name also appears in this Provider's own
	// spec.secretRefs, the engine's one existing secret-resolution
	// mechanism). The engine resolves this from state exactly like
	// SchemaRegistryURL/MetricsTargets above — the trino provider never
	// constructs these addresses itself (ADR 015). nil when
	// configuration.catalogRef is unset, Resource.Kind != "Provider", or the
	// referenced Catalog/warehouse Provider has not published its endpoint
	// yet in this state.
	//
	// Deprecated: this is now a thin wrapper assembled from two
	// Facts.Endpoint lookups ("iceberg-rest" on the referenced Catalog,
	// "s3" on the resolved warehouse Provider) — a new consumer should
	// read Facts directly (docs/planning/08 I9). Retained byte-identical;
	// not removed by this task.
	CatalogFacts *CatalogFacts
	// PrometheusURL is the resolved prometheus Provider's own published
	// "prometheus" endpoint fact's in-network address (docs/planning/08 C9
	// completion) — a grafana Provider's datasource-provisioning input,
	// resolved the same published-fact-only way SchemaRegistryURL is (ADR
	// 015): the grafana provider never constructs this address itself. The
	// engine resolves it from an explicit configuration.prometheusRef, or
	// (when unset) the sole prometheus-typed Provider in the manifest's
	// namespace — mirroring resolveCatalogFacts's warehouseProviderRef
	// inference. Empty when unresolved (0 or >1 candidates with no explicit
	// ref, or the referenced/inferred Provider has not yet published its
	// endpoint), Resource.Kind != "Provider", or the request is not for a
	// provider that reads it.
	//
	// Deprecated: this is now a thin wrapper over
	// Facts.Endpoint(promEnv.Key(), "prometheus") — a new consumer should
	// read Facts directly (docs/planning/08 I9). Retained byte-identical;
	// not removed by this task.
	PrometheusURL string
	// WarehouseFacts resolves Catalog.spec.warehouseRef (docs/planning/08
	// D8) into the facts a catalog-realizing provider needs to configure its
	// own default warehouse — the referenced Dataset's bucket/prefix (static
	// spec fields) plus the Dataset's own realizing (s3/minio) Provider's
	// published "s3" endpoint fact and credential SecretReference *name*.
	// The same published-facts-only discipline CatalogFacts above already
	// established (ADR 015): the engine resolves this from state, never
	// constructed by a provider itself. Populated only for a Catalog-kind
	// Resource declaring spec.warehouseRef; nil when warehouseRef is unset,
	// Resource.Kind != "Catalog", or the referenced Dataset/its realizing
	// Provider has not published its "s3" endpoint fact yet in this state —
	// a provider reading this field must treat nil as "not resolved yet",
	// never construct a substitute. graph.Build orders a warehouseRef'd
	// Dataset (and, transitively via the Dataset's own providerRef edge,
	// that Dataset's realizing Provider) strictly before the Catalog naming
	// it, so in practice this is always non-nil by the time a Catalog with a
	// valid warehouseRef reconciles within the same apply — unlike
	// CatalogFacts's optional warehouseProviderRef case, which has no such
	// graph edge and can genuinely need a second apply.
	//
	// Deprecated: this is now a thin wrapper over Facts.Endpoint on the
	// referenced Dataset's realizing Provider's "s3" fact, plus the
	// Dataset's own static Bucket/Prefix read directly off req.Resources —
	// a new consumer should read Facts directly (docs/planning/08 I9).
	// Retained byte-identical; not removed by this task.
	WarehouseFacts *WarehouseFacts
	// TunnelFacts previously resolved a managed Connection's spec.via
	// (docs/adr/023, closed by docs/planning/08 I1) into what its
	// realizing provider (proxy) needed to route its own forwarder's
	// egress through the named tunnel Provider.
	//
	// Deleted (docs/planning/08 I9): TunnelFacts was the newest and
	// narrowest of the bespoke fact fields — it shipped days before I9
	// with no external consumers to preserve compatibility for, so rather
	// than freeze it as a deprecated wrapper forever, it was migrated
	// fully: the tunnel Provider's transit network is read directly off
	// req.Resources (it was always a graph-resolved manifest fact, never
	// a Facts-style published one — see KafkaBootstrapServers's own note
	// on that distinction), and the tunnel-side dial address is now
	// req.Facts.Endpoint(viaProviderKey, connection.ViaFactName(ns,
	// name)) — see internal/adapters/providers/proxy's
	// reconcileConnection for the exact call site. Removing the field
	// outright (rather than keeping an unused wrapper) is the pattern any
	// future field with no external consumers should follow; a field with
	// real consumers stays a wrapper (see the five above).

	// Warn is the provider diagnostics channel (docs/adr/031): a
	// structural field (like Resource/Runtime/Provider), NOT a fact —
	// engine-populated at Request construction, presented by the host
	// (the CLI writes to stderr; stdout stays the machine-parsed output
	// contract). Providers call Warnf, never this field directly and
	// never a process-global stream (os.Stderr/os.Stdout are forbidden
	// in adapters by archtest): a warning is for outcomes that must not
	// fail the operation but must reach the operator — e.g. a
	// best-effort post-promote cleanup failing with a harmless, named
	// leftover.
	Warn func(format string, args ...any)
}

// Warnf reports a non-fatal warning through the Request's diagnostics
// channel (docs/adr/031). Safe to call when Warn is unwired (a host that
// configured no channel): the warning is dropped rather than panicking —
// the severity ladder is: return an error (fail), record a status
// condition (degrade), Warnf (inform).
func (r Request) Warnf(format string, args ...any) {
	if r.Warn != nil {
		r.Warn(format, args...)
	}
}

// CatalogFacts is Request.CatalogFacts's payload — see its doc comment.
type CatalogFacts struct {
	// RestInternal is the referenced Catalog's "iceberg-rest" endpoint
	// fact's in-network address (reachable from the trino coordinator
	// container).
	RestInternal string
	// S3Internal is the resolved warehouse-backing S3/MinIO Provider's "s3"
	// endpoint fact's in-network address ("host:port", no scheme).
	S3Internal string
	// S3SecretRef is the SecretReference *name* holding that Provider's
	// credentials (its own configuration.rootSecretRef, or the first entry
	// of its spec.secretRefs) — a graph fact, never a resolved value; the
	// trino provider looks this name up in its own Request.Secrets.
	S3SecretRef string
}

// WarehouseFacts is Request.WarehouseFacts's payload — see its doc comment.
type WarehouseFacts struct {
	// Bucket/Prefix are the referenced Dataset's own spec.bucket/spec.prefix
	// fields, verbatim (static — no state read needed for these two).
	Bucket string
	Prefix string
	// S3Internal is the Dataset's realizing S3/MinIO Provider's published
	// "s3" endpoint fact's in-network address ("host:port", no scheme) —
	// same shape as CatalogFacts.S3Internal.
	S3Internal string
	// S3SecretRef is the SecretReference *name* holding that Provider's
	// credentials — a graph fact, never a resolved value; a provider
	// consuming this looks the name up in its own Request.Secrets (which
	// requires that name to also be listed in its own spec.secretRefs, the
	// same convention CatalogFacts.S3SecretRef documents).
	S3SecretRef string
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

// Facts is Request.Facts's read-only query surface over every provider-
// published endpoint fact in state (docs/planning/08 I9). It is the
// generic form of the pattern SchemaRegistryURL/MetricsTargets/
// CatalogFacts/PrometheusURL/WarehouseFacts each hard-coded once per
// cross-provider need: a provider that publishes an endpoint.Endpoint in
// its own status.providerState (see endpoint.List) makes it visible to
// every OTHER provider's Request through this query — without either side
// changing when a third-party provider (Phase 8) wants to consume or
// publish a new named fact. Adding a new cross-provider need is now "read
// Facts with a new factName," never "patch the engine, the port, and every
// caller."
//
// Facts is engine-backed and populated once, at request-build time
// (resolveRequest), from a snapshot of state taken under the engine's own
// lock — every method call reads that fixed snapshot, never live,
// concurrently-mutating state (internal/application/engine's
// factsSnapshot). Two consequences follow directly:
//
//   - A call never blocks and never triggers a reconcile. A fact that
//     hasn't been published yet by the time this Request was built simply
//     isn't there — Endpoint's ok return is false, ByName's slice omits
//     it. "Not published yet" is reported honestly; Facts never waits for
//     it to appear.
//   - Ordering guarantees (which resource must reconcile before which)
//     stay exactly where they already lived: the manifest graph
//     (internal/domain/graph.Build's via/warehouseRef/catalogRef edges).
//     Facts is not a scheduling primitive and must never become one — a
//     provider needing "X before Y" declares a ref field that graph.Build
//     turns into an edge, the same way WarehouseFacts/TunnelFacts's own
//     doc history already relied on before this query existed.
//
// Two lookup shapes cover every consumer this repo has today:
//
//  1. Endpoint — one named fact on a resource whose resource.Key you
//     already hold (from a ref field resolved against Request.Resources):
//     the warehouse-backing S3/MinIO Provider's "s3" fact, a Catalog's
//     "iceberg-rest" fact, a via'd tunnel Provider's per-Connection fact
//     (connection.ViaFactName). This is what
//     SchemaRegistryURL/CatalogFacts/PrometheusURL/WarehouseFacts, and the
//     now-deleted TunnelFacts, each resolved by hand.
//  2. ByName — every currently-published fact anywhere in state carrying a
//     given name, when the consumer doesn't know or care which resource
//     published it (a prometheus Provider scraping every "metrics" fact in
//     its namespace). This is what MetricsTargets resolved by hand.
//
// Example — a hypothetical third-party provider whose Reconcile needs to
// dial a warehouse Provider's "s3" fact, resolved from its own Provider
// resource's configuration.warehouseProviderRef the same way
// resolveCatalogFacts's auto-infer path does:
//
//	ref := resource.RefFromSpec(cfg.Configuration, "warehouseProviderRef")
//	warehouseKey := ref.Key(req.Resource.Metadata.Namespace, "Provider")
//	ep, ok := req.Facts.Endpoint(warehouseKey, "s3")
//	if !ok {
//	    // Honest, not a guess: the graph should already order this
//	    // Provider after its warehouseProviderRef target (or the manifest
//	    // author should add that ordering) — surface the wait, never
//	    // construct ep.Internal from convention (docs/adr/015).
//	    return status.Status{}, fmt.Errorf("warehouse Provider %q has not published its \"s3\" endpoint yet — re-apply once it reconciles", warehouseKey.Name)
//	}
//	dial(ep.Internal) // e.g. "minio:9000" — never a loopback literal (docs/planning/08 F1)
//
// A provider consuming Facts must treat every miss (ok == false, or a name
// absent from ByName's result) the same way every existing bespoke field's
// own doc comment already instructs: "not published yet," never a
// substitute the provider constructs itself.
type Facts interface {
	// Endpoint returns providerKey's own published endpoint fact named
	// factName. ok is false when providerKey has published nothing under
	// that name in this Request's snapshot — including when providerKey
	// itself does not exist in state at all; the two cases are
	// indistinguishable by design; the same ambiguity every existing
	// bespoke field already carried (nil/empty meant "not there yet" for
	// any reason).
	Endpoint(providerKey resource.Key, factName string) (endpoint.Endpoint, bool)
	// ByName enumerates every published fact named factName across every
	// resource in this Request's snapshot, sorted by owning resource.Key
	// (Namespace, then Kind, then Name) for deterministic output —
	// scrape-config/property-file generation needs stable ordering run to
	// run. Empty (nil) when nothing has published factName yet.
	ByName(factName string) []PublishedFact
}

// PublishedFact is one Facts.ByName result: Owner is the resource.Key of
// the resource whose reconcile published Endpoint — Owner.Name is the
// stable per-fact identity a consumer like the former MetricsTargets's
// JobName needs (the owning Provider's own name, never re-derived from the
// endpoint's address).
type PublishedFact struct {
	Owner    resource.Key
	Endpoint endpoint.Endpoint
}

// StaticFacts is the simplest Facts implementation: an immutable map
// snapshot from a resource.Key to every endpoint fact that resource has
// published, keyed exactly the shape internal/application/engine's
// factsSnapshot builds once per Request (a nil/empty map answers every
// query with "not published"). It doubles as the natural test double for
// any provider/adapter test exercising Request.Facts without a real engine
// or state store — construct one as a literal
// (reconciler.StaticFacts{key: {ep1, ep2}}), the same test-double pattern
// CLAUDE.md documents for the fake runtime/localfile state adapters.
type StaticFacts map[resource.Key][]endpoint.Endpoint

// Endpoint implements Facts.
func (f StaticFacts) Endpoint(providerKey resource.Key, factName string) (endpoint.Endpoint, bool) {
	for _, ep := range f[providerKey] {
		if ep.Name == factName && ep.Internal != "" {
			return ep, true
		}
	}
	return endpoint.Endpoint{}, false
}

// ByName implements Facts.
func (f StaticFacts) ByName(factName string) []PublishedFact {
	var out []PublishedFact
	for key, eps := range f {
		for _, ep := range eps {
			if ep.Name == factName && ep.Internal != "" {
				out = append(out, PublishedFact{Owner: key, Endpoint: ep})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Owner.Namespace != out[j].Owner.Namespace {
			return out[i].Owner.Namespace < out[j].Owner.Namespace
		}
		if out[i].Owner.Kind != out[j].Owner.Kind {
			return out[i].Owner.Kind < out[j].Owner.Kind
		}
		return out[i].Owner.Name < out[j].Owner.Name
	})
	return out
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

// MediationCapableProvider is declared by a provider that realizes the
// docs/adr/027 Layer 1 zero-trust plane: the authoritative capability
// marker letting the engine and internal/application/graphaccess
// type-assert a constructed reconciler.Provider into a
// mediation.MediationProvider the same way every other capability is
// discovered (docs/planning/02 §4.2's "capability interface" pattern —
// ConnectionCapableProvider, above, is the shape this mirrors). A
// mediation-capable provider is, in practice, ALSO a
// ConnectionCapableProvider (docs/adr/022: a MediatedConnection is the
// existing Connection abstraction realized by a mediator instead of a
// plain forwarder) — the two interfaces are declared separately because
// they answer different questions (“can this provider realize a Connection
// resource at all” vs. “does this provider mint identity and compile
// per-edge authorization”), matching this file's existing precedent of
// narrow, single-purpose capability interfaces over one wide one.
type MediationCapableProvider interface {
	Provider
	// Mediation returns this provider's mediation.MediationProvider facet,
	// bound to the mediation plane instance req names (docs/planning/08
	// F5: connecting to a control plane is exactly the kind of "more than
	// static config" input Request exists for — mirroring how every other
	// capability method that needs live, resolved state takes Request
	// rather than the provider holding cross-call connection state).
	// req.Provider is the mediation Provider resource itself (its own
	// Kind: "Provider" envelope) whether the caller is reconciling that
	// Provider, a Connection it realizes, or a third resource entirely —
	// the same "req.Provider is the realizing Provider" contract every
	// other capability method already holds.
	Mediation(ctx context.Context, req Request) (mediation.MediationProvider, error)
}

// TunnelCapableProvider is declared by a provider that can serve as the
// egress leg named by Connection.spec.via (docs/adr/002's addendum,
// docs/adr/023) — a tunnel/VPN provider (wireguard first) another
// Connection's forwarder could chain its egress through. Checked
// structurally at validate time only, mirroring
// ConnectionCapableProvider's shape (a capability-declaration slice, not a
// bare marker) so a future consumer can capability-check the same way.
// Wiring a via-chained Connection's own realization through the named
// tunnel is deferred — see docs/adr/023's "Scope" section: a
// tunnel-mediated Connection is realized directly by the tunnel provider
// itself as a ConnectionCapableProvider today (see
// internal/adapters/providers/wireguard), not via chaining through a
// second provider's forwarder.
type TunnelCapableProvider interface {
	Provider
	SupportsTunnelChaining() []string
}

// ViaConsumingProvider is declared by a ConnectionCapableProvider that can
// realize a managed Connection whose own spec.via names a second,
// TunnelCapableProvider Provider — routing its own forwarder's egress
// through that named tunnel (docs/adr/023's Scope section, closed by
// docs/planning/08 I1). Implemented by `proxy`, deliberately not by
// `wireguard` itself: wireguard realizes a tunnel-mediated Connection
// directly (it IS the tunnel), it does not chain its own forwarder through
// a second tunnel Provider. Checked at validate time whenever a
// Connection's spec.via is set: the Connection's own realizing provider
// (its providerRef, resolved exactly like ConnectionCapableProvider is)
// must implement this, or via would silently apply as an unconsumed,
// inert field — the same completeness bar ConnectionCapableProvider's own
// scheme check already enforces.
type ViaConsumingProvider interface {
	ConnectionCapableProvider
	ConsumesVia() bool
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

// DesignLinter is declared by a provider that contributes technology-
// specific design-lint findings (docs/adr/020-design-lints.md §5) —
// mirroring SpecValidator's shape (ADR 009): pure, validate-time, no
// Request. Codes are namespaced DL-<type>-NNN. LintDesign is called once
// per distinct provider Type() that implements it (not once per Provider
// envelope) — envelopes is the full manifest set and g its dependency
// graph, exactly what the built-in engine (internal/application/lint)
// itself operates over, so a provider-specific check (e.g. "N Debezium
// connectors against one Postgres database = N replication slots") can see
// every relevant resource in one call rather than being told about a
// single envelope. Implementations must not mutate envelopes/g, must not
// touch live infrastructure, and must be deterministic for identical input
// (ADR 020's determinism bar).
type DesignLinter interface {
	Provider
	LintDesign(envelopes []resource.Envelope, g *graph.Graph) []lint.Finding
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
