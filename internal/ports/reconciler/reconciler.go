// Package reconciler defines the Provider port and capability sub-interfaces.
// See docs/planning/02-architecture.md §4.2.
package reconciler

import (
	"context"

	"github.com/rezarajan/platformctl/internal/domain/lineage"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/domain/versionprofile"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

type Provider interface {
	Type() string // "redpanda", "postgres", "debezium", "s3", "s3sink"
	Reconcile(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) (status.Status, error)
	Destroy(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) error
	Probe(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) (status.Status, error)
}

// ExternalConfigurer is the only provider capability allowed to mutate or
// configure resources declaring spec.external: true with a providerRef.
// External resources without providerRef remain connection/probe-only.
type ExternalConfigurer interface {
	Provider
	ConfigureExternal(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) (status.Status, error)
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
// implement this and remain single-profile (image-only).
type VersionedProvider interface {
	Provider
	VersionCatalog() versionprofile.Catalog
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

// ProviderResourceAware is optionally implemented by providers that need
// their own Provider resource's spec (configuration, runtime block) when
// reconciling dependent resources — e.g. redpanda needs the broker address it
// declared when reconciling an EventStream. The engine calls
// SetProviderResource after construction, before any Reconcile/Destroy/Probe.
// (Implementation-revealed addition; pending doc amendment to 02-architecture.md §4.2.)
type ProviderResourceAware interface {
	Provider
	SetProviderResource(env resource.Envelope)
}

// SecretsAware is optionally implemented by providers whose Provider resource
// declares spec.secretRefs. The engine resolves each named SecretReference via
// the SecretStore port and calls SetSecrets (keyed by reference name, then by
// key) before any Reconcile/Destroy/Probe.
type SecretsAware interface {
	Provider
	SetSecrets(secrets map[string]map[string]string)
}

// ResourceSetAware is optionally implemented by providers that must resolve
// other resources while reconciling one — e.g. debezium reconciling a Binding
// needs the Source's provider (database host) and the EventStream's provider
// (broker address). The engine passes the full validated resource set.
type ResourceSetAware interface {
	Provider
	SetResourceSet(byKey map[resource.Key]resource.Envelope)
}

// LineageAware is declared by a provider that knows how to consume a lineage
// backend's connection details and wire them into its own, real integration.
// Implemented by `debezium` in v1.0.0.
type LineageAware interface {
	Provider
	ConfigureLineage(ctx context.Context, endpoint lineage.LineageEndpoint) error
}
