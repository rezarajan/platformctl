// Package reconciler defines the Provider port and capability sub-interfaces.
// See docs/planning/02-architecture.md §4.2.
package reconciler

import (
	"context"

	"github.com/rezarajan/platformctl/internal/domain/lineage"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

type Provider interface {
	Type() string // "redpanda", "postgres", "debezium", "s3", "s3sink"
	Reconcile(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) (status.Status, error)
	Destroy(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) error
	Probe(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) (status.Status, error)
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
