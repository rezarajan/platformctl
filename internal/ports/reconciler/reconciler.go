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
// `mode: sink` Binding.
type SinkCapableProvider interface {
	Provider
	SupportedSinkFormats() []string
}

// LineageAware is declared by a provider that knows how to consume a lineage
// backend's connection details and wire them into its own, real integration.
// Implemented by `debezium` in v1.0.0.
type LineageAware interface {
	Provider
	ConfigureLineage(ctx context.Context, endpoint lineage.LineageEndpoint) error
}
