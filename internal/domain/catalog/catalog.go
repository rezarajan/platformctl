// Package catalog defines the Catalog kind: a table/metadata catalog
// (Iceberg REST, Hive Metastore, Glue, ...) as a provider-agnostic noun.
// Like Source, spec.engine is an open discriminator pairing with an
// engine-named nested block, so Nessie is one engine behind the Catalog
// abstraction — never a shape of its own.
// See docs/planning/03-resource-model-reference.md §8.1.
package catalog

import (
	"fmt"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

type Catalog struct {
	Engine        string  // "nessie" | "hive" | "glue" | ... — open-ended, not a closed enum
	ProviderRef   *string // required unless External
	External      bool
	ConnectionRef *string        // required when External
	EngineConfig  map[string]any // the spec.<engine> sub-block, validated by a per-engine schema fragment
	// WarehouseRef names a Dataset holding this Catalog's Iceberg warehouse
	// location (docs/planning/08 D8) — top-level, not nested inside
	// EngineConfig, so graph.Build's plain refFields pass (kind-checked to
	// Dataset) orders it with no engine-block introspection. Optional: a
	// realizing provider that knows how to derive warehouse config from a
	// Dataset (nessie today) consumes it via the engine-resolved
	// reconciler.Request.WarehouseFacts; engines that don't, ignore it.
	WarehouseRef *string
}

// FromEnvelope decodes a Catalog from a validated Envelope.
func FromEnvelope(e resource.Envelope) (Catalog, error) {
	c := Catalog{}
	engine, _ := e.Spec["engine"].(string)
	c.Engine = engine

	if ext, ok := e.Spec["external"].(bool); ok {
		c.External = ext
	}
	if ref := refName(e.Spec, "providerRef"); ref != "" {
		c.ProviderRef = &ref
	}
	if ref := refName(e.Spec, "connectionRef"); ref != "" {
		c.ConnectionRef = &ref
	}
	if ref := refName(e.Spec, "warehouseRef"); ref != "" {
		c.WarehouseRef = &ref
	}
	if engine != "" {
		if block, ok := e.Spec[engine].(map[string]any); ok {
			c.EngineConfig = block
		}
	}
	return c, c.validate(e.Metadata.Name)
}

func (c Catalog) validate(name string) error {
	if c.Engine == "" {
		return fmt.Errorf("Catalog %q: spec.engine is required", name)
	}
	if c.External {
		if c.ConnectionRef == nil {
			return fmt.Errorf("Catalog %q: spec.connectionRef is required when spec.external is true", name)
		}
	} else if c.ProviderRef == nil {
		return fmt.Errorf("Catalog %q: spec.providerRef is required unless spec.external is true", name)
	}
	return nil
}

func refName(spec map[string]any, field string) string {
	ref, ok := spec[field].(map[string]any)
	if !ok {
		return ""
	}
	name, _ := ref["name"].(string)
	return name
}
