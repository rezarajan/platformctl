// Package source defines the Source kind: a discriminator plus an extensible,
// per-engine block. See docs/planning/02-architecture.md §3.3.
package source

import (
	"fmt"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

// Deletion policies — see the identical vocabulary on Dataset
// (internal/domain/dataset): destroying the record must not destroy the
// data unless explicitly opted into.
const (
	DeletionRetain = "retain"
	DeletionDelete = "delete"
)

type Source struct {
	Engine        string  // "postgres" | "mysql" | ... — open-ended, not a closed enum
	ProviderRef   *string // required unless External
	External      bool
	ConnectionRef *string        // required when External
	EngineConfig  map[string]any // the spec.<engine> sub-block, validated by a per-engine schema fragment
	// DeletionPolicy: what Source destroy does to the database —
	// "retain" (default; data loss must be opted into) | "delete" (drop it).
	// Ignored for external sources, which are never touched (NFR-3).
	DeletionPolicy string
}

// FromEnvelope decodes a Source from a validated Envelope.
func FromEnvelope(e resource.Envelope) (Source, error) {
	s := Source{}
	engine, _ := e.Spec["engine"].(string)
	s.Engine = engine

	if ext, ok := e.Spec["external"].(bool); ok {
		s.External = ext
	}
	if ref := refName(e.Spec, "providerRef"); ref != "" {
		s.ProviderRef = &ref
	}
	if ref := refName(e.Spec, "connectionRef"); ref != "" {
		s.ConnectionRef = &ref
	}
	if engine != "" {
		if block, ok := e.Spec[engine].(map[string]any); ok {
			s.EngineConfig = block
		}
	}
	s.DeletionPolicy, _ = e.Spec["deletionPolicy"].(string)
	if s.DeletionPolicy == "" {
		s.DeletionPolicy = DeletionRetain
	}
	return s, s.validate(e.Metadata.Name)
}

func (s Source) validate(name string) error {
	if s.Engine == "" {
		return fmt.Errorf("Source %q: spec.engine is required", name)
	}
	if s.External {
		if s.ConnectionRef == nil {
			return fmt.Errorf("Source %q: spec.connectionRef is required when spec.external is true", name)
		}
	} else if s.ProviderRef == nil {
		return fmt.Errorf("Source %q: spec.providerRef is required unless spec.external is true", name)
	}
	if s.DeletionPolicy != DeletionRetain && s.DeletionPolicy != DeletionDelete {
		return fmt.Errorf("Source %q: spec.deletionPolicy must be %q or %q, got %q", name, DeletionRetain, DeletionDelete, s.DeletionPolicy)
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
