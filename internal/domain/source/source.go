// Package source defines the Source kind: a discriminator plus an extensible,
// per-engine block. See docs/planning/02-architecture.md §3.3.
package source

import (
	"fmt"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

type Source struct {
	Engine        string  // "postgres" | "mysql" | ... — open-ended, not a closed enum
	ProviderRef   *string // required unless External
	External      bool
	ConnectionRef *string        // required when External
	EngineConfig  map[string]any // the spec.<engine> sub-block, validated by a per-engine schema fragment
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
