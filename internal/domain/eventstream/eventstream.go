// Package eventstream defines the EventStream kind.
// See docs/planning/03-resource-model-reference.md §6.
package eventstream

import (
	"fmt"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

type EventStream struct {
	ProviderRef       string
	Partitions        int
	RetentionDuration string // e.g. "7d"; parsed/enforced by the provider
	External          bool
}

func FromEnvelope(e resource.Envelope) (EventStream, error) {
	es := EventStream{}
	if ref, ok := e.Spec["providerRef"].(map[string]any); ok {
		es.ProviderRef, _ = ref["name"].(string)
	}
	switch p := e.Spec["partitions"].(type) {
	case int:
		es.Partitions = p
	case float64:
		es.Partitions = int(p)
	}
	if ret, ok := e.Spec["retention"].(map[string]any); ok {
		es.RetentionDuration, _ = ret["duration"].(string)
	}
	if ext, ok := e.Spec["external"].(bool); ok {
		es.External = ext
	}
	return es, es.validate(e.Metadata.Name)
}

func (es EventStream) validate(name string) error {
	if !es.External && es.ProviderRef == "" {
		return fmt.Errorf("EventStream %q: spec.providerRef is required", name)
	}
	if es.Partitions < 0 {
		return fmt.Errorf("EventStream %q: spec.partitions must be >= 0", name)
	}
	return nil
}
