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
	// Replication is the topic's replication factor (docs/adr/017 §a.7).
	// 0 (unset) means 1 — a single copy, the pre-C2 behavior byte-for-byte.
	// A value above the realizing Provider's broker count is refused at
	// validate via reconciler.StreamReplicationValidator.
	Replication int
	External    bool
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
	switch r := e.Spec["replication"].(type) {
	case int:
		es.Replication = r
	case float64:
		es.Replication = int(r)
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
	if es.Replication < 0 {
		return fmt.Errorf("EventStream %q: spec.replication must be >= 0", name)
	}
	return nil
}

// ReplicationFactor normalizes Replication: 0 (unset) means exactly 1,
// preserving the pre-C2 single-copy default for every EventStream that never
// set the field.
func (es EventStream) ReplicationFactor() int {
	if es.Replication <= 0 {
		return 1
	}
	return es.Replication
}
