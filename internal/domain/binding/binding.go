// Package binding defines the Binding kind: mode-driven Kind pairing.
// See docs/planning/02-architecture.md §3.4 and
// docs/planning/03-resource-model-reference.md §7.
package binding

import (
	"fmt"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

type Mode string

const (
	ModeCDC   Mode = "cdc"   // sourceRef -> Source, targetRef -> EventStream
	ModeSink  Mode = "sink"  // sourceRef -> EventStream, targetRef -> Dataset
	ModeBatch Mode = "batch" // reserved, unimplemented
)

// KindPair is the structural rule of the Binding kind itself: which Kinds are
// even meaningful to connect for a given mode. Provider capability (whether a
// specific provider can actually do it) is a separate check — see
// application/compatibility.
type KindPair struct{ SourceKind, TargetKind string }

var AllowedKindPairs = map[Mode]KindPair{
	ModeCDC:  {SourceKind: "Source", TargetKind: "EventStream"},
	ModeSink: {SourceKind: "EventStream", TargetKind: "Dataset"},
}

type Binding struct {
	Mode        Mode
	SourceRef   string
	TargetRef   string
	ProviderRef string
	Options     map[string]any
}

func FromEnvelope(e resource.Envelope) (Binding, error) {
	b := Binding{}
	mode, _ := e.Spec["mode"].(string)
	b.Mode = Mode(mode)
	b.SourceRef = refName(e.Spec, "sourceRef")
	b.TargetRef = refName(e.Spec, "targetRef")
	b.ProviderRef = refName(e.Spec, "providerRef")
	if opts, ok := e.Spec["options"].(map[string]any); ok {
		b.Options = opts
	}
	return b, b.validate(e.Metadata.Name)
}

func (b Binding) validate(name string) error {
	switch b.Mode {
	case ModeCDC, ModeSink:
	case ModeBatch:
		return fmt.Errorf("Binding %q: mode \"batch\" is reserved and not implemented in v1", name)
	default:
		return fmt.Errorf("Binding %q: spec.mode must be one of: cdc, sink (batch is reserved)", name)
	}
	if b.SourceRef == "" {
		return fmt.Errorf("Binding %q: spec.sourceRef is required", name)
	}
	if b.TargetRef == "" {
		return fmt.Errorf("Binding %q: spec.targetRef is required", name)
	}
	if b.ProviderRef == "" {
		return fmt.Errorf("Binding %q: spec.providerRef is required", name)
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
