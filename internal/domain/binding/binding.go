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
	ModeCDC    Mode = "cdc"    // log-based change capture out of a database
	ModeSink   Mode = "sink"   // continuous delivery from a stream into a durable target
	ModeIngest Mode = "ingest" // continuous pickup from a durable origin into a stream
	ModeBatch  Mode = "batch"  // reserved, unimplemented
)

// KindPair is the structural rule of the Binding kind itself: which Kinds are
// even meaningful to connect for a given mode. Provider capability (whether a
// specific provider can actually do it) is a separate check — see
// application/compatibility.
type KindPair struct{ SourceKind, TargetKind string }

// AllowedKindPairs is deliberately a relation, not a function: a mode names
// the movement mechanism, and several endpoint pairings can realize it. The
// asset kinds themselves are role-neutral — a Source (an engine-backed
// database) is a legitimate *target* of a sink-mode Binding, and a Dataset
// (an object-store location) a legitimate *origin* of an ingest-mode one.
// Direction lives in sourceRef/targetRef, never in the noun.
var AllowedKindPairs = map[Mode][]KindPair{
	ModeCDC: {{SourceKind: "Source", TargetKind: "EventStream"}},
	ModeSink: {
		{SourceKind: "EventStream", TargetKind: "Dataset"},
		{SourceKind: "EventStream", TargetKind: "Source"}, // database as sink (e.g. JDBC-style connectors)
	},
	ModeIngest: {
		{SourceKind: "Dataset", TargetKind: "EventStream"}, // object store as source (e.g. S3 source connectors)
	},
}

// DeadLetter is spec.options.deadLetter (docs/planning/08 D6): a sink-mode
// Binding's opt-in dead-letter queue. Stream names an EventStream — an
// EventStream's resource name IS its Kafka topic name (the same convention
// redpanda.reconcileTopic uses), so no separate topic field is needed.
// Tolerance mirrors Kafka Connect's own errors.tolerance values verbatim
// (all|none) so provider translation (s3sink) is a direct pass-through.
type DeadLetter struct {
	Stream    string
	Tolerance string
}

type Binding struct {
	Mode        Mode
	SourceRef   string
	TargetRef   string
	ProviderRef string
	Options     map[string]any
	// DeadLetter is non-nil only when spec.options.deadLetter was declared;
	// Tolerance defaults to "all" when the sub-field is omitted (the only
	// tolerance value that makes declaring a DLQ meaningful — "none" still
	// fails the task on error, just also routes a copy to the DLQ topic per
	// Kafka Connect's own semantics, an advanced/rare combination that
	// remains expressible by setting it explicitly).
	DeadLetter *DeadLetter
	// Transport is docs/planning/08 L1 / docs/adr/034's per-edge escape
	// hatch: "" (unset, the default) means this Binding's own declared
	// edges are mediated when the MediatedTransport gate is on (ADR 034
	// inverts H6's opt-in boundary — every declared edge is mediated
	// unless the manifest says otherwise); "direct" opts this Binding's
	// edges out. Schema-valid and validated regardless of the gate (lint/
	// policy can flag a declared "direct" transport even before the gate
	// ships, per ADR 034's "lint-flagged, policy-deniable") but inert
	// without it — the same "schema-valid but inert" posture spec.access
	// already established for GraphScopedAccess.
	Transport string
}

func FromEnvelope(e resource.Envelope) (Binding, error) {
	b := Binding{}
	mode, _ := e.Spec["mode"].(string)
	b.Mode = Mode(mode)
	b.SourceRef = refName(e.Spec, "sourceRef")
	b.TargetRef = refName(e.Spec, "targetRef")
	b.ProviderRef = refName(e.Spec, "providerRef")
	b.Transport, _ = e.Spec["transport"].(string)
	if opts, ok := e.Spec["options"].(map[string]any); ok {
		b.Options = opts
		if raw, ok := opts["deadLetter"].(map[string]any); ok {
			dl := &DeadLetter{}
			dl.Stream, _ = raw["stream"].(string)
			dl.Tolerance, _ = raw["tolerance"].(string)
			if dl.Tolerance == "" {
				dl.Tolerance = "all"
			}
			b.DeadLetter = dl
		}
	}
	return b, b.validate(e.Metadata.Name)
}

func (b Binding) validate(name string) error {
	switch b.Mode {
	case ModeCDC, ModeSink, ModeIngest:
	case ModeBatch:
		return fmt.Errorf("Binding %q: mode \"batch\" is reserved and not implemented in v1", name)
	default:
		return fmt.Errorf("Binding %q: spec.mode must be one of: cdc, sink, ingest (batch is reserved)", name)
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
	if b.Transport != "" && b.Transport != "direct" {
		return fmt.Errorf("Binding %q: spec.transport must be \"direct\" when set (docs/adr/034: mediated is the unset default)", name)
	}
	if b.DeadLetter != nil {
		if b.Mode != ModeSink {
			return fmt.Errorf("Binding %q: spec.options.deadLetter is only valid for mode \"sink\", got %q", name, b.Mode)
		}
		if b.DeadLetter.Stream == "" {
			return fmt.Errorf("Binding %q: spec.options.deadLetter.stream is required", name)
		}
		if b.DeadLetter.Tolerance != "all" && b.DeadLetter.Tolerance != "none" {
			return fmt.Errorf("Binding %q: spec.options.deadLetter.tolerance must be \"all\" or \"none\", got %q", name, b.DeadLetter.Tolerance)
		}
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
