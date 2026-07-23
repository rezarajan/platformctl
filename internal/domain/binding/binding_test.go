package binding

import (
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

func sinkEnvelope(name string, options map[string]any) resource.Envelope {
	return resource.Envelope{
		Metadata: resource.Metadata{Name: name},
		Spec: map[string]any{
			"mode":        "sink",
			"sourceRef":   map[string]any{"name": "events"},
			"targetRef":   map[string]any{"name": "lake"},
			"providerRef": map[string]any{"name": "s3-sink"},
			"options":     options,
		},
	}
}

// TestDeadLetterParsedAndDefaulted covers docs/planning/08 D6: an explicit
// stream/tolerance parses through, and an omitted tolerance defaults to
// "all" (the only value that makes declaring a DLQ meaningful by default).
func TestDeadLetterParsedAndDefaulted(t *testing.T) {
	t.Parallel()
	b, err := FromEnvelope(sinkEnvelope("s1", map[string]any{
		"deadLetter": map[string]any{"stream": "dlq-events"},
	}))
	if err != nil {
		t.Fatalf("FromEnvelope: %v", err)
	}
	if b.DeadLetter == nil {
		t.Fatal("DeadLetter is nil, want parsed")
	}
	if b.DeadLetter.Stream != "dlq-events" {
		t.Errorf("Stream = %q, want dlq-events", b.DeadLetter.Stream)
	}
	if b.DeadLetter.Tolerance != "all" {
		t.Errorf("Tolerance = %q, want default \"all\"", b.DeadLetter.Tolerance)
	}
}

func TestDeadLetterExplicitToleranceNone(t *testing.T) {
	t.Parallel()
	b, err := FromEnvelope(sinkEnvelope("s1", map[string]any{
		"deadLetter": map[string]any{"stream": "dlq-events", "tolerance": "none"},
	}))
	if err != nil {
		t.Fatalf("FromEnvelope: %v", err)
	}
	if b.DeadLetter.Tolerance != "none" {
		t.Errorf("Tolerance = %q, want \"none\"", b.DeadLetter.Tolerance)
	}
}

func TestDeadLetterAbsentIsNil(t *testing.T) {
	t.Parallel()
	b, err := FromEnvelope(sinkEnvelope("s1", map[string]any{}))
	if err != nil {
		t.Fatalf("FromEnvelope: %v", err)
	}
	if b.DeadLetter != nil {
		t.Errorf("DeadLetter = %+v, want nil when options.deadLetter is unset", b.DeadLetter)
	}
}

func TestDeadLetterMissingStreamRejected(t *testing.T) {
	t.Parallel()
	_, err := FromEnvelope(sinkEnvelope("s1", map[string]any{
		"deadLetter": map[string]any{"tolerance": "all"},
	}))
	if err == nil {
		t.Fatal("want an error when deadLetter.stream is empty")
	}
}

func TestDeadLetterInvalidToleranceRejected(t *testing.T) {
	t.Parallel()
	_, err := FromEnvelope(sinkEnvelope("s1", map[string]any{
		"deadLetter": map[string]any{"stream": "dlq-events", "tolerance": "some"},
	}))
	if err == nil {
		t.Fatal("want an error for an unrecognized tolerance value")
	}
}

// TestDeadLetterRejectedOutsideSinkMode: docs/planning/08 D6 names sink-mode
// Bindings only; a cdc-mode Binding declaring options.deadLetter must fail
// at validate, not silently ignore it.
func TestDeadLetterRejectedOutsideSinkMode(t *testing.T) {
	t.Parallel()
	e := resource.Envelope{
		Metadata: resource.Metadata{Name: "c1"},
		Spec: map[string]any{
			"mode":        "cdc",
			"sourceRef":   map[string]any{"name": "db"},
			"targetRef":   map[string]any{"name": "events"},
			"providerRef": map[string]any{"name": "dbz"},
			"options": map[string]any{
				"deadLetter": map[string]any{"stream": "dlq-events"},
			},
		},
	}
	if _, err := FromEnvelope(e); err == nil {
		t.Fatal("want an error when deadLetter is declared on a non-sink Binding")
	}
}
