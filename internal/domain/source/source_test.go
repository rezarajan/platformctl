package source

import (
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

func env(spec map[string]any) resource.Envelope {
	e := resource.Envelope{}
	e.Kind = "Source"
	e.Metadata.Name = "s"
	e.Spec = spec
	return e
}

// TestDeletionPolicyDefaultsToRetain guards docs/planning/07 §2.2: data
// loss must be opted into, never implied.
func TestDeletionPolicyDefaultsToRetain(t *testing.T) {
	s, err := FromEnvelope(env(map[string]any{
		"engine":      "postgres",
		"providerRef": map[string]any{"name": "pg"},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if s.DeletionPolicy != DeletionRetain {
		t.Errorf("default deletionPolicy = %q, want %q", s.DeletionPolicy, DeletionRetain)
	}
}

func TestDeletionPolicyRejectsUnknownValue(t *testing.T) {
	_, err := FromEnvelope(env(map[string]any{
		"engine":         "postgres",
		"providerRef":    map[string]any{"name": "pg"},
		"deletionPolicy": "obliterate",
	}))
	if err == nil {
		t.Fatal("unknown deletionPolicy accepted")
	}
}
