package dataset

import (
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

func env(spec map[string]any) resource.Envelope {
	e := resource.Envelope{}
	e.Kind = "Dataset"
	e.Metadata.Name = "d"
	e.Spec = spec
	return e
}

// TestDeletionPolicyDefaultsToRetain guards docs/planning/07 §2.2: data
// loss must be opted into, never implied.
func TestDeletionPolicyDefaultsToRetain(t *testing.T) {
	d, err := FromEnvelope(env(map[string]any{
		"providerRef": map[string]any{"name": "minio"},
		"bucket":      "b", "format": "json",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if d.DeletionPolicy != DeletionRetain {
		t.Errorf("default deletionPolicy = %q, want %q", d.DeletionPolicy, DeletionRetain)
	}
}

func TestDeletionPolicyExplicitDelete(t *testing.T) {
	d, err := FromEnvelope(env(map[string]any{
		"providerRef": map[string]any{"name": "minio"},
		"bucket":      "b", "format": "json", "deletionPolicy": "delete",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if d.DeletionPolicy != DeletionDelete {
		t.Errorf("deletionPolicy = %q, want %q", d.DeletionPolicy, DeletionDelete)
	}
}

func TestDeletionPolicyRejectsUnknownValue(t *testing.T) {
	_, err := FromEnvelope(env(map[string]any{
		"providerRef": map[string]any{"name": "minio"},
		"bucket":      "b", "format": "json", "deletionPolicy": "obliterate",
	}))
	if err == nil {
		t.Fatal("unknown deletionPolicy accepted")
	}
}
