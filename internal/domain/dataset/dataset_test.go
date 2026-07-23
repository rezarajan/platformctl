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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
	_, err := FromEnvelope(env(map[string]any{
		"providerRef": map[string]any{"name": "minio"},
		"bucket":      "b", "format": "json", "deletionPolicy": "obliterate",
	}))
	if err == nil {
		t.Fatal("unknown deletionPolicy accepted")
	}
}

// TestLifecycleOmittedIsEmpty covers docs/planning/08 D7: a Dataset with no
// spec.lifecycle must leave the provider's lifecycle reconciliation a no-op
// (Empty() true), not an implicit "expire immediately"/"suspend versioning".
func TestLifecycleOmittedIsEmpty(t *testing.T) {
	t.Parallel()
	d, err := FromEnvelope(env(map[string]any{
		"providerRef": map[string]any{"name": "minio"},
		"bucket":      "b", "format": "json",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !d.Lifecycle.Empty() {
		t.Errorf("Lifecycle.Empty() = false for an omitted spec.lifecycle, want true")
	}
	if d.Lifecycle.HasExpiration() || d.Lifecycle.HasVersioning() {
		t.Errorf("Lifecycle = %+v, want zero value", d.Lifecycle)
	}
}

func TestLifecycleExpireAfterDaysAndVersioning(t *testing.T) {
	t.Parallel()
	d, err := FromEnvelope(env(map[string]any{
		"providerRef": map[string]any{"name": "minio"},
		"bucket":      "b", "format": "json",
		"lifecycle": map[string]any{"expireAfterDays": float64(30), "versioning": "enabled"},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !d.Lifecycle.HasExpiration() || d.Lifecycle.ExpireAfterDays != 30 {
		t.Errorf("Lifecycle.ExpireAfterDays = %d, want 30", d.Lifecycle.ExpireAfterDays)
	}
	if d.Lifecycle.Versioning != VersioningEnabled {
		t.Errorf("Lifecycle.Versioning = %q, want %q", d.Lifecycle.Versioning, VersioningEnabled)
	}
	if d.Lifecycle.Empty() {
		t.Error("Lifecycle.Empty() = true with both fields set")
	}
}

func TestLifecycleRejectsUnknownVersioningValue(t *testing.T) {
	t.Parallel()
	_, err := FromEnvelope(env(map[string]any{
		"providerRef": map[string]any{"name": "minio"},
		"bucket":      "b", "format": "json",
		"lifecycle": map[string]any{"versioning": "paused"},
	}))
	if err == nil {
		t.Fatal("unknown lifecycle.versioning value accepted")
	}
}

func TestLifecycleRejectsNegativeExpireAfterDays(t *testing.T) {
	t.Parallel()
	_, err := FromEnvelope(env(map[string]any{
		"providerRef": map[string]any{"name": "minio"},
		"bucket":      "b", "format": "json",
		"lifecycle": map[string]any{"expireAfterDays": float64(-1)},
	}))
	if err == nil {
		t.Fatal("negative lifecycle.expireAfterDays accepted")
	}
}
