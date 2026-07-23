package provider

import (
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

func env(spec map[string]any) resource.Envelope {
	e := resource.Envelope{}
	e.Kind = "Provider"
	e.Metadata.Name = "p"
	e.Spec = spec
	return e
}

// TestExternalRequiresConnectionRef covers docs/planning/03 §3.3's Provider
// row (docs/planning/08 C4): an external Provider with no connectionRef has
// nothing to verify reachable and must be refused, mirroring
// source.Source's identical requirement.
func TestExternalRequiresConnectionRef(t *testing.T) {
	t.Parallel()
	_, err := FromEnvelope(env(map[string]any{
		"type":     "s3",
		"runtime":  map[string]any{"type": "docker"},
		"external": true,
	}))
	if err == nil {
		t.Fatal("external Provider with no connectionRef accepted")
	}
}

func TestExternalWithConnectionRef(t *testing.T) {
	t.Parallel()
	p, err := FromEnvelope(env(map[string]any{
		"type":          "s3",
		"runtime":       map[string]any{"type": "docker"},
		"external":      true,
		"connectionRef": map[string]any{"name": "prod-lake"},
		"secretRefs":    []any{"minio-creds"},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !p.External {
		t.Error("External = false, want true")
	}
	if p.ConnectionRef == nil || *p.ConnectionRef != "prod-lake" {
		t.Errorf("ConnectionRef = %v, want \"prod-lake\"", p.ConnectionRef)
	}
}

func TestNonExternalConnectionRefOptional(t *testing.T) {
	t.Parallel()
	p, err := FromEnvelope(env(map[string]any{
		"type":    "s3",
		"runtime": map[string]any{"type": "docker"},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if p.External || p.ConnectionRef != nil {
		t.Errorf("Provider = %+v, want zero External/ConnectionRef", p)
	}
}
