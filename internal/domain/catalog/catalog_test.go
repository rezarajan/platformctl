package catalog

import (
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

func env(spec map[string]any) resource.Envelope {
	e := resource.Envelope{}
	e.Kind = "Catalog"
	e.Metadata.Name = "lakehouse-catalog"
	e.Spec = spec
	return e
}

// TestWarehouseRefDecodesWhenSet covers docs/planning/08 D8: warehouseRef
// is optional, top-level (a sibling of providerRef/connectionRef, not
// nested inside the engine block), and decodes to a bare name exactly like
// the pre-existing ProviderRef/ConnectionRef fields.
func TestWarehouseRefDecodesWhenSet(t *testing.T) {
	t.Parallel()
	c, err := FromEnvelope(env(map[string]any{
		"engine":       "nessie",
		"providerRef":  map[string]any{"name": "catalog-svc"},
		"warehouseRef": map[string]any{"name": "warehouse"},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if c.WarehouseRef == nil || *c.WarehouseRef != "warehouse" {
		t.Fatalf("WarehouseRef = %v, want %q", c.WarehouseRef, "warehouse")
	}
}

// TestWarehouseRefNilWhenUnset covers additive coexistence: a Catalog with
// no warehouseRef decodes exactly as it did before D8 — nil, not an error,
// not a required field.
func TestWarehouseRefNilWhenUnset(t *testing.T) {
	t.Parallel()
	c, err := FromEnvelope(env(map[string]any{
		"engine":      "nessie",
		"providerRef": map[string]any{"name": "catalog-svc"},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if c.WarehouseRef != nil {
		t.Fatalf("WarehouseRef = %v, want nil", *c.WarehouseRef)
	}
}
