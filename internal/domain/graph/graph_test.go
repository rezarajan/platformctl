package graph

import (
	"strings"
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

func graphEnv(namespace, kind, name string, spec map[string]any) resource.Envelope {
	return resource.Envelope{
		GroupVersionKind: resource.GroupVersionKind{APIVersion: "datascape.io/v1alpha1", Kind: kind},
		Metadata:         resource.Metadata{Namespace: namespace, Name: name},
		Spec:             spec,
	}
}

func TestRefsResolveWithinNamespace(t *testing.T) {
	left := graphEnv("left", "Provider", "shared", map[string]any{})
	right := graphEnv("right", "Provider", "shared", map[string]any{})
	stream := graphEnv("right", "EventStream", "events", map[string]any{"providerRef": map[string]any{"name": "shared"}})
	g, err := Build([]resource.Envelope{left, right, stream})
	if err != nil {
		t.Fatal(err)
	}
	deps := g.Edges[stream.Key()]
	if len(deps) != 1 || deps[0] != right.Key() {
		t.Fatalf("deps = %v, want %s", deps, right.Key())
	}
}

func TestExplicitRefNamespace(t *testing.T) {
	prov := graphEnv("infra", "Provider", "shared", map[string]any{})
	stream := graphEnv("apps", "EventStream", "events", map[string]any{
		"providerRef": map[string]any{"namespace": "infra", "name": "shared"},
	})
	g, err := Build([]resource.Envelope{prov, stream})
	if err != nil {
		t.Fatal(err)
	}
	if got := g.Edges[stream.Key()][0]; got != prov.Key() {
		t.Fatalf("providerRef resolved to %s, want %s", got, prov.Key())
	}
}

func TestAmbiguousBareRefRejectedWithinNamespace(t *testing.T) {
	src := graphEnv("default", "Source", "same", map[string]any{})
	ds := graphEnv("default", "Dataset", "same", map[string]any{})
	binding := graphEnv("default", "Binding", "b", map[string]any{"sourceRef": map[string]any{"name": "same"}})
	_, err := Build([]resource.Envelope{src, ds, binding})
	if err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("Build error = %v, want ambiguity", err)
	}
}

// TestNestedConfigurationRefResolvesAndOrders covers docs/planning/08 D10:
// Provider(type: trino).spec.configuration.catalogRef is a ref nested one
// level under spec.configuration, not a top-level spec field like
// providerRef — graph.go's configRefFields extraction must find it there
// and create the same kind of dependency edge (Catalog reconciles before
// the trino Provider that reads it).
func TestNestedConfigurationRefResolvesAndOrders(t *testing.T) {
	cat := graphEnv("default", "Catalog", "lakehouse-catalog", map[string]any{})
	trino := graphEnv("default", "Provider", "lake-trino", map[string]any{
		"configuration": map[string]any{"catalogRef": map[string]any{"name": "lakehouse-catalog"}},
	})
	g, err := Build([]resource.Envelope{cat, trino})
	if err != nil {
		t.Fatal(err)
	}
	deps := g.Edges[trino.Key()]
	if len(deps) != 1 || deps[0] != cat.Key() {
		t.Fatalf("deps = %v, want [%s]", deps, cat.Key())
	}
	levels := g.TopologicalLevels()
	if len(levels) != 2 {
		t.Fatalf("levels = %v, want 2 (Catalog before Provider)", levels)
	}
	if levels[0][0] != cat.Key() {
		t.Errorf("level 0 = %v, want Catalog first", levels[0])
	}
}

// TestNestedConfigurationRefRejectsWrongKind covers D10's negative-path
// accept item: catalogRef naming a resource that exists but is not a
// Catalog is rejected at Build (i.e. at validate), with the same
// "does not resolve to any resource" shape every other kind-checked ref
// (providerRef, connectionRef, ...) already uses — not a capability-error
// shape, since this is a structural kind mismatch, not a "can this provider
// do X" question (see docs/planning/03's trino section for the recorded
// reasoning).
func TestNestedConfigurationRefRejectsWrongKind(t *testing.T) {
	notACatalog := graphEnv("default", "Provider", "lakehouse-catalog", map[string]any{})
	trino := graphEnv("default", "Provider", "lake-trino", map[string]any{
		"configuration": map[string]any{"catalogRef": map[string]any{"name": "lakehouse-catalog"}},
	})
	_, err := Build([]resource.Envelope{notACatalog, trino})
	if err == nil || !strings.Contains(err.Error(), "does not resolve to any resource") {
		t.Fatalf("Build error = %v, want a kind-mismatch rejection", err)
	}
	if !strings.Contains(err.Error(), "configuration.catalogRef") {
		t.Errorf("error does not name spec.configuration.catalogRef: %v", err)
	}
}

// TestWarehouseProviderRefResolves covers the optional disambiguator
// (docs/planning/08 D10's TASK_PROGRESS.md design note): same nested-ref
// mechanism, allowed kind Provider instead of Catalog.
func TestWarehouseProviderRefResolves(t *testing.T) {
	minio := graphEnv("default", "Provider", "lake-minio", map[string]any{})
	trino := graphEnv("default", "Provider", "lake-trino", map[string]any{
		"configuration": map[string]any{"warehouseProviderRef": map[string]any{"name": "lake-minio"}},
	})
	g, err := Build([]resource.Envelope{minio, trino})
	if err != nil {
		t.Fatal(err)
	}
	deps := g.Edges[trino.Key()]
	if len(deps) != 1 || deps[0] != minio.Key() {
		t.Fatalf("deps = %v, want [%s]", deps, minio.Key())
	}
}

// TestCatalogWarehouseRefResolvesAndOrders covers docs/planning/08 D8:
// Catalog.spec.warehouseRef is top-level (unlike trino's configuration-
// nested catalogRef/warehouseProviderRef above) — it belongs in the plain
// refFields pass, kind-checked to Dataset, so a Catalog reconciles after the
// Dataset it names.
func TestCatalogWarehouseRefResolvesAndOrders(t *testing.T) {
	ds := graphEnv("default", "Dataset", "warehouse", map[string]any{})
	cat := graphEnv("default", "Catalog", "lakehouse-catalog", map[string]any{
		"warehouseRef": map[string]any{"name": "warehouse"},
	})
	g, err := Build([]resource.Envelope{ds, cat})
	if err != nil {
		t.Fatal(err)
	}
	deps := g.Edges[cat.Key()]
	if len(deps) != 1 || deps[0] != ds.Key() {
		t.Fatalf("deps = %v, want [%s]", deps, ds.Key())
	}
	levels := g.TopologicalLevels()
	if len(levels) != 2 {
		t.Fatalf("levels = %v, want 2 (Dataset before Catalog)", levels)
	}
	if levels[0][0] != ds.Key() {
		t.Errorf("level 0 = %v, want Dataset first", levels[0])
	}
}

// TestCatalogWarehouseRefRejectsWrongKind covers D8's negative-path accept
// item: a warehouseRef naming a resource that exists but is not a Dataset
// is rejected at Build (i.e. at validate) with the same structural
// "does not resolve to any resource" shape D10's catalogRef negative test
// established — not a capability-error shape.
func TestCatalogWarehouseRefRejectsWrongKind(t *testing.T) {
	notADataset := graphEnv("default", "Provider", "warehouse", map[string]any{})
	cat := graphEnv("default", "Catalog", "lakehouse-catalog", map[string]any{
		"warehouseRef": map[string]any{"name": "warehouse"},
	})
	_, err := Build([]resource.Envelope{notADataset, cat})
	if err == nil || !strings.Contains(err.Error(), "does not resolve to any resource") {
		t.Fatalf("Build error = %v, want a kind-mismatch rejection", err)
	}
	if !strings.Contains(err.Error(), "warehouseRef") {
		t.Errorf("error does not name spec.warehouseRef: %v", err)
	}
}
