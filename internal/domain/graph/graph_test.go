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
// established — not a capability-error shape. On the accept item's
// "ambiguity" half (doc 07 §0.2): a kind-checked ref cannot be ambiguous
// beyond the generic rules already pinned in this file
// (TestAmbiguousBareRefRejectedWithinNamespace; Build's duplicate-resource
// rejection) — filterKinds narrows candidates to Datasets, and two
// same-namespace same-name Datasets are a duplicate, rejected before any
// ref resolves — so this wrong-kind rejection is the field-specific
// negative path for warehouseRef.
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

// TestManagedConnectionTargetOrdersAfterNamedUpstream covers the I4
// follow-up ordering fix (docs/planning/08 §7.8 I4 Done-note): a managed
// Connection whose spec.target host names another in-set resource's
// runtime object must reconcile AFTER it — found live when Connection
// "minio" (target "ing-test-minio:9000") reconciled before Provider
// "ing-test-minio" existed and I4's settle-poll honestly failed against a
// nonexistent upstream. RuntimeObjectName is the identity function today,
// so the runtime-name and metadata-name match forms coincide — this test
// pins the resolved edge either way (byRuntimeName indexes both).
func TestManagedConnectionTargetOrdersAfterNamedUpstream(t *testing.T) {
	edge := graphEnv("default", "Provider", "edge", map[string]any{})
	upstream := graphEnv("default", "Provider", "ing-test-minio", map[string]any{})
	conn := graphEnv("default", "Connection", "minio", map[string]any{
		"providerRef": map[string]any{"name": "edge"},
		"port":        9000,
		"target":      "ing-test-minio:9000",
	})
	g, err := Build([]resource.Envelope{edge, upstream, conn})
	if err != nil {
		t.Fatal(err)
	}
	deps := g.Edges[conn.Key()]
	found := false
	for _, d := range deps {
		if d == upstream.Key() {
			found = true
		}
	}
	if !found {
		t.Fatalf("Connection deps = %v, want an edge to %s (its target's upstream)", deps, upstream.Key())
	}
	levels := g.TopologicalLevels()
	levelOf := func(k resource.Key) int {
		for i, level := range levels {
			for _, lk := range level {
				if lk == k {
					return i
				}
			}
		}
		t.Fatalf("%s not found in any level", k)
		return -1
	}
	if levelOf(conn.Key()) <= levelOf(upstream.Key()) {
		t.Errorf("Connection at level %d, upstream at level %d — Connection must come after its target's upstream", levelOf(conn.Key()), levelOf(upstream.Key()))
	}
}

// TestExternalConnectionTargetAddsNoEdge: an external Connection is
// consumed as-is — even if a spec field named an in-set resource, no
// target edge applies (external Connections have no spec.target at all
// per domain validation; this pins the graph's own guard independently).
func TestExternalConnectionTargetAddsNoEdge(t *testing.T) {
	upstream := graphEnv("default", "Provider", "warehouse-db", map[string]any{})
	conn := graphEnv("default", "Connection", "warehouse-conn", map[string]any{
		"external": true,
		"host":     "warehouse-db",
		"port":     5432,
		// Deliberately malformed on purpose for this pin: a real external
		// Connection would never carry target, but if one sneaks through,
		// the graph must still not manufacture an ordering edge for it.
		"target": "warehouse-db:5432",
	})
	g, err := Build([]resource.Envelope{upstream, conn})
	if err != nil {
		t.Fatal(err)
	}
	if deps := g.Edges[conn.Key()]; len(deps) != 0 {
		t.Errorf("external Connection deps = %v, want none", deps)
	}
}

// TestManagedConnectionTargetMatchingNothingAddsNoEdge: a target host that
// is a genuinely external address (an IP, a DNS name outside the set) is
// the entire reason managed Connections exist — it must add no edge and,
// unlike refFields, must NOT error.
func TestManagedConnectionTargetMatchingNothingAddsNoEdge(t *testing.T) {
	edge := graphEnv("default", "Provider", "edge", map[string]any{})
	conn := graphEnv("default", "Connection", "vpc-db", map[string]any{
		"providerRef": map[string]any{"name": "edge"},
		"port":        5432,
		"target":      "10.13.13.10:5432",
	})
	g, err := Build([]resource.Envelope{edge, conn})
	if err != nil {
		t.Fatal(err)
	}
	deps := g.Edges[conn.Key()]
	if len(deps) != 1 || deps[0] != edge.Key() {
		t.Errorf("deps = %v, want only the providerRef edge to %s", deps, edge.Key())
	}
}

// TestManagedConnectionSelfTargetAddsNoSelfEdge: a Connection whose target
// host equals its own name (a degenerate loopback declaration) must not
// gain a self-edge — a self-edge is a one-node cycle and would fail every
// Build for a mistake that harms nothing at the graph level.
func TestManagedConnectionSelfTargetAddsNoSelfEdge(t *testing.T) {
	edge := graphEnv("default", "Provider", "edge", map[string]any{})
	conn := graphEnv("default", "Connection", "loop", map[string]any{
		"providerRef": map[string]any{"name": "edge"},
		"port":        9000,
		"target":      "loop:9000",
	})
	g, err := Build([]resource.Envelope{edge, conn})
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range g.Edges[conn.Key()] {
		if d == conn.Key() {
			t.Fatal("Connection gained a self-edge from a self-naming target")
		}
	}
}

// TestManagedConnectionTargetCycleIsReportedNotSkipped: when the target
// edge closes a loop (the Connection's upstream depends back on the
// Connection via connectionRef), Build must report the cycle — a manifest
// like this is a genuine design error the user must see, never silently
// routed around by dropping the edge.
func TestManagedConnectionTargetCycleIsReportedNotSkipped(t *testing.T) {
	edge := graphEnv("default", "Provider", "edge", map[string]any{})
	upstream := graphEnv("default", "Provider", "upstream-db", map[string]any{
		"external":      true,
		"connectionRef": map[string]any{"name": "db-conn"},
	})
	conn := graphEnv("default", "Connection", "db-conn", map[string]any{
		"providerRef": map[string]any{"name": "edge"},
		"port":        5432,
		"target":      "upstream-db:5432",
	})
	_, err := Build([]resource.Envelope{edge, upstream, conn})
	if err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("Build error = %v, want a dependency-cycle report", err)
	}
}
