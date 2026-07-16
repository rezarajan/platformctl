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
