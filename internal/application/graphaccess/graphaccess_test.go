package graphaccess

import (
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/graph"
	"github.com/rezarajan/platformctl/internal/domain/resource"
)

func env(namespace, kind, name string, spec map[string]any) resource.Envelope {
	return resource.Envelope{
		GroupVersionKind: resource.GroupVersionKind{APIVersion: "datascape.io/v1alpha1", Kind: kind},
		Metadata:         resource.Metadata{Namespace: namespace, Name: name},
		Spec:             spec,
	}
}

// buildScenario constructs the doc 08 H6 accept scenario's graph shape: a
// cross-namespace CDC Binding (analytics) reaching a mediated Connection
// (payments, realized by a Provider named "ziti") whose target is the
// actual source database (payments), plus a second, ordinary Connection
// realized by a non-mediation Provider ("proxy") to prove the negative.
func buildScenario(t *testing.T) (*graph.Graph, map[resource.Key]resource.Envelope, resource.Key, resource.Key, resource.Key, resource.Key, resource.Key) {
	t.Helper()
	zitiProv := env("payments", "Provider", "ziti", map[string]any{})
	proxyProv := env("payments", "Provider", "proxy", map[string]any{})
	pgProv := env("payments", "Provider", "pg", map[string]any{})
	source := env("payments", "Source", "orders-db", map[string]any{"providerRef": map[string]any{"name": "pg"}})
	mediated := env("payments", "Connection", "orders-mediated", map[string]any{
		"providerRef": map[string]any{"name": "ziti"},
		"target":      "orders-db:5432",
		"port":        5432,
	})
	plain := env("payments", "Connection", "orders-plain", map[string]any{
		"providerRef": map[string]any{"name": "proxy"},
		"target":      "orders-db:5432",
		"port":        5433,
	})
	binding := env("analytics", "Binding", "cdc-orders", map[string]any{
		"connectionRef": map[string]any{"namespace": "payments", "name": "orders-mediated"},
	})

	g, err := graph.Build([]resource.Envelope{zitiProv, proxyProv, pgProv, source, mediated, plain, binding})
	if err != nil {
		t.Fatalf("graph.Build: %v", err)
	}
	resources := make(map[resource.Key]resource.Envelope)
	for _, e := range []resource.Envelope{zitiProv, proxyProv, pgProv, source, mediated, plain, binding} {
		resources[e.Key()] = e
	}
	return g, resources, binding.Key(), mediated.Key(), plain.Key(), source.Key(), zitiProv.Key()
}

func zitiOnly(zitiKey resource.Key) MediationCapable {
	return func(providerEnv resource.Envelope) bool {
		return providerEnv.Metadata.Name == zitiKey.Name && providerEnv.Metadata.Namespace == zitiKey.Namespace
	}
}

func TestDeriveEdgesIsCompleteAndDeterministic(t *testing.T) {
	g, _, bindingKey, mediatedKey, plainKey, sourceKey, zitiKey := buildScenario(t)

	first := DeriveEdges(g)
	second := DeriveEdges(g)
	if len(first) != len(second) {
		t.Fatalf("DeriveEdges not deterministic in length: %d vs %d", len(first), len(second))
	}
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("DeriveEdges not deterministic at %d: %v vs %v", i, first[i], second[i])
		}
	}

	want := map[Edge]bool{
		{From: bindingKey, To: mediatedKey}: true,
		{From: mediatedKey, To: zitiKey}:    true,
		{From: mediatedKey, To: sourceKey}:  true,
		{From: plainKey, To: sourceKey}:     true, // plain's target string also resolves to source-db; MediatedSubset (not DeriveEdges) is what excludes non-mediated Connections
	}
	got := make(map[Edge]bool)
	for _, e := range first {
		got[e] = true
	}
	for e, expect := range want {
		if got[e] != expect {
			t.Errorf("edge %v present=%v, want %v", e, got[e], expect)
		}
	}
}

func TestMediatedSubsetIncludesOnlyMediationCapableConnections(t *testing.T) {
	g, resources, bindingKey, mediatedKey, plainKey, _, zitiKey := buildScenario(t)
	edges := DeriveEdges(g)
	subset := MediatedSubset(edges, resources, zitiOnly(zitiKey))

	foundMediated := false
	for _, e := range subset {
		if e.To == plainKey {
			t.Fatalf("MediatedSubset included edge into non-mediated Connection %v", plainKey)
		}
		if e.From == bindingKey && e.To == mediatedKey {
			foundMediated = true
		}
	}
	if !foundMediated {
		t.Fatalf("MediatedSubset missing the Binding -> mediated Connection edge; got %v", subset)
	}
}

func TestMediatedSubsetEmptyWhenNoProviderIsMediationCapable(t *testing.T) {
	g, resources, _, _, _, _, _ := buildScenario(t)
	edges := DeriveEdges(g)
	subset := MediatedSubset(edges, resources, func(resource.Envelope) bool { return false })
	if len(subset) != 0 {
		t.Fatalf("MediatedSubset = %v, want empty when no provider is mediation-capable", subset)
	}
}

func TestCompileMediatedConnectionsProducesDialAndBindSides(t *testing.T) {
	g, resources, bindingKey, mediatedKey, plainKey, sourceKey, zitiKey := buildScenario(t)
	mcs := CompileMediatedConnections(g, resources, zitiOnly(zitiKey))

	if len(mcs) != 1 {
		t.Fatalf("CompileMediatedConnections = %d entries, want 1 (the plain Connection must be excluded); got %+v", len(mcs), mcs)
	}
	mc := mcs[0]
	if mc.Connection != mediatedKey {
		t.Fatalf("Connection = %v, want %v", mc.Connection, mediatedKey)
	}
	if len(mc.Consumers) != 1 || mc.Consumers[0] != bindingKey {
		t.Fatalf("Consumers = %v, want [%v]", mc.Consumers, bindingKey)
	}
	if len(mc.Targets) != 1 || mc.Targets[0] != sourceKey {
		t.Fatalf("Targets = %v, want [%v]", mc.Targets, sourceKey)
	}
	for _, mc := range mcs {
		if mc.Connection == plainKey {
			t.Fatalf("plain Connection %v leaked into CompileMediatedConnections output", plainKey)
		}
	}
}

func TestCompileMediatedConnectionsIsDeterministic(t *testing.T) {
	g, resources, _, _, _, _, zitiKey := buildScenario(t)
	first := CompileMediatedConnections(g, resources, zitiOnly(zitiKey))
	second := CompileMediatedConnections(g, resources, zitiOnly(zitiKey))
	if len(first) != len(second) || len(first) == 0 {
		t.Fatalf("non-deterministic or empty output: %+v vs %+v", first, second)
	}
	for i := range first {
		if first[i].Connection != second[i].Connection {
			t.Fatalf("order not deterministic at %d: %v vs %v", i, first[i], second[i])
		}
	}
}

func TestCompileMediatedConnectionsNoTargetsWhenTargetIsExternal(t *testing.T) {
	zitiProv := env("payments", "Provider", "ziti", map[string]any{})
	mediated := env("payments", "Connection", "external-mediated", map[string]any{
		"providerRef": map[string]any{"name": "ziti"},
		"target":      "10.13.13.10:5432",
		"port":        5432,
	})
	binding := env("payments", "Binding", "cdc-external", map[string]any{
		"connectionRef": map[string]any{"name": "external-mediated"},
	})
	g, err := graph.Build([]resource.Envelope{zitiProv, mediated, binding})
	if err != nil {
		t.Fatalf("graph.Build: %v", err)
	}
	resources := map[resource.Key]resource.Envelope{
		zitiProv.Key(): zitiProv, mediated.Key(): mediated, binding.Key(): binding,
	}
	mcs := CompileMediatedConnections(g, resources, zitiOnly(zitiProv.Key()))
	if len(mcs) != 1 {
		t.Fatalf("mcs = %+v, want exactly 1", mcs)
	}
	if len(mcs[0].Targets) != 0 {
		t.Fatalf("Targets = %v, want empty for an unresolvable (genuinely external) target host", mcs[0].Targets)
	}
	if len(mcs[0].Consumers) != 1 || mcs[0].Consumers[0] != binding.Key() {
		t.Fatalf("Consumers = %v, want [%v]", mcs[0].Consumers, binding.Key())
	}
}
