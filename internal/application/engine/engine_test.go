package engine

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/rezarajan/platformctl/internal/adapters/providers/noop"
	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	"github.com/rezarajan/platformctl/internal/adapters/state/localfile"
	"github.com/rezarajan/platformctl/internal/application/featuregate"
	"github.com/rezarajan/platformctl/internal/application/plan"
	"github.com/rezarajan/platformctl/internal/application/registry"
	"github.com/rezarajan/platformctl/internal/domain/graph"
	"github.com/rezarajan/platformctl/internal/domain/lineage"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/clock"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// fakeLineageProvider records the endpoint it receives.
type fakeLineageProvider struct {
	noop.Provider
	received *lineage.LineageEndpoint
}

func (f *fakeLineageProvider) Type() string { return "fakelineage" }

func (f *fakeLineageProvider) ConfigureLineage(_ context.Context, ep lineage.LineageEndpoint) error {
	f.received = &ep
	return nil
}

func envelope(kind, name string, spec map[string]any, observers ...string) resource.Envelope {
	e := resource.Envelope{}
	e.APIVersion = "datascape.io/v1alpha1"
	e.Kind = kind
	e.Metadata.Name = name
	e.Spec = spec
	for _, o := range observers {
		e.Metadata.Observers = append(e.Metadata.Observers, resource.ObserverRef{Name: o})
	}
	return e
}

func newTestEngine(t *testing.T, reg *registry.Registry) *Engine {
	return &Engine{
		Registry:   reg,
		StateStore: localfile.New(filepath.Join(t.TempDir(), "state.json")),
		Clock:      &clock.Fake{T: time.Date(2026, 7, 13, 0, 0, 0, 0, time.UTC)},
	}
}

func applyAll(t *testing.T, eng *Engine, envelopes []resource.Envelope) Result {
	t.Helper()
	g, err := graph.Build(envelopes)
	if err != nil {
		t.Fatalf("graph: %v", err)
	}
	st, err := eng.StateStore.Load(context.Background())
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	p, err := plan.Compute(envelopes, st, g)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	result, err := eng.Apply(context.Background(), p, envelopes, g)
	if err != nil {
		t.Fatalf("apply: %v (failed: %v)", err, result.Failed)
	}
	return result
}

// TestLineageEndpointForwarded covers the Phase 3 exit criterion: a resource
// with metadata.observers whose provider is LineageAware receives a
// correctly-populated LineageEndpoint.
func TestLineageEndpointForwarded(t *testing.T) {
	gates := featuregate.NewRegistry()
	reg := registry.New(gates)

	lineageProv := &fakeLineageProvider{}
	reg.RegisterProvider("fakelineage", func() reconciler.Provider { return lineageProv }, "")
	reg.RegisterProvider("noop", func() reconciler.Provider { return noop.New() }, "")
	reg.RegisterRuntime("fake", func(_ map[string]any) (runtime.ContainerRuntime, error) {
		return fakeruntime.New(), nil
	})

	envelopes := []resource.Envelope{
		envelope("Provider", "local-marquez", map[string]any{
			"type":          "noop",
			"runtime":       map[string]any{"type": "fake"},
			"configuration": map[string]any{"url": "http://local-marquez:5000"},
		}),
		envelope("Provider", "observed-provider", map[string]any{
			"type":    "fakelineage",
			"runtime": map[string]any{"type": "fake"},
		}, "local-marquez"),
	}

	applyAll(t, newTestEngine(t, reg), envelopes)

	if lineageProv.received == nil {
		t.Fatal("LineageAware provider never received an endpoint")
	}
	if lineageProv.received.URL != "http://local-marquez:5000" {
		t.Errorf("endpoint URL = %q, want %q", lineageProv.received.URL, "http://local-marquez:5000")
	}
	if lineageProv.received.Namespace != "datascape" {
		t.Errorf("endpoint namespace = %q, want %q", lineageProv.received.Namespace, "datascape")
	}
}

// TestLineageNotConsumedCondition covers the Phase 3 exit criterion: an
// observers entry on a resource whose provider is not LineageAware produces
// the informational LineageEndpointDeclaredNotConsumed condition and does not
// block Ready.
func TestLineageNotConsumedCondition(t *testing.T) {
	gates := featuregate.NewRegistry()
	reg := registry.New(gates)
	reg.RegisterProvider("noop", func() reconciler.Provider { return noop.New() }, "")
	reg.RegisterRuntime("fake", func(_ map[string]any) (runtime.ContainerRuntime, error) {
		return fakeruntime.New(), nil
	})

	envelopes := []resource.Envelope{
		envelope("Provider", "local-marquez", map[string]any{
			"type":          "noop",
			"runtime":       map[string]any{"type": "fake"},
			"configuration": map[string]any{"url": "http://local-marquez:5000"},
		}),
		envelope("Provider", "plain-provider", map[string]any{
			"type":    "noop",
			"runtime": map[string]any{"type": "fake"},
		}, "local-marquez"),
	}

	eng := newTestEngine(t, reg)
	applyAll(t, eng, envelopes)

	st, err := eng.StateStore.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rs, ok := st.Resources[resource.Key{Kind: "Provider", Name: "plain-provider"}]
	if !ok {
		t.Fatal("plain-provider missing from state")
	}
	if !rs.Status.IsReady() {
		t.Errorf("resource with unconsumed observers is not Ready; conditions: %+v", rs.Status.Conditions)
	}
	foundInfo := false
	for _, c := range rs.Status.Conditions {
		if c.Reason == status.ReasonLineageNotConsumed {
			foundInfo = true
		}
	}
	if !foundInfo {
		t.Errorf("missing %s condition; conditions: %+v", status.ReasonLineageNotConsumed, rs.Status.Conditions)
	}
}
