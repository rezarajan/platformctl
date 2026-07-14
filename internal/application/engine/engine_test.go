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

// driftingProvider reports drift until it has been reconciled a second time,
// simulating a resource killed out-of-band and then healed.
type driftingProvider struct {
	noop.Provider
}

func (d *driftingProvider) Type() string { return "drifty" }

func (d *driftingProvider) Probe(_ context.Context, _ resource.Envelope, _ runtime.ContainerRuntime) (status.Status, error) {
	st := status.Status{}
	now := time.Now()
	if d.ReconcileCount < 2 {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: "GoneMissing"}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: "GoneMissing"}, now)
		return st, nil
	}
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "Healthy"}, now)
	st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: "NoDrift"}, now)
	return st, nil
}

func driftFixture(t *testing.T) (*Engine, *driftingProvider, []resource.Envelope) {
	t.Helper()
	gates := featuregate.NewRegistry()
	reg := registry.New(gates)
	prov := &driftingProvider{}
	reg.RegisterProvider("drifty", func() reconciler.Provider { return prov }, "")
	reg.RegisterRuntime("fake", func(_ map[string]any) (runtime.ContainerRuntime, error) {
		return fakeruntime.New(), nil
	})
	envelopes := []resource.Envelope{
		envelope("Provider", "drifter", map[string]any{
			"type":    "drifty",
			"runtime": map[string]any{"type": "fake"},
		}),
	}
	return newTestEngine(t, reg), prov, envelopes
}

// TestApplyHealsDrift: an unchanged manifest set re-applied with HealDrift
// probes plan-noop resources and re-reconciles the drifted ones.
func TestApplyHealsDrift(t *testing.T) {
	eng, prov, envelopes := driftFixture(t)
	applyAll(t, eng, envelopes)
	if prov.ReconcileCount != 1 {
		t.Fatalf("ReconcileCount after first apply = %d, want 1", prov.ReconcileCount)
	}

	eng.HealDrift = true
	result := applyAll(t, eng, envelopes)
	if prov.ReconcileCount != 2 {
		t.Errorf("ReconcileCount after healing apply = %d, want 2 (drift must trigger re-reconcile)", prov.ReconcileCount)
	}
	if len(result.Succeeded) != 1 {
		t.Errorf("healed resources reported = %d, want 1", len(result.Succeeded))
	}

	// No drift anymore: a further apply must be a true no-op.
	applyAll(t, eng, envelopes)
	if prov.ReconcileCount != 2 {
		t.Errorf("ReconcileCount after clean apply = %d, want 2 (no drift, no reconcile)", prov.ReconcileCount)
	}
}

// TestApplyWithoutHealDriftLeavesDrift: with the gate off, apply trusts
// recorded state and never probes.
func TestApplyWithoutHealDriftLeavesDrift(t *testing.T) {
	eng, prov, envelopes := driftFixture(t)
	applyAll(t, eng, envelopes)
	applyAll(t, eng, envelopes)
	if prov.ReconcileCount != 1 {
		t.Errorf("ReconcileCount = %d, want 1 (HealDrift off must not reconcile)", prov.ReconcileCount)
	}
}

// TestProbeRecordsDrift: Probe merges observed DriftDetected/Ready
// conditions into recorded state so `status` reflects the last observation.
func TestProbeRecordsDrift(t *testing.T) {
	eng, _, envelopes := driftFixture(t)
	applyAll(t, eng, envelopes)

	results, err := eng.Probe(context.Background(), envelopes)
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if len(results) != 1 || !HasDrift(results[0].Status) {
		t.Fatalf("probe results = %+v, want one drifted resource", results)
	}

	st, err := eng.StateStore.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rs := st.Resources[envelopes[0].Key()]
	if !HasDrift(rs.Status) {
		t.Errorf("DriftDetected not persisted to state: %+v", rs.Status.Conditions)
	}
	if c, _ := rs.Status.Condition(status.Ready); c.Status != status.False {
		t.Errorf("probed Ready not persisted, got %+v", c)
	}
}
