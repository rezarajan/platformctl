package engine

import (
	"context"
	"testing"

	"github.com/rezarajan/platformctl/internal/application/featuregate"
	"github.com/rezarajan/platformctl/internal/application/plan"
	"github.com/rezarajan/platformctl/internal/application/registry"
	"github.com/rezarajan/platformctl/internal/domain/graph"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/state"
)

// TestFakeKindHandlerReachesAllFourDispatchPoints is G2's accept criterion:
// registering one entry in the engine's single kindHandlers table is enough
// for reconcileOne, probeOneAgainstState, applyDeleteOne, and Destroy to all
// honor a new special-cased kind — none of the four methods is touched.
//
// The fake kind below declares no providerRef and isn't Kind ==
// "SecretReference" or spec.external == true, so if any of the four methods
// fell through to its default provider-driven path instead of consulting the
// table, this test would fail with "no providerRef to resolve a provider
// from" rather than exercising the fake handler.
func TestFakeKindHandlerReachesAllFourDispatchPoints(t *testing.T) {
	const fakeKind = "FakeSpecialKind"
	var reconcileCalls, probeCalls, deleteCalls int

	fake := &kindHandler{
		name:  "test-fake",
		match: func(env resource.Envelope) bool { return env.Kind == fakeKind },
		reconcile: func(e *Engine, ctx context.Context, entry plan.Entry, env resource.Envelope, _ map[resource.Key]resource.Envelope, deps DependencyGraph, st *state.State) error {
			reconcileCalls++
			newStatus := status.Status{}
			newStatus.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "FakeReconciled"}, e.Clock.Now())
			e.stateMu.Lock()
			defer e.stateMu.Unlock()
			st.Resources[env.Key()] = e.resourceState(env, entry.SpecHash, newStatus, resource.Managed, false, deps)
			return e.StateStore.Save(ctx, *st)
		},
		probe: func(e *Engine, ctx context.Context, env resource.Envelope, _ map[resource.Key]resource.Envelope, _ state.ResourceState, _ *state.State) status.Status {
			probeCalls++
			st := status.Status{}
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "FakeProbed"}, e.Clock.Now())
			return st
		},
		del: func(e *Engine, ctx context.Context, env resource.Envelope, key resource.Key, st *state.State) error {
			deleteCalls++
			return deleteStateOnly(e, ctx, env, key, st)
		},
	}

	// The only edit this test makes to reach all four dispatch points: append
	// one row to the table and restore it afterward. No engine method
	// changes.
	kindHandlers = append(kindHandlers, fake)
	t.Cleanup(func() { kindHandlers = kindHandlers[:len(kindHandlers)-1] })

	eng := newTestEngine(t, registry.New(featuregate.NewRegistry()))
	env := envelope(fakeKind, "widget", map[string]any{})
	key := env.Key()

	// 1. reconcileOne, via Apply's create path.
	applyAll(t, eng, []resource.Envelope{env})
	if reconcileCalls != 1 {
		t.Fatalf("reconcile calls after apply = %d, want 1", reconcileCalls)
	}
	st, err := eng.StateStore.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rs, ok := st.Resources[key]; !ok || !rs.Status.IsReady() {
		t.Fatalf("state after apply = %+v, want a Ready entry for %s", st.Resources[key], key)
	}

	// 2. probeOneAgainstState, via Probe.
	results, err := eng.Probe(context.Background(), []resource.Envelope{env})
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if probeCalls != 1 {
		t.Fatalf("probe calls = %d, want 1", probeCalls)
	}
	if len(results) != 1 || !results[0].Status.IsReady() {
		t.Fatalf("probe results = %+v, want one Ready result", results)
	}

	// 3. applyDeleteOne, via a hand-built delete plan (mirrors
	// TestApplyRefusesLegacyOrphanUnknown's pattern for exact control over
	// the plan entry).
	g, err := graph.Build([]resource.Envelope{env})
	if err != nil {
		t.Fatal(err)
	}
	deleteResult, err := eng.Apply(context.Background(), plan.Plan{
		Entries: []plan.Entry{{Key: key, Action: plan.ActionDelete}},
		Levels:  [][]resource.Key{{key}},
	}, []resource.Envelope{env}, g)
	if err != nil {
		t.Fatalf("apply delete: %v (failed: %v)", err, deleteResult.Failed)
	}
	if deleteCalls != 1 {
		t.Fatalf("delete calls after apply-delete = %d, want 1", deleteCalls)
	}
	st, err = eng.StateStore.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := st.Resources[key]; ok {
		t.Fatalf("state still has %s after apply-delete", key)
	}

	// 4. Destroy, on a freshly re-applied instance of the same resource.
	applyAll(t, eng, []resource.Envelope{env})
	if reconcileCalls != 2 {
		t.Fatalf("reconcile calls after second apply = %d, want 2", reconcileCalls)
	}
	st, err = eng.StateStore.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	destroyPlan, err := plan.ComputeDestroy([]resource.Envelope{env}, st, g, false, false)
	if err != nil {
		t.Fatal(err)
	}
	destroyResult, err := eng.Destroy(context.Background(), destroyPlan, []resource.Envelope{env}, g)
	if err != nil {
		t.Fatalf("destroy: %v (failed: %v)", err, destroyResult.Failed)
	}
	if deleteCalls != 2 {
		t.Fatalf("delete calls after Destroy = %d, want 2", deleteCalls)
	}
	st, err = eng.StateStore.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := st.Resources[key]; ok {
		t.Fatalf("state still has %s after Destroy", key)
	}
}
