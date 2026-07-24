package engine

import (
	"context"
	"sync"
	"testing"

	"github.com/rezarajan/platformctl/internal/application/plan"
	"github.com/rezarajan/platformctl/internal/domain/graph"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/mediation"
)

// fakeFabricProvisioner is docs/planning/08 L2's honest fake
// mediation.FabricProvisioner (ADR 028) — no Docker/Kubernetes calls, no
// real controller/router, just call-counting and a fixed, deterministic
// FabricState, mirroring stubAddressResolver's own "same inputs, same
// result" discipline (mediation_transport_test.go).
type fakeFabricProvisioner struct {
	mu           sync.Mutex
	ensureCalls  int
	destroyCalls int
	ensureErr    error
	destroyErr   error
	lastLabels   map[string]string
}

func (f *fakeFabricProvisioner) EnsureFabric(_ context.Context, req mediation.FabricRequest) (mediation.FabricState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensureCalls++
	f.lastLabels = req.Labels
	if f.ensureErr != nil {
		return mediation.FabricState{}, f.ensureErr
	}
	return mediation.FabricState{
		ControllerContainerID: "fake-ctrl-id",
		RouterID:              "fake-router-id",
		ControllerInternal:    "fake-ctrl:1280",
	}, nil
}

func (f *fakeFabricProvisioner) DestroyFabric(_ context.Context, _ mediation.FabricRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.destroyCalls++
	return f.destroyErr
}

func (f *fakeFabricProvisioner) ensureCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ensureCalls
}

func (f *fakeFabricProvisioner) destroyCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.destroyCalls
}

func envelopesOf(byKey map[resource.Key]resource.Envelope) []resource.Envelope {
	out := make([]resource.Envelope, 0, len(byKey))
	for _, e := range byKey {
		out = append(out, e)
	}
	return out
}

// TestEnsureMediationFabricStandsUpWhenGateOnAndEdgeMediated is docs/
// planning/08 L2's core accept criterion: "apply on a gate-on scenario
// stands the fabric up exactly once" — mtManifest's Binding declares no
// spec.transport (mediated by default, ADR 034), so a single Apply must
// call EnsureFabric exactly once and record a Ready=True state entry under
// mediationFabricKey(), carrying no credential material.
func TestEnsureMediationFabricStandsUpWhenGateOnAndEdgeMediated(t *testing.T) {
	t.Parallel()
	eng := mtEngine(t, true)
	fake := &fakeFabricProvisioner{}
	eng.Fabric = fake
	byKey, _, _, _ := mtManifest("")
	applyAll(t, eng, envelopesOf(byKey))

	if got := fake.ensureCallCount(); got != 1 {
		t.Fatalf("EnsureFabric call count = %d, want exactly 1", got)
	}

	st, err := eng.StateStore.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rs := st.MediationFabric
	if rs == nil {
		t.Fatal("no state entry recorded for the platform mediation fabric")
	}
	c, ok := rs.Status.Condition(status.Ready)
	if !ok || c.Status != status.True {
		t.Fatalf("fabric Ready condition = %+v, ok=%v, want Ready=True", c, ok)
	}
	for k := range rs.Provider {
		if k == "controllerContainerId" || k == "controllerInternal" || k == "routerId" || k == "runtimeType" || k == "runtimeConfig" {
			continue
		}
		t.Errorf("unexpected ProviderState key %q — docs/adr/013 forbids any credential/secret material in state", k)
	}
	if rs.Provider["controllerInternal"] != "fake-ctrl:1280" {
		t.Errorf("controllerInternal = %v, want the fabric's own reported value", rs.Provider["controllerInternal"])
	}
}

// TestEnsureMediationFabricGateOffIsNoop pins the gate-off byte-identical
// cost the whole codebase holds this pattern to (H7/L1's own precedent):
// with the gate off, EnsureFabric must never be called and no state entry
// appears, even though the manifest declares a mediated-by-default edge
// and a willing Fabric is wired.
func TestEnsureMediationFabricGateOffIsNoop(t *testing.T) {
	t.Parallel()
	eng := mtEngine(t, false)
	fake := &fakeFabricProvisioner{}
	eng.Fabric = fake
	byKey, _, _, _ := mtManifest("")
	applyAll(t, eng, envelopesOf(byKey))

	if got := fake.ensureCallCount(); got != 0 {
		t.Fatalf("EnsureFabric call count = %d, want 0 (gate off)", got)
	}
	st, err := eng.StateStore.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.MediationFabric != nil {
		t.Fatal("fabric state entry recorded with the gate off")
	}
}

// TestEnsureMediationFabricNoopWhenNoFabricWired proves nil Fabric disables
// the facility entirely (mirrors Engine.Mediation's own nil-disables
// convention) — Apply must not panic and must record nothing.
func TestEnsureMediationFabricNoopWhenNoFabricWired(t *testing.T) {
	t.Parallel()
	eng := mtEngine(t, true)
	// eng.Fabric intentionally left nil.
	byKey, _, _, _ := mtManifest("")
	applyAll(t, eng, envelopesOf(byKey))

	st, err := eng.StateStore.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.MediationFabric != nil {
		t.Fatal("fabric state entry recorded with no Fabric wired")
	}
}

// TestEnsureMediationFabricNoopWhenNoMediatedEdgeDeclared proves the
// trigger condition is genuinely "at least one mediated edge", not merely
// "the gate is on": every edge-declaring resource in the manifest opting
// into transport: direct means the fabric is never even attempted.
func TestEnsureMediationFabricNoopWhenNoMediatedEdgeDeclared(t *testing.T) {
	t.Parallel()
	eng := mtEngine(t, true)
	fake := &fakeFabricProvisioner{}
	eng.Fabric = fake
	byKey, _, _, _ := mtManifest("direct")
	applyAll(t, eng, envelopesOf(byKey))

	if got := fake.ensureCallCount(); got != 0 {
		t.Fatalf("EnsureFabric call count = %d, want 0 (every declared edge is transport: direct)", got)
	}
}

// TestEnsureMediationFabricStableAcrossReapply is the idempotency proof
// (docs/planning/08 L2's accept: "second apply zero API calls" at the
// runtime.ContainerRuntime layer, which this fake does not itself model —
// the real openziti.FabricProvisioner reuses instance.go's own
// EnsureContainer/EnsureNetwork/EnsureVolume calls, already held to that
// bar by the existing docker/kubernetes conformance suites). At the
// ENGINE orchestration layer proven here: EnsureFabric is asked on every
// apply (the provisioner's own job to be idempotent, exactly like
// mediatedAddress asks DialAddress on every resolveRequest — L1's own
// TestMediatedTransportIdempotent proof for the sibling seam), and the
// resulting state stays stable and credential-free across repeated
// applies.
func TestEnsureMediationFabricStableAcrossReapply(t *testing.T) {
	t.Parallel()
	eng := mtEngine(t, true)
	fake := &fakeFabricProvisioner{}
	eng.Fabric = fake
	byKey, _, _, _ := mtManifest("")
	envelopes := envelopesOf(byKey)

	applyAll(t, eng, envelopes)
	st1, err := eng.StateStore.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rs1 := st1.MediationFabric

	applyAll(t, eng, envelopes)
	if got := fake.ensureCallCount(); got != 2 {
		t.Fatalf("EnsureFabric call count = %d, want 2 (once per apply)", got)
	}
	st2, err := eng.StateStore.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rs2 := st2.MediationFabric

	if rs1.Provider["controllerContainerId"] != rs2.Provider["controllerContainerId"] {
		t.Errorf("controllerContainerId changed across reapply: %v -> %v", rs1.Provider["controllerContainerId"], rs2.Provider["controllerContainerId"])
	}
}

// TestMaybeDestroyMediationFabricTearsDownWhenNoMediatedEdgeRemains is
// docs/planning/08 L2's other accept criterion: "destroy of the last
// mediated edge removes it." Destroying every resource in mtManifest
// (including the mediated Binding) must call DestroyFabric exactly once
// and remove the fabric's own state entry.
func TestMaybeDestroyMediationFabricTearsDownWhenNoMediatedEdgeRemains(t *testing.T) {
	t.Parallel()
	eng := mtEngine(t, true)
	fake := &fakeFabricProvisioner{}
	eng.Fabric = fake
	byKey, _, _, _ := mtManifest("")
	envelopes := envelopesOf(byKey)
	applyAll(t, eng, envelopes)
	if got := fake.ensureCallCount(); got != 1 {
		t.Fatalf("precondition: EnsureFabric call count = %d, want 1", got)
	}

	g, err := graph.Build(envelopes)
	if err != nil {
		t.Fatal(err)
	}
	st, err := eng.StateStore.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	p, err := plan.ComputeDestroy(envelopes, st, g, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Destroy(context.Background(), p, envelopes, g); err != nil {
		t.Fatalf("destroy: %v", err)
	}

	if got := fake.destroyCallCount(); got != 1 {
		t.Fatalf("DestroyFabric call count = %d, want exactly 1", got)
	}
	finalSt, err := eng.StateStore.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if finalSt.MediationFabric != nil {
		t.Fatal("fabric state entry still present after every mediated edge was destroyed")
	}
}

// TestMaybeDestroyMediationFabricKeptWhileMediatedEdgeRemains is
// docs/adr/013's implicit-infrastructure bar, the negative case: as long
// as ANY mediated edge remains anywhere in the deployment's state, the
// fabric must never be torn down. Exercises maybeDestroyMediationFabric
// directly against a hand-built state.State (rather than driving a real
// Destroy call, which — with every resource in mtManifest transitively
// feeding the one Binding — has no "destroy something unrelated while the
// Binding survives" shape to exercise): the precise boundary this test
// cares about is the function's own remaining-state computation, not
// dependency-graph teardown ordering (covered elsewhere).
func TestMaybeDestroyMediationFabricKeptWhileMediatedEdgeRemains(t *testing.T) {
	t.Parallel()
	eng := mtEngine(t, true)
	fake := &fakeFabricProvisioner{}
	eng.Fabric = fake
	byKey, bindingEnv, _, _ := mtManifest("")
	applyAll(t, eng, envelopesOf(byKey))
	if got := fake.ensureCallCount(); got != 1 {
		t.Fatalf("precondition: EnsureFabric call count = %d, want 1", got)
	}

	st, err := eng.StateStore.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// Sanity: the mediated Binding is still recorded in state (applyAll
	// applied the whole manifest, nothing was destroyed).
	if _, ok := st.Resources[bindingEnv.Key()]; !ok {
		t.Fatal("test setup: the mediated Binding is not recorded in state")
	}

	if err := eng.maybeDestroyMediationFabric(context.Background(), &st); err != nil {
		t.Fatalf("maybeDestroyMediationFabric: %v", err)
	}

	if got := fake.destroyCallCount(); got != 0 {
		t.Fatalf("DestroyFabric call count = %d, want 0 — a mediated edge (the Binding) still remains in state", got)
	}
	finalSt, err := eng.StateStore.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if finalSt.MediationFabric == nil {
		t.Fatal("fabric state entry removed even though a mediated edge remains")
	}
}
