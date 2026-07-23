package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/rezarajan/platformctl/internal/adapters/providers/noop"
	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	"github.com/rezarajan/platformctl/internal/application/featuregate"
	"github.com/rezarajan/platformctl/internal/application/plan"
	"github.com/rezarajan/platformctl/internal/application/registry"
	"github.com/rezarajan/platformctl/internal/domain/graph"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// fakeConnProvider is a local stub ConnectionCapableProvider (CLAUDE.md's
// application-test-double rule: no technology adapter — ingress included —
// may be imported from internal/application tests; a local stub of the
// capability interface stands in, mirroring compatibility_test.go's
// versionedStub pattern).
type fakeConnProvider struct{ noop.Provider }

func (f *fakeConnProvider) Type() string                         { return "fakeconn" }
func (f *fakeConnProvider) SupportedConnectionSchemes() []string { return []string{"http", "https"} }

func tlsConnectionEnvelopes() []resource.Envelope {
	prov := envelope("Provider", "edge", map[string]any{
		"type":    "fakeconn",
		"runtime": map[string]any{"type": "fake", "network": "datascape"},
	})
	conn := envelope("Connection", "nessie", map[string]any{
		"providerRef": map[string]any{"name": "edge"},
		"scheme":      "https",
		"port":        float64(443),
		"target":      "nessie:19120",
		"tls":         map[string]any{"selfSigned": true},
	})
	return []resource.Envelope{prov, conn}
}

// applyTolerant is applyAll without the "any error is a harness bug"
// assumption: Engine.Apply returns a non-nil top-level error whenever
// Result.Failed is non-empty (see engine.go's Apply, the closing
// "%d resource(s) failed to reconcile"), which is exactly the case these
// gate tests are checking for, not a test-setup mistake.
func applyTolerant(t *testing.T, eng *Engine, envelopes []resource.Envelope) Result {
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
	result, _ := eng.Apply(context.Background(), p, envelopes, g)
	return result
}

func newTLSGateTestEngine(t *testing.T, gates *featuregate.Registry) *Engine {
	t.Helper()
	reg := registry.New(gates)
	reg.RegisterProvider("fakeconn", func() reconciler.Provider { return &fakeConnProvider{} }, "")
	reg.RegisterRuntime("fake", func(_ map[string]any) (runtime.ContainerRuntime, error) { return fakeruntime.New(), nil })
	return newTestEngine(t, reg)
}

// TestTLSTerminationGateBlocksUngatedApply covers docs/planning/08 C8's gate
// wiring: a Connection declaring spec.tls fails to reconcile — naming the
// TLSTermination gate — when the gate is unregistered or registered-but-
// disabled, and succeeds once it's enabled. registry.RequireGate is the
// single choke point (engine.resolveRequest), mirroring HighAvailability's
// own backstop-at-point-of-use pattern.
func TestTLSTerminationGateBlocksUngatedApply(t *testing.T) {
	t.Parallel()
	envelopes := tlsConnectionEnvelopes()
	connKey := envelopes[1].Key()

	// Case 1: TLSTermination never registered.
	eng := newTLSGateTestEngine(t, featuregate.NewRegistry())
	result := applyTolerant(t, eng, envelopes)
	err, failed := result.Failed[connKey]
	if !failed {
		t.Fatal("expected Connection reconcile to fail with TLSTermination unregistered")
	}
	if !strings.Contains(err.Error(), "TLSTermination") {
		t.Errorf("error = %q, want it to name the TLSTermination gate", err.Error())
	}

	// Case 2: gate registered but disabled (the Alpha default).
	gates2 := featuregate.NewRegistry()
	gates2.Register("TLSTermination", featuregate.Alpha, false)
	eng2 := newTLSGateTestEngine(t, gates2)
	result2 := applyTolerant(t, eng2, envelopes)
	err2, failed2 := result2.Failed[connKey]
	if !failed2 || !strings.Contains(err2.Error(), "TLSTermination") {
		t.Fatalf("expected disabled-gate failure naming TLSTermination, got failed=%v err=%v", failed2, err2)
	}

	// Case 3: gate enabled — reconcile succeeds.
	gates3 := featuregate.NewRegistry()
	gates3.Register("TLSTermination", featuregate.Alpha, true)
	eng3 := newTLSGateTestEngine(t, gates3)
	result3 := applyAll(t, eng3, envelopes)
	if _, failed3 := result3.Failed[connKey]; failed3 {
		t.Fatalf("Connection reconcile failed with TLSTermination enabled: %v", result3.Failed[connKey])
	}
}

// TestPlainHTTPConnectionUnaffectedByTLSGate: a Connection with no
// spec.tls never consults the TLSTermination gate at all — the pre-C8
// plaintext path is untouched even when the gate is unregistered.
func TestPlainHTTPConnectionUnaffectedByTLSGate(t *testing.T) {
	t.Parallel()
	prov := envelope("Provider", "edge", map[string]any{
		"type":    "fakeconn",
		"runtime": map[string]any{"type": "fake", "network": "datascape"},
	})
	conn := envelope("Connection", "nessie", map[string]any{
		"providerRef": map[string]any{"name": "edge"},
		"scheme":      "http",
		"port":        float64(80),
		"target":      "nessie:19120",
	})
	envelopes := []resource.Envelope{prov, conn}
	eng := newTLSGateTestEngine(t, featuregate.NewRegistry())
	result := applyAll(t, eng, envelopes)
	if _, failed := result.Failed[conn.Key()]; failed {
		t.Fatalf("plain http Connection reconcile failed even though it declares no spec.tls: %v", result.Failed[conn.Key()])
	}
}
