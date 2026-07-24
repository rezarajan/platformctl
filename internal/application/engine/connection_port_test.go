package engine

import (
	"context"
	"testing"
	"time"

	"github.com/rezarajan/platformctl/internal/adapters/providers/noop"
	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	"github.com/rezarajan/platformctl/internal/application/featuregate"
	"github.com/rezarajan/platformctl/internal/application/registry"
	"github.com/rezarajan/platformctl/internal/domain/connection"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/hostport"
	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// fakeForwarderProvider stands in for the real `proxy` provider's own
// Connection realization (internal/adapters/providers/proxy's
// reconcileConnection): it reads connection.FromEnvelope(req.Resource) —
// exactly what proxy does — creates a container listening on the resolved
// conn.Port, and publishes that same value as the endpoint fact's
// ContainerPort, exactly like proxy's own
// `endpoint.List{{..., ContainerPort: conn.Port, ...}}`. This test's only
// job is to prove the *port value* domain auto-allocation produced is what
// a realizing provider publishes and what a consumer then resolves through
// that fact — not to re-test proxy's own health-check mechanics (covered by
// proxy's own adapter-level tests).
type fakeForwarderProvider struct {
	noop.Provider
}

func (p *fakeForwarderProvider) Type() string { return "fakeforwarder" }

func (p *fakeForwarderProvider) SupportedConnectionSchemes() []string { return []string{"tcp"} }

func (p *fakeForwarderProvider) Reconcile(ctx context.Context, req reconciler.Request) (status.Status, error) {
	if req.Resource.Kind != "Connection" {
		return p.Provider.Reconcile(ctx, req)
	}
	conn, err := connection.FromEnvelope(req.Resource)
	if err != nil {
		return status.Status{}, err
	}
	name := naming.RuntimeObjectName(req.Resource)
	if _, err := req.Runtime.EnsureContainer(ctx, runtime.ContainerSpec{
		Name:  name,
		Image: "alpine/socat:1.8.0.3@sha256:beb4a68d9e4fe6b0f21ea774a0fde6c31f580dde6368939ed70100c5385b015e",
		Ports: []runtime.PortBinding{{HostPort: conn.Port, ContainerPort: conn.Port, Audience: runtime.AudienceHost}},
	}); err != nil {
		return status.Status{}, err
	}
	st := status.Status{}
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonReconcileComplete}, time.Now())
	st.ProviderState = map[string]any{
		endpoint.Key: endpoint.List{
			{Name: "forward", Scheme: conn.Scheme, RuntimeName: name, ContainerPort: conn.Port, Audience: runtime.AudienceHost},
		}.ToState(),
	}
	return st, nil
}

// TestConnectionAutoAllocatedPortPublishedAndResolved is M2's fast-tier
// pipeline proof (docs/adr/035 decision 2, docs/planning/08 §7.12 M2): a
// managed Connection with no spec.port auto-allocates deterministically
// (internal/domain/connection.FromEnvelope, keyed on the Connection's own
// runtime object name via internal/domain/hostport — the same allocator a
// Provider's own omitted host port uses); the realizing provider publishes
// that resolved port as its endpoint fact's ContainerPort (exactly what
// `proxy` does); and a consumer resolves the connection's address through
// that published fact (Engine.connectionDialAddress), not a literal — the
// full auto-port chain, start to finish, through a real graph.Build ->
// plan.Compute -> Engine.Apply run.
func TestConnectionAutoAllocatedPortPublishedAndResolved(t *testing.T) {
	t.Parallel()
	gates := featuregate.NewRegistry()
	reg := registry.New(gates)
	fwd := &fakeForwarderProvider{}
	reg.RegisterProvider("fakeforwarder", func() reconciler.Provider { return fwd }, "")
	// A single shared fake runtime instance, not a fresh one per resolve:
	// the forwarder container Reconcile creates during applyAll must still
	// be there when connectionDialAddress below independently resolves the
	// runtime again to dial it (mirrors
	// TestConnectionDialAddressUsesPublishedFactNotResourceName's own
	// fakeRT setup).
	fakeRT := fakeruntime.New()
	reg.RegisterRuntime("fake", func(_ map[string]any) (runtime.ContainerRuntime, error) {
		return fakeRT, nil
	})

	eng := newTestEngine(t, reg)
	provEnv := envelope("Provider", "edge", map[string]any{
		"type":    "fakeforwarder",
		"runtime": map[string]any{"type": "fake"},
	})
	connEnv := envelope("Connection", "orders-db", map[string]any{
		"providerRef": map[string]any{"name": "edge"},
		"target":      "upstream:5432",
		// spec.port intentionally omitted — the M2 auto-allocation path.
	})
	envelopes := []resource.Envelope{provEnv, connEnv}

	applyAll(t, eng, envelopes)

	// Reconcile's returned status.ProviderState is persisted into the state
	// store (state.ResourceState.Status), not back onto the envelopes slice
	// applyAll was given — read it back the same way a real `apply` ->
	// `status`/`inventory` round-trip would, then attach it to the
	// Connection envelope the same shape connectionDialAddress expects
	// (mirrors TestConnectionDialAddressUsesPublishedFactNotResourceName's
	// own byKey construction above).
	loaded, err := eng.StateStore.Load(context.Background())
	if err != nil {
		t.Fatalf("StateStore.Load: %v", err)
	}
	rs, ok := loaded.Resources[connEnv.Key()]
	if !ok {
		t.Fatal("test setup invalid: the Connection was never reconciled")
	}
	reconciled := connEnv
	reconciled.Status = rs.Status

	wantPort := hostport.For(naming.RuntimeObjectName(connEnv))

	conn, err := connection.FromEnvelope(reconciled)
	if err != nil {
		t.Fatalf("connection.FromEnvelope: %v", err)
	}
	if conn.Port != wantPort {
		t.Fatalf("auto-allocated conn.Port = %d, want %d (hostport.For(%q))", conn.Port, wantPort, naming.RuntimeObjectName(connEnv))
	}

	facts := endpoint.FromState(reconciled.Status.ProviderState[endpoint.Key])
	if len(facts) != 1 || facts[0].ContainerPort != wantPort {
		t.Fatalf("published endpoint facts = %+v, want exactly one with ContainerPort %d", facts, wantPort)
	}

	// Full pipeline: a consumer resolves the Connection's address through
	// the published fact (never a literal port) and actually reaches the
	// auto-allocated listener.
	byKey := map[resource.Key]resource.Envelope{}
	for _, e := range envelopes {
		byKey[e.Key()] = e
	}
	byKey[reconciled.Key()] = reconciled
	addr, closeFn := eng.connectionDialAddress(context.Background(), reconciled, conn, byKey)
	if closeFn != nil {
		defer closeFn()
	}
	if addr == "" {
		t.Fatal("connectionDialAddress returned no address; want it to resolve the auto-allocated port via the published endpoint fact")
	}
}
