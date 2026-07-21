package registry

import (
	"context"
	"testing"

	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	"github.com/rezarajan/platformctl/internal/application/featuregate"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

func newTestRegistry(t *testing.T, haEnabled bool) *Registry {
	t.Helper()
	gates := featuregate.NewRegistry()
	gates.Register("HighAvailability", featuregate.Alpha, haEnabled)
	reg := New(gates)
	reg.RegisterRuntime("fake", func(_ map[string]any) (runtime.ContainerRuntime, error) {
		return fakeruntime.New(), nil
	})
	return reg
}

// TestRuntime_HighAvailabilityGate_BlocksMultiReplica proves the
// haGuardRuntime decorator (docs/adr/004-replicas-and-identity.md,
// "Feature gate enforcement"): every runtime returned by Registry.Runtime
// refuses an EnsureContainer call requesting more than one replica unless
// the HighAvailability gate is enabled — the backstop that holds even
// though no provider yet exposes a schema field setting Replicas.
func TestRuntime_HighAvailabilityGate_BlocksMultiReplica(t *testing.T) {
	reg := newTestRegistry(t, false)
	rt, err := reg.Runtime("fake", nil)
	if err != nil {
		t.Fatalf("Runtime: %v", err)
	}
	ctx := context.Background()

	// A single-replica spec is never gated.
	if _, err := rt.EnsureContainer(ctx, runtime.ContainerSpec{Name: "single", Image: "alpine:3.20"}); err != nil {
		t.Fatalf("EnsureContainer(Replicas: 0): %v", err)
	}

	_, err = rt.EnsureContainer(ctx, runtime.ContainerSpec{Name: "multi", Image: "alpine:3.20", Replicas: 3})
	if err == nil {
		t.Fatal("EnsureContainer(Replicas: 3) with HighAvailability disabled: want error, got nil")
	}
}

func TestRuntime_HighAvailabilityGate_AllowsMultiReplicaWhenEnabled(t *testing.T) {
	reg := newTestRegistry(t, true)
	rt, err := reg.Runtime("fake", nil)
	if err != nil {
		t.Fatalf("Runtime: %v", err)
	}
	ctx := context.Background()
	if _, err := rt.EnsureContainer(ctx, runtime.ContainerSpec{Name: "multi", Image: "alpine:3.20", Replicas: 3}); err != nil {
		t.Fatalf("EnsureContainer(Replicas: 3) with HighAvailability enabled: %v", err)
	}
}

// ingressCapableFake wraps the fake runtime and adds a bare-bones
// runtime.IngressCapableRuntime implementation — enough to prove
// Registry.Runtime's wrapper promotes the capability correctly, without
// pulling in the real Kubernetes adapter package (which registry_test.go,
// an application-layer test, is not allowed to import — CLAUDE.md's test
// exception only allows fake/localfile/env/noop as test doubles).
type ingressCapableFake struct {
	*fakeruntime.Runtime
	ensured bool
}

func (f *ingressCapableFake) EnsureIngress(ctx context.Context, spec runtime.IngressSpec) (runtime.IngressState, error) {
	f.ensured = true
	return runtime.IngressState{Host: spec.Host, TargetName: spec.TargetName, TargetPort: spec.TargetPort}, nil
}

func (f *ingressCapableFake) GetIngress(ctx context.Context, namespace, name string) (runtime.IngressState, bool, error) {
	return runtime.IngressState{}, f.ensured, nil
}

func (f *ingressCapableFake) RemoveIngress(ctx context.Context, namespace, name string) error {
	f.ensured = false
	return nil
}

// TestRuntime_PromotesIngressCapableRuntime is the F6 conformance
// reproduction for a live-caught bug (docs/planning/08 C7, docs/adr/018):
// haGuardRuntime originally embedded the runtime.ContainerRuntime
// *interface*, which only promotes that interface's own declared methods —
// so a provider's `req.Runtime.(runtime.IngressCapableRuntime)` type
// assertion (the pattern docs/adr/018 documents for a provider learning
// whether its runtime is Kubernetes) always failed for every runtime
// obtained through this registry, including a real adapter that genuinely
// implements the capability. Only an end-to-end apply against a live
// cluster caught this — the Kubernetes adapter's own fake-clientset unit
// tests call EnsureIngress directly and never go through this wrapper.
func TestRuntime_PromotesIngressCapableRuntime(t *testing.T) {
	gates := featuregate.NewRegistry()
	gates.Register("HighAvailability", featuregate.Alpha, false)
	reg := New(gates)
	reg.RegisterRuntime("ingress-fake", func(_ map[string]any) (runtime.ContainerRuntime, error) {
		return &ingressCapableFake{Runtime: fakeruntime.New()}, nil
	})
	reg.RegisterRuntime("plain-fake", func(_ map[string]any) (runtime.ContainerRuntime, error) {
		return fakeruntime.New(), nil
	})

	rt, err := reg.Runtime("ingress-fake", nil)
	if err != nil {
		t.Fatalf("Runtime: %v", err)
	}
	ic, ok := rt.(runtime.IngressCapableRuntime)
	if !ok {
		t.Fatal("registry-wrapped runtime does not implement IngressCapableRuntime even though the underlying adapter does")
	}
	ctx := context.Background()
	if _, err := ic.EnsureIngress(ctx, runtime.IngressSpec{Name: "route-x", Host: "x.localhost"}); err != nil {
		t.Fatalf("EnsureIngress: %v", err)
	}
	if _, found, err := ic.GetIngress(ctx, "ns", "route-x"); err != nil || !found {
		t.Fatalf("GetIngress: found=%v err=%v", found, err)
	}

	// The negative path: a runtime whose underlying adapter genuinely does
	// not implement the capability (Docker/fake in production) still
	// satisfies the IngressCapableRuntime type assertion — haGuardRuntime
	// itself always declares the three methods now — but calling through
	// them must surface a clear "not supported" error, never a panic or a
	// silent no-op; the wrapper delegates, it doesn't fabricate support.
	plainRt, err := reg.Runtime("plain-fake", nil)
	if err != nil {
		t.Fatalf("Runtime: %v", err)
	}
	plainIC, ok := plainRt.(runtime.IngressCapableRuntime)
	if !ok {
		t.Fatal("registry-wrapped runtime does not implement IngressCapableRuntime at all (haGuardRuntime should always declare these three methods)")
	}
	if _, err := plainIC.EnsureIngress(ctx, runtime.IngressSpec{Name: "route-y"}); err == nil {
		t.Fatal("EnsureIngress against a runtime whose underlying adapter is not ingress-capable should error, got nil")
	}
}
