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
	t.Parallel()
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
	t.Parallel()
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
	ensured    bool
	tlsSecrets map[string][2][]byte
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

// EnsureTLSSecret/GetTLSSecret/RemoveTLSSecret (docs/planning/08 C8): the
// same bare-bones-but-real-behavior fake as the three Ingress methods
// above, so TestRuntime_PromotesIngressCapableRuntime can cover the C8
// addition to IngressCapableRuntime with the identical reproduction shape
// ADR 018's addendum already established.
func (f *ingressCapableFake) EnsureTLSSecret(ctx context.Context, namespace, name string, certPEM, keyPEM []byte, labels map[string]string) error {
	if f.tlsSecrets == nil {
		f.tlsSecrets = map[string][2][]byte{}
	}
	f.tlsSecrets[namespace+"/"+name] = [2][]byte{certPEM, keyPEM}
	return nil
}

func (f *ingressCapableFake) GetTLSSecret(ctx context.Context, namespace, name string) ([]byte, []byte, bool, error) {
	v, ok := f.tlsSecrets[namespace+"/"+name]
	if !ok {
		return nil, nil, false, nil
	}
	return v[0], v[1], true, nil
}

func (f *ingressCapableFake) RemoveTLSSecret(ctx context.Context, namespace, name string) error {
	delete(f.tlsSecrets, namespace+"/"+name)
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
	t.Parallel()
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

	// TLS-secret trio: the same promotion must hold for the C8 addition.
	if err := ic.EnsureTLSSecret(ctx, "ns", "tls-x", []byte("cert"), []byte("key"), nil); err != nil {
		t.Fatalf("EnsureTLSSecret: %v", err)
	}
	cert, key, found, err := ic.GetTLSSecret(ctx, "ns", "tls-x")
	if err != nil || !found || string(cert) != "cert" || string(key) != "key" {
		t.Fatalf("GetTLSSecret: cert=%q key=%q found=%v err=%v", cert, key, found, err)
	}
	if err := ic.RemoveTLSSecret(ctx, "ns", "tls-x"); err != nil {
		t.Fatalf("RemoveTLSSecret: %v", err)
	}
	if _, _, found, _ := ic.GetTLSSecret(ctx, "ns", "tls-x"); found {
		t.Error("tls secret still present after RemoveTLSSecret")
	}
	if err := plainIC.EnsureTLSSecret(ctx, "ns", "tls-y", []byte("c"), []byte("k"), nil); err == nil {
		t.Fatal("EnsureTLSSecret against a non-ingress-capable runtime should error, got nil")
	}
}

// memberSetCapableFake wraps the fake runtime and adds a bare-bones
// runtime.MemberSetRuntime implementation — enough to prove
// Registry.Runtime's wrapper promotes the capability correctly, without
// pulling in the real Kubernetes adapter package (the same constraint
// ingressCapableFake above documents).
type memberSetCapableFake struct {
	*fakeruntime.Runtime
}

func (memberSetCapableFake) AddressesMembersCollectively() bool { return true }

// TestRuntime_PromotesMemberSetRuntime is the I7 counterpart of
// TestRuntime_PromotesIngressCapableRuntime above (docs/adr/004's I7
// addendum, docs/planning/08 §7.8): haGuardRuntime embeds the
// runtime.ContainerRuntime *interface*, so without an explicit delegating
// AddressesMembersCollectively method, a provider's own
// req.Runtime.(runtime.MemberSetRuntime) type assertion (providerkit's new
// collective-addressing branch) would always fail for every runtime
// obtained through this registry — including a real Kubernetes adapter that
// genuinely implements it — the identical bug class ADR 018's addendum
// caught live for IngressCapableRuntime.
func TestRuntime_PromotesMemberSetRuntime(t *testing.T) {
	t.Parallel()
	gates := featuregate.NewRegistry()
	gates.Register("HighAvailability", featuregate.Alpha, false)
	reg := New(gates)
	reg.RegisterRuntime("memberset-fake", func(_ map[string]any) (runtime.ContainerRuntime, error) {
		return memberSetCapableFake{Runtime: fakeruntime.New()}, nil
	})
	reg.RegisterRuntime("plain-fake", func(_ map[string]any) (runtime.ContainerRuntime, error) {
		return fakeruntime.New(), nil
	})

	rt, err := reg.Runtime("memberset-fake", nil)
	if err != nil {
		t.Fatalf("Runtime: %v", err)
	}
	ms, ok := rt.(runtime.MemberSetRuntime)
	if !ok {
		t.Fatal("registry-wrapped runtime does not implement MemberSetRuntime even though the underlying adapter does")
	}
	if !ms.AddressesMembersCollectively() {
		t.Error("AddressesMembersCollectively() = false, want true (delegated to the underlying adapter)")
	}

	// The negative path: a runtime whose underlying adapter genuinely does
	// not implement the capability (Docker/fake in production) still
	// satisfies the MemberSetRuntime type assertion — haGuardRuntime itself
	// always declares the method now — but answers false, the legitimate
	// "ordinal addressing applies" default, never a panic.
	plainRt, err := reg.Runtime("plain-fake", nil)
	if err != nil {
		t.Fatalf("Runtime: %v", err)
	}
	plainMS, ok := plainRt.(runtime.MemberSetRuntime)
	if !ok {
		t.Fatal("registry-wrapped runtime does not implement MemberSetRuntime at all (haGuardRuntime should always declare this method)")
	}
	if plainMS.AddressesMembersCollectively() {
		t.Error("AddressesMembersCollectively() = true for a non-capable underlying runtime, want false")
	}
}

// isolationCapableFake wraps the fake runtime and adds a bare-bones
// runtime.IsolationObserver implementation — enough to prove
// Registry.Runtime's wrapper promotes the capability correctly, without
// pulling in the real Kubernetes adapter package (the same constraint
// ingressCapableFake/memberSetCapableFake above document).
type isolationCapableFake struct {
	*fakeruntime.Runtime
}

func (isolationCapableFake) ObserveIsolationEnforcement(context.Context) (runtime.IsolationStatus, error) {
	return runtime.IsolationStatus{State: runtime.IsolationEnforced, Reason: "fake: enforced by construction"}, nil
}

// TestRuntime_PromotesIsolationObserver is the H8 counterpart of
// TestRuntime_PromotesIngressCapableRuntime/TestRuntime_PromotesMemberSetRuntime
// above (docs/adr/027-enforcement-layering.md, docs/planning/08 H8):
// haGuardRuntime embeds the runtime.ContainerRuntime *interface*, so
// without an explicit delegating ObserveIsolationEnforcement method, a
// caller's own rt.(runtime.IsolationObserver) type assertion would always
// fail for every runtime obtained through this registry — including a
// real Kubernetes adapter that genuinely implements it — the identical bug
// class ADR 018's addendum caught live for IngressCapableRuntime.
func TestRuntime_PromotesIsolationObserver(t *testing.T) {
	t.Parallel()
	gates := featuregate.NewRegistry()
	gates.Register("HighAvailability", featuregate.Alpha, false)
	reg := New(gates)
	reg.RegisterRuntime("isolation-fake", func(_ map[string]any) (runtime.ContainerRuntime, error) {
		return isolationCapableFake{Runtime: fakeruntime.New()}, nil
	})
	reg.RegisterRuntime("plain-fake", func(_ map[string]any) (runtime.ContainerRuntime, error) {
		return fakeruntime.New(), nil
	})

	rt, err := reg.Runtime("isolation-fake", nil)
	if err != nil {
		t.Fatalf("Runtime: %v", err)
	}
	io, ok := rt.(runtime.IsolationObserver)
	if !ok {
		t.Fatal("registry-wrapped runtime does not implement IsolationObserver even though the underlying adapter does")
	}
	status, err := io.ObserveIsolationEnforcement(context.Background())
	if err != nil {
		t.Fatalf("ObserveIsolationEnforcement: %v", err)
	}
	if status.State != runtime.IsolationEnforced {
		t.Errorf("State = %q, want %q (delegated to the underlying adapter)", status.State, runtime.IsolationEnforced)
	}

	// The negative path: a runtime whose underlying adapter genuinely does
	// not implement the capability (Docker/fake in production — Docker
	// gets its own real implementation, but the plain fake test double
	// here doesn't) still satisfies the IsolationObserver type assertion —
	// haGuardRuntime itself always declares the method now — but answers
	// IsolationUnknown, never an error, ADR 027's own tri-state.
	plainRt, err := reg.Runtime("plain-fake", nil)
	if err != nil {
		t.Fatalf("Runtime: %v", err)
	}
	plainIO, ok := plainRt.(runtime.IsolationObserver)
	if !ok {
		t.Fatal("registry-wrapped runtime does not implement IsolationObserver at all (haGuardRuntime should always declare this method)")
	}
	plainStatus, err := plainIO.ObserveIsolationEnforcement(context.Background())
	if err != nil {
		t.Fatalf("ObserveIsolationEnforcement (plain): %v", err)
	}
	if plainStatus.State != runtime.IsolationUnknown {
		t.Errorf("State = %q for a non-capable underlying runtime, want %q", plainStatus.State, runtime.IsolationUnknown)
	}
	if plainStatus.Reason == "" {
		t.Error("Reason is empty for IsolationUnknown; the tri-state contract requires naming why")
	}
}
