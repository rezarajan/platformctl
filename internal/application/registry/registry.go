// Package registry is the single place mapping provider.spec.type → Provider
// constructor and runtime.type → ContainerRuntime constructor. Registration is
// explicit, called from cmd/platformctl's wiring — domain and ports never
// import adapters directly. See docs/planning/02-architecture.md §5.6.
package registry

import (
	"context"
	"fmt"
	"sort"

	"github.com/rezarajan/platformctl/internal/application/featuregate"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

type ProviderConstructor func() reconciler.Provider
type RuntimeConstructor func(config map[string]any) (runtime.ContainerRuntime, error)

// PlannedRuntimes are accepted by schema for forward compatibility but
// rejected at registry-construction time with a "planned, not yet available"
// error — never silently ignored. See docs/planning/05-v1-first-version-spec.md §4.
// "kubernetes" is no longer planned-only: a real adapter exists (Alpha,
// behind the KubernetesRuntime gate) — see
// internal/adapters/runtime/kubernetes and docs/planning/04 §10.
var PlannedRuntimes = map[string]bool{
	"external":  true,
	"terraform": true,
}

type Registry struct {
	providers map[string]ProviderConstructor
	runtimes  map[string]RuntimeConstructor
	// providerGate maps provider type → feature gate name guarding it.
	providerGate map[string]string
	gates        *featuregate.Registry
}

func New(gates *featuregate.Registry) *Registry {
	return &Registry{
		providers:    make(map[string]ProviderConstructor),
		runtimes:     make(map[string]RuntimeConstructor),
		providerGate: make(map[string]string),
		gates:        gates,
	}
}

func (r *Registry) RegisterProvider(typeName string, ctor ProviderConstructor, gateName string) {
	r.providers[typeName] = ctor
	if gateName != "" {
		r.providerGate[typeName] = gateName
	}
}

func (r *Registry) RegisterRuntime(typeName string, ctor RuntimeConstructor) {
	r.runtimes[typeName] = ctor
}

// RequireGate reports a clear error when the named feature gate is disabled
// or unknown — a thin public wrapper so a manifest-declared field with no
// natural provider-construction or runtime-call choke point of its own
// (docs/planning/08 C8's Connection.spec.tls: not a distinct provider type
// like IngressProvider/BackupRestore, not a CLI-flag behavior like
// DriftDetection/ParallelReconciliation) still has exactly one place to
// enforce its gate, mirroring HighAvailability's own admitted-imperfect
// backstop-at-point-of-use pattern (haGuardRuntime.EnsureContainer below)
// rather than inventing a second gating mechanism.
func (r *Registry) RequireGate(name string) error {
	return r.gates.Require(name)
}

func (r *Registry) Provider(typeName string) (reconciler.Provider, error) {
	ctor, ok := r.providers[typeName]
	if !ok {
		return nil, fmt.Errorf("unknown provider type %q (registered: %s)", typeName, joinKeys(r.providers))
	}
	if gate, gated := r.providerGate[typeName]; gated {
		if err := r.gates.Require(gate); err != nil {
			return nil, fmt.Errorf("provider type %q: %w", typeName, err)
		}
	}
	return ctor(), nil
}

func (r *Registry) Runtime(typeName string, config map[string]any) (runtime.ContainerRuntime, error) {
	if PlannedRuntimes[typeName] {
		return nil, fmt.Errorf("runtime type %q is planned but not yet available in this version", typeName)
	}
	ctor, ok := r.runtimes[typeName]
	if !ok {
		return nil, fmt.Errorf("unknown runtime type %q (registered: %s)", typeName, joinKeys(r.runtimes))
	}
	rt, err := ctor(config)
	if err != nil {
		return nil, err
	}
	// Every runtime, on every call, is wrapped with the HighAvailability
	// gate guard (docs/adr/004-replicas-and-identity.md, "Feature gate
	// enforcement"): no provider yet exposes a schema field that sets
	// ContainerSpec.Replicas (that first happens in a later task), so there
	// is no per-provider SpecValidator to attach the check to yet. Wrapping
	// the single choke point every provider's Request.Runtime passes
	// through enforces the invariant once, for every current and future
	// provider, rather than depending on each new replica-capable provider
	// remembering its own validate-time check (which they should still add,
	// for the better DX of failing before apply — this wrapper is the
	// correctness backstop, not a replacement for that).
	return &haGuardRuntime{ContainerRuntime: rt, gates: r.gates}, nil
}

// haGuardRuntime wraps a runtime.ContainerRuntime so that any
// EnsureContainer call requesting more than one replica requires the
// HighAvailability feature gate to be enabled, refusing with a clear error
// otherwise. Every other method delegates to the embedded ContainerRuntime
// unchanged.
type haGuardRuntime struct {
	runtime.ContainerRuntime
	gates *featuregate.Registry
}

func (g *haGuardRuntime) EnsureContainer(ctx context.Context, spec runtime.ContainerSpec) (runtime.ContainerState, error) {
	if spec.ReplicaCount() > 1 {
		if err := g.gates.Require("HighAvailability"); err != nil {
			return runtime.ContainerState{}, fmt.Errorf("container %q requests %d replicas: %w", spec.Name, spec.Replicas, err)
		}
	}
	return g.ContainerRuntime.EnsureContainer(ctx, spec)
}

// EnsureIngress/GetIngress/RemoveIngress make haGuardRuntime itself satisfy
// runtime.IngressCapableRuntime, delegating to the embedded runtime when it
// implements the capability. Without these three explicit methods, a
// provider's own `req.Runtime.(runtime.IngressCapableRuntime)` type
// assertion (docs/adr/018 "Layering") would always fail for every runtime
// obtained through this registry — including a real Kubernetes adapter that
// genuinely implements it — because embedding the runtime.ContainerRuntime
// *interface* (not the concrete adapter type) only promotes that interface's
// own declared method set, never a concrete implementation's extra methods.
// Found live (2026-07-21) against a real cluster: the fake-clientset unit
// tests call the Kubernetes adapter's EnsureIngress directly and never
// exercise this wrapper, so only an end-to-end apply through the registry
// caught it (docs/planning/08 F6 conformance ratchet).
func (g *haGuardRuntime) EnsureIngress(ctx context.Context, spec runtime.IngressSpec) (runtime.IngressState, error) {
	ic, ok := g.ContainerRuntime.(runtime.IngressCapableRuntime)
	if !ok {
		return runtime.IngressState{}, fmt.Errorf("ingress provider: runtime does not implement IngressCapableRuntime (expected on a Kubernetes-runtime Provider)")
	}
	return ic.EnsureIngress(ctx, spec)
}

func (g *haGuardRuntime) GetIngress(ctx context.Context, namespace, name string) (runtime.IngressState, bool, error) {
	ic, ok := g.ContainerRuntime.(runtime.IngressCapableRuntime)
	if !ok {
		return runtime.IngressState{}, false, fmt.Errorf("ingress provider: runtime does not implement IngressCapableRuntime (expected on a Kubernetes-runtime Provider)")
	}
	return ic.GetIngress(ctx, namespace, name)
}

func (g *haGuardRuntime) RemoveIngress(ctx context.Context, namespace, name string) error {
	ic, ok := g.ContainerRuntime.(runtime.IngressCapableRuntime)
	if !ok {
		return fmt.Errorf("ingress provider: runtime does not implement IngressCapableRuntime (expected on a Kubernetes-runtime Provider)")
	}
	return ic.RemoveIngress(ctx, namespace, name)
}

// EnsureTLSSecret/GetTLSSecret/RemoveTLSSecret (docs/planning/08 C8) get the
// identical explicit-delegation treatment as EnsureIngress/GetIngress/
// RemoveIngress above, for the identical reason (docs/adr/018 addendum): an
// embedded runtime.ContainerRuntime *interface* only promotes that
// interface's own declared method set, never a concrete implementation's
// extra methods — so without these three, a provider's own
// req.Runtime.(runtime.IngressCapableRuntime) assertion would fail for
// every runtime obtained through this registry, including a real
// Kubernetes adapter that genuinely implements them.
func (g *haGuardRuntime) EnsureTLSSecret(ctx context.Context, namespace, name string, certPEM, keyPEM []byte, labels map[string]string) error {
	ic, ok := g.ContainerRuntime.(runtime.IngressCapableRuntime)
	if !ok {
		return fmt.Errorf("ingress provider: runtime does not implement IngressCapableRuntime (expected on a Kubernetes-runtime Provider)")
	}
	return ic.EnsureTLSSecret(ctx, namespace, name, certPEM, keyPEM, labels)
}

func (g *haGuardRuntime) GetTLSSecret(ctx context.Context, namespace, name string) ([]byte, []byte, bool, error) {
	ic, ok := g.ContainerRuntime.(runtime.IngressCapableRuntime)
	if !ok {
		return nil, nil, false, fmt.Errorf("ingress provider: runtime does not implement IngressCapableRuntime (expected on a Kubernetes-runtime Provider)")
	}
	return ic.GetTLSSecret(ctx, namespace, name)
}

func (g *haGuardRuntime) RemoveTLSSecret(ctx context.Context, namespace, name string) error {
	ic, ok := g.ContainerRuntime.(runtime.IngressCapableRuntime)
	if !ok {
		return fmt.Errorf("ingress provider: runtime does not implement IngressCapableRuntime (expected on a Kubernetes-runtime Provider)")
	}
	return ic.RemoveTLSSecret(ctx, namespace, name)
}

func joinKeys[V any](m map[string]V) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := ""
	for i, k := range keys {
		if i > 0 {
			out += ", "
		}
		out += k
	}
	return out
}
