package engine

import (
	"context"
	"testing"

	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// TestDomainRuntimeInjectsDeclaredResources pins docs/planning/08 J5's
// chokepoint rule: spec.runtime.resources reaches every EnsureContainer
// spec with ZERO provider changes — the engine's decorator injects it,
// and a provider-set Resources (none exist today) would win.
func TestDomainRuntimeInjectsDeclaredResources(t *testing.T) {
	t.Parallel()
	rt := fakeruntime.New()
	env := resource.Envelope{
		GroupVersionKind: resource.GroupVersionKind{APIVersion: "datascape.io/v1alpha1", Kind: "Provider"},
		Metadata:         resource.Metadata{Name: "p"},
		Spec:             map[string]any{"type": "noop"},
	}
	cfg := map[string]any{
		"type": "fake",
		"resources": map[string]any{
			"cpu":               1.5,
			"cpuReservation":    0.25,
			"memory":            "512Mi",
			"memoryReservation": "256Mi",
		},
	}
	d := newDomainRuntime(rt, cfg, env, env, nil, false, false, nil, "fake", "", nil)
	if _, err := d.EnsureContainer(context.Background(), runtime.ContainerSpec{Name: "w", Image: "img"}); err != nil {
		t.Fatalf("EnsureContainer: %v", err)
	}
	spec, ok := rt.Spec("w")
	if !ok {
		t.Fatal("container w not recorded by the fake runtime")
	}
	got := spec.Resources
	if got == nil {
		t.Fatal("declared spec.runtime.resources did not reach the container spec")
	}
	want := runtime.Resources{CPULimit: 1.5, CPUReservation: 0.25, MemoryLimitBytes: 512 << 20, MemoryReservationBytes: 256 << 20}
	if *got != want {
		t.Fatalf("injected Resources = %+v, want %+v", *got, want)
	}

	// A provider-set Resources wins — injection must not override.
	own := &runtime.Resources{MemoryLimitBytes: 1 << 30}
	if _, err := d.EnsureContainer(context.Background(), runtime.ContainerSpec{Name: "w2", Image: "img", Resources: own}); err != nil {
		t.Fatalf("EnsureContainer(w2): %v", err)
	}
	if spec2, _ := rt.Spec("w2"); spec2.Resources != own {
		t.Fatal("provider-set Resources was overridden by the injected default")
	}

	// No resources declared -> nothing injected.
	d2 := newDomainRuntime(rt, map[string]any{"type": "fake"}, env, env, nil, false, false, nil, "fake", "", nil)
	if _, err := d2.EnsureContainer(context.Background(), runtime.ContainerSpec{Name: "w3", Image: "img"}); err != nil {
		t.Fatalf("EnsureContainer(w3): %v", err)
	}
	if spec3, _ := rt.Spec("w3"); spec3.Resources != nil {
		t.Fatal("Resources injected despite no spec.runtime.resources block")
	}
}

// TestDomainRuntimeAppliesProviderDefaultResources pins M3 (docs/adr/035
// decision 4): a provider that declares no resources anywhere still gets
// its sensible per-technology default at the chokepoint — an undecorated
// manifest yields bounded containers. An explicit resources still wins
// (proven above); a type with no meaningful default stays unbounded.
func TestDomainRuntimeAppliesProviderDefaultResources(t *testing.T) {
	t.Parallel()
	rt := fakeruntime.New()
	env := resource.Envelope{
		GroupVersionKind: resource.GroupVersionKind{APIVersion: "datascape.io/v1alpha1", Kind: "Provider"},
		Metadata:         resource.Metadata{Name: "db"},
		Spec:             map[string]any{"type": "postgres"},
	}
	// providerType "postgres", no resources config at all -> the postgres default.
	d := newDomainRuntime(rt, map[string]any{"type": "docker"}, env, env, nil, false, false, nil, "docker", "postgres", nil)
	if _, err := d.EnsureContainer(context.Background(), runtime.ContainerSpec{Name: "pg", Image: "postgres:16"}); err != nil {
		t.Fatalf("EnsureContainer: %v", err)
	}
	spec, _ := rt.Spec("pg")
	if spec.Resources == nil {
		t.Fatal("postgres provider got no default resources — M3 floor missing")
	}
	if spec.Resources.MemoryLimitBytes != 512<<20 {
		t.Errorf("postgres default memory limit = %d, want 512Mi", spec.Resources.MemoryLimitBytes)
	}

	// A type with no meaningful default stays unbounded.
	d2 := newDomainRuntime(rt, map[string]any{"type": "docker"}, env, env, nil, false, false, nil, "docker", "noop", nil)
	if _, err := d2.EnsureContainer(context.Background(), runtime.ContainerSpec{Name: "n", Image: "x"}); err != nil {
		t.Fatalf("EnsureContainer(noop): %v", err)
	}
	if spec2, _ := rt.Spec("n"); spec2.Resources != nil {
		t.Error("noop provider got a default it should not have")
	}
}
