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
	d := newDomainRuntime(rt, cfg, env, env, nil, false, false, nil, "fake", nil)
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
	d2 := newDomainRuntime(rt, map[string]any{"type": "fake"}, env, env, nil, false, false, nil, "fake", nil)
	if _, err := d2.EnsureContainer(context.Background(), runtime.ContainerSpec{Name: "w3", Image: "img"}); err != nil {
		t.Fatalf("EnsureContainer(w3): %v", err)
	}
	if spec3, _ := rt.Spec("w3"); spec3.Resources != nil {
		t.Fatal("Resources injected despite no spec.runtime.resources block")
	}
}
