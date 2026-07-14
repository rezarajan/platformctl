//go:build integration

package docker

import (
	"context"
	"testing"

	"github.com/rezarajan/platformctl/internal/ports/runtime"
	"github.com/rezarajan/platformctl/internal/ports/runtime/conformance"
)

// TestConformance runs the same suite the fake adapter passes, against the
// real Docker daemon — the Phase 1 exit criterion.
func TestConformance(t *testing.T) {
	rt, err := New(nil)
	if err != nil {
		t.Fatalf("connect to Docker: %v", err)
	}

	prefix := "datascape-conf"
	cleanup := func() {
		ctx := context.Background()
		_ = rt.Remove(ctx, prefix+"-ctr")
		_ = rt.RemoveNetwork(ctx, prefix+"-net")
		_ = rt.RemoveVolume(ctx, prefix+"-vol")
	}
	cleanup()
	t.Cleanup(cleanup)

	conformance.Run(t, rt, prefix)
}

// TestOutOfBandKillSurfacesUnhealthy covers the Phase 1 exit criterion:
// killing a managed container out-of-band surfaces it as not healthy.
func TestOutOfBandKillSurfacesUnhealthy(t *testing.T) {
	rt, err := New(nil)
	if err != nil {
		t.Fatalf("connect to Docker: %v", err)
	}
	ctx := context.Background()
	name := "datascape-oob-kill"
	t.Cleanup(func() { _ = rt.Remove(ctx, name) })

	spec := runtime.ContainerSpec{
		Name:  name,
		Image: "alpine:3.20",
		Cmd:   []string{"sleep", "300"},
		Labels: map[string]string{
			runtime.LabelManagedBy: runtime.ManagedByValue,
		},
	}
	if _, err := rt.EnsureContainer(ctx, spec); err != nil {
		t.Fatalf("EnsureContainer: %v", err)
	}

	// Kill it behind the runtime's back.
	if err := rt.cli.ContainerKill(ctx, name, "KILL"); err != nil {
		t.Fatalf("out-of-band kill: %v", err)
	}

	st, found, err := rt.Inspect(ctx, name)
	if err != nil || !found {
		t.Fatalf("Inspect after kill: found=%v err=%v", found, err)
	}
	if st.Running || st.Healthy {
		t.Errorf("killed container reported running=%v healthy=%v; want false/false", st.Running, st.Healthy)
	}
}
