//go:build integration

package docker

import (
	"context"
	"testing"

	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/api/types/volume"

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

// TestEnsureNetworkRefusesUnmanagedExisting guards docs/planning/07 §0.7: a
// same-name network created out-of-band (no platformctl ownership label)
// must be refused, not silently reused, when a real Docker daemon is asked
// to ensure it.
func TestEnsureNetworkRefusesUnmanagedExisting(t *testing.T) {
	rt, err := New(nil)
	if err != nil {
		t.Fatalf("connect to Docker: %v", err)
	}
	ctx := context.Background()
	name := "datascape-unmanaged-net"
	// The network created below is deliberately unmanaged, so RemoveNetwork
	// would itself refuse it (the same ownership guard under test) — clean
	// up through the raw client instead.
	t.Cleanup(func() { _ = rt.cli.NetworkRemove(ctx, name) })

	if _, err := rt.cli.NetworkCreate(ctx, name, network.CreateOptions{}); err != nil {
		t.Fatalf("create unmanaged network out-of-band: %v", err)
	}

	err = rt.EnsureNetwork(ctx, runtime.NetworkSpec{Name: name})
	if err == nil {
		t.Fatal("EnsureNetwork silently reused an unmanaged same-name network; want refusal error")
	}
}

// TestEnsureVolumeRefusesUnmanagedExisting is the volume equivalent of
// TestEnsureNetworkRefusesUnmanagedExisting.
func TestEnsureVolumeRefusesUnmanagedExisting(t *testing.T) {
	rt, err := New(nil)
	if err != nil {
		t.Fatalf("connect to Docker: %v", err)
	}
	ctx := context.Background()
	name := "datascape-unmanaged-vol"
	// Deliberately unmanaged — RemoveVolume would refuse it too (the same
	// guard under test), so clean up through the raw client instead.
	t.Cleanup(func() { _ = rt.cli.VolumeRemove(ctx, name, false) })

	if _, err := rt.cli.VolumeCreate(ctx, volume.CreateOptions{Name: name}); err != nil {
		t.Fatalf("create unmanaged volume out-of-band: %v", err)
	}

	err = rt.EnsureVolume(ctx, runtime.VolumeSpec{Name: name})
	if err == nil {
		t.Fatal("EnsureVolume silently reused an unmanaged same-name volume; want refusal error")
	}
}

// TestPublishedPortBindsToLoopbackByDefault guards docs/planning/07 §0.7: a
// PortBinding with no HostIP must be published on 127.0.0.1, not on every
// interface, against a real Docker daemon — not just in the nat.PortBinding
// construction unit test.
func TestPublishedPortBindsToLoopbackByDefault(t *testing.T) {
	rt, err := New(nil)
	if err != nil {
		t.Fatalf("connect to Docker: %v", err)
	}
	ctx := context.Background()
	name := "datascape-bind-default"
	t.Cleanup(func() { _ = rt.Remove(ctx, name) })

	spec := runtime.ContainerSpec{
		Name:   name,
		Image:  "alpine:3.20",
		Cmd:    []string{"sleep", "300"},
		Ports:  []runtime.PortBinding{{HostPort: 29999, ContainerPort: 80}},
		Labels: map[string]string{runtime.LabelManagedBy: runtime.ManagedByValue},
	}
	if _, err := rt.EnsureContainer(ctx, spec); err != nil {
		t.Fatalf("EnsureContainer: %v", err)
	}

	inspect, err := rt.cli.ContainerInspect(ctx, name)
	if err != nil {
		t.Fatalf("ContainerInspect: %v", err)
	}
	bindings, ok := inspect.NetworkSettings.Ports["80/tcp"]
	if !ok || len(bindings) == 0 {
		t.Fatalf("no published-port binding recorded for 80/tcp: %+v", inspect.NetworkSettings.Ports)
	}
	for _, b := range bindings {
		if b.HostIP != "127.0.0.1" {
			t.Errorf("published port bound to HostIP %q, want 127.0.0.1 (default-safe)", b.HostIP)
		}
	}
}
