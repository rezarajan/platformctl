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
