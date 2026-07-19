package fake

import (
	"context"
	"testing"

	"github.com/rezarajan/platformctl/internal/ports/runtime"
	"github.com/rezarajan/platformctl/internal/ports/runtime/conformance"
)

// Mutations implements conformance.MutationCounter.
func (r *Runtime) Mutations() int { return r.MutationCount }

func TestConformance(t *testing.T) {
	conformance.Run(t, New(), "fake-conf")
}

// TestEnsureContainerRejectsUndeclaredPortAudience proves the F2 strict
// interpreter: an omitted or misspelled Audience fails EnsureContainer in a
// unit test, rather than reaching a permissive runtime (docs/planning/08 F2,
// docs/planning/09 K10).
func TestEnsureContainerRejectsUndeclaredPortAudience(t *testing.T) {
	rt := New()
	_, err := rt.EnsureContainer(context.Background(), runtime.ContainerSpec{
		Name:  "audience-missing",
		Image: "alpine:3.20",
		Ports: []runtime.PortBinding{{ContainerPort: 80}},
	})
	if err == nil {
		t.Fatal("EnsureContainer with an undeclared port Audience: want error, got nil")
	}
}

// TestEnsureReachableRefusesInternalAudience proves the fake's strictness at
// the EnsureReachable seam: a host-audience port resolves; the same
// container's internal-audience port refuses, distinctly and by name — this
// is deliberately stricter than Kubernetes' default (port-forward) access
// mode, which can reach any pod port regardless of declared audience.
func TestEnsureReachableRefusesInternalAudience(t *testing.T) {
	rt := New()
	ctx := context.Background()
	spec := runtime.ContainerSpec{
		Name:  "audience-mixed",
		Image: "alpine:3.20",
		Ports: []runtime.PortBinding{
			{HostPort: 15432, ContainerPort: 5432, Audience: runtime.AudienceHost},
			{ContainerPort: 5433, Audience: runtime.AudienceInternal},
		},
	}
	if _, err := rt.EnsureContainer(ctx, spec); err != nil {
		t.Fatalf("EnsureContainer: %v", err)
	}

	addr, closeFn, err := rt.EnsureReachable(ctx, spec.Name, 5432)
	if err != nil {
		t.Fatalf("EnsureReachable(host-audience port): %v", err)
	}
	defer closeFn()
	if addr == "" {
		t.Fatal("EnsureReachable(host-audience port): empty address")
	}

	if _, _, err := rt.EnsureReachable(ctx, spec.Name, 5433); err == nil {
		t.Fatal("EnsureReachable(internal-audience port): want error, got nil")
	}
}
