// Package conformance is the shared contract test suite every
// ContainerRuntime adapter must pass — both adapters/runtime/fake and
// adapters/runtime/docker. This is what keeps the fake honest and catches
// adapters that violate the Ensure* idempotency contract.
// See docs/planning/02-architecture.md §9.
package conformance

import (
	"context"
	"testing"
	"time"

	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// MutationCounter is optionally implemented by adapters that can report how
// many real state mutations occurred (the fake does; the Docker adapter may
// approximate via API call counting in its test harness).
type MutationCounter interface {
	Mutations() int
}

// Run executes the conformance suite against the given runtime. namePrefix
// isolates this run's objects (important against a real Docker daemon).
func Run(t *testing.T, rt runtime.ContainerRuntime, namePrefix string) {
	t.Helper()
	ctx := context.Background()

	labels := map[string]string{
		runtime.LabelManagedBy:  runtime.ManagedByValue,
		runtime.LabelGeneration: "conformance",
	}

	netSpec := runtime.NetworkSpec{Name: namePrefix + "-net", Labels: labels}
	volSpec := runtime.VolumeSpec{Name: namePrefix + "-vol", Labels: labels}
	ctrSpec := runtime.ContainerSpec{
		Name:     namePrefix + "-ctr",
		Image:    "alpine:3.20",
		Networks: []string{netSpec.Name},
		Labels:   labels,
	}

	t.Run("EnsureNetwork_idempotent", func(t *testing.T) {
		if err := rt.EnsureNetwork(ctx, netSpec); err != nil {
			t.Fatalf("first EnsureNetwork: %v", err)
		}
		if err := rt.EnsureNetwork(ctx, netSpec); err != nil {
			t.Fatalf("second EnsureNetwork: %v", err)
		}
	})

	t.Run("EnsureVolume_idempotent", func(t *testing.T) {
		if err := rt.EnsureVolume(ctx, volSpec); err != nil {
			t.Fatalf("first EnsureVolume: %v", err)
		}
		if err := rt.EnsureVolume(ctx, volSpec); err != nil {
			t.Fatalf("second EnsureVolume: %v", err)
		}
	})

	t.Run("EnsureContainer_idempotent", func(t *testing.T) {
		if _, err := rt.EnsureContainer(ctx, ctrSpec); err != nil {
			t.Fatalf("first EnsureContainer: %v", err)
		}
		mc, hasCounter := rt.(MutationCounter)
		before := 0
		if hasCounter {
			before = mc.Mutations()
		}
		if _, err := rt.EnsureContainer(ctx, ctrSpec); err != nil {
			t.Fatalf("second EnsureContainer: %v", err)
		}
		if hasCounter && mc.Mutations() != before {
			t.Fatalf("second EnsureContainer with identical spec mutated state (NFR-2 violation)")
		}
	})

	t.Run("Inspect_found", func(t *testing.T) {
		st, found, err := rt.Inspect(ctx, ctrSpec.Name)
		if err != nil {
			t.Fatalf("Inspect: %v", err)
		}
		if !found {
			t.Fatalf("Inspect: container %q not found after EnsureContainer", ctrSpec.Name)
		}
		if st.Name != ctrSpec.Name {
			t.Errorf("Inspect name = %q, want %q", st.Name, ctrSpec.Name)
		}
	})

	t.Run("Inspect_absent", func(t *testing.T) {
		_, found, err := rt.Inspect(ctx, namePrefix+"-does-not-exist")
		if err != nil {
			t.Fatalf("Inspect absent: %v", err)
		}
		if found {
			t.Fatalf("Inspect reported a nonexistent container as found")
		}
	})

	t.Run("WaitHealthy", func(t *testing.T) {
		if err := rt.WaitHealthy(ctx, ctrSpec.Name, 30*time.Second); err != nil {
			t.Fatalf("WaitHealthy: %v", err)
		}
	})

	t.Run("ListManaged_only_labeled", func(t *testing.T) {
		states, err := rt.ListManaged(ctx)
		if err != nil {
			t.Fatalf("ListManaged: %v", err)
		}
		foundOurs := false
		for _, s := range states {
			if s.Labels[runtime.LabelManagedBy] != runtime.ManagedByValue {
				t.Errorf("ListManaged returned unlabeled object %q", s.Name)
			}
			if s.Name == ctrSpec.Name {
				foundOurs = true
			}
		}
		if !foundOurs {
			t.Errorf("ListManaged did not include %q", ctrSpec.Name)
		}
	})

	t.Run("Remove_then_absent", func(t *testing.T) {
		if err := rt.Remove(ctx, ctrSpec.Name); err != nil {
			t.Fatalf("Remove: %v", err)
		}
		_, found, err := rt.Inspect(ctx, ctrSpec.Name)
		if err != nil {
			t.Fatalf("Inspect after Remove: %v", err)
		}
		if found {
			t.Fatalf("container still present after Remove")
		}
		if err := rt.RemoveNetwork(ctx, netSpec.Name); err != nil {
			t.Fatalf("RemoveNetwork: %v", err)
		}
		if err := rt.RemoveVolume(ctx, volSpec.Name); err != nil {
			t.Fatalf("RemoveVolume: %v", err)
		}
	})
}
