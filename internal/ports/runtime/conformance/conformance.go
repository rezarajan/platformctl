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
	volSpec := runtime.VolumeSpec{Name: namePrefix + "-vol", Labels: labels, Networks: []string{netSpec.Name}}
	ctrSpec := runtime.ContainerSpec{
		Name:     namePrefix + "-ctr",
		Image:    "alpine:3.20",
		Cmd:      []string{"sleep", "300"}, // must outlive the suite against a real daemon
		Networks: []string{netSpec.Name},
		Volumes:  []runtime.VolumeMount{{VolumeName: volSpec.Name, MountPath: "/data"}},
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

	t.Run("EnsureContainer_productionFields_idempotent", func(t *testing.T) {
		name := namePrefix + "-prod-ctr"
		t.Cleanup(func() { _ = rt.Remove(ctx, name) })
		prodSpec := runtime.ContainerSpec{
			Name:     name,
			Image:    "alpine:3.20",
			Cmd:      []string{"sleep", "300"},
			Networks: []string{netSpec.Name},
			Labels:   labels,
			RestartPolicy: &runtime.RestartPolicy{
				Mode:       "on-failure",
				MaxRetries: 3,
			},
			Resources: &runtime.Resources{
				CPULimit:               0.5,
				MemoryLimitBytes:       128 * 1024 * 1024,
				MemoryReservationBytes: 64 * 1024 * 1024,
			},
			Security: &runtime.SecurityContext{
				ReadOnlyRootFS: false, // alpine needs a writable rootfs to sleep
			},
			LogConfig: &runtime.LogConfig{Driver: "json-file"},
		}
		if _, err := rt.EnsureContainer(ctx, prodSpec); err != nil {
			t.Fatalf("first EnsureContainer with production fields: %v", err)
		}
		mc, hasCounter := rt.(MutationCounter)
		before := 0
		if hasCounter {
			before = mc.Mutations()
		}
		if _, err := rt.EnsureContainer(ctx, prodSpec); err != nil {
			t.Fatalf("second EnsureContainer with production fields: %v", err)
		}
		if hasCounter && mc.Mutations() != before {
			t.Fatalf("second EnsureContainer with identical production-field spec mutated state (NFR-2 violation)")
		}

		// Changing a production field (not name/image/labels/env/networks)
		// must still be detected as drift, not silently ignored.
		changed := prodSpec
		changed.RestartPolicy = &runtime.RestartPolicy{Mode: "always"}
		if _, err := rt.EnsureContainer(ctx, changed); err != nil {
			t.Fatalf("EnsureContainer with changed restart policy: %v", err)
		}
		if hasCounter && mc.Mutations() == before {
			t.Fatalf("changing RestartPolicy alone was not detected as a spec change")
		}
	})

	t.Run("Logs_returns_without_error", func(t *testing.T) {
		if _, err := rt.Logs(ctx, ctrSpec.Name, 5); err != nil {
			t.Fatalf("Logs: %v", err)
		}
	})

	t.Run("Inspect_reports_observed_ports", func(t *testing.T) {
		name := namePrefix + "-ports-ctr"
		t.Cleanup(func() { _ = rt.Remove(ctx, name) })
		spec := runtime.ContainerSpec{
			Name:     name,
			Image:    "alpine:3.20",
			Cmd:      []string{"sleep", "300"},
			Networks: []string{netSpec.Name},
			Ports:    []runtime.PortBinding{{HostPort: 28999, ContainerPort: 80}},
			Labels:   labels,
		}
		if _, err := rt.EnsureContainer(ctx, spec); err != nil {
			t.Fatalf("EnsureContainer: %v", err)
		}
		st, found, err := rt.Inspect(ctx, name)
		if err != nil || !found {
			t.Fatalf("Inspect: found=%v err=%v", found, err)
		}
		var got *runtime.PortBinding
		for i := range st.Ports {
			if st.Ports[i].ContainerPort == 80 {
				got = &st.Ports[i]
			}
		}
		if got == nil {
			t.Fatalf("Inspect did not report the published container port 80; ports = %+v", st.Ports)
		}
		// A runtime with host publishing (Docker, fake) must report the
		// concrete bind address, never an empty HostIP for a bound port.
		// A runtime without host publishing (Kubernetes) reports HostPort 0
		// and may leave HostIP empty.
		if got.HostPort != 0 && got.HostIP == "" {
			t.Errorf("published port reported with empty HostIP: %+v", *got)
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
