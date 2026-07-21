package conformance

import (
	"testing"
	"time"

	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// runContainerLifecycle registers EnsureContainer_idempotent, Inspect_found,
// Inspect_absent, WaitHealthy and ListManaged_only_labeled. This is where
// fx.ctrSpec's container is created (see the sequencing comment in Run,
// conformance.go) — every later subtest across the whole suite that
// references fx.ctrSpec.Name assumes it already exists.
func runContainerLifecycle(t *testing.T, rt runtime.ContainerRuntime, fx fixtures) {
	ctx := fx.ctx
	ctrSpec := fx.ctrSpec

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
		_, found, err := rt.Inspect(ctx, fx.namePrefix+"-does-not-exist")
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
		// The port contract: a non-replicated container reports ReadyReplicas
		// 1 when Healthy, 0 otherwise — never left unset by an adapter.
		st, found, err := rt.Inspect(ctx, ctrSpec.Name)
		if err != nil || !found {
			t.Fatalf("Inspect after WaitHealthy: found=%v err=%v", found, err)
		}
		if st.Healthy && st.ReadyReplicas != 1 {
			t.Errorf("healthy single container reports ReadyReplicas = %d, want 1", st.ReadyReplicas)
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
}

// runContainerPorts registers Inspect_reports_observed_ports.
func runContainerPorts(t *testing.T, rt runtime.ContainerRuntime, fx fixtures) {
	t.Run("Inspect_reports_observed_ports", func(t *testing.T) {
		ctx := fx.ctx
		name := fx.namePrefix + "-ports-ctr"
		t.Cleanup(func() { _ = rt.Remove(ctx, name) })
		spec := runtime.ContainerSpec{
			Name:     name,
			Image:    "alpine:3.20",
			Cmd:      []string{"sleep", "300"},
			Networks: []string{fx.netSpec.Name},
			Ports:    []runtime.PortBinding{{HostPort: 28999, ContainerPort: 80, Audience: runtime.AudienceHost}},
			Labels:   fx.labels,
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
}
