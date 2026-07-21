// Package conformance is the shared contract test suite every
// ContainerRuntime adapter must pass — both adapters/runtime/fake and
// adapters/runtime/docker. This is what keeps the fake honest and catches
// adapters that violate the Ensure* idempotency contract.
// See docs/planning/02-architecture.md §9.
//
// The suite is split into per-area files (network.go, volume.go,
// container.go, container_fields.go, entrypoint.go, reachability.go,
// replicas.go), each exposing one or more unexported run*(t, rt, fx)
// helpers. Run below calls them in exactly the order their subtests
// appeared in the pre-split, single-file version of this suite — subtest
// names and count (and, where a later subtest depends on state an earlier
// one left behind, order) must stay byte-identical, since other tests and
// CI logs key on them. Several areas are not contiguous in the original
// ordering (e.g. network's ListManagedNetworks_and_Volumes_only_labeled
// falls in the middle of the container subtests), so a single file may
// contribute more than one call site below; each is labeled with the
// subtest(s) it registers.
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

// fixtures holds the spec objects and context every subtest in this suite
// builds on, as explicit parameters rather than closures over Run's locals —
// this is what makes the suite's implicit cross-subtest ordering
// dependencies visible (see Run's sequencing comments and the per-subtest
// comments called out there).
type fixtures struct {
	ctx        context.Context
	namePrefix string
	labels     map[string]string
	netSpec    runtime.NetworkSpec
	volSpec    runtime.VolumeSpec
	ctrSpec    runtime.ContainerSpec
}

// newFixtures builds the suite's shared specs. namePrefix isolates one Run's
// objects (important against a real Docker daemon).
func newFixtures(namePrefix string) fixtures {
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

	return fixtures{
		ctx:        context.Background(),
		namePrefix: namePrefix,
		labels:     labels,
		netSpec:    netSpec,
		volSpec:    volSpec,
		ctrSpec:    ctrSpec,
	}
}

// waitReadyReplicas polls Inspect until the aggregate ReadyReplicas reaches
// want, in the same deadline-poll style the adapters' own WaitHealthy loops
// use. WaitHealthy deliberately returns at "at least one replica ready"
// (docs/adr/004's provider-decides rule), so asserting ReadyReplicas == N
// immediately after it races the remaining replicas on a real cluster — every
// full-set assertion must converge through here instead.
func waitReadyReplicas(ctx context.Context, t *testing.T, rt runtime.ContainerRuntime, name string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		st, found, err := rt.Inspect(ctx, name)
		if err != nil {
			t.Fatalf("Inspect(%q) while waiting for %d ready replicas: %v", name, want, err)
		}
		if found && st.ReadyReplicas == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("ReadyReplicas for %q = %d (found=%v), want %d — did not converge within %s", name, st.ReadyReplicas, found, want, timeout)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// Run executes the conformance suite against the given runtime. namePrefix
// isolates this run's objects (important against a real Docker daemon).
func Run(t *testing.T, rt runtime.ContainerRuntime, namePrefix string) {
	t.Helper()
	fx := newFixtures(namePrefix)

	// EnsureNetwork_idempotent.
	runNetworkEnsure(t, rt, fx)
	// EnsureVolume_idempotent.
	runVolumeEnsure(t, rt, fx)
	// EnsureContainer_idempotent, Inspect_found, Inspect_absent, WaitHealthy,
	// ListManaged_only_labeled. This is where fx.ctrSpec's container is
	// created; it stays up for the rest of the suite — container_fields.go's
	// Logs_returns_without_error, and network.go's
	// RemoveNetwork_refuses_while_container_attached / Remove_then_absent
	// (the suite's final teardown) all depend on it still existing.
	runContainerLifecycle(t, rt, fx)
	// ListManagedNetworks_and_Volumes_only_labeled.
	runNetworkAndVolumeListing(t, rt, fx)
	// EnsureContainer_imagePullAuth_accepted, EnsureContainer_
	// productionFields_idempotent, Logs_returns_without_error,
	// EnsureContainer_aliases_idempotent, FileMount_readable_by_process_and_
	// ReadFile.
	runContainerFields(t, rt, fx)
	// Volume_persists_across_container_update.
	runVolumePersistence(t, rt, fx)
	// Inspect_reports_observed_ports.
	runContainerPorts(t, rt, fx)
	// PortBinding_audience_internal_never_host_bound.
	runReachabilityAudience(t, rt, fx)
	// EnsureReachable_dialable_immediately_after_WaitHealthy.
	runReachabilityDial(t, rt, fx)
	// DelayedListenReadiness_HealthyBeforeListening.
	runReachabilityDelayedListen(t, rt, fx)
	// EntrypointFaithfulness_CmdAppendsNotReplaces, EntrypointFaithfulness_
	// EntrypointReplaces.
	runEntrypointFaithfulness(t, rt, fx)
	// ReplicaSet_ScaleUp_Idempotent, ReplicaSet_OrdinalHostnameResolution.
	runReplicasScaleAndOrdinal(t, rt, fx)
	// ReplicaSet_PerOrdinalVolumePersistence.
	runVolumeReplicaPersistence(t, rt, fx)
	// ReplicaSet_ShapeTransition_Refused.
	runReplicasShapeTransition(t, rt, fx)
	// ProbeReachable_InNetwork_reachable_and_undeclared_errors.
	runReachabilityProbe(t, rt, fx)
	// RemoveNetwork_refuses_while_container_attached, Remove_then_absent —
	// must run last: RemoveNetwork_refuses_while_container_attached depends
	// on fx.ctrSpec's container (created by runContainerLifecycle above)
	// still being attached to fx.netSpec, and Remove_then_absent then tears
	// down the container/network/volume trio every earlier subtest in this
	// suite shares.
	runNetworkTeardown(t, rt, fx)
}
