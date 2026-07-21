package providerkit

import (
	"context"
	"time"

	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// InstanceVolume is the single data volume a single-container instance
// mounts. A provider whose instance has no data volume (nessie, the
// Connect-worker shape of debezium/s3sink) passes a nil *InstanceVolume to
// InstanceSpec instead.
type InstanceVolume struct {
	// Name is the volume's own name (conventionally "<instance>-data").
	Name string
	// MountPath is where it is mounted inside the container.
	MountPath string
	// SizeBytes/StorageClass are docs/planning/08 B3's runtime-agnostic
	// sizing knobs — 0/"" keeps the runtime adapter's own default.
	SizeBytes    int64
	StorageClass string
}

// InstanceSpec is the input to EnsureInstance.
type InstanceSpec struct {
	// Namespace is the owning resource's metadata.namespace, passed straight
	// through to runtime.ManagedLabels.
	Namespace string
	// Name is the instance's runtime object name (both the container name
	// and the label generation value).
	Name    string
	Network string
	// IsolationPolicy resolves spec.runtime.networkPolicy (docs/planning/08
	// B7) for the providers that support it; "" keeps the runtime adapter's
	// own default and is what every provider not yet wired to B7 already
	// passes implicitly (a zero-value NetworkSpec.IsolationPolicy).
	IsolationPolicy string
	// Volume is the instance's data volume, or nil for a stateless
	// instance — EnsureInstance skips EnsureVolume and leaves
	// Container.Volumes untouched (nil) in that case.
	Volume *InstanceVolume
	// Container is the provider-specific container shape: everything except
	// Name, Networks, Volumes, and Labels, which EnsureInstance sets from
	// the fields above so a provider's own ContainerSpec literal states only
	// what's actually provider-specific (image, cmd, env, ports,
	// healthcheck, ...). A caller must leave those four fields zero.
	Container runtime.ContainerSpec
	// WaitTimeout bounds the post-EnsureContainer WaitHealthy call.
	WaitTimeout time.Duration
}

// EnsureInstance runs the single-container reconcile-instance skeleton
// (labels → EnsureNetwork → EnsureVolume → EnsureContainer → WaitHealthy)
// shared by every provider that reconciles exactly one container with at
// most one data volume (docs/planning/08 G1). A provider with a genuinely
// different shape (a multi-container instance, a per-sub-resource label
// scheme) does not use this and keeps its own skeleton inline — see G1's
// commit body for which providers that applies to and why.
func EnsureInstance(ctx context.Context, rt runtime.ContainerRuntime, spec InstanceSpec) (runtime.ContainerState, error) {
	labels := runtime.ManagedLabels(spec.Namespace, "Provider", spec.Name, spec.Name)
	if err := rt.EnsureNetwork(ctx, runtime.NetworkSpec{Name: spec.Network, Labels: labels, IsolationPolicy: spec.IsolationPolicy}); err != nil {
		return runtime.ContainerState{}, err
	}
	var volumes []runtime.VolumeMount
	if spec.Volume != nil {
		if err := rt.EnsureVolume(ctx, runtime.VolumeSpec{
			Name:         spec.Volume.Name,
			Labels:       labels,
			Networks:     []string{spec.Network},
			SizeBytes:    spec.Volume.SizeBytes,
			StorageClass: spec.Volume.StorageClass,
		}); err != nil {
			return runtime.ContainerState{}, err
		}
		volumes = []runtime.VolumeMount{{VolumeName: spec.Volume.Name, MountPath: spec.Volume.MountPath}}
	}
	spec.Container.Name = spec.Name
	spec.Container.Networks = []string{spec.Network}
	spec.Container.Volumes = volumes
	spec.Container.Labels = labels
	ctrState, err := rt.EnsureContainer(ctx, spec.Container)
	if err != nil {
		return ctrState, err
	}
	if err := rt.WaitHealthy(ctx, spec.Name, spec.WaitTimeout); err != nil {
		return ctrState, err
	}
	return ctrState, nil
}
