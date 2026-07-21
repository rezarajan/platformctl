// Package docker implements ContainerRuntime against the real Docker Engine
// API. Every created object carries the Datascape ownership labels;
// ListManaged/Remove never touch unlabeled resources.
// See docs/planning/02-architecture.md §4.1 and §10.
package docker

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/api/types/volume"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"

	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

type Runtime struct {
	cli *client.Client
}

// New connects to the Docker daemon from the environment (DOCKER_HOST etc.).
func New(_ map[string]any) (*Runtime, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("connect to Docker daemon: %w", err)
	}
	return &Runtime{cli: cli}, nil
}

func managedFilter() filters.Args {
	return filters.NewArgs(filters.Arg("label", runtime.LabelManagedBy+"="+runtime.ManagedByValue))
}

func withOwnership(labels map[string]string) map[string]string {
	out := map[string]string{runtime.LabelManagedBy: runtime.ManagedByValue}
	for k, v := range labels {
		out[k] = v
	}
	return out
}

func (r *Runtime) EnsureNetwork(ctx context.Context, spec runtime.NetworkSpec) error {
	nets, err := r.cli.NetworkList(ctx, network.ListOptions{
		Filters: filters.NewArgs(filters.Arg("name", spec.Name)),
	})
	if err != nil {
		return fmt.Errorf("list networks: %w", err)
	}
	for _, n := range nets {
		if n.Name == spec.Name {
			if n.Labels[runtime.LabelManagedBy] != runtime.ManagedByValue {
				return fmt.Errorf("network %q exists but is not managed by platformctl; refusing to reuse it", spec.Name)
			}
			return nil // exists — Ensure* is a no-op
		}
	}
	_, err = r.cli.NetworkCreate(ctx, spec.Name, network.CreateOptions{
		Labels: withOwnership(spec.Labels),
	})
	if err != nil && !errdefs.IsConflict(err) {
		return fmt.Errorf("create network %q: %w", spec.Name, err)
	}
	return nil
}

func (r *Runtime) EnsureVolume(ctx context.Context, spec runtime.VolumeSpec) error {
	if vol, err := r.cli.VolumeInspect(ctx, spec.Name); err == nil {
		if vol.Labels[runtime.LabelManagedBy] != runtime.ManagedByValue {
			return fmt.Errorf("volume %q exists but is not managed by platformctl; refusing to reuse it", spec.Name)
		}
		return nil // exists
	} else if !errdefs.IsNotFound(err) {
		return fmt.Errorf("inspect volume %q: %w", spec.Name, err)
	}
	_, err := r.cli.VolumeCreate(ctx, volume.CreateOptions{
		Name:   spec.Name,
		Labels: withOwnership(spec.Labels),
	})
	if err != nil {
		return fmt.Errorf("create volume %q: %w", spec.Name, err)
	}
	return nil
}

// specGenLabel carries a hash of the container spec so EnsureContainer can
// detect "already matches" without diffing every field against inspect output.
const specGenLabel = "io.datascape.spec-hash"

// EnsureContainer dispatches to the single-container path (spec.Replicas <=
// 1, today's exact pre-existing behavior, byte-for-byte) or the replica-set
// path (docs/design/004-replicas-and-identity.md): Docker has no native
// replica-set object, so N > 1 always fans out to N separately-named
// containers ("<Name>-0".."<Name>-(N-1)") — forced by Docker's own
// unique-container-name requirement, independent of StableIdentity.
func (r *Runtime) EnsureContainer(ctx context.Context, spec runtime.ContainerSpec) (runtime.ContainerState, error) {
	n := spec.ReplicaCount()
	if n <= 1 {
		return r.ensureOneContainer(ctx, spec)
	}
	return r.ensureReplicaSet(ctx, spec, n)
}

// ensureReplicaSet fans spec out to n ordinal containers, prunes any stale
// ordinal left over from a previous, larger generation (scale-down), and
// returns the aggregate ContainerState (docs/design/004): Healthy/Running
// are true when at least one member is; ReadyReplicas is the count of
// healthy members.
func (r *Runtime) ensureReplicaSet(ctx context.Context, spec runtime.ContainerSpec, n int) (runtime.ContainerState, error) {
	if err := r.pruneStaleOrdinals(ctx, spec.Name, n); err != nil {
		return runtime.ContainerState{}, err
	}
	states := make([]runtime.ContainerState, 0, n)
	for i := 0; i < n; i++ {
		ordSpec, err := r.ordinalContainerSpec(ctx, spec, i)
		if err != nil {
			return runtime.ContainerState{}, err
		}
		st, err := r.ensureOneContainer(ctx, ordSpec)
		if err != nil {
			return runtime.ContainerState{}, fmt.Errorf("replica %d of %q: %w", i, spec.Name, err)
		}
		states = append(states, st)
	}
	return aggregateContainerStates(spec.Name, states), nil
}

// ordinalContainerSpec derives ordinal i's own ContainerSpec: an
// ordinal-suffixed Name, the base name added as a shared network alias
// (Docker's embedded DNS round-robins across containers sharing one alias —
// the closest analog to a Kubernetes Service's virtual-IP round robin it
// has), replica-membership labels, and — when StableIdentity — an
// ordinal-suffixed, adapter-owned volume per declared VolumeMount (the
// runtime owns this volume's entire lifecycle; the caller must not call
// EnsureVolume for it itself — docs/design/004).
func (r *Runtime) ordinalContainerSpec(ctx context.Context, spec runtime.ContainerSpec, i int) (runtime.ContainerSpec, error) {
	out := spec
	out.Name = runtime.OrdinalName(spec.Name, i)
	out.Aliases = append(append([]string{}, spec.Aliases...), spec.Name)

	labels := make(map[string]string, len(spec.Labels)+2)
	for k, v := range spec.Labels {
		labels[k] = v
	}
	labels[runtime.LabelReplicaBase] = spec.Name
	labels[runtime.LabelReplicaOrdinal] = strconv.Itoa(i)
	out.Labels = labels

	if spec.StableIdentity && len(spec.Volumes) > 0 {
		vols := make([]runtime.VolumeMount, len(spec.Volumes))
		for j, vm := range spec.Volumes {
			volName := runtime.OrdinalName(vm.VolumeName, i)
			if err := r.EnsureVolume(ctx, runtime.VolumeSpec{Name: volName, Labels: labels}); err != nil {
				return runtime.ContainerSpec{}, fmt.Errorf("ensure ordinal volume %q: %w", volName, err)
			}
			vols[j] = runtime.VolumeMount{VolumeName: volName, MountPath: vm.MountPath}
		}
		out.Volumes = vols
	}
	return out, nil
}

// pruneStaleOrdinals removes any replica-set member of base whose ordinal is
// >= n — the scale-down complement of ensureReplicaSet's scale-up. Per-ordinal
// volumes are deliberately left in place (docs/design/004's conservative
// removal default: Remove never touches volumes, matching the single-
// container path, where RemoveVolume has always been a separate, explicit
// call).
func (r *Runtime) pruneStaleOrdinals(ctx context.Context, base string, n int) error {
	members, err := r.listReplicaMembers(ctx, base)
	if err != nil {
		return err
	}
	for _, c := range members {
		ord, err := strconv.Atoi(c.Labels[runtime.LabelReplicaOrdinal])
		if err != nil || ord < n {
			continue
		}
		if err := r.Remove(ctx, containerSummaryName(c)); err != nil {
			return fmt.Errorf("remove stale replica %q: %w", containerSummaryName(c), err)
		}
	}
	return nil
}

// listReplicaMembers returns every container labeled as a member of the
// replica set named base, sorted by ordinal ascending.
func (r *Runtime) listReplicaMembers(ctx context.Context, base string) ([]container.Summary, error) {
	f := filters.NewArgs(
		filters.Arg("label", runtime.LabelReplicaBase+"="+base),
		filters.Arg("label", runtime.LabelManagedBy+"="+runtime.ManagedByValue),
	)
	members, err := r.cli.ContainerList(ctx, container.ListOptions{All: true, Filters: f})
	if err != nil {
		return nil, fmt.Errorf("list replica members of %q: %w", base, err)
	}
	sort.Slice(members, func(i, j int) bool {
		oi, _ := strconv.Atoi(members[i].Labels[runtime.LabelReplicaOrdinal])
		oj, _ := strconv.Atoi(members[j].Labels[runtime.LabelReplicaOrdinal])
		return oi < oj
	})
	return members, nil
}

func containerSummaryName(c container.Summary) string {
	if len(c.Names) == 0 {
		return ""
	}
	return strings.TrimPrefix(c.Names[0], "/")
}

// aggregateContainerStates builds the collective ContainerState for a
// replica set from its members' individually-inspected states: Running/
// Healthy are true when at least one member is (docs/design/004's "provider
// decides quorum meaning" rule — the port never fails the set merely because
// one of N is down); ReadyReplicas is the count of healthy members; the
// representative Image/Labels/Env/Ports come from the lowest-ordinal member.
func aggregateContainerStates(name string, states []runtime.ContainerState) runtime.ContainerState {
	st := runtime.ContainerState{Name: name}
	for _, s := range states {
		if s.Running {
			st.Running = true
		}
		if s.Healthy {
			st.Healthy = true
			st.ReadyReplicas++
		}
	}
	if len(states) > 0 {
		st.ID = states[0].ID
		st.Image = states[0].Image
		st.Labels = states[0].Labels
		st.Env = states[0].Env
		st.Ports = states[0].Ports
	}
	return st
}

// ensureOneContainer is the single-container EnsureContainer path — the
// pre-existing implementation, unchanged, so that a Replicas <= 1 spec
// behaves exactly as it always has.
func (r *Runtime) ensureOneContainer(ctx context.Context, spec runtime.ContainerSpec) (runtime.ContainerState, error) {
	desiredHash := specHash(spec)

	existing, err := r.cli.ContainerInspect(ctx, spec.Name)
	if err == nil {
		if existing.Config != nil && existing.Config.Labels[specGenLabel] == desiredHash && networksAttached(existing, spec.Networks) {
			if existing.State != nil && existing.State.Running {
				return stateFromInspect(existing), nil // matches and running — no-op
			}
			if err := r.cli.ContainerStart(ctx, existing.ID, container.StartOptions{}); err != nil {
				return runtime.ContainerState{}, fmt.Errorf("start container %q: %w", spec.Name, err)
			}
			return r.inspectState(ctx, spec.Name)
		}
		// Spec changed (or network attachment drifted): replace. Refuse to
		// touch unmanaged containers.
		if existing.Config == nil || existing.Config.Labels[runtime.LabelManagedBy] != runtime.ManagedByValue {
			return runtime.ContainerState{}, fmt.Errorf("container %q exists but is not managed by platformctl; refusing to replace it", spec.Name)
		}
		if err := r.cli.ContainerRemove(ctx, existing.ID, container.RemoveOptions{Force: true}); err != nil {
			return runtime.ContainerState{}, fmt.Errorf("remove outdated container %q: %w", spec.Name, err)
		}
	} else if !errdefs.IsNotFound(err) {
		return runtime.ContainerState{}, fmt.Errorf("inspect container %q: %w", spec.Name, err)
	}

	if err := r.ensureImage(ctx, spec.Image, spec.PullPolicy, spec.ImagePullAuth); err != nil {
		return runtime.ContainerState{}, err
	}

	labels := withOwnership(spec.Labels)
	labels[specGenLabel] = desiredHash

	env := make([]string, 0, len(spec.Env))
	for k, v := range spec.Env {
		env = append(env, k+"="+v)
	}

	exposed, bindings, err := portMaps(spec.Ports)
	if err != nil {
		return runtime.ContainerState{}, err
	}

	cfg := &container.Config{
		Image:        spec.Image,
		Cmd:          spec.Cmd,
		Env:          env,
		Labels:       labels,
		ExposedPorts: exposed,
	}
	// A StableIdentity ordinal's in-container hostname must be its own
	// ordinal name, so `$(hostname)` (and anything an entrypoint derives
	// from it — a broker id, a seed-list peer) matches the stable identity
	// the port promises. Docker otherwise defaults Hostname to the
	// container's random short ID, which diverges from Kubernetes, where a
	// StatefulSet pod's hostname already IS "<name>-<ordinal>" via the
	// headless Service (docs/design/004-replicas-and-identity.md). spec.Name
	// is already the ordinal name here (ordinalContainerSpec set it).
	// Scoped to StableIdentity only: a non-StableIdentity scaled set's
	// replicas are interchangeable and have non-deterministic hostnames on
	// Kubernetes too, so leaving Docker's default there preserves parity.
	if spec.StableIdentity {
		cfg.Hostname = spec.Name
	}
	if spec.HealthCheck != nil {
		cfg.Healthcheck = &container.HealthConfig{
			Test:     spec.HealthCheck.Test,
			Interval: spec.HealthCheck.Interval,
			Timeout:  spec.HealthCheck.Timeout,
			Retries:  spec.HealthCheck.Retries,
		}
	}
	if spec.Security != nil {
		cfg.User = spec.Security.User
	}

	hostCfg := &container.HostConfig{PortBindings: bindings}
	for _, m := range spec.Volumes {
		hostCfg.Binds = append(hostCfg.Binds, m.VolumeName+":"+m.MountPath)
	}
	if spec.RestartPolicy != nil {
		hostCfg.RestartPolicy = container.RestartPolicy{
			Name:              container.RestartPolicyMode(spec.RestartPolicy.Mode),
			MaximumRetryCount: spec.RestartPolicy.MaxRetries,
		}
	}
	if spec.Resources != nil {
		hostCfg.Resources = container.Resources{
			NanoCPUs:          int64(spec.Resources.CPULimit * 1e9),
			Memory:            spec.Resources.MemoryLimitBytes,
			MemoryReservation: spec.Resources.MemoryReservationBytes,
			// Docker has no absolute CPU reservation; CPUShares is a
			// relative scheduling weight. 1024 shares per core is the
			// conventional conversion (also used historically by
			// Kubernetes' own dockershim) — best-effort, not a guarantee.
			CPUShares: int64(spec.Resources.CPUReservation * 1024),
		}
	}
	if spec.Security != nil {
		hostCfg.ReadonlyRootfs = spec.Security.ReadOnlyRootFS
		hostCfg.CapAdd = spec.Security.CapAdd
		hostCfg.CapDrop = spec.Security.CapDrop
		hostCfg.SecurityOpt = spec.Security.SecurityOpt
	}
	if spec.LogConfig != nil {
		hostCfg.LogConfig = container.LogConfig{
			Type:   spec.LogConfig.Driver,
			Config: spec.LogConfig.Options,
		}
	}

	var netCfg *network.NetworkingConfig
	if len(spec.Networks) > 0 {
		endpoints := make(map[string]*network.EndpointSettings, len(spec.Networks))
		for _, n := range spec.Networks {
			endpoints[n] = &network.EndpointSettings{Aliases: spec.Aliases}
		}
		netCfg = &network.NetworkingConfig{EndpointsConfig: endpoints}
	}

	created, err := r.cli.ContainerCreate(ctx, cfg, hostCfg, netCfg, nil, spec.Name)
	if err != nil {
		return runtime.ContainerState{}, fmt.Errorf("create container %q: %w", spec.Name, err)
	}
	if err := r.copyFilesIn(ctx, created.ID, spec.Files); err != nil {
		return runtime.ContainerState{}, fmt.Errorf("container %q: %w", spec.Name, err)
	}
	if err := r.cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		return runtime.ContainerState{}, fmt.Errorf("start container %q: %w", spec.Name, err)
	}
	return r.inspectState(ctx, spec.Name)
}

// copyFilesIn places FileMount contents into a created-but-not-started
// container via a tar stream — the file exists before PID 1 runs, so
// entrypoint scripts (e.g. postgres's initdb reading *_FILE) see it.
func (r *Runtime) copyFilesIn(ctx context.Context, containerID string, files []runtime.FileMount) error {
	if len(files) == 0 {
		return nil
	}
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, f := range files {
		if !strings.HasPrefix(f.Path, "/") {
			return fmt.Errorf("file mount path %q must be absolute", f.Path)
		}
		mode := f.Mode
		if mode == 0 {
			mode = 0o444
		}
		hdr := &tar.Header{
			Name: strings.TrimPrefix(f.Path, "/"),
			Mode: int64(mode),
			Size: int64(len(f.Content)),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return fmt.Errorf("tar file mount %q: %w", f.Path, err)
		}
		if _, err := tw.Write(f.Content); err != nil {
			return fmt.Errorf("tar file mount %q: %w", f.Path, err)
		}
	}
	if err := tw.Close(); err != nil {
		return fmt.Errorf("tar file mounts: %w", err)
	}
	if err := r.cli.CopyToContainer(ctx, containerID, "/", &buf, container.CopyToContainerOptions{}); err != nil {
		return fmt.Errorf("copy file mounts in: %w", err)
	}
	return nil
}

// ReadFile retrieves a file previously placed by ContainerSpec.Files. Against
// the aggregate name of a replica set (no literal container by that name
// exists), resolves to ordinal 0 as a best-effort default (docs/design/004,
// "Known limitations") — a caller wanting a *specific* replica's file should
// always address it by ordinal name.
func (r *Runtime) ReadFile(ctx context.Context, name, path string) ([]byte, error) {
	rc, _, err := r.cli.CopyFromContainer(ctx, name, path)
	if err != nil && errdefs.IsNotFound(err) {
		if rc2, _, err2 := r.cli.CopyFromContainer(ctx, runtime.OrdinalName(name, 0), path); err2 == nil {
			rc, err = rc2, nil
		}
	}
	if err != nil {
		return nil, fmt.Errorf("read %q from container %q: %w", path, name, err)
	}
	defer rc.Close()
	tr := tar.NewReader(rc)
	if _, err := tr.Next(); err != nil {
		return nil, fmt.Errorf("read %q from container %q: %w", path, name, err)
	}
	data, err := io.ReadAll(io.LimitReader(tr, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read %q from container %q: %w", path, name, err)
	}
	return data, nil
}

func (r *Runtime) WaitHealthy(ctx context.Context, name string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		info, err := r.cli.ContainerInspect(ctx, name)
		switch {
		case err == nil:
			if healthy, dead := containerHealthState(info); healthy {
				return nil
			} else if dead {
				return fmt.Errorf("container %q exited before becoming healthy%s", name, r.tailLogs(ctx, name))
			}
		case errdefs.IsNotFound(err):
			// name may be the aggregate base of a replica set: healthy means
			// "at least one member healthy" (docs/design/004's "provider
			// decides quorum meaning" rule).
			members, merr := r.listReplicaMembers(ctx, name)
			if merr != nil {
				return merr
			}
			if len(members) == 0 {
				return fmt.Errorf("container %q not found", name)
			}
			for _, m := range members {
				minfo, ierr := r.cli.ContainerInspect(ctx, m.ID)
				if ierr != nil {
					continue
				}
				if healthy, _ := containerHealthState(minfo); healthy {
					return nil
				}
			}
		default:
			return fmt.Errorf("inspect container %q: %w", name, err)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("container %q did not become healthy within %s%s", name, timeout, r.tailLogs(ctx, name))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// containerHealthState reports whether info is currently healthy (Docker
// says so for a health-checked container; running, for one without a
// healthcheck) and whether it is dead/exited — the two terminal signals
// WaitHealthy's poll loop acts on, factored out so both the single-container
// and replica-set aggregate paths share exactly one interpretation.
func containerHealthState(info container.InspectResponse) (healthy, dead bool) {
	if info.State == nil {
		return false, false
	}
	if info.State.Health != nil {
		healthy = info.State.Health.Status == container.Healthy
	} else {
		healthy = info.State.Running
	}
	dead = info.State.Dead || info.State.Status == "exited"
	return healthy, dead
}

// networksAttached reports whether the container is attached to every network
// the spec declares. A stopped container whose network was removed out-of-band
// keeps its matching spec hash but loses the endpoint — restarting it would
// bring up a container that cannot resolve its peers.
func networksAttached(info container.InspectResponse, want []string) bool {
	if len(want) == 0 {
		return true
	}
	if info.NetworkSettings == nil {
		return false
	}
	for _, n := range want {
		if _, ok := info.NetworkSettings.Networks[n]; !ok {
			return false
		}
	}
	return true
}

// tailLogs returns the container's last log lines formatted for inclusion in
// an error message, or "" if logs are unavailable. Failing containers are
// otherwise a black box to the CLI user.
func (r *Runtime) tailLogs(ctx context.Context, name string) string {
	out, err := r.rawLogs(ctx, name, 10)
	if err != nil || out == "" {
		return ""
	}
	if len(out) > 2000 {
		out = out[len(out)-2000:]
	}
	return "; last log lines:\n" + out
}

// RunsContainerCommands marks this adapter as one whose containers actually
// execute their declared Cmd — the conformance suite's persistence subtest
// (docs/planning/08 B3) uses this to know whether a "the process writes a
// file, does it survive a recreate" proof is meaningful (Docker,
// Kubernetes) or structurally untestable (the fake adapter never executes
// anything).
func (r *Runtime) RunsContainerCommands() bool { return true }

// Logs returns the last `tail` lines of the container's combined
// stdout/stderr for diagnostics. tail <= 0 uses a sane default. Against the
// aggregate name of a replica set, resolves to ordinal 0 as a best-effort
// default (docs/design/004, "Known limitations").
func (r *Runtime) Logs(ctx context.Context, name string, tail int) (string, error) {
	if tail <= 0 {
		tail = 200
	}
	out, err := r.rawLogs(ctx, name, tail)
	if err != nil && errdefs.IsNotFound(err) {
		if out2, err2 := r.rawLogs(ctx, runtime.OrdinalName(name, 0), tail); err2 == nil {
			return out2, nil
		}
	}
	if err != nil {
		return "", fmt.Errorf("read logs for container %q: %w", name, err)
	}
	return out, nil
}

// rawLogs returns the unwrapped error from ContainerLogs so callers (Logs,
// tailLogs) can distinguish errdefs.IsNotFound (e.g. to try an ordinal-0
// fallback) from other failures.
func (r *Runtime) rawLogs(ctx context.Context, name string, tail int) (string, error) {
	rc, err := r.cli.ContainerLogs(ctx, name, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Tail:       strconv.Itoa(tail),
	})
	if err != nil {
		return "", err
	}
	defer rc.Close()
	var buf bytes.Buffer
	// Container logs arrive stdout/stderr-multiplexed; demux into one stream.
	_, _ = stdcopy.StdCopy(&buf, &buf, io.LimitReader(rc, 256*1024))
	return strings.TrimSpace(buf.String()), nil
}

func (r *Runtime) Inspect(ctx context.Context, name string) (runtime.ContainerState, bool, error) {
	info, err := r.cli.ContainerInspect(ctx, name)
	if err == nil {
		return stateFromInspect(info), true, nil
	}
	if !errdefs.IsNotFound(err) {
		return runtime.ContainerState{}, false, fmt.Errorf("inspect container %q: %w", name, err)
	}
	// name is not a literal container — it may be the aggregate base name of
	// a replica set (docs/design/004): Docker has no native object to name
	// directly, so aggregate by the replica-membership label instead.
	members, merr := r.listReplicaMembers(ctx, name)
	if merr != nil {
		return runtime.ContainerState{}, false, merr
	}
	if len(members) == 0 {
		return runtime.ContainerState{}, false, nil
	}
	states := make([]runtime.ContainerState, 0, len(members))
	for _, c := range members {
		info, ierr := r.cli.ContainerInspect(ctx, c.ID)
		if ierr != nil {
			continue
		}
		states = append(states, stateFromInspect(info))
	}
	return aggregateContainerStates(name, states), true, nil
}

// EnsureReachable returns the already-published host address for
// containerPort — Docker's HostConfig.PortBindings already made it
// host-reachable at container-creation time, so there is nothing to open;
// close is a no-op kept only to satisfy the port's cross-runtime contract.
// Per the port contract (docs/planning/08 F3), a returned address must be
// *currently dialable*, not merely "Docker says this port is published": a
// published port accepts the TCP handshake as soon as the daemon proxies it,
// which can be before the container's own process calls listen() — the
// adapter, not every caller, absorbs that race with a bounded direct dial
// check rather than trusting the port-binding metadata alone.
func (r *Runtime) EnsureReachable(ctx context.Context, name string, containerPort int) (string, func() error, error) {
	state, found, err := r.Inspect(ctx, name)
	if err != nil {
		return "", nil, err
	}
	if !found {
		return "", nil, fmt.Errorf("container %q not found", name)
	}
	addr := state.HostAddr(containerPort)
	if addr == "" {
		return "", nil, fmt.Errorf("container %q publishes no host binding for port %d", name, containerPort)
	}
	if !dialable(ctx, addr) {
		return "", nil, fmt.Errorf("container %q: resolved address %q for port %d is not currently accepting connections", name, addr, containerPort)
	}
	return addr, func() error { return nil }, nil
}

// dialable reports whether a TCP connection to addr succeeds right now,
// honoring ctx's deadline (bounded to 2s when ctx has none) rather than
// hanging on an address nothing will ever answer.
func dialable(ctx context.Context, addr string) bool {
	timeout := 2 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < timeout {
			timeout = remaining
		}
	}
	if timeout <= 0 {
		return false
	}
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// Remove deletes the named container, or — if name is not a literal
// container but the aggregate base name of a replica set — every member of
// that set (docs/design/004). Per-ordinal volumes are deliberately left in
// place, matching the single-container path's existing behavior of never
// touching volumes as a side effect of Remove.
func (r *Runtime) Remove(ctx context.Context, name string) error {
	info, err := r.cli.ContainerInspect(ctx, name)
	if err == nil {
		if info.Config == nil || info.Config.Labels[runtime.LabelManagedBy] != runtime.ManagedByValue {
			return fmt.Errorf("container %q is not managed by platformctl; refusing to remove it", name)
		}
		if err := r.cli.ContainerRemove(ctx, info.ID, container.RemoveOptions{Force: true}); err != nil {
			return fmt.Errorf("remove container %q: %w", name, err)
		}
		return nil
	}
	if !errdefs.IsNotFound(err) {
		return fmt.Errorf("inspect container %q: %w", name, err)
	}
	members, merr := r.listReplicaMembers(ctx, name)
	if merr != nil {
		return merr
	}
	for _, c := range members {
		if err := r.Remove(ctx, containerSummaryName(c)); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runtime) RemoveNetwork(ctx context.Context, name string) error {
	nets, err := r.cli.NetworkList(ctx, network.ListOptions{
		Filters: filters.NewArgs(filters.Arg("name", name)),
	})
	if err != nil {
		return fmt.Errorf("list networks: %w", err)
	}
	for _, n := range nets {
		if n.Name != name {
			continue
		}
		if n.Labels[runtime.LabelManagedBy] != runtime.ManagedByValue {
			return fmt.Errorf("network %q is not managed by platformctl; refusing to remove it", name)
		}
		if err := r.cli.NetworkRemove(ctx, n.ID); err != nil && !errdefs.IsNotFound(err) {
			return fmt.Errorf("remove network %q: %w", name, err)
		}
	}
	return nil
}

func (r *Runtime) RemoveVolume(ctx context.Context, name string) error {
	vol, err := r.cli.VolumeInspect(ctx, name)
	if errdefs.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect volume %q: %w", name, err)
	}
	if vol.Labels[runtime.LabelManagedBy] != runtime.ManagedByValue {
		return fmt.Errorf("volume %q is not managed by platformctl; refusing to remove it", name)
	}
	if err := r.cli.VolumeRemove(ctx, name, false); err != nil && !errdefs.IsNotFound(err) {
		return fmt.Errorf("remove volume %q: %w", name, err)
	}
	return nil
}

func (r *Runtime) ListManaged(ctx context.Context) ([]runtime.ContainerState, error) {
	containers, err := r.cli.ContainerList(ctx, container.ListOptions{
		All:     true,
		Filters: managedFilter(),
	})
	if err != nil {
		return nil, fmt.Errorf("list managed containers: %w", err)
	}
	out := make([]runtime.ContainerState, 0, len(containers))
	for _, c := range containers {
		name := ""
		if len(c.Names) > 0 {
			name = c.Names[0]
			if len(name) > 0 && name[0] == '/' {
				name = name[1:]
			}
		}
		out = append(out, runtime.ContainerState{
			Name:    name,
			ID:      c.ID,
			Image:   c.Image,
			Running: c.State == "running",
			Healthy: c.State == "running", // list API has no health detail; Inspect refines this
			Labels:  c.Labels,
		})
	}
	return out, nil
}

func (r *Runtime) ListManagedNetworks(ctx context.Context) ([]runtime.ManagedNetwork, error) {
	nets, err := r.cli.NetworkList(ctx, network.ListOptions{Filters: managedFilter()})
	if err != nil {
		return nil, fmt.Errorf("list managed networks: %w", err)
	}
	out := make([]runtime.ManagedNetwork, 0, len(nets))
	for _, n := range nets {
		out = append(out, runtime.ManagedNetwork{Name: n.Name, Labels: n.Labels})
	}
	return out, nil
}

func (r *Runtime) ListManagedVolumes(ctx context.Context) ([]runtime.ManagedVolume, error) {
	vols, err := r.cli.VolumeList(ctx, volume.ListOptions{Filters: managedFilter()})
	if err != nil {
		return nil, fmt.Errorf("list managed volumes: %w", err)
	}
	out := make([]runtime.ManagedVolume, 0, len(vols.Volumes))
	for _, v := range vols.Volumes {
		out = append(out, runtime.ManagedVolume{Name: v.Name, Labels: v.Labels})
	}
	return out, nil
}

func (r *Runtime) inspectState(ctx context.Context, name string) (runtime.ContainerState, error) {
	info, err := r.cli.ContainerInspect(ctx, name)
	if err != nil {
		return runtime.ContainerState{}, fmt.Errorf("inspect container %q: %w", name, err)
	}
	return stateFromInspect(info), nil
}

func stateFromInspect(info container.InspectResponse) runtime.ContainerState {
	st := runtime.ContainerState{ID: info.ID}
	if info.Name != "" {
		st.Name = info.Name
		if st.Name[0] == '/' {
			st.Name = st.Name[1:]
		}
	}
	if info.Config != nil {
		st.Image = info.Config.Image
		st.Labels = info.Config.Labels
		st.Env = envMap(info.Config.Env)
	}
	if info.State != nil {
		st.Running = info.State.Running
		if info.State.Health != nil {
			st.Healthy = info.State.Health.Status == container.Healthy
		} else {
			st.Healthy = info.State.Running
		}
	}
	st.Ports = portsFromInspect(info)
	return st
}

// portsFromInspect reports the ports Docker actually bound — observed
// exposure, not requested intent (docs/planning/07 §1.1). Sorted for
// deterministic output.
func portsFromInspect(info container.InspectResponse) []runtime.PortBinding {
	if info.NetworkSettings == nil || len(info.NetworkSettings.Ports) == 0 {
		return nil
	}
	var out []runtime.PortBinding
	for port, bindings := range info.NetworkSettings.Ports {
		for _, b := range bindings {
			hostPort, err := strconv.Atoi(b.HostPort)
			if err != nil {
				continue
			}
			out = append(out, runtime.PortBinding{
				HostIP:        b.HostIP,
				HostPort:      hostPort,
				ContainerPort: port.Int(),
				Protocol:      port.Proto(),
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ContainerPort != out[j].ContainerPort {
			return out[i].ContainerPort < out[j].ContainerPort
		}
		return out[i].HostPort < out[j].HostPort
	})
	return out
}

func envMap(env []string) map[string]string {
	out := make(map[string]string, len(env))
	for _, entry := range env {
		k, v, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		out[k] = v
	}
	return out
}

func portMaps(ports []runtime.PortBinding) (nat.PortSet, nat.PortMap, error) {
	if len(ports) == 0 {
		return nil, nil, nil
	}
	exposed := nat.PortSet{}
	bindings := nat.PortMap{}
	for _, p := range ports {
		proto := p.Protocol
		if proto == "" {
			proto = "tcp"
		}
		port, err := nat.NewPort(proto, strconv.Itoa(p.ContainerPort))
		if err != nil {
			return nil, nil, fmt.Errorf("invalid port %d/%s: %w", p.ContainerPort, proto, err)
		}
		exposed[port] = struct{}{}
		// AudienceInternal (docs/planning/08 F2): no host publish — the
		// Docker network already reaches every container port regardless of
		// publish status, so this is a deliberate no-op on the host-binding
		// side. Anything else (including the empty string, for callers that
		// predate the Audience field) is treated as AudienceHost.
		if p.Audience == runtime.AudienceInternal {
			continue
		}
		hostIP := p.HostIP
		if hostIP == "" {
			hostIP = "127.0.0.1"
		}
		// HostPort 0 for a host-audience port means "let Docker assign one";
		// nat.PortBinding wants an empty string for that, not the literal
		// "0", which some daemon versions reject as an invalid port.
		hostPort := ""
		if p.HostPort != 0 {
			hostPort = strconv.Itoa(p.HostPort)
		}
		bindings[port] = []nat.PortBinding{{HostIP: hostIP, HostPort: hostPort}}
	}
	return exposed, bindings, nil
}
