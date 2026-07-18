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

func (r *Runtime) EnsureContainer(ctx context.Context, spec runtime.ContainerSpec) (runtime.ContainerState, error) {
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

// ReadFile retrieves a file previously placed by ContainerSpec.Files.
func (r *Runtime) ReadFile(ctx context.Context, name, path string) ([]byte, error) {
	rc, _, err := r.cli.CopyFromContainer(ctx, name, path)
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
		if err != nil {
			return fmt.Errorf("inspect container %q: %w", name, err)
		}
		if info.State != nil {
			// Health-checked containers are healthy when Docker says so;
			// containers without a healthcheck are healthy when running.
			if info.State.Health != nil {
				if info.State.Health.Status == container.Healthy {
					return nil
				}
			} else if info.State.Running {
				return nil
			}
			if info.State.Dead || (info.State.Status == "exited") {
				return fmt.Errorf("container %q exited before becoming healthy%s", name, r.tailLogs(ctx, name))
			}
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
	out, err := r.fetchLogs(ctx, name, 10)
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
// stdout/stderr for diagnostics. tail <= 0 uses a sane default.
func (r *Runtime) Logs(ctx context.Context, name string, tail int) (string, error) {
	if tail <= 0 {
		tail = 200
	}
	return r.fetchLogs(ctx, name, tail)
}

func (r *Runtime) fetchLogs(ctx context.Context, name string, tail int) (string, error) {
	rc, err := r.cli.ContainerLogs(ctx, name, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Tail:       strconv.Itoa(tail),
	})
	if err != nil {
		return "", fmt.Errorf("read logs for container %q: %w", name, err)
	}
	defer rc.Close()
	var buf bytes.Buffer
	// Container logs arrive stdout/stderr-multiplexed; demux into one stream.
	_, _ = stdcopy.StdCopy(&buf, &buf, io.LimitReader(rc, 256*1024))
	return strings.TrimSpace(buf.String()), nil
}

func (r *Runtime) Inspect(ctx context.Context, name string) (runtime.ContainerState, bool, error) {
	info, err := r.cli.ContainerInspect(ctx, name)
	if errdefs.IsNotFound(err) {
		return runtime.ContainerState{}, false, nil
	}
	if err != nil {
		return runtime.ContainerState{}, false, fmt.Errorf("inspect container %q: %w", name, err)
	}
	return stateFromInspect(info), true, nil
}

// EnsureReachable returns the already-published host address for
// containerPort — Docker's HostConfig.PortBindings already made it
// host-reachable at container-creation time, so there is nothing to open;
// close is a no-op kept only to satisfy the port's cross-runtime contract.
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
	return addr, func() error { return nil }, nil
}

func (r *Runtime) Remove(ctx context.Context, name string) error {
	info, err := r.cli.ContainerInspect(ctx, name)
	if errdefs.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect container %q: %w", name, err)
	}
	if info.Config == nil || info.Config.Labels[runtime.LabelManagedBy] != runtime.ManagedByValue {
		return fmt.Errorf("container %q is not managed by platformctl; refusing to remove it", name)
	}
	if err := r.cli.ContainerRemove(ctx, info.ID, container.RemoveOptions{Force: true}); err != nil {
		return fmt.Errorf("remove container %q: %w", name, err)
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
		hostIP := p.HostIP
		if hostIP == "" {
			hostIP = "127.0.0.1"
		}
		bindings[port] = []nat.PortBinding{{HostIP: hostIP, HostPort: strconv.Itoa(p.HostPort)}}
	}
	return exposed, bindings, nil
}
