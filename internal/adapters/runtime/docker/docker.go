// Package docker implements ContainerRuntime against the real Docker Engine
// API. Every created object carries the Datascape ownership labels;
// ListManaged/Remove never touch unlabeled resources.
// See docs/planning/02-architecture.md §4.1 and §10.
package docker

import (
	"bytes"
	"context"
	"fmt"
	"io"
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

	if err := r.ensureImage(ctx, spec.Image); err != nil {
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

	hostCfg := &container.HostConfig{PortBindings: bindings}
	for _, m := range spec.Volumes {
		hostCfg.Binds = append(hostCfg.Binds, m.VolumeName+":"+m.MountPath)
	}

	var netCfg *network.NetworkingConfig
	if len(spec.Networks) > 0 {
		endpoints := make(map[string]*network.EndpointSettings, len(spec.Networks))
		for _, n := range spec.Networks {
			endpoints[n] = &network.EndpointSettings{}
		}
		netCfg = &network.NetworkingConfig{EndpointsConfig: endpoints}
	}

	created, err := r.cli.ContainerCreate(ctx, cfg, hostCfg, netCfg, nil, spec.Name)
	if err != nil {
		return runtime.ContainerState{}, fmt.Errorf("create container %q: %w", spec.Name, err)
	}
	if err := r.cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		return runtime.ContainerState{}, fmt.Errorf("start container %q: %w", spec.Name, err)
	}
	return r.inspectState(ctx, spec.Name)
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
	rc, err := r.cli.ContainerLogs(ctx, name, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Tail:       "10",
	})
	if err != nil {
		return ""
	}
	defer rc.Close()
	var buf bytes.Buffer
	// Container logs arrive stdout/stderr-multiplexed; demux into one stream.
	_, _ = stdcopy.StdCopy(&buf, &buf, io.LimitReader(rc, 64*1024))
	out := strings.TrimSpace(buf.String())
	if out == "" {
		return ""
	}
	if len(out) > 2000 {
		out = out[len(out)-2000:]
	}
	return "; last log lines:\n" + out
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
	return st
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
