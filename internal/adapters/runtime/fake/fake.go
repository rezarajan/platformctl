// Package fake provides an in-memory ContainerRuntime for unit and contract
// tests. It honors the Ensure* idempotency contract: a second call with the
// same spec is a no-op, observable via call counters.
package fake

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

type Runtime struct {
	mu         sync.Mutex
	networks   map[string]runtime.NetworkSpec
	volumes    map[string]runtime.VolumeSpec
	containers map[string]runtime.ContainerSpec

	// MutationCount increments only when state actually changes — the
	// conformance suite asserts idempotency against it.
	MutationCount int
	nextID        int
}

func New() *Runtime {
	return &Runtime{
		networks:   make(map[string]runtime.NetworkSpec),
		volumes:    make(map[string]runtime.VolumeSpec),
		containers: make(map[string]runtime.ContainerSpec),
	}
}

func (r *Runtime) EnsureNetwork(_ context.Context, spec runtime.NetworkSpec) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.networks[spec.Name]; ok && specEqual(existing.Labels, spec.Labels) {
		return nil
	}
	r.networks[spec.Name] = spec
	r.MutationCount++
	return nil
}

func (r *Runtime) EnsureVolume(_ context.Context, spec runtime.VolumeSpec) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.volumes[spec.Name]; ok && specEqual(existing.Labels, spec.Labels) {
		return nil
	}
	r.volumes[spec.Name] = spec
	r.MutationCount++
	return nil
}

func (r *Runtime) EnsureContainer(_ context.Context, spec runtime.ContainerSpec) (runtime.ContainerState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.containers[spec.Name]; ok && containerSpecEqual(existing, spec) {
		return r.stateOf(existing), nil
	}
	r.containers[spec.Name] = spec
	r.MutationCount++
	r.nextID++
	return r.stateOf(spec), nil
}

func (r *Runtime) WaitHealthy(_ context.Context, name string, _ time.Duration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.containers[name]; !ok {
		return fmt.Errorf("container %q not found", name)
	}
	return nil // fake containers are immediately healthy
}

func (r *Runtime) Inspect(_ context.Context, name string) (runtime.ContainerState, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	spec, ok := r.containers[name]
	if !ok {
		return runtime.ContainerState{}, false, nil
	}
	return r.stateOf(spec), true, nil
}

func (r *Runtime) Remove(_ context.Context, name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.containers[name]; ok {
		delete(r.containers, name)
		r.MutationCount++
	}
	return nil
}

func (r *Runtime) RemoveNetwork(_ context.Context, name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.networks[name]; ok {
		delete(r.networks, name)
		r.MutationCount++
	}
	return nil
}

func (r *Runtime) RemoveVolume(_ context.Context, name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.volumes[name]; ok {
		delete(r.volumes, name)
		r.MutationCount++
	}
	return nil
}

func (r *Runtime) ListManaged(_ context.Context) ([]runtime.ContainerState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []runtime.ContainerState
	for _, spec := range r.containers {
		if spec.Labels[runtime.LabelManagedBy] == runtime.ManagedByValue {
			out = append(out, r.stateOf(spec))
		}
	}
	return out, nil
}

func (r *Runtime) stateOf(spec runtime.ContainerSpec) runtime.ContainerState {
	return runtime.ContainerState{
		Name:    spec.Name,
		ID:      fmt.Sprintf("fake-%s", spec.Name),
		Image:   spec.Image,
		Running: true,
		Healthy: true,
		Labels:  spec.Labels,
		Env:     spec.Env,
	}
}

func specEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func containerSpecEqual(a, b runtime.ContainerSpec) bool {
	if a.Name != b.Name || a.Image != b.Image {
		return false
	}
	if !specEqual(a.Labels, b.Labels) || !specEqual(a.Env, b.Env) {
		return false
	}
	if len(a.Networks) != len(b.Networks) {
		return false
	}
	for i := range a.Networks {
		if a.Networks[i] != b.Networks[i] {
			return false
		}
	}
	return true
}
