// Package fake provides an in-memory ContainerRuntime for unit and contract
// tests. It honors the Ensure* idempotency contract: a second call with the
// same spec is a no-op, observable via call counters.
package fake

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// containerRecord is a single runtime-managed unit: either an ordinary,
// non-replicated container (replicaBase == "") or one ordinal member of a
// Replicas > 1 set (docs/adr/004-replicas-and-identity.md). Keyed in
// Runtime.containers by its own literal name — the base ContainerSpec.Name
// for a non-replicated container, or the ordinal name ("<base>-<i>") for a
// replica-set member — mirroring exactly how Docker has no separate
// "replica set" object either: every managed unit is a real, individually
// named entry, and a base name only resolves to something by aggregating
// members that share the same replicaBase.
type containerRecord struct {
	spec        runtime.ContainerSpec
	replicaBase string // "" unless this is one ordinal of a Replicas > 1 set
	ordinal     int
}

type Runtime struct {
	mu         sync.Mutex
	networks   map[string]runtime.NetworkSpec
	volumes    map[string]runtime.VolumeSpec
	containers map[string]containerRecord
	// volumeFiles simulates a real volume's persistence independent of
	// container lifecycle: content written under a mounted volume's path
	// survives even once a later EnsureContainer generation no longer
	// declares the FileMount that first placed it (docs/planning/08 B3's
	// persistence conformance subtest needs this to be meaningful against
	// the fake, not just Docker/Kubernetes). Keyed by volume name, then
	// absolute path.
	volumeFiles map[string]map[string][]byte

	// MutationCount increments only when state actually changes — the
	// conformance suite asserts idempotency against it.
	MutationCount int
	nextID        int
}

func New() *Runtime {
	return &Runtime{
		networks:    make(map[string]runtime.NetworkSpec),
		volumes:     make(map[string]runtime.VolumeSpec),
		containers:  make(map[string]containerRecord),
		volumeFiles: make(map[string]map[string][]byte),
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
	if err := validatePortAudiences(spec); err != nil {
		return runtime.ContainerState{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	n := spec.ReplicaCount()
	if n <= 1 {
		// Shape-transition guard (docs/adr/004): collapsing an existing
		// replica set to a single container in place is refused, exactly as
		// the Docker and Kubernetes adapters refuse it, so unit tests catch
		// the transition before an integration run does.
		memberCount := 0
		for _, rec := range r.containers {
			if rec.replicaBase == spec.Name {
				memberCount++
			}
		}
		if memberCount > 0 {
			return runtime.ContainerState{}, fmt.Errorf("container %q exists as a %d-member replica set; refusing to convert it to a single container in place — remove it first (destroy and recreate) if collapsing this set to one replica", spec.Name, memberCount)
		}
		r.ensureOneLocked(spec, "", 0)
		return r.stateOf(spec), nil
	}
	return r.ensureReplicaSetLocked(spec, n), nil
}

// ensureOneLocked ensures a single runtime-managed unit (spec.Name is
// whatever the caller wants stored under — the base name for a
// non-replicated container, or an ordinal name for a replica-set member).
// Caller must hold r.mu.
func (r *Runtime) ensureOneLocked(spec runtime.ContainerSpec, replicaBase string, ordinal int) {
	r.persistVolumeFiles(spec)
	if existing, ok := r.containers[spec.Name]; ok && existing.replicaBase == replicaBase && containerSpecEqual(existing.spec, spec) {
		return // matches — no-op
	}
	r.containers[spec.Name] = containerRecord{spec: spec, replicaBase: replicaBase, ordinal: ordinal}
	r.MutationCount++
	r.nextID++
}

// ensureReplicaSetLocked fans spec out to n ordinal members
// ("<spec.Name>-0".."<spec.Name>-(n-1)"), removing any stale ordinal left
// over from a previous, larger generation (scale-down), and returns the
// aggregate ContainerState. Caller must hold r.mu.
func (r *Runtime) ensureReplicaSetLocked(spec runtime.ContainerSpec, n int) runtime.ContainerState {
	for name, rec := range r.containers {
		if rec.replicaBase == spec.Name && rec.ordinal >= n {
			delete(r.containers, name)
			r.MutationCount++
		}
	}
	for i := 0; i < n; i++ {
		ordSpec := ordinalContainerSpec(spec, i)
		if spec.StableIdentity {
			// The runtime owns the entire per-ordinal volume lifecycle for
			// a StableIdentity set (docs/adr/004) — a caller must not
			// call EnsureVolume itself for these derived names.
			for _, vm := range ordSpec.Volumes {
				if _, ok := r.volumes[vm.VolumeName]; !ok {
					r.volumes[vm.VolumeName] = runtime.VolumeSpec{Name: vm.VolumeName, Labels: spec.Labels, Networks: spec.Networks}
					r.MutationCount++
				}
			}
		}
		r.ensureOneLocked(ordSpec, spec.Name, i)
	}
	return r.aggregateStateLocked(spec.Name)
}

// ordinalContainerSpec derives ordinal i's own ContainerSpec from the
// replica set's base spec: ordinal-suffixed Name, the base name added as a
// shared alias (the fake's analog of Docker's round-robin network alias —
// docs/adr/004), and, when StableIdentity, ordinal-suffixed volume
// names so each replica's storage is isolated.
func ordinalContainerSpec(spec runtime.ContainerSpec, i int) runtime.ContainerSpec {
	out := spec
	out.Name = runtime.OrdinalName(spec.Name, i)
	out.Aliases = append(append([]string{}, spec.Aliases...), spec.Name)
	// Replicas is set-level state, not a property of any one ordinal: kept in
	// the per-ordinal spec it would make a 2 -> 3 scale-up look like a spec
	// change on ordinals 0 and 1 (containerSpecEqual is a deep compare),
	// mirroring the Docker adapter's identical hash-stability rule.
	out.Replicas = 0
	if spec.StableIdentity && len(spec.Volumes) > 0 {
		vols := make([]runtime.VolumeMount, len(spec.Volumes))
		for j, vm := range spec.Volumes {
			vols[j] = runtime.VolumeMount{VolumeName: runtime.OrdinalName(vm.VolumeName, i), MountPath: vm.MountPath}
		}
		out.Volumes = vols
	}
	labels := make(map[string]string, len(spec.Labels)+2)
	for k, v := range spec.Labels {
		labels[k] = v
	}
	labels[runtime.LabelReplicaBase] = spec.Name
	labels[runtime.LabelReplicaOrdinal] = strconv.Itoa(i)
	out.Labels = labels
	return out
}

// aggregateStateLocked builds the collective ContainerState for every
// current member of the replica set named base. Caller must hold r.mu.
func (r *Runtime) aggregateStateLocked(base string) runtime.ContainerState {
	var members []containerRecord
	for _, rec := range r.containers {
		if rec.replicaBase == base {
			members = append(members, rec)
		}
	}
	sort.Slice(members, func(i, j int) bool { return members[i].ordinal < members[j].ordinal })
	st := runtime.ContainerState{Name: base, ID: fmt.Sprintf("fake-%s", base)}
	if len(members) > 0 {
		first := members[0].spec
		st.Image = first.Image
		st.Labels = first.Labels
		st.Env = first.Env
		st.Ports = observedPorts(first.Ports)
	}
	// Every fake container is immediately healthy once created (see
	// WaitHealthy), so ReadyReplicas is simply the current member count.
	st.ReadyReplicas = len(members)
	st.Running = len(members) > 0
	st.Healthy = len(members) > 0
	return st
}

// resolveRecord finds the record a bare name argument to ReadFile/Logs/
// EnsureReachable refers to: the literal member/single container if one
// exists, else ordinal 0 of the replica set based named name — the
// aggregate best-effort default documented in docs/adr/004 ("Known
// limitations": these three methods resolve an aggregate name to ordinal 0
// rather than supporting a genuinely collective operation). Caller must
// hold r.mu.
func (r *Runtime) resolveRecord(name string) (containerRecord, bool) {
	if rec, ok := r.containers[name]; ok {
		return rec, true
	}
	if rec, ok := r.containers[runtime.OrdinalName(name, 0)]; ok && rec.replicaBase == name {
		return rec, true
	}
	return containerRecord{}, false
}

// persistVolumeFiles records this generation's FileMount content against
// whichever mounted volume's path prefix contains it, so ReadFile can still
// return it in a later generation that no longer declares that FileMount —
// see the volumeFiles field doc.
func (r *Runtime) persistVolumeFiles(spec runtime.ContainerSpec) {
	for _, f := range spec.Files {
		for _, vm := range spec.Volumes {
			if !strings.HasPrefix(f.Path, vm.MountPath) {
				continue
			}
			if r.volumeFiles[vm.VolumeName] == nil {
				r.volumeFiles[vm.VolumeName] = make(map[string][]byte)
			}
			r.volumeFiles[vm.VolumeName][f.Path] = f.Content
		}
	}
}

func (r *Runtime) WaitHealthy(_ context.Context, name string, _ time.Duration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.containers[name]; ok {
		return nil // fake containers are immediately healthy
	}
	for _, rec := range r.containers {
		if rec.replicaBase == name {
			return nil // at least one member exists — aggregate Healthy rule
		}
	}
	return fmt.Errorf("container %q not found", name)
}

func (r *Runtime) Inspect(_ context.Context, name string) (runtime.ContainerState, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if rec, ok := r.containers[name]; ok {
		st := r.stateOf(rec.spec)
		if rec.replicaBase != "" {
			// An individual replica-set member is not itself the aggregate;
			// ReadyReplicas only has a meaning at the set level.
			st.ReadyReplicas = 0
		}
		return st, true, nil
	}
	found := false
	for _, rec := range r.containers {
		if rec.replicaBase == name {
			found = true
			break
		}
	}
	if !found {
		return runtime.ContainerState{}, false, nil
	}
	return r.aggregateStateLocked(name), true, nil
}

func (r *Runtime) Remove(_ context.Context, name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.containers[name]; ok {
		delete(r.containers, name)
		r.MutationCount++
		return nil
	}
	removedAny := false
	for cname, rec := range r.containers {
		if rec.replicaBase == name {
			delete(r.containers, cname)
			removedAny = true
		}
	}
	if removedAny {
		r.MutationCount++
	}
	return nil
}

func (r *Runtime) RemoveNetwork(_ context.Context, name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.networks[name]; !ok {
		return nil
	}
	// A network with a container still attached cannot be removed — the same
	// refusal Docker gives ("network has active endpoints") and the Kubernetes
	// adapter gives (a Namespace that still holds a Deployment). Providers that
	// share one network rely on it: each best-effort-calls RemoveNetwork on
	// Destroy, and the network must outlive every member but the last rather
	// than be torn down (with its members) by the first. Pinned by the
	// conformance suite's RemoveNetwork_refuses_while_container_attached.
	for _, rec := range r.containers {
		for _, n := range rec.spec.Networks {
			if n == name {
				return fmt.Errorf("network %q still has container %q attached; refusing to remove it", name, rec.spec.Name)
			}
		}
	}
	delete(r.networks, name)
	r.MutationCount++
	return nil
}

func (r *Runtime) RemoveVolume(_ context.Context, name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.volumes[name]; ok {
		delete(r.volumes, name)
		delete(r.volumeFiles, name)
		r.MutationCount++
	}
	return nil
}

func (r *Runtime) Logs(_ context.Context, name string, _ int) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.resolveRecord(name); !ok {
		return "", fmt.Errorf("container %q not found", name)
	}
	return "", nil // fake containers produce no logs
}

func (r *Runtime) ReadFile(_ context.Context, name, path string) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.resolveRecord(name)
	if !ok {
		return nil, fmt.Errorf("container %q not found", name)
	}
	spec := rec.spec
	for _, f := range spec.Files {
		if f.Path == path {
			return f.Content, nil
		}
	}
	// Not declared by the current generation — a currently-mounted volume
	// may still carry it from an earlier one (persistVolumeFiles).
	for _, vm := range spec.Volumes {
		if content, ok := r.volumeFiles[vm.VolumeName][path]; ok {
			return content, nil
		}
	}
	return nil, fmt.Errorf("file %q not found in container %q", path, name)
}

// EnsureReachable mirrors the Docker adapter's trivial passthrough: a fake
// container's host-audience ports are already host-reachable by
// construction. Per F2, the fake is the strict interpreter of port
// audience: an internal-audience port — or one never declared at all —
// refuses, rather than silently succeeding the way a more permissive
// runtime's default access mode might (docs/planning/08 F2, docs/planning/09
// K10). This is deliberately stricter than Kubernetes' port-forward access
// mode, which can reach any pod port regardless of audience; the fake exists
// to catch under-declaration in `go test ./...` before a cluster ever does.
// Against the aggregate name of a replica set, resolves to ordinal 0 (a
// documented best-effort default — docs/adr/004 "Known limitations").
func (r *Runtime) EnsureReachable(_ context.Context, name string, containerPort int) (string, func() error, error) {
	r.mu.Lock()
	rec, ok := r.resolveRecord(name)
	r.mu.Unlock()
	if !ok {
		return "", nil, fmt.Errorf("container %q not found", name)
	}
	spec := rec.spec
	for _, p := range spec.Ports {
		if p.ContainerPort != containerPort {
			continue
		}
		if p.Audience == runtime.AudienceInternal {
			return "", nil, fmt.Errorf("container %q port %d is declared Audience: internal; it has no host-reachable address", name, containerPort)
		}
	}
	addr := r.stateOf(spec).HostAddr(containerPort)
	if addr == "" {
		return "", nil, fmt.Errorf("container %q publishes no host binding for port %d", name, containerPort)
	}
	return addr, func() error { return nil }, nil
}

// ProbeReachable is the strict interpreter of C10's in-network reachability
// contract (docs/planning/08 C10, ADR 015): reachable only when target names
// a fake-managed container attached to network, on a port that container's
// spec declares — either audience, since an in-network vantage point is not
// limited to Audience: host the way EnsureReachable is. Any other target
// (unknown host, wrong network, a port never declared) errors, so a provider
// that under-declares a port a Binding will dial in-network fails here in
// `go test ./...` before any live cluster does.
func (r *Runtime) ProbeReachable(_ context.Context, network, target string) error {
	host, portStr, err := net.SplitHostPort(target)
	if err != nil {
		return fmt.Errorf("ProbeReachable: invalid target %q: %w", target, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("ProbeReachable: invalid target %q: port %q is not numeric", target, portStr)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.resolveRecord(host)
	if !ok {
		return fmt.Errorf("ProbeReachable: %q does not name a fake-managed container", host)
	}
	onNetwork := false
	for _, n := range rec.spec.Networks {
		if n == network {
			onNetwork = true
			break
		}
	}
	if !onNetwork {
		return fmt.Errorf("ProbeReachable: container %q is not attached to network %q", host, network)
	}
	for _, p := range rec.spec.Ports {
		if p.ContainerPort == port {
			return nil // declared — reachable from an in-network vantage point
		}
	}
	return fmt.Errorf("ProbeReachable: container %q declares no port %d", host, port)
}

// validatePortAudiences enforces the F2 port-contract requirement: every
// declared port states, explicitly, who may dial it. A blank or misspelled
// Audience is exactly the class of under-declaration this check exists to
// catch — in a unit test, not on a live cluster.
func validatePortAudiences(spec runtime.ContainerSpec) error {
	for _, p := range spec.Ports {
		switch p.Audience {
		case runtime.AudienceHost, runtime.AudienceInternal:
		default:
			return fmt.Errorf("container %q: port %d declares Audience %q; must be %q or %q", spec.Name, p.ContainerPort, p.Audience, runtime.AudienceHost, runtime.AudienceInternal)
		}
	}
	return nil
}

func (r *Runtime) ListManaged(_ context.Context) ([]runtime.ContainerState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []runtime.ContainerState
	for _, rec := range r.containers {
		if rec.spec.Labels[runtime.LabelManagedBy] == runtime.ManagedByValue {
			out = append(out, r.stateOf(rec.spec))
		}
	}
	return out, nil
}

func (r *Runtime) ListManagedNetworks(_ context.Context) ([]runtime.ManagedNetwork, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []runtime.ManagedNetwork
	for name, spec := range r.networks {
		if spec.Labels[runtime.LabelManagedBy] == runtime.ManagedByValue {
			out = append(out, runtime.ManagedNetwork{Name: name, Labels: spec.Labels})
		}
	}
	return out, nil
}

func (r *Runtime) ListManagedVolumes(_ context.Context) ([]runtime.ManagedVolume, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []runtime.ManagedVolume
	for name, spec := range r.volumes {
		if spec.Labels[runtime.LabelManagedBy] == runtime.ManagedByValue {
			out = append(out, runtime.ManagedVolume{Name: name, Labels: spec.Labels})
		}
	}
	return out, nil
}

func (r *Runtime) stateOf(spec runtime.ContainerSpec) runtime.ContainerState {
	return runtime.ContainerState{
		Name:          spec.Name,
		ID:            fmt.Sprintf("fake-%s", spec.Name),
		Image:         spec.Image,
		Running:       true,
		Healthy:       true,
		Labels:        spec.Labels,
		Env:           spec.Env,
		Ports:         observedPorts(spec.Ports),
		ReadyReplicas: 1,
	}
}

// observedPorts mirrors what the Docker adapter reports from inspect:
// host-audience ports get the concrete bind address filled in (127.0.0.1
// when the spec left HostIP empty); internal-audience ports report no host
// binding at all, matching Docker's own real non-publish behavior for them
// (docs/planning/08 F2) — the fake must present observed exposure the same
// way the real runtime does.
func observedPorts(ports []runtime.PortBinding) []runtime.PortBinding {
	if len(ports) == 0 {
		return nil
	}
	out := make([]runtime.PortBinding, len(ports))
	for i, p := range ports {
		if p.Audience == runtime.AudienceInternal {
			p.HostIP = ""
			p.HostPort = 0
		} else if p.HostIP == "" {
			p.HostIP = "127.0.0.1"
		}
		if p.Protocol == "" {
			p.Protocol = "tcp"
		}
		out[i] = p
	}
	return out
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

// containerSpecEqual compares every field of ContainerSpec (command, ports,
// volumes, health checks, restart policy, resources, security context, log
// config, replicas/stable identity — not just name/image/labels/env/
// networks), so the fake stays honest about the NFR-2 idempotency contract:
// a second EnsureContainer call only counts as a no-op when nothing
// meaningful actually changed.
func containerSpecEqual(a, b runtime.ContainerSpec) bool {
	return reflect.DeepEqual(a, b)
}
