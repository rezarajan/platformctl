// Package runtime defines the ContainerRuntime port and its value types.
// Every method is Ensure*, not Create* — idempotent-by-contract at the
// interface boundary. See docs/planning/02-architecture.md §4.1.
package runtime

import (
	"context"
	"time"
)

type NetworkSpec struct {
	Name   string
	Labels map[string]string
}

type VolumeSpec struct {
	Name   string
	Labels map[string]string
	// Networks names the network(s) (see ContainerSpec.Networks) the
	// volume's container(s) join. Docker volumes are cluster-global and
	// ignore this; a namespace-scoped runtime (e.g. Kubernetes, where a
	// PersistentVolumeClaim can only be mounted by a Pod in the same
	// namespace) needs it to know where to place the volume. Always set
	// this to the same value as the container that will mount it.
	Networks []string
}

type HealthCheck struct {
	Test     []string
	Interval time.Duration
	Timeout  time.Duration
	Retries  int
}

type VolumeMount struct {
	VolumeName string
	MountPath  string
}

type PortBinding struct {
	HostIP        string
	HostPort      int
	ContainerPort int
	Protocol      string // "tcp" (default) | "udp"
}

// RestartPolicy controls what the runtime does when a container's process
// exits. Mode mirrors the Docker/Compose vocabulary, which maps cleanly onto
// Kubernetes' restartPolicy too (Always/OnFailure/Never), so this stays
// runtime-agnostic despite living next to Docker-specific controls.
type RestartPolicy struct {
	Mode       string // "no" (default) | "always" | "on-failure" | "unless-stopped"
	MaxRetries int    // only meaningful for "on-failure"
}

// Resources caps what a container may consume. CPUReservation has a clean,
// well-defined meaning on Kubernetes (resources.requests.cpu — a scheduling
// guarantee); Docker has no true absolute-reservation equivalent, only CPU
// shares (a relative weight against other containers), so the Docker
// adapter maps it there on a best-effort basis and documents the gap rather
// than silently dropping it.
type Resources struct {
	CPULimit               float64 // cores; 0 = unlimited
	CPUReservation         float64 // cores; 0 = none (Docker: best-effort via CPU shares)
	MemoryLimitBytes       int64   // 0 = unlimited
	MemoryReservationBytes int64   // soft limit; 0 = none
}

// SecurityContext narrows what a container's process can do. Field names
// follow Kubernetes' PodSecurityContext/SecurityContext vocabulary
// (runAsUser, readOnlyRootFilesystem, capabilities) since that is the more
// restrictive, more portable model; the Docker adapter maps it onto
// container.HostConfig.
type SecurityContext struct {
	User           string // "uid[:gid]"; empty = image default
	ReadOnlyRootFS bool
	CapAdd         []string
	CapDrop        []string
	SecurityOpt    []string // escape hatch for runtime-specific options (e.g. Docker's "no-new-privileges")
}

// LogConfig selects the container's log driver. Left unset, the runtime's
// own default applies.
type LogConfig struct {
	Driver  string
	Options map[string]string
}

type ContainerSpec struct {
	Name          string
	Image         string
	Cmd           []string // implementation-revealed addition: real providers need command/args; pending doc amendment to 02-architecture.md §4.1
	Networks      []string
	Volumes       []VolumeMount
	Env           map[string]string
	Ports         []PortBinding
	HealthCheck   *HealthCheck
	Labels        map[string]string // Datascape ownership + generation labels for drift/GC
	RestartPolicy *RestartPolicy
	Resources     *Resources
	Security      *SecurityContext
	LogConfig     *LogConfig
}

type ContainerState struct {
	Name    string
	ID      string
	Image   string
	Running bool
	Healthy bool
	Labels  map[string]string
	Env     map[string]string
}

type ContainerRuntime interface {
	EnsureNetwork(ctx context.Context, spec NetworkSpec) error
	EnsureVolume(ctx context.Context, spec VolumeSpec) error
	EnsureContainer(ctx context.Context, spec ContainerSpec) (ContainerState, error)
	WaitHealthy(ctx context.Context, name string, timeout time.Duration) error
	Inspect(ctx context.Context, name string) (ContainerState, bool, error)
	Remove(ctx context.Context, name string) error
	RemoveNetwork(ctx context.Context, name string) error
	RemoveVolume(ctx context.Context, name string) error
	ListManaged(ctx context.Context) ([]ContainerState, error) // everything labeled as Datascape-owned
	// Logs returns the last `tail` lines of the container's combined
	// stdout/stderr, for diagnostics (`platformctl doctor`, CLI log
	// retrieval). tail <= 0 requests the runtime's default tail length.
	Logs(ctx context.Context, name string, tail int) (string, error)
}

// Datascape ownership labels — applied to every created object so
// ListManaged/destroy never touch unlabeled resources.
const (
	LabelManagedBy  = "io.datascape.managed-by"
	LabelGeneration = "io.datascape.generation"
	LabelNamespace  = "io.datascape.namespace"
	LabelKind       = "io.datascape.kind"
	LabelName       = "io.datascape.name"
	LabelProject    = "io.datascape.project"
	ManagedByValue  = "platformctl"
)

func ManagedLabels(namespace, kind, name, generation string) map[string]string {
	if namespace == "" {
		namespace = "default"
	}
	if generation == "" {
		generation = name
	}
	return map[string]string{
		LabelManagedBy:  ManagedByValue,
		LabelGeneration: generation,
		LabelNamespace:  namespace,
		LabelKind:       kind,
		LabelName:       name,
		LabelProject:    namespace,
	}
}
