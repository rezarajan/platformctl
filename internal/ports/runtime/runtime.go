// Package runtime defines the ContainerRuntime port and its value types.
// Every method is Ensure*, not Create* — idempotent-by-contract at the
// interface boundary. See docs/planning/02-architecture.md §4.1.
package runtime

import (
	"context"
	"net"
	"strconv"
	"time"
)

// Network isolation policy modes for NetworkSpec.IsolationPolicy.
const (
	// IsolationDefault gives the network a real isolation boundary where
	// the runtime supports one: Docker networks always were one; Kubernetes
	// additionally provisions a default-deny + allow-same-namespace
	// NetworkPolicy pair (docs/planning/08 B7) so a Namespace stops being
	// DNS-parity-only and actually matches Docker's isolation semantics.
	IsolationDefault = ""
	// IsolationNone opts out of Kubernetes' NetworkPolicy provisioning —
	// for clusters whose CNI doesn't enforce NetworkPolicy (the objects
	// would sit inert) or where an operator has their own policy story.
	// Docker ignores this field either way; a network is always isolated
	// there.
	IsolationNone = "none"
)

type NetworkSpec struct {
	Name   string
	Labels map[string]string
	// IsolationPolicy is one of the Isolation* constants above.
	IsolationPolicy string
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
	// SizeBytes requests a sized volume. Docker volumes are unsized and
	// ignore this; Kubernetes sets it as the PersistentVolumeClaim's
	// storage request (0 = the adapter's own default, currently 10Gi). A
	// size *increase* on an existing PVC is a live expansion (allowed when
	// the StorageClass supports it); a decrease is refused — Kubernetes
	// itself does not support shrinking a bound PVC.
	SizeBytes int64
	// StorageClass selects a Kubernetes StorageClass by name. Empty means
	// the cluster's default StorageClass. Docker ignores this.
	StorageClass string
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

// FileMount places literal file content into the container at Path — the
// runtime-supported safer channel for secret material than environment
// variables, which any `docker inspect` reveals (docs/planning/07 Gate 1
// checkbox 4, §2.5). Docker copies the content in before start; Kubernetes
// mounts it from a Secret object. Content participates in the spec hash
// (one-way), so changing it replaces the container like any other field.
type FileMount struct {
	Path    string // absolute path inside the container
	Content []byte
	Mode    uint32 // e.g. 0o444; 0 = default 0o444
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

// Image pull policies. The zero value defers to the runtime default
// (if-not-present). Policy applies when a container is (re)created — the
// idempotent no-op path of an already-matching container never re-pulls.
const (
	PullIfNotPresent = ""       // pull only when the image is absent locally (default)
	PullAlways       = "always" // pull on every container (re)creation — mutable tags
	PullNever        = "never"  // never pull; fail if the image is absent (air-gapped/local-only)
)

// ImagePullAuth carries resolved registry credentials for a private image
// pull (docs/planning/07 §1.1 deferral: `ImagePull` previously sent no
// RegistryAuth header at all). The runtime port has no SecretStore access,
// so this always carries already-resolved values — a provider resolves
// `configuration.imagePullSecretRef` via the same SecretsAware/SetSecrets
// plumbing it uses for any other credential (e.g. postgres's superuser
// password), never a bare reference the adapter would need to re-resolve.
type ImagePullAuth struct {
	Username string
	Password string
	// Registry is the registry hostname these credentials are for (e.g.
	// "ghcr.io"). Optional — Docker infers it from the image reference when
	// empty; Kubernetes' dockerconfigjson Secret needs it explicit.
	Registry string
}

type ContainerSpec struct {
	Name string
	// Image is any runtime-resolvable reference, including digest-pinned
	// form ("repo@sha256:..."), which is the recommended way to guarantee
	// an exact image (docs/planning/07 §1.1/§2.5) — a tag is mutable, a
	// digest is not.
	Image string
	// PullPolicy is one of the Pull* constants above.
	PullPolicy string
	// ImagePullAuth carries resolved credentials for a private image pull.
	// Nil means the runtime's ambient/daemon-level credentials apply
	// unchanged (the pre-existing behavior).
	ImagePullAuth *ImagePullAuth
	Cmd           []string // implementation-revealed addition: real providers need command/args; pending doc amendment to 02-architecture.md §4.1
	Networks      []string
	// Aliases are additional in-network DNS names for this container beyond
	// its Name, so a stable internal address can outlive a container rename
	// (docs/planning/07 §1.1/§2.4). Docker: per-network endpoint aliases;
	// Kubernetes: additional Services selecting the same pod.
	Aliases       []string
	Volumes       []VolumeMount
	Files         []FileMount
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
	// Ports reports the *actually bound* published ports as observed from
	// the runtime (Docker: NetworkSettings.Ports from inspect), not the
	// requested intent — so endpoint discovery and drift checks can verify
	// real exposure (docs/planning/07 §0.7/§1.1). HostIP is the concrete
	// bind address (e.g. "127.0.0.1"), never empty for a published port.
	// Runtimes with no host-port concept for a container (Kubernetes
	// ClusterIP Services) report HostIP/HostPort zero-valued with only
	// ContainerPort set.
	Ports []PortBinding
}

// HostAddr returns the observed host address ("ip:port") the given container
// port is published on, or "" when the runtime reports no host binding for
// it (in-network only — e.g. Kubernetes ClusterIP). Providers should publish
// endpoint hosts from this, not from their configured intent, so `platformctl
// inventory` reports real exposure (docs/planning/07 Gate 1 checkbox 3).
func (s ContainerState) HostAddr(containerPort int) string {
	for _, p := range s.Ports {
		if p.ContainerPort == containerPort && p.HostPort != 0 {
			host := p.HostIP
			if host == "" || host == "0.0.0.0" || host == "::" {
				// An all-interfaces bind is reachable via loopback; report
				// the loopback form so pasted configs work everywhere.
				host = "127.0.0.1"
			}
			return net.JoinHostPort(host, strconv.Itoa(p.HostPort))
		}
	}
	return ""
}

// ManagedNetwork/ManagedVolume report a labeled non-container object for GC
// inspection (`platformctl gc plan`, docs/planning/07 §1.3) — the same
// ownership-label contract as ContainerState, without the container-specific
// fields neither object has.
type ManagedNetwork struct {
	Name   string
	Labels map[string]string
}

type ManagedVolume struct {
	Name   string
	Labels map[string]string
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
	// ListManagedNetworks/ListManagedVolumes report every network/volume
	// (Docker: networks and volumes; Kubernetes: managed Namespaces and
	// PersistentVolumeClaims) carrying the ownership label, independent of
	// whether any container currently references them — the GC surface for
	// `platformctl gc plan`.
	ListManagedNetworks(ctx context.Context) ([]ManagedNetwork, error)
	ListManagedVolumes(ctx context.Context) ([]ManagedVolume, error)
	// Logs returns the last `tail` lines of the container's combined
	// stdout/stderr, for diagnostics (`platformctl doctor`, CLI log
	// retrieval). tail <= 0 requests the runtime's default tail length.
	Logs(ctx context.Context, name string, tail int) (string, error)
	// ReadFile returns the content of a file previously placed by
	// ContainerSpec.Files. Providers use it to recover bootstrap material
	// (e.g. the previous admin password during rotation) without that
	// material ever living in inspectable env vars.
	ReadFile(ctx context.Context, name, path string) ([]byte, error)
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
