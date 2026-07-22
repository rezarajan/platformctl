// Package runtime defines the ContainerRuntime port and its value types.
// Every method is Ensure*, not Create* — idempotent-by-contract at the
// interface boundary. See docs/planning/02-architecture.md §4.1.
package runtime

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
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

// Port audience constants (docs/planning/08 F2, docs/planning/09 K10):
// every declared port states, explicitly, who may dial it — replacing the
// retired HostPort: 0 "in-network only" convention, which by construction
// left that intent implicit in whether a provider happened to zero a
// numeric field. AudienceHost/AudienceInternal below are the only valid values;
// the fake runtime refuses an EnsureContainer spec that leaves a port's
// Audience unset or misspelled, so an omission fails in unit tests before
// any adapter — permissive by design — has a chance to paper over it.
const (
	// AudienceHost: a dependent outside the runtime (this CLI process, an
	// operator) may need to dial this port. Docker publishes it to the
	// host; Kubernetes additionally promotes it per the container's
	// AccessMode (node-port/load-balancer/port-forward).
	AudienceHost = "host"
	// AudienceInternal: only other containers/pods on the same network may
	// dial this port. Docker never publishes it to the host (the network's
	// own DNS already reaches it); Kubernetes still creates a Service port
	// for it (in-cluster DNS needs one), just never promotes it to a
	// host-reachable address.
	AudienceInternal = "internal"
)

type PortBinding struct {
	HostIP string
	// HostPort is meaningful only when Audience is AudienceHost: the host
	// port to publish to, or 0 to let the runtime assign one. Ignored for
	// AudienceInternal.
	HostPort      int
	ContainerPort int
	// Audience is one of the Audience* constants above. Required — see the
	// package-level doc on those constants for what an omission costs.
	Audience string
	Protocol string // "tcp" (default) | "udp"
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
// `configuration.imagePullSecretRef` via the same reconciler.Request.Secrets
// it uses for any other credential (e.g. postgres's superuser password),
// never a bare reference the adapter would need to re-resolve.
type ImagePullAuth struct {
	Username string
	Password string
	// Registry is the registry hostname these credentials are for (e.g.
	// "ghcr.io"). Optional — Docker infers it from the image reference when
	// empty; Kubernetes' dockerconfigjson Secret needs it explicit.
	Registry string
}

// Access modes for a container's CLI-side (outside-the-cluster) admin
// reachability (docs/planning/08 B1). Docker always publishes to the host,
// so these only differentiate Kubernetes behavior; the Docker and fake
// adapters accept every value and ignore it.
const (
	AccessPortForward  = ""              // default: an ephemeral client-go port-forward tunnel per EnsureReachable call
	AccessNodePort     = "node-port"     // Service type NodePort; reachable at <a-node-ip>:<node-port>
	AccessLoadBalancer = "load-balancer" // Service type LoadBalancer; reachable at the provisioned ingress address
	AccessInCluster    = "in-cluster"    // no host-reachable address; EnsureReachable refuses, naming the mode
)

type ContainerSpec struct {
	Name string
	// AccessMode selects how EnsureReachable makes this container's ports
	// reachable from outside the runtime (one of the Access* constants
	// above). Docker/fake ignore it — a published port is already
	// host-reachable by construction.
	AccessMode string
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
	// Entrypoint, when non-nil, REPLACES the image's own ENTRYPOINT — unlike
	// Cmd, which APPENDS to it (docs/planning/09 Class 5, the K1 lesson).
	// nil (the default) leaves the image's ENTRYPOINT untouched, the
	// pre-existing behavior every provider that only sets Cmd still gets.
	// Set this when Cmd alone cannot be trusted to run under a shell
	// regardless of the image's own entrypoint — the dbjob mechanism
	// (internal/adapters/providers/dbjob) is the motivating case: minio/mc's
	// image ENTRYPOINT is ["mc"], so a bare Cmd: ["sh", "-c", script] ran as
	// "mc sh -c ...", an instant, silent failure (docs/planning/08 C6 review
	// finding 1; docs/adr/007-backup-restore.md). Docker maps this to
	// Config.Entrypoint; Kubernetes maps it to container.Command (the mirror
	// image of Cmd/Args, per the same K1 lesson).
	Entrypoint []string
	Networks   []string
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
	// Sysctls sets per-namespace kernel parameters at container-create time
	// (Docker: HostConfig.Sysctls / `docker run --sysctl`) — the only way
	// to make some of them writable at all: e.g. net.ipv4.ip_forward
	// cannot be flipped from inside an already-running unprivileged
	// container (writing /proc/sys/net/ipv4/ip_forward fails "read-only
	// file system" even with SecurityContext.CapAdd: ["NET_ADMIN"]) unless
	// it was named here when the container was created (docs/adr/023
	// Decision 5, found live). nil/empty is today's behavior, byte-for-byte,
	// for every provider that doesn't set this. Kubernetes does not
	// implement this field: a pod's securityContext.sysctls entry needs
	// the sysctl to be in the cluster's "safe" allowlist or the node's
	// kubelet to opt into unsafe sysctls — a cluster-operator decision
	// this codebase has no way to make on the operator's behalf, unlike
	// NET_ADMIN (granted per-pod unconditionally). The fake adapter
	// records the value (round-trips through Inspect for tests) without
	// interpreting it.
	Sysctls map[string]string
	// Replicas is the desired size of this spec's replica set
	// (docs/adr/004-replicas-and-identity.md). 0 and 1 are equivalent to
	// today's single-container behavior, byte-for-byte — use ReplicaCount()
	// rather than comparing this field directly. N > 1 requires the
	// HighAvailability feature gate (enforced by application/registry's
	// runtime decorator, checked on every EnsureContainer call) and fans
	// out to N distinct runtime-managed units, addressed collectively by
	// Name and individually by ordinal name (OrdinalName, "<Name>-0" ..
	// "<Name>-(N-1)").
	Replicas int
	// StableIdentity selects the ordinal-set shape at any replica count
	// (docs/adr/017-redpanda-multibroker-and-replica-state.md §a.2, amending
	// docs/adr/004's original "meaningful only when Replicas > 1"): even a
	// ReplicaCount() of 1 produces ordinal "<Name>-0" (K8s: a 1-replica
	// StatefulSet; Docker/fake: one ordinal-named container), never the bare
	// single-container shape — which is what lets a stateful cluster scale
	// 1 -> N -> 1 in place without ever crossing a shape boundary. It
	// additionally gives each ordinal its own persistent volume set (K8s:
	// StatefulSet + volumeClaimTemplates; Docker: one Docker volume per
	// "<VolumeMount.VolumeName>-<ordinal>") and a stable per-ordinal
	// hostname reachable independent of which other ordinals are up (K8s:
	// headless Service; Docker: the ordinal's own container name, always
	// resolvable). false means replicas are interchangeable pure-compute
	// units with no per-ordinal storage or identity beyond the ordinal name
	// itself (e.g. Trino workers, docs/adr/006-compute-engines.md) —
	// Replicas > 1 alone is enough for horizontal scaling, and Replicas <= 1
	// with StableIdentity false remains today's single-container behavior,
	// byte-for-byte.
	StableIdentity bool
}

// ReplicaCount normalizes Replicas: 0 (or any value <= 1) means exactly 1,
// preserving today's single-container default for every ContainerSpec that
// never set the field.
func (s ContainerSpec) ReplicaCount() int {
	if s.Replicas <= 0 {
		return 1
	}
	return s.Replicas
}

// OrdinalName returns the stable, ordinal-suffixed name of replica i (0-based)
// of a replica set called name: "<name>-<i>". Every adapter uses this exact
// format, so an ordinal name is portable across runtimes and predictable to
// callers (e.g. a provider assembling a seed list without calling into the
// runtime first — docs/adr/004-replicas-and-identity.md).
func OrdinalName(name string, i int) string {
	return fmt.Sprintf("%s-%d", name, i)
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
	// ReadyReplicas is the number of replicas currently observed ready, out
	// of the ContainerSpec.Replicas requested (1 for a non-replicated
	// container: ReadyReplicas is 1 when Healthy, 0 otherwise). Healthy
	// reports "at least one replica ready" — it never flips false merely
	// because ReadyReplicas < the desired count; a provider that cares
	// about full-quorum health (e.g. comparing ReadyReplicas against an
	// EventStream's declared replication factor) computes that itself
	// (docs/adr/004-replicas-and-identity.md, "the provider decides
	// meaning").
	ReadyReplicas int
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
	// RemoveNetwork must never delete containers/workloads as a side effect,
	// and must refuse (return a non-nil error, leaving the network intact)
	// while any container is still attached to it — the way Docker's
	// NetworkRemove reports "network has active endpoints". Providers that
	// share one network each call RemoveNetwork on Destroy and rely on this
	// refusal so the shared network outlives every member but the last; a
	// runtime that instead cascade-deletes the network's members (as deleting
	// a Kubernetes Namespace naively would) breaks that contract. Pinned by
	// the conformance suite's RemoveNetwork_refuses_while_container_attached.
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
	// EnsureReachable returns a "host:port" address this process (running
	// outside the cluster/daemon) can dial right now to reach the named
	// container's containerPort, plus a close func to release any tunnel
	// opened to provide it — callers must always call close when done, even
	// on the trivial passthrough adapters, so provider code stays portable.
	//
	// Contract (docs/planning/08 F3): the returned address must be
	// *currently dialable*, not merely plausible from declared intent or
	// object metadata — "the Service has a NodePort assigned" and "the
	// port-forward tunnel's SPDY stream is up" are both exists/ready
	// signals that can be true while the container's own process hasn't
	// called listen() yet (docs/planning/09 Class 2, K3/K11). Every adapter
	// absorbs that race itself — with a direct dial check (Docker), by
	// polling the observed address (Kubernetes node-port/load-balancer), or
	// by construction (fake) — so a caller that dials on the first attempt
	// sees a real connection, not a race it has to paper over itself. A
	// caller that wants to *wait* for readiness rather than fail once
	// should use runtime.WithReachable, which retries and re-resolves a
	// fresh address on each attempt rather than reusing one across the
	// whole wait window.
	//
	// Docker/fake: the already-published host address (ContainerSpec.Ports
	// intent already makes this host-reachable; close is a no-op).
	// Kubernetes: depends on the container's ContainerSpec.AccessMode —
	// port-forward opens an ephemeral client-go tunnel (close tears it
	// down), node-port/load-balancer resolve the Service's observed
	// address, in-cluster refuses with an error naming the mode
	// (docs/planning/08 B1).
	EnsureReachable(ctx context.Context, name string, containerPort int) (addr string, close func() error, err error)

	// ProbeReachable answers "can a container on network reach target
	// (host:port) right now" — from a vantage point *inside* network, not
	// from this process (docs/planning/08 C10). EnsureReachable/WithReachable
	// answer a different, host-audience question: "can this CLI process
	// dial in" — a managed forwarder, firewall, or network policy can make
	// those two answers diverge (an endpoint dialable from the host can be
	// unreachable from inside the network a dependent container actually
	// runs in, and vice versa), so a caller must not treat one as a proxy
	// for the other.
	//
	// Contract: dial target as a plain TCP connection from a vantage point
	// attached to the named network — an existing managed container/pod
	// already on it, or a transient probe unit created and torn down for
	// this call alone. A nil error means that TCP connect succeeded from
	// in-network. Implementations must never fall back to a host-side (or
	// any other out-of-network) dial to answer this — doing so would silently
	// report the wrong audience's truth, exactly the host/in-network
	// conflation this method exists to remove (ADR 015).
	//
	// Docker: exec's a dial inside an existing managed container attached to
	// network when one is available and conclusively answers; otherwise runs
	// a transient probe container (pinned image, scripts/pinned-images.txt)
	// on network and reads its exit status. Kubernetes: the same two-tier
	// strategy against an existing managed pod / an ephemeral pod in the
	// namespace named by network. Fake: the strict interpreter (ADR 015) —
	// reachable only when target names a fake-managed container attached to
	// network on a port that container's spec declares (either audience, since
	// an in-network vantage point isn't limited to Audience: host the way
	// EnsureReachable is); any other target (wrong network, undeclared port,
	// unknown host) errors.
	ProbeReachable(ctx context.Context, network, target string) error
}

// MemberSetRuntime is an optional ContainerRuntime capability (the same
// type-assert-an-optional-capability pattern as IngressCapableRuntime below)
// answering docs/adr/004's I7 addendum (docs/planning/08 §7.8): for a
// StableIdentity:false ("Deployment-shaped") replica set with more than one
// member, does OrdinalName(name, i) address a real, individually resolvable
// object on this runtime — or is the set's own bare Name the only address
// that resolves to anything, with "any one currently live member" as that
// address's whole meaning?
//
// Docker and the fake runtime do NOT implement this interface: ADR 004
// forces every replica onto a literal, ordinal-named object on those two
// runtimes regardless of StableIdentity (Docker containers must be uniquely
// named per host), so OrdinalName already addresses something real there,
// and the pre-existing per-ordinal EnsureReachable/Inspect loop in
// providerkit.ReachableURLs/ProbeConnectWorkerSet is correct as it stands —
// a runtime that doesn't implement this interface keeps that behavior,
// unchanged. Kubernetes' Deployment shape is different in kind, not degree:
// exactly one Deployment/Service pair exists for the whole set, and pod
// names are Kubernetes-assigned random suffixes, never "<name>-<i>"
// (internal/adapters/runtime/kubernetes/container_remove.go's findOrdinalPod
// doc comment) — there is no ordinal object on that runtime to resolve at
// all, so every OrdinalName lookup fails outright
// ("no member of %q (%d ordinals) is currently reachable"), even though the
// set itself is healthy. Kubernetes implements this interface to say so;
// its own EnsureReachable/Inspect, called with the set's bare Name, already
// give the right answer (a Service or ready-pod label selector picks a live
// member; Inspect(name) already reports the Deployment's own aggregate
// ReadyReplicas) — no new Kubernetes-adapter reachability code was needed,
// only this signal plus the two callers above choosing when to use it.
//
// Connect workers are interchangeable members of one Kafka-consumer-group
// rebalancing set (debezium/s3sink's workers > 1, docs/planning/08 C3): "any
// one live member can serve the group's REST API" is the semantically
// correct address, exactly what a Kubernetes Service already gives for
// free — a genuinely better address than Docker's own per-ordinal failover
// list, not a lesser one.
type MemberSetRuntime interface {
	// AddressesMembersCollectively is true when this runtime has no
	// separately-addressable ordinal object for a StableIdentity:false
	// replica set — callers must resolve the whole set once, by its own
	// Name, instead of iterating OrdinalName(name, 0..members-1).
	AddressesMembersCollectively() bool
}

// IngressSpec describes one HTTP route to publish through the runtime's own
// native ingress mechanism (docs/planning/08 C7, docs/adr/018): Kubernetes
// realizes it as a networking.k8s.io/v1 Ingress object routing Host(Host) to
// an existing Service. Docker has no native ingress concept — the `ingress`
// provider realizes Docker-runtime HTTP routing entirely itself, via a
// shared reverse-proxy container it manages with EnsureContainer plus its
// own admin-API reconciliation (see internal/adapters/providers/ingress) —
// so this spec, and IngressCapableRuntime below, exist for Kubernetes only.
type IngressSpec struct {
	// Name is the Ingress object's own name (conventionally the realizing
	// Connection's runtime object name).
	Name string
	// Namespace is the Connection's owning Provider's runtime.network
	// (already-created by EnsureNetwork).
	Namespace string
	// Host is the Host(...) match rule, e.g. "nessie.localhost".
	Host string
	// TargetName is the existing Service name to route to — the upstream
	// resource's own runtime object name (naming.RuntimeObjectName),
	// resolved from Connection.spec.target by the ingress provider, never
	// constructed by this port (docs/adr/015).
	TargetName string
	TargetPort int
	Labels     map[string]string
	// TLSSecretName, when non-empty, adds spec.tls to the Ingress: this
	// Host routes through the named kubernetes.io/tls Secret
	// (docs/planning/08 C8, docs/adr/018 addendum). The Secret must
	// already exist — either materialized by this same provider via
	// EnsureTLSSecret (spec.tls.secretRef/selfSigned) or referenced by
	// name only (spec.tls.secretName, typically cert-manager-managed;
	// platformctl never creates/deletes that one). Empty means plaintext
	// HTTP, the pre-C8 behavior unchanged.
	TLSSecretName string
}

// IngressState reports what EnsureIngress/GetIngress observed — Host,
// TargetName, and TargetPort mirror the live object's own rule/backend (so a
// caller can drift-check them against IngressSpec without a second read),
// Address is the ingress controller's own observed load-balancer address
// when the cluster's controller publishes one (best-effort; empty when
// unknown, e.g. no LoadBalancer support on a local cluster).
type IngressState struct {
	Host       string
	TargetName string
	TargetPort int
	Address    string
	// TLSSecretName mirrors IngressSpec.TLSSecretName as observed on the
	// live object — empty when the Ingress carries no spec.tls block.
	TLSSecretName string
}

// IngressCapableRuntime is an optional ContainerRuntime capability
// (docs/adr/018's "provider type-asserts a runtime," the mirror image of the
// engine type-asserting an optional Provider capability like SpecValidator):
// implemented only by the Kubernetes adapter. A provider that wants
// native-ingress realization type-asserts req.Runtime against this
// interface rather than importing any concrete adapter package (the
// domain/ports/adapters layering invariant forbids that); on Docker/fake the
// assertion fails and the caller takes its own Docker-native path instead.
type IngressCapableRuntime interface {
	// EnsureIngress creates or updates the named Ingress to match spec —
	// idempotent, like every other Ensure* method on this port.
	EnsureIngress(ctx context.Context, spec IngressSpec) (IngressState, error)
	// GetIngress reads the current state of a previously-ensured Ingress
	// without mutating it, for drift detection. found is false when no such
	// Ingress exists (e.g. removed out-of-band).
	GetIngress(ctx context.Context, namespace, name string) (state IngressState, found bool, err error)
	RemoveIngress(ctx context.Context, namespace, name string) error

	// EnsureTLSSecret/GetTLSSecret/RemoveTLSSecret (docs/planning/08 C8,
	// docs/adr/018 addendum) manage a kubernetes.io/tls-shaped Secret
	// (keys "tls.crt"/"tls.key") — used both for a Connection's own
	// provided/self-signed leaf certificate (referenced by an Ingress's
	// TLSSecretName) and for the ingress provider's Provider-scoped local
	// CA keypair (never referenced by any Ingress — stored only so it can
	// be read back and reused rather than regenerated on every apply).
	// Kubernetes-only, like the three Ingress methods above; a
	// cert-manager-managed Secret (spec.tls.secretName) is only ever read
	// via GetTLSSecret, never written by Ensure/RemoveTLSSecret.
	EnsureTLSSecret(ctx context.Context, namespace, name string, certPEM, keyPEM []byte, labels map[string]string) error
	// GetTLSSecret reads an existing Secret's cert/key material. found is
	// false when no such Secret exists (not yet provisioned, or — for a
	// cert-manager-managed name — not yet issued).
	GetTLSSecret(ctx context.Context, namespace, name string) (certPEM, keyPEM []byte, found bool, err error)
	// RemoveTLSSecret deletes a Secret this provider created (never called
	// for a cert-manager-managed spec.tls.secretName). A no-op, not an
	// error, if already gone — the same idempotent-Destroy contract as
	// RemoveIngress.
	RemoveTLSSecret(ctx context.Context, namespace, name string) error
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
	// LabelReplicaBase and LabelReplicaOrdinal (docs/adr/004) let an
	// adapter with no native replica-set object (Docker: N separately-named
	// containers) group them back into one logical set via a label query,
	// mirroring the grouping Kubernetes gets for free from StatefulSet/
	// Deployment ownership. LabelReplicaBase names the replica set's
	// logical ContainerSpec.Name; LabelReplicaOrdinal is the 0-based index.
	// Only set on a container that is part of a Replicas > 1 set.
	LabelReplicaBase    = "io.datascape.replica-base"
	LabelReplicaOrdinal = "io.datascape.replica-ordinal"
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

// ScaledWait multiplies a wait/settle deadline by DATASCAPE_WAIT_SCALE
// (float, default 1). Deadlines in this codebase bound FAILURE REPORTING,
// never success — success is always an observed condition (doc 02 §4.1,
// NFR-11) — so they can never be provably sufficient for every
// environment (emulated architectures, cold caches, starved CI runners).
// Rather than each site guessing bigger constants, slow environments set
// one knob and every bounded wait widens proportionally; the conditions
// being waited for are unchanged. Values below 1 are permitted (fast-fail
// experimentation) but clamped to 0.1.
func ScaledWait(d time.Duration) time.Duration {
	return time.Duration(float64(d) * waitScale())
}

var waitScaleOnce sync.Once
var waitScaleVal float64

func waitScale() float64 {
	waitScaleOnce.Do(func() {
		waitScaleVal = 1
		if s := os.Getenv("DATASCAPE_WAIT_SCALE"); s != "" {
			if f, err := strconv.ParseFloat(s, 64); err == nil {
				if f < 0.1 {
					f = 0.1
				}
				waitScaleVal = f
			}
		}
	})
	return waitScaleVal
}
