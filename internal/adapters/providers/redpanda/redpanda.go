// Package redpanda reconciles a Redpanda broker container (via the container
// runtime) and, post-health, creates/updates topics and retention settings
// via the Kafka admin protocol. First real technology provider (Phase 2).
package redpanda

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rezarajan/platformctl/internal/adapters/providers/providerkit"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/eventstream"
	"github.com/rezarajan/platformctl/internal/domain/hostport"
	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

var httpClient = &http.Client{Timeout: 10 * time.Second}

const (
	internalKafkaPort = 29092
	externalKafkaPort = 9092
	// schemaRegistryPort is Redpanda's built-in Confluent-compatible schema
	// registry (pandaproxy's sibling listener) — one fixed port, unlike
	// Kafka's dual INTERNAL/EXTERNAL listeners, because HTTP schema-registry
	// clients (Debezium/Connect converters) dial it directly with no
	// broker-style redirect protocol to decouple (docs/planning/08 D1).
	schemaRegistryPort = 8081
	// adminPort is Redpanda's admin API — always on, unlike the opt-in
	// schema registry. It also carries the /public_metrics
	// Prometheus-compatible metrics endpoint natively, zero extra
	// containers (docs/planning/08 C9).
	adminPort = 9644
	// rpcPort is Redpanda's internal broker-to-broker RPC (Raft, data
	// movement) listener — declared Audience: internal on the multi-broker
	// path (docs/adr/017 §a.3) per ADR 015 F2: brokers dial each other's
	// RPC, and every listener a dependent dials must be declared.
	rpcPort      = 33145
	defaultImage = "docker.redpanda.com/redpandadata/redpanda:v24.2.1@sha256:f60d828ed6cafd7ce4c9b987ff71699895b81fe53f1d0e27ebf045277fcff21a"
)

// Provider holds no cross-call state (docs/planning/08 F5): every method
// receives everything it needs — the resource being acted on, the runtime,
// and the realizing Provider's own resource/config — via reconciler.Request.
type Provider struct{}

func New() *Provider { return &Provider{} }

func (p *Provider) Type() string { return "redpanda" }

// SupportedSchemaFormats implements reconciler.SchemaRegistryCapableProvider:
// the answer is config-dependent (configuration.schemaRegistry: enabled),
// not a static per-type capability — a Binding declaring avro/protobuf
// against a broker without the registry enabled fails at validate with the
// standard capability-error shape (docs/planning/08 D1).
func (p *Provider) SupportedSchemaFormats(cfg provider.Provider) []string {
	if schemaRegistryEnabled(cfg) {
		return []string{"avro", "json", "protobuf"}
	}
	return []string{"json"}
}

// schemaRegistryEnabled reads spec.configuration.schemaRegistry (an
// enabled|disabled enum, mirroring D7's lifecycle.versioning:
// enabled|suspended convention) — unset/anything else is disabled.
func schemaRegistryEnabled(cfg provider.Provider) bool {
	v, _ := cfg.Configuration["schemaRegistry"].(string)
	return v == "enabled"
}

// schemaRegistryHostPort resolves the schema registry's host-published port,
// auto-allocated (like every other host port here) from a name distinct from
// the broker's own Kafka host port — Resolve hashes on name alone, so reusing
// brokerName would collide the two ports whenever both are auto-allocated.
func schemaRegistryHostPort(cfg provider.Provider, name string) int {
	configured := 0
	if v, ok := cfg.Configuration["schemaRegistryPort"]; ok {
		switch n := v.(type) {
		case int:
			configured = n
		case float64:
			configured = int(n)
		}
	}
	return hostport.Resolve(configured, name+"-schema-registry")
}

// schemaRegistryInternalAddr is the registry's address reachable from other
// containers on the shared network (Debezium's Avro/Protobuf converters) —
// deterministic by construction (Docker/Kubernetes DNS resolves a
// container/Service name within the shared network), exactly like
// internalAddr for Kafka. This is the *published* value (providerState +
// endpoint fact), not a guess a consumer re-derives independently.
func schemaRegistryInternalAddr(name string) string {
	return fmt.Sprintf("http://%s:%d", name, schemaRegistryPort)
}

// adminHostPort resolves the admin API's host-published port, auto-allocated
// from a name distinct from the broker's own Kafka host port (see
// schemaRegistryHostPort's doc comment — same reasoning, a different fixed
// suffix so the two never collide when both are auto-allocated).
func adminHostPort(cfg provider.Provider, name string) int {
	configured := 0
	if v, ok := cfg.Configuration["adminPort"]; ok {
		switch n := v.(type) {
		case int:
			configured = n
		case float64:
			configured = int(n)
		}
	}
	return hostport.Resolve(configured, name+"-admin")
}

// metricsInternalAddr is the broker's Prometheus-compatible metrics
// endpoint, reachable from other containers on the shared network (the
// prometheus provider's scrape target, docs/planning/08 C9) — the
// *published* value (providerState + endpoint fact), never a guess a
// consumer re-derives independently.
func metricsInternalAddr(name string) string {
	return fmt.Sprintf("http://%s:%d/public_metrics", name, adminPort)
}

func brokerName(provEnv resource.Envelope) string { return naming.RuntimeObjectName(provEnv) }

// hostPort is providerkit.HostPort at the "kafkaPort" config key — kept as a
// named wrapper (rather than inlined at each call site) because
// advertisedAddr's redpanda_test.go coverage dials it directly by name.
func hostPort(cfg provider.Provider, name string) int {
	return providerkit.HostPort(cfg, name, "kafkaPort")
}

// internalAddr is the broker address reachable from containers on the shared
// network (Debezium, sink connectors).
func internalAddr(name string) string {
	return name + ":" + strconv.Itoa(internalKafkaPort)
}

// KafkaBootstrapAddress implements reconciler.KafkaBootstrapAddressProvider:
// the in-network bootstrap address(es), exposed as a capability method so a
// Connect worker's bootstrapServers can be inferred from the manifest graph
// without this Provider having reconciled yet (docs/planning/08 E2). With
// configuration.brokers declared it is the comma-joined ordinal list
// (docs/adr/017 §a.4) — still computed from manifest facts alone (name +
// declared count + fixed internal port); otherwise it is the legacy single
// internalAddr(name), unchanged.
func (p *Provider) KafkaBootstrapAddress(name string, cfg provider.Provider) string {
	if n, declared := brokersDeclared(cfg); declared && n >= 1 {
		return brokerInternalList(name, n)
	}
	return internalAddr(name)
}

// ValidateStreamReplication implements reconciler.StreamReplicationValidator
// (docs/adr/017 §a.7): an EventStream's spec.replication must not exceed
// this Provider's broker count — checked at validate, before anything is
// scheduled. The error names both numbers; the compatibility layer prefixes
// the resource identity.
func (p *Provider) ValidateStreamReplication(cfg provider.Provider, replication int) error {
	n, declared := brokersDeclared(cfg)
	if !declared || n < 1 {
		n = 1
	}
	if replication > n {
		return fmt.Errorf("spec.replication %d exceeds the configured broker count %d (spec.configuration.brokers); raise brokers or lower replication", replication, n)
	}
	// Redpanda refuses even replication factors outright ("replication
	// factor must be odd" — Raft quorum), so an even factor is an
	// apply-time-only failure unless refused here (ADR 011; caught live on
	// the C2 Kubernetes leg with replication: 2).
	if replication > 1 && replication%2 == 0 {
		return fmt.Errorf("spec.replication %d is even; redpanda requires an odd replication factor (Raft quorum)", replication)
	}
	return nil
}

// accessMode selects how CLI-side admin calls (reconcileTopic, Probe,
// Destroy for EventStream) reach the broker on Kubernetes — one of the
// runtime.Access* constants (docs/planning/08 B1). Docker ignores it: the
// broker's host port is already reachable by construction.
func accessMode(cfg provider.Provider) string {
	m, _ := cfg.RuntimeConfig["access"].(string)
	return m
}

// advertisedAddr is the address baked into the broker's own EXTERNAL
// listener config at startup (see reconcileBroker's --advertise-kafka-addr)
// — the address the broker itself tells a connected Kafka client to use for
// follow-up requests (Kafka's own client/broker protocol, independent of
// platformctl). On Kubernetes this string is not necessarily dialable at
// all: node-port's real port isn't known until the Service exists, and
// port-forward's tunnel port is different on every call, so nothing fixed
// at container-start time could ever be correct. kafka.go's adminClient
// resolves this: every client dial to exactly this address is intercepted
// and redirected to whatever reachableAddr just resolved to, decoupling
// "what the broker advertises" from "where a request actually goes" — the
// broker's own protocol never needs to be told the (changing) truth.
func advertisedAddr(cfg provider.Provider, name string) string {
	return "127.0.0.1:" + strconv.Itoa(providerkit.HostPort(cfg, name, "kafkaPort")) // archtest:allow-loopback: sentinel never dialed directly, only matched+redirected by kafka.go's kgo.Dialer
}

// brokersDeclared reads spec.configuration.brokers. declared=false (the key
// absent) selects the pre-C2 single-container shape, byte-for-byte;
// declared=true (any value >= 1, validated by ValidateSpec) opts into the
// ordinal-set shape — docs/adr/017 §a.1's deliberate asymmetry between
// "unset" and "explicitly 1", which is what makes brokers a same-shape
// in-place scale knob while grandfathering every pre-C2 deployment
// untouched.
func brokersDeclared(cfg provider.Provider) (int, bool) {
	v, ok := cfg.Configuration["brokers"]
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case int:
		return n, true
	case float64:
		return int(n), true
	}
	return 0, true
}

// ordinalToken is ordinal i's advertised EXTERNAL address — a deliberately
// undialable, stable, per-broker-unique token (docs/adr/017 §a.4): host
// ports are auto-assigned per ordinal and unknowable at container-start
// time, so nothing dialable could be baked in; kafka.go's dialer map
// redirects every dial of this token to the ordinal's currently-resolved
// EnsureReachable address. It must equal what the broker's own
// --advertise-kafka-addr expands to ("${HOSTNAME}:9092" — the in-container
// hostname of a StableIdentity ordinal is its ordinal name on every
// adapter).
func ordinalToken(name string, i int) string {
	return runtime.OrdinalName(name, i) + ":" + strconv.Itoa(externalKafkaPort)
}

// brokerInternalList is the comma-joined in-network bootstrap list of every
// ordinal's INTERNAL listener — real, resolvable addresses on both runtimes
// (ordinal container names / StatefulSet headless-Service pod DNS), derived
// from the naming authority plus the declared port, never constructed from
// runtime observation (ADR 015 F4).
func brokerInternalList(name string, n int) string {
	addrs := make([]string, n)
	for i := range addrs {
		addrs[i] = internalAddr(runtime.OrdinalName(name, i))
	}
	return strings.Join(addrs, ",")
}

// clusterDial resolves, per ordinal, an address this process can dial right
// now (EnsureReachable against the ordinal name — both adapters support
// ordinal addressing) and pairs it with the ordinal's advertised token for
// kafka.go's dialer map. An unreachable ordinal (killed, mid-heal) is
// skipped: admin operations proceed against the survivors, which is what
// lets drift-probe and re-apply work while a broker is down (docs/adr/017
// §a.4). Errors only when no broker at all is reachable.
func clusterDial(ctx context.Context, rt runtime.ContainerRuntime, name string, n int) (map[string]string, []string, func(), error) {
	dialMap := make(map[string]string, n)
	seeds := make([]string, 0, n)
	closers := make([]func() error, 0, n)
	for i := 0; i < n; i++ {
		ord := runtime.OrdinalName(name, i)
		addr, closeAddr, err := rt.EnsureReachable(ctx, ord, externalKafkaPort)
		if err != nil {
			continue
		}
		token := ordinalToken(name, i)
		dialMap[token] = addr
		seeds = append(seeds, token)
		closers = append(closers, closeAddr)
	}
	if len(seeds) == 0 {
		return nil, nil, nil, fmt.Errorf("no broker of %q (%d ordinals) is currently reachable", name, n)
	}
	closeAll := func() {
		for _, c := range closers {
			_ = c()
		}
	}
	return dialMap, seeds, closeAll, nil
}

// clusterCmdScript is the multi-broker start command — one identical script
// for every ordinal, per ADR 004's identical-spec contract; all per-broker
// differentiation derives from HOSTNAME, which equals the ordinal name on
// both runtimes (docs/adr/017 §a.3): the node ID is the ordinal index,
// ordinal 0 founds the cluster and the rest join via it as seed (membership
// persists in each ordinal's data volume after first join), and the
// advertised addresses are the ordinal's own INTERNAL DNS name plus the
// EXTERNAL token ordinalToken documents.
func clusterCmdScript(name string) string {
	seed0 := runtime.OrdinalName(name, 0)
	return fmt.Sprintf(`ORD="${HOSTNAME##*-}"
SEEDS=""
if [ "$ORD" != "0" ]; then SEEDS="--seeds %s:%d"; fi
exec rpk redpanda start --overprovisioned --smp 1 --memory 512M --reserve-memory 0M --check=false \
  --node-id "$ORD" $SEEDS \
  --kafka-addr INTERNAL://0.0.0.0:%d,EXTERNAL://0.0.0.0:%d \
  --advertise-kafka-addr "INTERNAL://${HOSTNAME}:%d,EXTERNAL://${HOSTNAME}:%d" \
  --rpc-addr 0.0.0.0:%d \
  --advertise-rpc-addr "${HOSTNAME}:%d"`,
		seed0, rpcPort,
		internalKafkaPort, externalKafkaPort,
		internalKafkaPort, externalKafkaPort,
		rpcPort, rpcPort)
}

// waitSchemaRegistryReady polls the registry's /subjects endpoint via
// runtime.WithReachable (docs/planning/09 Class 2 / F1) so every attempt gets
// a freshly-resolved address rather than reusing one across the whole wait —
// the same defensive pattern nessie's waitAPIReady documents for a
// port-forward tunnel opened while the app is still starting.
func waitSchemaRegistryReady(ctx context.Context, rt runtime.ContainerRuntime, name string, timeout time.Duration) error {
	opts := runtime.ReachableOptions{Timeout: timeout, Interval: 2 * time.Second}
	err := runtime.WithReachable(ctx, rt, name, schemaRegistryPort, opts, func(ctx context.Context, addr string) error {
		if !httpOK(ctx, "http://"+addr+"/subjects") {
			return fmt.Errorf("schema registry did not answer 200 on /subjects")
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("schema registry did not become ready within %s: %w", timeout, err)
	}
	return nil
}

func httpOK(ctx context.Context, url string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func (p *Provider) Reconcile(ctx context.Context, req reconciler.Request) (status.Status, error) {
	res := req.Resource
	switch res.Kind {
	case "Provider":
		return p.reconcileBroker(ctx, req)
	case "EventStream":
		return p.reconcileTopic(ctx, req)
	default:
		return status.Status{}, fmt.Errorf("redpanda provider cannot reconcile kind %s", res.Kind)
	}
}

func (p *Provider) reconcileBroker(ctx context.Context, req reconciler.Request) (status.Status, error) {
	rt := req.Runtime
	st := status.Status{}
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return st, err
	}
	name := brokerName(req.Provider)
	image, _ := cfg.Configuration["image"].(string)
	if image == "" {
		image = defaultImage
	}

	// configuration.brokers declared (any N >= 1) opts into the ordinal-set
	// shape (docs/adr/017 §a.1); undeclared keeps the pre-C2 single-container
	// path below, byte-for-byte.
	if n, declared := brokersDeclared(cfg); declared {
		if n < 1 {
			return st, fmt.Errorf("spec.configuration.brokers must be a positive integer, got %v", cfg.Configuration["brokers"])
		}
		return p.reconcileBrokerSet(ctx, req, cfg, name, image, n)
	}

	registryEnabled := schemaRegistryEnabled(cfg)
	cmd := []string{
		"redpanda", "start",
		"--overprovisioned", "--smp", "1", "--memory", "512M", "--reserve-memory", "0M",
		"--node-id", "0", "--check=false",
		"--kafka-addr", fmt.Sprintf("INTERNAL://0.0.0.0:%d,EXTERNAL://0.0.0.0:%d", internalKafkaPort, externalKafkaPort),
		"--advertise-kafka-addr", fmt.Sprintf("INTERNAL://%s:%d,EXTERNAL://%s", name, internalKafkaPort, advertisedAddr(cfg, name)),
		// No --admin-addr flag: `rpk redpanda start` has none (unlike
		// --kafka-addr/--schema-registry-addr) — the image's own
		// /etc/redpanda/redpanda.yaml already binds the admin API (metrics,
		// cluster control) to 0.0.0.0:9644 by default, so nothing further is
		// needed to make it reachable on the shared network.
	}
	ports := []runtime.PortBinding{
		{HostPort: hostPort(cfg, name), ContainerPort: externalKafkaPort, Audience: runtime.AudienceHost},
		// INTERNAL (29092) is Audience: internal — no host publish, but
		// still declared so the Kubernetes adapter's Service actually
		// carries a port for it — a Service only forwards ports present in
		// ContainerSpec.Ports (docs/planning/08 B8), unlike a Docker
		// bridge network, which reaches every container port regardless of
		// what's published. Docker itself already reached INTERNAL fine
		// without this; this declaration is a documented no-op there
		// (portMaps skips the host-binding side for Audience: internal).
		{ContainerPort: internalKafkaPort, Audience: runtime.AudienceInternal},
		{HostPort: adminHostPort(cfg, name), ContainerPort: adminPort, Audience: runtime.AudienceHost},
	}
	if registryEnabled {
		// One listener bound to all interfaces: unlike Kafka, the schema
		// registry's HTTP clients dial it directly with no advertised-address
		// redirect protocol to decouple (see reachableAddr's doc comment for
		// why Kafka needs one and this doesn't).
		cmd = append(cmd, "--schema-registry-addr", fmt.Sprintf("0.0.0.0:%d", schemaRegistryPort))
		ports = append(ports, runtime.PortBinding{HostPort: schemaRegistryHostPort(cfg, name), ContainerPort: schemaRegistryPort, Audience: runtime.AudienceHost})
	}

	ctrState, err := providerkit.EnsureInstance(ctx, rt, providerkit.InstanceSpec{
		Namespace: req.Provider.Metadata.Namespace,
		Name:      name,
		Network:   providerkit.Network(cfg),
		Volume:    &providerkit.InstanceVolume{Name: name + "-data", MountPath: "/var/lib/redpanda/data"},
		Container: runtime.ContainerSpec{
			Image:      image,
			AccessMode: accessMode(cfg),
			Cmd:        cmd,
			Ports:      ports,
			HealthCheck: &runtime.HealthCheck{
				Test:     []string{"CMD-SHELL", "rpk cluster health --exit-when-healthy || exit 1"},
				Interval: 2 * time.Second,
				Timeout:  5 * time.Second,
				Retries:  30,
			},
		},
		WaitTimeout: 120 * time.Second,
	})
	if err != nil {
		return st, err
	}
	if registryEnabled {
		if err := waitSchemaRegistryReady(ctx, rt, name, 120*time.Second); err != nil {
			return st, err
		}
	}

	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonBrokerHealthy}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	hostAddr := ctrState.HostAddr(externalKafkaPort) // observed binding, not intent
	metricsHostAddr := ctrState.HostAddr(adminPort)  // observed binding, not intent
	metricsHostURL := ""
	if metricsHostAddr != "" {
		metricsHostURL = "http://" + metricsHostAddr + "/public_metrics"
	}
	endpoints := endpoint.List{
		{Name: "kafka", Scheme: "kafka", Host: hostAddr, Internal: internalAddr(name), Insecure: true},
		{
			Name: "metrics", Scheme: "http", Host: metricsHostURL, Internal: metricsInternalAddr(name),
			Insecure: true, RuntimeName: name, ContainerPort: adminPort, Audience: runtime.AudienceHost,
		},
	}
	if registryEnabled {
		registryHostAddr := ctrState.HostAddr(schemaRegistryPort) // observed binding, not intent
		registryHostURL := ""
		if registryHostAddr != "" {
			registryHostURL = "http://" + registryHostAddr
		}
		endpoints = append(endpoints, endpoint.Endpoint{
			Name: "schema-registry", Scheme: "http", Host: registryHostURL, Internal: schemaRegistryInternalAddr(name),
			Insecure: true, RuntimeName: name, ContainerPort: schemaRegistryPort, Audience: runtime.AudienceHost,
		})
	}
	st.ProviderState = map[string]any{
		"containerId":  ctrState.ID,
		"kafkaAddr":    hostAddr,
		"internalAddr": internalAddr(name),
		endpoint.Key:   endpoints.ToState(),
	}
	return st, nil
}

// reconcileBrokerSet is reconcileBroker's multi-broker counterpart
// (docs/adr/017): n ordinal brokers via ContainerSpec.Replicas +
// StableIdentity, Ready gated on explicit cluster membership
// (waitClusterFormed), per-ordinal endpoint facts published alongside the
// aggregate. It does not
// use providerkit.EnsureInstance: the runtime owns the entire per-ordinal
// volume lifecycle for a StableIdentity set (docs/adr/004 — the provider
// must not call EnsureVolume for ordinal storage), so the single-volume
// skeleton doesn't fit (docs/planning/08 G1's "genuinely different shape"
// carve-out).
func (p *Provider) reconcileBrokerSet(ctx context.Context, req reconciler.Request, cfg provider.Provider, name, image string, n int) (status.Status, error) {
	rt := req.Runtime
	st := status.Status{}

	// Scale-down refusal (docs/adr/017 §a.5): an observed ordinal at or
	// beyond the desired count means the set was last applied larger —
	// pruning it would discard that broker's partition replicas. Refused
	// unconditionally; destructive intent is not plumbed into reconcile
	// (the engine's AllowDestructive is a destroy-time flag pair).
	observed := n
	for {
		_, found, err := rt.Inspect(ctx, runtime.OrdinalName(name, observed))
		if err != nil {
			return st, err
		}
		if !found {
			break
		}
		observed++
	}
	if observed > n {
		return st, fmt.Errorf("scaling spec.configuration.brokers down from %d to %d risks data loss (partition replicas on the removed brokers would be discarded) and is refused; restore brokers: %d, or destroy and recreate the Provider to shrink the cluster (docs/adr/017 §a.5)", observed, n, observed)
	}

	labels := runtime.ManagedLabels(req.Provider.Metadata.Namespace, "Provider", name, name)
	network := providerkit.Network(cfg)
	if err := rt.EnsureNetwork(ctx, runtime.NetworkSpec{Name: network, Labels: labels}); err != nil {
		return st, err
	}
	ctrState, err := rt.EnsureContainer(ctx, runtime.ContainerSpec{
		Name:       name,
		AccessMode: accessMode(cfg),
		Image:      image,
		// Entrypoint REPLACES the image's own ENTRYPOINT so the start
		// command runs under a shell regardless of it — required for the
		// HOSTNAME-derived per-ordinal identity (docs/adr/017 §a.3).
		Entrypoint: []string{"/bin/bash", "-c"},
		Cmd:        []string{clusterCmdScript(name)},
		Networks:   []string{network},
		// The runtime suffixes this volume per ordinal and owns its
		// lifecycle (docs/adr/004): "<name>-data-<i>" on Docker,
		// volumeClaimTemplates on Kubernetes.
		Volumes: []runtime.VolumeMount{{VolumeName: name + "-data", MountPath: "/var/lib/redpanda/data"}},
		Ports: []runtime.PortBinding{
			// Host ports auto-assigned per ordinal (HostPort 0): a fixed
			// pin cannot be combined with Replicas > 1 (docs/adr/004 known
			// limitation; ValidateSpec refuses the pin keys — docs/adr/017
			// §a.4).
			{ContainerPort: externalKafkaPort, Audience: runtime.AudienceHost},
			{ContainerPort: internalKafkaPort, Audience: runtime.AudienceInternal},
			{ContainerPort: rpcPort, Audience: runtime.AudienceInternal},
			{ContainerPort: adminPort, Audience: runtime.AudienceHost},
		},
		HealthCheck: &runtime.HealthCheck{
			// Cluster-scoped on purpose: WaitHealthy below then means "the
			// cluster formed", not merely "a process is up". Probe does NOT
			// rely on this signal for per-broker drift (docs/adr/017 §a.6).
			Test:     []string{"CMD-SHELL", "rpk cluster health --exit-when-healthy || exit 1"},
			Interval: 2 * time.Second,
			Timeout:  5 * time.Second,
			Retries:  30,
		},
		Labels:         labels,
		Replicas:       n,
		StableIdentity: true,
	})
	if err != nil {
		return st, err
	}
	if err := rt.WaitHealthy(ctx, name, 240*time.Second); err != nil {
		return st, err
	}
	// WaitHealthy returns at one-member-healthy (docs/adr/004's deliberate
	// at-least-one rule) — and ordinal 0 alone is a healthy 1-node cluster
	// before its peers join. Ready for a broker SET means "all brokers
	// joined" (docs/adr/017 §a.6), so wait for full membership before
	// declaring it (and before any same-apply EventStream reconcile creates
	// replicated topics against a partial cluster).
	if err := waitClusterFormed(ctx, rt, name, n, 180*time.Second); err != nil {
		return st, err
	}

	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonBrokerHealthy}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	providerState, err := brokerSetProviderState(ctx, rt, name, n, ctrState.ID)
	if err != nil {
		return st, err
	}
	st.ProviderState = providerState
	return st, nil
}

// brokerSetProviderState assembles the set shape's published facts
// (docs/adr/017, question b): the aggregate "kafka"/"metrics" endpoints plus
// one per-ordinal "kafka-<i>" fact each carrying the ordinal's observed host
// address — the only per-ordinal information that legitimately belongs in
// state, because a consumer cannot re-derive a runtime-allocated host
// binding (ADR 015 "publish, don't construct"). Everything else in the map
// stays aggregate; per-broker liveness is Probe-time observation, never
// persisted.
func brokerSetProviderState(ctx context.Context, rt runtime.ContainerRuntime, name string, n int, containerID string) (map[string]any, error) {
	internalList := brokerInternalList(name, n)
	ord0 := runtime.OrdinalName(name, 0)
	endpoints := endpoint.List{}
	var kafkaHost0, metricsHost0 string
	for i := 0; i < n; i++ {
		ord := runtime.OrdinalName(name, i)
		ordState, found, err := rt.Inspect(ctx, ord)
		if err != nil {
			return nil, err
		}
		host := ""
		if found {
			host = ordState.HostAddr(externalKafkaPort) // observed binding, not intent
		}
		if i == 0 {
			kafkaHost0 = host
			if found {
				if adminAddr := ordState.HostAddr(adminPort); adminAddr != "" {
					metricsHost0 = "http://" + adminAddr + "/public_metrics"
				}
			}
		}
		endpoints = append(endpoints, endpoint.Endpoint{
			Name: fmt.Sprintf("kafka-%d", i), Scheme: "kafka", Host: host, Internal: internalAddr(ord),
			Insecure: true, RuntimeName: ord, ContainerPort: externalKafkaPort, Audience: runtime.AudienceHost,
		})
	}
	endpoints = append(endpoint.List{
		// The aggregate "kafka" fact: in-network consumers bootstrap
		// against the full ordinal list; the host address is ordinal 0's
		// (host-side clients of a multi-broker cluster need client-side
		// address mapping regardless — docs/adr/017 §a.4).
		{Name: "kafka", Scheme: "kafka", Host: kafkaHost0, Internal: internalList, Insecure: true},
		// Ordinal 0's metrics only, for C9's single-target scrape model —
		// docs/adr/017 follow-ups.
		{
			Name: "metrics", Scheme: "http", Host: metricsHost0, Internal: metricsInternalAddr(ord0),
			Insecure: true, RuntimeName: ord0, ContainerPort: adminPort, Audience: runtime.AudienceHost,
		},
	}, endpoints...)

	return map[string]any{
		"containerId":  containerID,
		"kafkaAddr":    kafkaHost0,
		"internalAddr": internalList,
		"brokers":      n,
		endpoint.Key:   endpoints.ToState(),
	}, nil
}

func (p *Provider) reconcileTopic(ctx context.Context, req reconciler.Request) (status.Status, error) {
	res, rt := req.Resource, req.Runtime
	st := status.Status{}
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return st, err
	}
	name := brokerName(req.Provider)
	es, err := eventstream.FromEnvelope(res)
	if err != nil {
		return st, err
	}
	topic := res.Metadata.Name
	partitions := es.Partitions
	if partitions == 0 {
		partitions = 1
	}
	retentionMS, err := retentionMillis(es.RetentionDuration)
	if err != nil {
		return st, err
	}

	dialMap, seeds, closeAll, err := topicDial(ctx, rt, cfg, name)
	if err != nil {
		return st, err
	}
	defer closeAll()
	if err := ensureTopic(ctx, dialMap, seeds, topic, partitions, es.ReplicationFactor(), retentionMS); err != nil {
		return st, err
	}
	// Ready means serving (docs/planning/09 F3), applied at the topic level:
	// ensureTopic returning is not the same as the topic being probe-clean.
	// After a broker heal, membership rejoin precedes partition leadership
	// and metadata settling — a drift snapshot taken right after a
	// successful healing apply transiently failed ListTopics on a slow CI
	// runner (live-caught, 2026-07-22). Settle to a clean probe before
	// declaring Ready: on a healthy cluster the first attempt passes
	// immediately (zero added latency); on timeout, fail the reconcile
	// honestly with the last probe state instead of over-promising.
	if err := waitTopicSettled(ctx, dialMap, seeds, topic, partitions, es.ReplicationFactor(), retentionMS); err != nil {
		return st, err
	}

	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonTopicReconciled}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	st.ProviderState = map[string]any{"topic": topic, "partitions": partitions, "replication": es.ReplicationFactor()}
	return st, nil
}

// topicDial resolves the admin-connection inputs for a topic operation
// against this Provider's broker(s): the multi-broker dialer map when
// configuration.brokers is declared (docs/adr/017 §a.4), else the legacy
// single advertised-sentinel pair, unchanged.
func topicDial(ctx context.Context, rt runtime.ContainerRuntime, cfg provider.Provider, name string) (map[string]string, []string, func(), error) {
	if n, declared := brokersDeclared(cfg); declared && n >= 1 {
		return clusterDial(ctx, rt, name, n)
	}
	addr, closeAddr, err := providerkit.ReachableAddr(ctx, rt, name, externalKafkaPort)
	if err != nil {
		return nil, nil, nil, err
	}
	adv := advertisedAddr(cfg, name)
	return map[string]string{adv: addr}, []string{adv}, func() { _ = closeAddr() }, nil
}

func (p *Provider) Destroy(ctx context.Context, req reconciler.Request) error {
	res, rt := req.Resource, req.Runtime
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return err
	}
	name := brokerName(req.Provider)
	switch res.Kind {
	case "Provider":
		if err := rt.Remove(ctx, name); err != nil {
			return err
		}
		if _, declared := brokersDeclared(cfg); declared {
			// A StableIdentity set's per-ordinal volumes are runtime-owned
			// and adapter-named (Docker: "<name>-data-<i>"; Kubernetes STS
			// PVCs: "<name>-data-<name>-<i>"), so reclaim them through the
			// labeled-volume listing rather than guessing adapter naming
			// (docs/adr/004 "Removal is deliberately conservative";
			// docs/adr/017 §a.1).
			vols, err := rt.ListManagedVolumes(ctx)
			if err != nil {
				return err
			}
			prefix := name + "-data"
			for _, v := range vols {
				if v.Name == prefix || strings.HasPrefix(v.Name, prefix+"-") {
					if err := rt.RemoveVolume(ctx, v.Name); err != nil {
						return err
					}
				}
			}
		} else if err := rt.RemoveVolume(ctx, name+"-data"); err != nil {
			return err
		}
		// Network may still be shared; ignore removal failure from active endpoints.
		_ = rt.RemoveNetwork(ctx, providerkit.Network(cfg))
		return nil
	case "EventStream":
		// A dead broker takes its topics with it; requiring a live admin
		// API here would make destroy unable to converge after out-of-band
		// failures. Inspect(name) covers both shapes: the literal container
		// (legacy) or the set aggregate (docs/adr/004).
		if ctr, found, err := rt.Inspect(ctx, name); err != nil || !found || !ctr.Running {
			return err
		}
		dialMap, seeds, closeAll, err := topicDial(ctx, rt, cfg, name)
		if err != nil {
			return err
		}
		defer closeAll()
		return deleteTopic(ctx, dialMap, seeds, res.Metadata.Name)
	default:
		return fmt.Errorf("redpanda provider cannot destroy kind %s", res.Kind)
	}
}

func (p *Provider) Probe(ctx context.Context, req reconciler.Request) (status.Status, error) {
	res, rt := req.Resource, req.Runtime
	st := status.Status{}
	now := time.Now()
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return st, err
	}
	name := brokerName(req.Provider)
	switch res.Kind {
	case "Provider":
		if n, declared := brokersDeclared(cfg); declared && n >= 1 {
			return p.probeBrokerSet(ctx, rt, name, n)
		}
		ctrState, found, err := rt.Inspect(ctx, name)
		if err != nil {
			return st, err
		}
		if !found || !ctrState.Healthy {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonBrokerUnhealthy}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonBrokerUnhealthy}, now)
			return st, nil
		}
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonBrokerHealthy}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
		return st, nil
	case "EventStream":
		es, err := eventstream.FromEnvelope(res)
		if err != nil {
			return st, err
		}
		wantPartitions := es.Partitions
		if wantPartitions == 0 {
			wantPartitions = 1
		}
		wantRetentionMS, err := retentionMillis(es.RetentionDuration)
		if err != nil {
			return st, err
		}
		dialMap, seeds, closeAll, err := topicDial(ctx, rt, cfg, name)
		if err != nil {
			return st, err
		}
		defer closeAll()
		// retryTransientProbe: a transport error is "undetermined", not a
		// verdict — see topicProbeRetryWindow's doc (kafka.go) for the
		// live-caught heal-window race this absorbs.
		drift, reason, err := retryTransientProbe(ctx, func() (bool, string, error) {
			return probeTopic(ctx, dialMap, seeds, res.Metadata.Name, wantPartitions, es.ReplicationFactor(), wantRetentionMS)
		})
		if err != nil {
			return st, err
		}
		if drift {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: reason}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: reason}, now)
		} else {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonTopicHealthy}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
		}
		return st, nil
	default:
		return st, fmt.Errorf("redpanda provider cannot probe kind %s", res.Kind)
	}
}

// waitClusterFormed polls cluster membership (the admin API's broker list,
// reached through freshly-resolved per-ordinal addresses on every attempt —
// the docs/planning/09 F1 re-resolve-per-attempt discipline, multi-ordinal)
// until all n brokers have joined.
func waitClusterFormed(ctx context.Context, rt runtime.ContainerRuntime, name string, n int, timeout time.Duration) error {
	deadline := time.Now().Add(runtime.ScaledWait(timeout))
	var last error
	for {
		joined := 0
		dialMap, seeds, closeAll, err := clusterDial(ctx, rt, name, n)
		if err == nil {
			// Minimum view across EVERY member, not one client's answer —
			// see countJoinedBrokersMinView: Ready must mean a bar no
			// same-instant probe can disagree with from another vantage.
			joined = countJoinedBrokersMinView(ctx, dialMap, seeds)
			closeAll()
		}
		last = err
		if err == nil && joined >= n {
			return nil
		}
		if time.Now().After(deadline) {
			if last != nil {
				return fmt.Errorf("cluster %q did not reach %d joined brokers within %s: %w", name, n, timeout, last)
			}
			return fmt.Errorf("cluster %q did not reach %d joined brokers within %s (last observed: %d)", name, n, timeout, joined)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// probeBrokerSet is the Provider probe for the multi-broker shape
// (docs/adr/017 §a.6): per-ordinal container presence first (a missing or
// stopped ordinal is drift the runtime can report even with the whole
// cluster down), then cluster membership per the admin API (all brokers
// joined). It deliberately ignores the container healthcheck's aggregate:
// that check is cluster-scoped (rpk cluster health), so one dead broker
// flips every survivor's health signal — useless for naming WHICH broker is
// missing.
func (p *Provider) probeBrokerSet(ctx context.Context, rt runtime.ContainerRuntime, name string, n int) (status.Status, error) {
	st := status.Status{}
	now := time.Now()
	var missing []string
	for i := 0; i < n; i++ {
		ord := runtime.OrdinalName(name, i)
		ordState, found, err := rt.Inspect(ctx, ord)
		if err != nil {
			return st, err
		}
		if !found || !ordState.Running {
			missing = append(missing, ord)
		}
	}
	if len(missing) > 0 {
		reason := fmt.Sprintf("%s(%s)", status.ReasonBrokerMissing, strings.Join(missing, ","))
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: reason}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: reason}, now)
		return st, nil
	}
	dialMap, seeds, closeAll, err := clusterDial(ctx, rt, name, n)
	if err != nil {
		// Every ordinal exists but none answers a connection — degraded,
		// not a per-ordinal absence.
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonBrokerUnhealthy}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonBrokerUnhealthy}, now)
		return st, nil //nolint:nilerr // the unreachability IS the probe finding, reported as drift
	}
	defer closeAll()
	joined, err := countJoinedBrokers(ctx, dialMap, seeds)
	if err != nil {
		return st, err
	}
	if joined != n {
		reason := fmt.Sprintf("%s(%d!=%d)", status.ReasonBrokerNotJoined, joined, n)
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: reason}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: reason}, now)
		return st, nil
	}
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonBrokerHealthy}, now)
	st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
	return st, nil
}

// retentionMillis parses durations like "7d", "12h", "30m", "45s".
func retentionMillis(s string) (int64, error) {
	if s == "" {
		return -1, nil // broker default
	}
	unit := s[len(s)-1]
	n, err := strconv.ParseInt(s[:len(s)-1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid retention duration %q", s)
	}
	switch unit {
	case 's':
		return n * 1000, nil
	case 'm':
		return n * 60 * 1000, nil
	case 'h':
		return n * 3600 * 1000, nil
	case 'd':
		return n * 24 * 3600 * 1000, nil
	default:
		return 0, fmt.Errorf("invalid retention duration %q (allowed suffixes: s, m, h, d)", s)
	}
}

// ValidateSpec implements SpecValidator: a typo'd schemaRegistry value or a
// malformed/incompatible brokers declaration fails at validate, never as a
// half-applied platform. The HighAvailability gate requirement for
// brokers > 1 is NOT here — a SpecValidator has no feature-gate access by
// design (docs/adr/017 §a.8); cmd/platformctl's checkHighAvailabilityGate
// enforces it in loadAndValidate, the same mechanism as
// checkSchemaRegistryGate.
// ValidateSpec implements reconciler.SpecValidator. schemaRegistry's enum
// shape and brokers' positive-integer shape (docs/planning/08 E5) are now
// enforced by schemas/v1alpha1/fragments/provider/redpanda.json, composed
// into manifest.Validate ahead of this method in every real CLI path
// (ADR 011's loadAndValidate order) — the checks below are the cross-field
// rules a static JSON Schema fragment cannot express (mutual exclusion
// between spec.configuration.brokers and a sibling host-port pin/
// schemaRegistry value).
func (p *Provider) ValidateSpec(cfg provider.Provider) error {
	if _, declared := cfg.Configuration["brokers"]; declared {
		// Host-port pins cannot be combined with the ordinal-set shape:
		// every ordinal's host port is auto-assigned (docs/adr/004 known
		// limitation, closed at validate here per docs/adr/017 §a.4).
		for _, key := range []string{"kafkaPort", "adminPort", "schemaRegistryPort"} {
			if _, pinned := cfg.Configuration[key]; pinned {
				return fmt.Errorf("spec.configuration.%s cannot be combined with spec.configuration.brokers: each broker's host port is auto-assigned (docs/adr/017 §a.4)", key)
			}
		}
		if schemaRegistryEnabled(cfg) {
			return fmt.Errorf("spec.configuration.schemaRegistry: enabled is not yet supported together with spec.configuration.brokers (docs/adr/017, follow-ups)")
		}
	}
	return nil
}
