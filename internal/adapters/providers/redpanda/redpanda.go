// Package redpanda reconciles a Redpanda broker container (via the container
// runtime) and, post-health, creates/updates topics and retention settings
// via the Kafka admin protocol. First real technology provider (Phase 2).
package redpanda

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
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
	defaultImage       = "docker.redpanda.com/redpandadata/redpanda:v24.2.1@sha256:f60d828ed6cafd7ce4c9b987ff71699895b81fe53f1d0e27ebf045277fcff21a"
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

	registryEnabled := schemaRegistryEnabled(cfg)
	cmd := []string{
		"redpanda", "start",
		"--overprovisioned", "--smp", "1", "--memory", "512M", "--reserve-memory", "0M",
		"--node-id", "0", "--check=false",
		"--kafka-addr", fmt.Sprintf("INTERNAL://0.0.0.0:%d,EXTERNAL://0.0.0.0:%d", internalKafkaPort, externalKafkaPort),
		"--advertise-kafka-addr", fmt.Sprintf("INTERNAL://%s:%d,EXTERNAL://%s", name, internalKafkaPort, advertisedAddr(cfg, name)),
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
	endpoints := endpoint.List{
		{Name: "kafka", Scheme: "kafka", Host: hostAddr, Internal: internalAddr(name), Insecure: true},
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

	addr, closeAddr, err := providerkit.ReachableAddr(ctx, rt, name, externalKafkaPort)
	if err != nil {
		return st, err
	}
	defer closeAddr()
	if err := ensureTopic(ctx, addr, advertisedAddr(cfg, name), topic, partitions, retentionMS); err != nil {
		return st, err
	}

	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonTopicReconciled}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	st.ProviderState = map[string]any{"topic": topic, "partitions": partitions}
	return st, nil
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
		if err := rt.RemoveVolume(ctx, name+"-data"); err != nil {
			return err
		}
		// Network may still be shared; ignore removal failure from active endpoints.
		_ = rt.RemoveNetwork(ctx, providerkit.Network(cfg))
		return nil
	case "EventStream":
		// A dead broker takes its topics with it; requiring a live admin
		// API here would make destroy unable to converge after out-of-band
		// failures.
		if ctr, found, err := rt.Inspect(ctx, name); err != nil || !found || !ctr.Running {
			return err
		}
		addr, closeAddr, err := providerkit.ReachableAddr(ctx, rt, name, externalKafkaPort)
		if err != nil {
			return err
		}
		defer closeAddr()
		return deleteTopic(ctx, addr, advertisedAddr(cfg, name), res.Metadata.Name)
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
		addr, closeAddr, err := providerkit.ReachableAddr(ctx, rt, name, externalKafkaPort)
		if err != nil {
			return st, err
		}
		defer closeAddr()
		drift, reason, err := probeTopic(ctx, addr, advertisedAddr(cfg, name), res.Metadata.Name, wantPartitions, wantRetentionMS)
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

// ValidateSpec implements SpecValidator: a typo'd schemaRegistry value fails
// at validate, never as a half-applied platform.
func (p *Provider) ValidateSpec(cfg provider.Provider) error {
	if v, ok := cfg.Configuration["schemaRegistry"]; ok {
		s, _ := v.(string)
		if s != "enabled" && s != "disabled" {
			return fmt.Errorf("spec.configuration.schemaRegistry must be \"enabled\" or \"disabled\", got %v", v)
		}
	}
	return nil
}
