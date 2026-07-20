// Package redpanda reconciles a Redpanda broker container (via the container
// runtime) and, post-health, creates/updates topics and retention settings
// via the Kafka admin protocol. First real technology provider (Phase 2).
package redpanda

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/eventstream"
	"github.com/rezarajan/platformctl/internal/domain/hostport"
	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

const (
	internalKafkaPort = 29092
	externalKafkaPort = 9092
	defaultImage      = "docker.redpanda.com/redpandadata/redpanda:v24.2.1@sha256:f60d828ed6cafd7ce4c9b987ff71699895b81fe53f1d0e27ebf045277fcff21a"
)

type Provider struct {
	providerRes resource.Envelope
	cfg         provider.Provider
}

func New() *Provider { return &Provider{} }

func (p *Provider) Type() string { return "redpanda" }

func (p *Provider) SetProviderResource(env resource.Envelope) {
	p.providerRes = env
	p.cfg, _ = provider.FromEnvelope(env)
}

func (p *Provider) brokerName() string { return naming.RuntimeObjectName(p.providerRes) }

func (p *Provider) hostPort() int {
	configured := 0
	if v, ok := p.cfg.Configuration["kafkaPort"]; ok {
		switch n := v.(type) {
		case int:
			configured = n
		case float64:
			configured = int(n)
		}
	}
	return hostport.Resolve(configured, p.brokerName())
}

// InternalAddr is the broker address reachable from containers on the shared
// network (Debezium, sink connectors).
func (p *Provider) InternalAddr() string {
	return p.brokerName() + ":" + strconv.Itoa(internalKafkaPort)
}

func (p *Provider) network() string {
	if n, ok := p.cfg.RuntimeConfig["network"].(string); ok && n != "" {
		return n
	}
	return "datascape"
}

// accessMode selects how CLI-side admin calls (reconcileTopic, Probe,
// Destroy for EventStream) reach the broker on Kubernetes — one of the
// runtime.Access* constants (docs/planning/08 B1). Docker ignores it: the
// broker's host port is already reachable by construction.
func (p *Provider) accessMode() string {
	m, _ := p.cfg.RuntimeConfig["access"].(string)
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
func (p *Provider) advertisedAddr() string {
	return "127.0.0.1:" + strconv.Itoa(p.hostPort()) // archtest:allow-loopback: sentinel never dialed directly, only matched+redirected by kafka.go's kgo.Dialer
}

// reachableAddr returns an address this process can dial right now to reach
// the broker's admin (external Kafka) port, plus a close func that must
// always be called — on Docker this is a cheap no-op; on Kubernetes it may
// tear down a port-forward tunnel opened just for this call.
func (p *Provider) reachableAddr(ctx context.Context, rt runtime.ContainerRuntime) (string, func() error, error) {
	return rt.EnsureReachable(ctx, p.brokerName(), externalKafkaPort)
}

func (p *Provider) Reconcile(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) (status.Status, error) {
	switch res.Kind {
	case "Provider":
		return p.reconcileBroker(ctx, rt)
	case "EventStream":
		return p.reconcileTopic(ctx, res, rt)
	default:
		return status.Status{}, fmt.Errorf("redpanda provider cannot reconcile kind %s", res.Kind)
	}
}

func (p *Provider) reconcileBroker(ctx context.Context, rt runtime.ContainerRuntime) (status.Status, error) {
	st := status.Status{}
	name := p.brokerName()
	image, _ := p.cfg.Configuration["image"].(string)
	if image == "" {
		image = defaultImage
	}
	labels := runtime.ManagedLabels(p.providerRes.Metadata.Namespace, "Provider", name, name)

	if err := rt.EnsureNetwork(ctx, runtime.NetworkSpec{Name: p.network(), Labels: labels}); err != nil {
		return st, err
	}
	if err := rt.EnsureVolume(ctx, runtime.VolumeSpec{Name: name + "-data", Labels: labels, Networks: []string{p.network()}}); err != nil {
		return st, err
	}

	hostPort := p.hostPort()
	ctrState, err := rt.EnsureContainer(ctx, runtime.ContainerSpec{
		Name:       name,
		Image:      image,
		AccessMode: p.accessMode(),
		Cmd: []string{
			"redpanda", "start",
			"--overprovisioned", "--smp", "1", "--memory", "512M", "--reserve-memory", "0M",
			"--node-id", "0", "--check=false",
			"--kafka-addr", fmt.Sprintf("INTERNAL://0.0.0.0:%d,EXTERNAL://0.0.0.0:%d", internalKafkaPort, externalKafkaPort),
			"--advertise-kafka-addr", fmt.Sprintf("INTERNAL://%s:%d,EXTERNAL://%s", name, internalKafkaPort, p.advertisedAddr()),
		},
		Networks: []string{p.network()},
		Volumes:  []runtime.VolumeMount{{VolumeName: name + "-data", MountPath: "/var/lib/redpanda/data"}},
		// INTERNAL (29092) is Audience: internal — no host publish, but
		// still declared so the Kubernetes adapter's Service actually
		// carries a port for it — a Service only forwards ports present in
		// ContainerSpec.Ports (docs/planning/08 B8), unlike a Docker
		// bridge network, which reaches every container port regardless of
		// what's published. Docker itself already reached INTERNAL fine
		// without this; this declaration is a documented no-op there
		// (portMaps skips the host-binding side for Audience: internal).
		Ports: []runtime.PortBinding{
			{HostPort: hostPort, ContainerPort: externalKafkaPort, Audience: runtime.AudienceHost},
			{ContainerPort: internalKafkaPort, Audience: runtime.AudienceInternal},
		},
		HealthCheck: &runtime.HealthCheck{
			Test:     []string{"CMD-SHELL", "rpk cluster health --exit-when-healthy || exit 1"},
			Interval: 2 * time.Second,
			Timeout:  5 * time.Second,
			Retries:  30,
		},
		Labels: labels,
	})
	if err != nil {
		return st, err
	}
	if err := rt.WaitHealthy(ctx, name, 120*time.Second); err != nil {
		return st, err
	}

	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "BrokerHealthy"}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: "ReconcileComplete"}, now)
	hostAddr := ctrState.HostAddr(externalKafkaPort) // observed binding, not intent
	st.ProviderState = map[string]any{
		"containerId":  ctrState.ID,
		"kafkaAddr":    hostAddr,
		"internalAddr": p.InternalAddr(),
		endpoint.Key: endpoint.List{
			{Name: "kafka", Scheme: "kafka", Host: hostAddr, Internal: p.InternalAddr(), Insecure: true},
		}.ToState(),
	}
	return st, nil
}

func (p *Provider) reconcileTopic(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) (status.Status, error) {
	st := status.Status{}
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

	addr, closeAddr, err := p.reachableAddr(ctx, rt)
	if err != nil {
		return st, err
	}
	defer closeAddr()
	if err := ensureTopic(ctx, addr, p.advertisedAddr(), topic, partitions, retentionMS); err != nil {
		return st, err
	}

	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "TopicReconciled"}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: "ReconcileComplete"}, now)
	st.ProviderState = map[string]any{"topic": topic, "partitions": partitions}
	return st, nil
}

func (p *Provider) Destroy(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) error {
	switch res.Kind {
	case "Provider":
		name := p.brokerName()
		if err := rt.Remove(ctx, name); err != nil {
			return err
		}
		if err := rt.RemoveVolume(ctx, name+"-data"); err != nil {
			return err
		}
		// Network may still be shared; ignore removal failure from active endpoints.
		_ = rt.RemoveNetwork(ctx, p.network())
		return nil
	case "EventStream":
		// A dead broker takes its topics with it; requiring a live admin
		// API here would make destroy unable to converge after out-of-band
		// failures.
		if ctr, found, err := rt.Inspect(ctx, p.brokerName()); err != nil || !found || !ctr.Running {
			return err
		}
		addr, closeAddr, err := p.reachableAddr(ctx, rt)
		if err != nil {
			return err
		}
		defer closeAddr()
		return deleteTopic(ctx, addr, p.advertisedAddr(), res.Metadata.Name)
	default:
		return fmt.Errorf("redpanda provider cannot destroy kind %s", res.Kind)
	}
}

func (p *Provider) Probe(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) (status.Status, error) {
	st := status.Status{}
	now := time.Now()
	switch res.Kind {
	case "Provider":
		ctrState, found, err := rt.Inspect(ctx, p.brokerName())
		if err != nil {
			return st, err
		}
		if !found || !ctrState.Healthy {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: "BrokerUnhealthy"}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: "BrokerUnhealthy"}, now)
			return st, nil
		}
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "BrokerHealthy"}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: "NoDrift"}, now)
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
		addr, closeAddr, err := p.reachableAddr(ctx, rt)
		if err != nil {
			return st, err
		}
		defer closeAddr()
		drift, reason, err := probeTopic(ctx, addr, p.advertisedAddr(), res.Metadata.Name, wantPartitions, wantRetentionMS)
		if err != nil {
			return st, err
		}
		if drift {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: reason}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: reason}, now)
		} else {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "TopicHealthy"}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: "NoDrift"}, now)
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
