// Package redpanda reconciles a Redpanda broker container (via the container
// runtime) and, post-health, creates/updates topics and retention settings
// via the Kafka admin protocol. First real technology provider (Phase 2).
package redpanda

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/rezarajan/platformctl/internal/domain/eventstream"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

const (
	internalKafkaPort = 29092
	externalKafkaPort = 9092
	defaultImage      = "docker.redpanda.com/redpandadata/redpanda:v24.2.1"
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

func (p *Provider) brokerName() string { return p.providerRes.Metadata.Name }

func (p *Provider) hostPort() int {
	if v, ok := p.cfg.Configuration["kafkaPort"]; ok {
		switch n := v.(type) {
		case int:
			return n
		case float64:
			return int(n)
		}
	}
	return externalKafkaPort
}

// HostAddr is the broker address reachable from the host (admin operations).
func (p *Provider) HostAddr() string { return "127.0.0.1:" + strconv.Itoa(p.hostPort()) }

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

func (p *Provider) Reconcile(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) (status.Status, error) {
	switch res.Kind {
	case "Provider":
		return p.reconcileBroker(ctx, rt)
	case "EventStream":
		return p.reconcileTopic(ctx, res)
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
	labels := map[string]string{
		runtime.LabelManagedBy:  runtime.ManagedByValue,
		runtime.LabelGeneration: name,
	}

	if err := rt.EnsureNetwork(ctx, runtime.NetworkSpec{Name: p.network(), Labels: labels}); err != nil {
		return st, err
	}
	if err := rt.EnsureVolume(ctx, runtime.VolumeSpec{Name: name + "-data", Labels: labels}); err != nil {
		return st, err
	}

	hostPort := p.hostPort()
	ctrState, err := rt.EnsureContainer(ctx, runtime.ContainerSpec{
		Name:  name,
		Image: image,
		Cmd: []string{
			"redpanda", "start",
			"--overprovisioned", "--smp", "1", "--memory", "512M", "--reserve-memory", "0M",
			"--node-id", "0", "--check=false",
			"--kafka-addr", fmt.Sprintf("INTERNAL://0.0.0.0:%d,EXTERNAL://0.0.0.0:%d", internalKafkaPort, externalKafkaPort),
			"--advertise-kafka-addr", fmt.Sprintf("INTERNAL://%s:%d,EXTERNAL://127.0.0.1:%d", name, internalKafkaPort, hostPort),
		},
		Networks: []string{p.network()},
		Volumes:  []runtime.VolumeMount{{VolumeName: name + "-data", MountPath: "/var/lib/redpanda/data"}},
		Ports:    []runtime.PortBinding{{HostPort: hostPort, ContainerPort: externalKafkaPort}},
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
	st.ProviderState = map[string]any{
		"containerId":  ctrState.ID,
		"kafkaAddr":    p.HostAddr(),
		"internalAddr": p.InternalAddr(),
	}
	return st, nil
}

func (p *Provider) reconcileTopic(ctx context.Context, res resource.Envelope) (status.Status, error) {
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

	if err := ensureTopic(ctx, p.HostAddr(), topic, partitions, retentionMS); err != nil {
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
		return deleteTopic(ctx, p.HostAddr(), res.Metadata.Name)
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
		drift, reason, err := probeTopic(ctx, p.HostAddr(), res.Metadata.Name, wantPartitions)
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
