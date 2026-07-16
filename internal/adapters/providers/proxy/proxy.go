// Package proxy realizes managed Connection resources: the platform-owned
// entrypoint surface to systems that live elsewhere. Each Connection runs
// one socat forwarder container named after the Connection, listening on
// spec.port inside the shared network and published to the host —
// in-network consumers use <connection-name>:<port>, host tools (Dagster,
// psql, Metabase) use 127.0.0.1:<port>, and only the Connection's
// spec.target knows where the system actually lives. Credentials never pass
// through here; spec.secretRef names them, the proxy is transport only.
// Implements ConnectionCapableProvider (scheme: tcp). Tunnel chaining for
// VPC reach is deliberately deferred — see docs/design/002.
package proxy

import (
	"context"
	"fmt"
	"time"

	"github.com/rezarajan/platformctl/internal/domain/connection"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

const defaultImage = "alpine/socat:latest"

type Provider struct {
	providerRes resource.Envelope
	cfg         provider.Provider
}

func New() *Provider { return &Provider{} }

func (p *Provider) Type() string { return "proxy" }

// SupportedConnectionSchemes implements ConnectionCapableProvider: socat
// forwards raw TCP; anything TCP-framed (postgres, mysql, http, kafka)
// works through it.
func (p *Provider) SupportedConnectionSchemes() []string { return []string{"tcp"} }

func (p *Provider) SetProviderResource(env resource.Envelope) {
	p.providerRes = env
	p.cfg, _ = provider.FromEnvelope(env)
}

func (p *Provider) name() string { return p.providerRes.Metadata.Name }

func (p *Provider) network() string {
	if n, ok := p.cfg.RuntimeConfig["network"].(string); ok && n != "" {
		return n
	}
	return "datascape"
}

func (p *Provider) image() string {
	if img, ok := p.cfg.Configuration["image"].(string); ok && img != "" {
		return img
	}
	return defaultImage
}

func (p *Provider) labels() map[string]string {
	return runtime.ManagedLabels(p.providerRes.Metadata.Namespace, "Provider", p.name(), p.name())
}

func (p *Provider) Reconcile(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) (status.Status, error) {
	switch res.Kind {
	case "Provider":
		return p.reconcileInstance(ctx, rt)
	case "Connection":
		return p.reconcileConnection(ctx, res, rt)
	default:
		return status.Status{}, fmt.Errorf("proxy provider cannot reconcile kind %s", res.Kind)
	}
}

// reconcileInstance: the proxy has no central container — its Provider
// resource only anchors the shared network and the configuration defaults
// each Connection's forwarder inherits.
func (p *Provider) reconcileInstance(ctx context.Context, rt runtime.ContainerRuntime) (status.Status, error) {
	st := status.Status{}
	if err := rt.EnsureNetwork(ctx, runtime.NetworkSpec{Name: p.network(), Labels: p.labels()}); err != nil {
		return st, err
	}
	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "EntrypointSurfaceReady"}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: "ReconcileComplete"}, now)
	st.ProviderState = map[string]any{"network": p.network(), "image": p.image()}
	return st, nil
}

// reconcileConnection runs the Connection's forwarder: one socat container
// named after the Connection, listening on spec.port (network + host),
// forwarding to spec.target.
func (p *Provider) reconcileConnection(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) (status.Status, error) {
	st := status.Status{}
	conn, err := connection.FromEnvelope(res)
	if err != nil {
		return st, err
	}
	name := res.Metadata.Name
	if err := rt.EnsureNetwork(ctx, runtime.NetworkSpec{Name: p.network(), Labels: p.labels()}); err != nil {
		return st, err
	}
	connLabels := runtime.ManagedLabels(res.Metadata.Namespace, res.Kind, name, name)
	ctrState, err := rt.EnsureContainer(ctx, runtime.ContainerSpec{
		Name:  name,
		Image: p.image(),
		Cmd: []string{
			fmt.Sprintf("tcp-listen:%d,fork,reuseaddr", conn.Port),
			"tcp-connect:" + conn.Target,
		},
		Networks: []string{p.network()},
		Ports:    []runtime.PortBinding{{HostPort: conn.Port, ContainerPort: conn.Port}},
		Labels:   connLabels,
	})
	if err != nil {
		return st, err
	}
	if err := rt.WaitHealthy(ctx, name, 60*time.Second); err != nil {
		return st, err
	}

	host, port := conn.Endpoint(name)
	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "Forwarding"}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: "ReconcileComplete"}, now)
	st.ProviderState = map[string]any{
		"containerId": ctrState.ID,
		"internal":    fmt.Sprintf("%s:%d", host, port),
		"host":        conn.HostEndpoint(),
		"target":      conn.Target,
		endpoint.Key: endpoint.List{
			{Name: "forward", Scheme: conn.Scheme, Host: conn.HostEndpoint(), Internal: fmt.Sprintf("%s:%d", host, port)},
		}.ToState(),
	}
	return st, nil
}

func (p *Provider) Destroy(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) error {
	switch res.Kind {
	case "Provider":
		_ = rt.RemoveNetwork(ctx, p.network())
		return nil
	case "Connection":
		return rt.Remove(ctx, res.Metadata.Name)
	default:
		return fmt.Errorf("proxy provider cannot destroy kind %s", res.Kind)
	}
}

func (p *Provider) Probe(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) (status.Status, error) {
	st := status.Status{}
	now := time.Now()
	switch res.Kind {
	case "Provider":
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "EntrypointSurfaceReady"}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: "NoDrift"}, now)
		return st, nil
	case "Connection":
		ctr, found, err := rt.Inspect(ctx, res.Metadata.Name)
		if err != nil {
			return st, err
		}
		if !found || !ctr.Healthy {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: "ForwarderDown"}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: "ForwarderDown"}, now)
			return st, nil
		}
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "Forwarding"}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: "NoDrift"}, now)
		return st, nil
	default:
		return st, fmt.Errorf("proxy provider cannot probe kind %s", res.Kind)
	}
}
