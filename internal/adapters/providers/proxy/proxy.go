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
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/rezarajan/platformctl/internal/domain/connection"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

const defaultImage = "alpine/socat:1.8.0.3@sha256:beb4a68d9e4fe6b0f21ea774a0fde6c31f580dde6368939ed70100c5385b015e"

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

func (p *Provider) name() string { return naming.RuntimeObjectName(p.providerRes) }

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
	name := naming.RuntimeObjectName(res)
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
		Ports:    []runtime.PortBinding{{HostPort: conn.Port, ContainerPort: conn.Port, Audience: runtime.AudienceHost}},
		Labels:   connLabels,
	})
	if err != nil {
		return st, err
	}
	if err := rt.WaitHealthy(ctx, name, 60*time.Second); err != nil {
		return st, err
	}

	host, port := conn.Endpoint(name)
	// Observed binding, not intent.
	hostAddr := ctrState.HostAddr(conn.Port)
	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "Forwarding"}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: "ReconcileComplete"}, now)
	st.ProviderState = map[string]any{
		"containerId": ctrState.ID,
		"internal":    fmt.Sprintf("%s:%d", host, port),
		"host":        hostAddr,
		"target":      conn.Target,
		endpoint.Key: endpoint.List{
			{Name: "forward", Scheme: conn.Scheme, Host: hostAddr, Internal: fmt.Sprintf("%s:%d", host, port), Insecure: true, RuntimeName: name, ContainerPort: conn.Port, Audience: runtime.AudienceHost},
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
		return rt.Remove(ctx, naming.RuntimeObjectName(res))
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
		ctr, found, err := rt.Inspect(ctx, naming.RuntimeObjectName(res))
		if err != nil {
			return st, err
		}
		if !found || !ctr.Healthy {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: "ForwarderDown"}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: "ForwarderDown"}, now)
			return st, nil
		}
		// Beyond the forwarder container's health (docs/planning/07 §2.1):
		// dial *through* it. socat accepts, then connects to the upstream
		// per session — a dead upstream shows as an immediate close after
		// accept, so a connection that stays open past a short read
		// deadline means the upstream answered.
		conn, err := connection.FromEnvelope(res)
		if err != nil {
			return st, err
		}
		if addr := ctr.HostAddr(conn.Port); addr != "" {
			if err := probeThroughForwarder(addr); err != nil {
				msg := fmt.Sprintf("forwarder is up but upstream %s is unreachable: %v", conn.Target, err)
				st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: "UpstreamUnreachable", Message: msg}, now)
				st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: "UpstreamUnreachable", Message: msg}, now)
				return st, nil
			}
		}
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "Forwarding"}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: "NoDrift"}, now)
		return st, nil
	default:
		return st, fmt.Errorf("proxy provider cannot probe kind %s", res.Kind)
	}
}

// probeThroughForwarder dials the forwarder's published port and holds the
// connection through a short read deadline. socat closes the accepted
// session immediately when its upstream connect fails, so a quick
// EOF/reset means the upstream is unreachable; a read timeout with the
// session still open means the upstream accepted.
func probeThroughForwarder(addr string) error {
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return err
	}
	defer c.Close()
	_ = c.SetReadDeadline(time.Now().Add(1500 * time.Millisecond))
	buf := make([]byte, 1)
	_, err = c.Read(buf)
	if err == nil {
		return nil // upstream even sent a banner (e.g. mysql) — alive
	}
	var nerr net.Error
	if errors.As(err, &nerr) && nerr.Timeout() {
		return nil // session held open past the deadline — upstream accepted
	}
	return fmt.Errorf("session closed immediately: %w", err)
}
