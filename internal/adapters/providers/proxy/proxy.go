// Package proxy realizes managed Connection resources: the platform-owned
// entrypoint surface to systems that live elsewhere. Each Connection runs
// one socat forwarder container named after the Connection, listening on
// spec.port inside the shared network and published to the host —
// in-network consumers use <connection-name>:<port>, host tools (Dagster,
// psql, Metabase) use 127.0.0.1:<port>, and only the Connection's
// spec.target knows where the system actually lives. Credentials never pass
// through here; spec.secretRef names them, the proxy is transport only.
// Implements ConnectionCapableProvider (scheme: tcp) and
// reconciler.ViaConsumingProvider: a Connection declaring spec.via routes
// this forwarder's egress through the named tunnel-capable Provider
// (docs/planning/08 I1, closing docs/adr/023's Scope deviation) — the
// forwarder joins ONLY the tunnel's transit network in addition to the
// shared platform network (never the consumer workloads: blast-minimized),
// and dials the tunnel's own published forward address instead of
// spec.target directly. See reconcileConnection's doc comment.
package proxy

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/rezarajan/platformctl/internal/adapters/providers/providerkit"
	"github.com/rezarajan/platformctl/internal/domain/connection"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

const defaultImage = "alpine/socat:1.8.0.3@sha256:beb4a68d9e4fe6b0f21ea774a0fde6c31f580dde6368939ed70100c5385b015e"

// Provider holds no cross-call state (docs/planning/08 F5): every method
// receives what it needs via reconciler.Request.
type Provider struct{}

func New() *Provider { return &Provider{} }

func (p *Provider) Type() string { return "proxy" }

// SupportedConnectionSchemes implements ConnectionCapableProvider: socat
// forwards raw TCP; anything TCP-framed (postgres, mysql, http, kafka)
// works through it.
func (p *Provider) SupportedConnectionSchemes() []string { return []string{"tcp"} }

// ConsumesVia implements reconciler.ViaConsumingProvider (docs/planning/08
// I1): a proxy-realized Connection whose spec.via names a tunnel-capable
// Provider routes its forwarder's egress through it (reconcileConnection
// below) rather than applying via as an inert, unconsumed field.
func (p *Provider) ConsumesVia() bool { return true }

func image(cfg provider.Provider) string {
	if img, ok := cfg.Configuration["image"].(string); ok && img != "" {
		return img
	}
	return defaultImage
}

func (p *Provider) Reconcile(ctx context.Context, req reconciler.Request) (status.Status, error) {
	switch req.Resource.Kind {
	case "Provider":
		return p.reconcileInstance(ctx, req)
	case "Connection":
		return p.reconcileConnection(ctx, req)
	default:
		return status.Status{}, fmt.Errorf("proxy provider cannot reconcile kind %s", req.Resource.Kind)
	}
}

// reconcileInstance: the proxy has no central container — its Provider
// resource only anchors the shared network and the configuration defaults
// each Connection's forwarder inherits.
func (p *Provider) reconcileInstance(ctx context.Context, req reconciler.Request) (status.Status, error) {
	st := status.Status{}
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return st, err
	}
	name := naming.RuntimeObjectName(req.Provider)
	labels := runtime.ManagedLabels(req.Provider.Metadata.Namespace, "Provider", name, name)
	if err := req.Runtime.EnsureNetwork(ctx, runtime.NetworkSpec{Name: providerkit.Network(cfg), Labels: labels}); err != nil {
		return st, err
	}
	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonEntrypointSurfaceReady}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	st.ProviderState = map[string]any{"network": providerkit.Network(cfg), "image": image(cfg)}
	return st, nil
}

// reconcileConnection runs the Connection's forwarder: one socat container
// named after the Connection, listening on spec.port (network + host),
// forwarding to spec.target — or, when spec.via names a tunnel-capable
// Provider (docs/planning/08 I1, closing docs/adr/023's Scope deviation),
// forwarding to that tunnel's own published dial address instead, with the
// forwarder additionally joined ONLY to the tunnel's transit network
// (blast-minimized: never the consumer workloads, never the tunnel
// Provider's own platform network). Which address socat actually dials is
// decided once, at container-create time (dialTarget below) — Probe's own
// dial-the-forwarder settle check (probeThroughForwarder,
// waitForwarderServing) needs no via-awareness at all: it already verifies
// "does a session held open through the forwarder's listen port stay open,"
// which is equally true whether the forwarder's own upstream is spec.target
// directly or a tunnel's forwarder standing in for it.
func (p *Provider) reconcileConnection(ctx context.Context, req reconciler.Request) (status.Status, error) {
	res, rt := req.Resource, req.Runtime
	st := status.Status{}
	conn, err := connection.FromEnvelope(res)
	if err != nil {
		return st, err
	}
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return st, err
	}
	name := naming.RuntimeObjectName(res)
	providerName := naming.RuntimeObjectName(req.Provider)
	providerLabels := runtime.ManagedLabels(req.Provider.Metadata.Namespace, "Provider", providerName, providerName)
	if err := rt.EnsureNetwork(ctx, runtime.NetworkSpec{Name: providerkit.Network(cfg), Labels: providerLabels}); err != nil {
		return st, err
	}
	networks := []string{providerkit.Network(cfg)}
	dialTarget := conn.Target
	var transitNetwork string
	if conn.Via != nil {
		if req.TunnelFacts == nil {
			return st, fmt.Errorf("Connection %q: spec.via names Provider %q, whose tunnel is not yet published — re-apply once it reconciles", res.Metadata.Name, *conn.Via)
		}
		transitNetwork = req.TunnelFacts.TransitNetwork
		if err := rt.EnsureNetwork(ctx, runtime.NetworkSpec{Name: transitNetwork, Labels: providerLabels}); err != nil {
			return st, err
		}
		networks = append(networks, transitNetwork)
		dialTarget = req.TunnelFacts.Internal
	}
	connLabels := runtime.ManagedLabels(res.Metadata.Namespace, res.Kind, name, name)
	ctrState, err := rt.EnsureContainer(ctx, runtime.ContainerSpec{
		Name:  name,
		Image: image(cfg),
		Cmd: []string{
			fmt.Sprintf("tcp-listen:%d,fork,reuseaddr", conn.Port),
			"tcp-connect:" + dialTarget,
		},
		Networks: networks,
		Ports:    []runtime.PortBinding{{HostPort: conn.Port, ContainerPort: conn.Port, Audience: runtime.AudienceHost}},
		// A real healthcheck, not just "container Running" (docs/planning/11
		// B1 finding 3): dial socat's own listener from inside the
		// container. connect-timeout bounds a hung attempt so the
		// healthcheck itself can't wedge.
		HealthCheck: &runtime.HealthCheck{
			Test:     []string{"CMD-SHELL", fmt.Sprintf("socat -u OPEN:/dev/null TCP:127.0.0.1:%d,connect-timeout=2 || exit 1", conn.Port)},
			Interval: 2 * time.Second,
			Timeout:  5 * time.Second,
			Retries:  30,
		},
		Labels: connLabels,
	})
	if err != nil {
		return st, err
	}
	if err := rt.WaitHealthy(ctx, name, 60*time.Second); err != nil {
		return st, err
	}
	// Ready means serving (docs/planning/01 NFR-11), not just "the container
	// is Running" — socat accepting on its listen port says nothing about
	// whether spec.target actually answers (docs/planning/11 B1 finding 3).
	// Settle to the SAME dial-through-forwarder check Probe verifies before
	// declaring Ready.
	if err := waitForwarderServing(ctx, rt, name, conn); err != nil {
		return st, err
	}

	host, port := conn.Endpoint(name)
	// Observed binding, not intent.
	hostAddr := ctrState.HostAddr(conn.Port)
	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonForwarding}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	st.ProviderState = map[string]any{
		"containerId": ctrState.ID,
		"internal":    fmt.Sprintf("%s:%d", host, port),
		"host":        hostAddr,
		"target":      conn.Target,
		endpoint.Key: endpoint.List{
			{Name: "forward", Scheme: conn.Scheme, Host: hostAddr, Internal: fmt.Sprintf("%s:%d", host, port), Insecure: true, RuntimeName: name, ContainerPort: conn.Port, Audience: runtime.AudienceHost},
		}.ToState(),
	}
	if conn.Via != nil {
		st.ProviderState["via"] = *conn.Via
		st.ProviderState["transit"] = transitNetwork
	}
	return st, nil
}

func (p *Provider) Destroy(ctx context.Context, req reconciler.Request) error {
	switch req.Resource.Kind {
	case "Provider":
		cfg, err := provider.FromEnvelope(req.Provider)
		if err != nil {
			return err
		}
		_ = req.Runtime.RemoveNetwork(ctx, providerkit.Network(cfg))
		return nil
	case "Connection":
		return req.Runtime.Remove(ctx, naming.RuntimeObjectName(req.Resource))
	default:
		return fmt.Errorf("proxy provider cannot destroy kind %s", req.Resource.Kind)
	}
}

func (p *Provider) Probe(ctx context.Context, req reconciler.Request) (status.Status, error) {
	res, rt := req.Resource, req.Runtime
	st := status.Status{}
	now := time.Now()
	switch res.Kind {
	case "Provider":
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonEntrypointSurfaceReady}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
		return st, nil
	case "Connection":
		ctr, found, err := rt.Inspect(ctx, naming.RuntimeObjectName(res))
		if err != nil {
			return st, err
		}
		if !found || !ctr.Healthy {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonForwarderDown}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonForwarderDown}, now)
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
				st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonUpstreamUnreachable, Message: msg}, now)
				st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonUpstreamUnreachable, Message: msg}, now)
				return st, nil
			}
		}
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonForwarding}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
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

// forwarderSettleTimeout/forwarderSettlePoll bound reconcileConnection's
// Ready determination to a genuinely serving forwarder — the redpanda
// waitTopicSettled pattern (docs/planning/11 B1 findings 1-3). Vars, not
// consts: tests shrink them instead of waiting out a real 45s timeout to
// exercise the honest-failure path.
var (
	forwarderSettleTimeout = 45 * time.Second
	forwarderSettlePoll    = 2 * time.Second
)

// waitForwarderServing bounds reconcileConnection's Ready determination to
// the SAME check Probe uses — container health, then probeThroughForwarder
// (a dial-through the socat forwarder) exactly where Probe performs it. A
// healthy forwarder passes on the first attempt (zero added latency); on
// timeout, reconcile fails honestly with the last observed state instead
// of setting Ready from container health alone (docs/planning/11 B1
// finding 3).
//
// The dial-through is conditional on ctr.HostAddr publishing a host-side
// address, mirroring Probe's own `if addr != ""` guard verbatim — NOT an
// oversight (found live, 2026-07-22, TestLakehouseExampleOnKubernetes): on
// Kubernetes under the default ClusterIP/port-forward access mode, Inspect
// reports no HostIP/HostPort at all (only NodePort/LoadBalancer Services
// get one — see the K8s adapter's Inspect), so treating addr=="" as a wait
// state could never resolve and timed out a healthy forwarder after 45s.
// Symmetry with Probe is I4's whole bar: on a runtime where Probe itself
// skips the dial-through, reconcile's serving check is container health,
// the same as Probe's. Holding reconcile STRICTER than Probe there (e.g.
// dialing via a per-attempt port-forward instead) would break the symmetry
// in the opposite direction and wrongly fail Connections whose target is a
// genuinely external, unresolvable-from-the-cluster host (the lakehouse
// example's placeholder upstream): serving means "the forwarder accepts
// and forwards"; upstream reachability on such runtimes stays Probe/
// drift's job, exactly as before I4. Docker behavior is unchanged — the
// published address exists there from the moment the container does, so
// the dial-through always runs.
func waitForwarderServing(ctx context.Context, rt runtime.ContainerRuntime, name string, conn connection.Connection) error {
	deadline := time.Now().Add(runtime.ScaledWait(forwarderSettleTimeout))
	var lastErr error
	var lastReason string
	for {
		ctr, found, err := rt.Inspect(ctx, name)
		if err != nil {
			return err
		}
		if found && ctr.Healthy {
			addr := ctr.HostAddr(conn.Port)
			if addr == "" {
				// No published host binding on this runtime — Probe's own
				// serving bar here is container health alone (see the doc
				// comment above); already met.
				return nil
			}
			perr := probeThroughForwarder(addr)
			if perr == nil {
				return nil
			}
			lastErr = perr
			lastReason = fmt.Sprintf("upstream %s unreachable through forwarder: %v", conn.Target, perr)
		} else {
			lastErr = nil
			lastReason = "forwarder container not healthy"
		}
		if time.Now().After(deadline) {
			if lastErr != nil {
				return fmt.Errorf("forwarder %q did not settle to a serving state within %s: %w", name, forwarderSettleTimeout, lastErr)
			}
			return fmt.Errorf("forwarder %q did not settle to a serving state within %s (last observed: %s)", name, forwarderSettleTimeout, lastReason)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(forwarderSettlePoll):
		}
	}
}
