// This file holds the Docker/fake realization of the ingress provider: one
// shared Caddy container per ingress Provider (bootstrapped once via
// EnsureContainer), with every Connection's route reconciled through Caddy's
// admin API afterward (caddy.go) rather than by rewriting
// ContainerSpec.Files (docs/adr/018 Decision 3).
package ingress

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/rezarajan/platformctl/internal/adapters/providers/providerkit"
	"github.com/rezarajan/platformctl/internal/domain/connection"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// Fixed container-internal ports for the shared Caddy container — the host
// side (published, auto-allocated per component name unless pinned) is what
// varies; see providerkit.HostPort.
const (
	httpContainerPort  = 80
	adminContainerPort = 2019
	bootstrapPath      = "/etc/caddy/config.json"
)

func httpHostPort(cfg provider.Provider, name string) int {
	return providerkit.HostPort(cfg, name, "port")
}

func adminHostPort(cfg provider.Provider, name string) int {
	return providerkit.HostPort(cfg, name, "adminPort")
}

func reconcileInstanceDocker(ctx context.Context, req reconciler.Request, cfg provider.Provider) (status.Status, error) {
	name := containerName(req.Provider)
	configJSON, err := json.Marshal(bootstrapConfig(httpContainerPort, adminContainerPort))
	if err != nil {
		return status.Status{}, fmt.Errorf("marshal bootstrap caddy config: %w", err)
	}
	ctrState, err := providerkit.EnsureInstance(ctx, req.Runtime, providerkit.InstanceSpec{
		Namespace: req.Provider.Metadata.Namespace,
		Name:      name,
		Network:   providerkit.Network(cfg),
		Container: runtime.ContainerSpec{
			Image: image(cfg),
			// No --adapter flag: the bootstrap config is already Caddy's
			// native JSON document, not a format (Caddyfile, etc.) that
			// needs converting — found live (2026-07-21): "--adapter json"
			// is not a recognized adapter name (json is the format Caddy
			// speaks with no adapter at all; "json" as an adapter name
			// caused an immediate "unrecognized config adapter" exit).
			Cmd:   []string{"caddy", "run", "--config", bootstrapPath},
			Files: []runtime.FileMount{{Path: bootstrapPath, Content: configJSON, Mode: 0o444}},
			Ports: []runtime.PortBinding{
				{HostPort: httpHostPort(cfg, name), ContainerPort: httpContainerPort, Audience: runtime.AudienceHost},
				{HostPort: adminHostPort(cfg, name), ContainerPort: adminContainerPort, Audience: runtime.AudienceHost},
			},
			HealthCheck: &runtime.HealthCheck{
				Test:     []string{"CMD-SHELL", fmt.Sprintf("wget -q --spider http://localhost:%d/config/ || exit 1", adminContainerPort)},
				Interval: 2 * time.Second,
				Timeout:  5 * time.Second,
				Retries:  30,
			},
		},
		WaitTimeout: 60 * time.Second,
	})
	if err != nil {
		return status.Status{}, err
	}

	now := time.Now()
	st := status.Status{}
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonProxySurfaceReady}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	st.ProviderState = map[string]any{
		"containerId": ctrState.ID,
		"network":     providerkit.Network(cfg),
		"image":       image(cfg),
		"domain":      domainSuffix(cfg),
	}
	return st, nil
}

func destroyInstanceDocker(ctx context.Context, req reconciler.Request, cfg provider.Provider) error {
	name := containerName(req.Provider)
	if err := req.Runtime.Remove(ctx, name); err != nil {
		return err
	}
	// The network may still be shared with other providers; RemoveNetwork
	// refuses while anything remains attached, and this ignores that
	// refusal — the same convention every shared-network provider's Destroy
	// follows (proxy, prometheus).
	_ = req.Runtime.RemoveNetwork(ctx, providerkit.Network(cfg))
	return nil
}

func probeInstanceDocker(ctx context.Context, req reconciler.Request, cfg provider.Provider) (status.Status, error) {
	name := containerName(req.Provider)
	st := status.Status{}
	now := time.Now()
	ctrState, found, err := req.Runtime.Inspect(ctx, name)
	if err != nil {
		return st, err
	}
	if !found || !ctrState.Healthy {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonProxySurfaceDown}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonProxySurfaceDown}, now)
		return st, nil
	}
	addr, closeAddr, err := providerkit.ReachableAddr(ctx, req.Runtime, name, adminContainerPort)
	if err != nil {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonProxySurfaceDown, Message: err.Error()}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonProxySurfaceDown}, now)
		return st, nil
	}
	defer closeAddr()
	if !caddyReady(ctx, "http://"+addr) {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonProxySurfaceDown}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonProxySurfaceDown}, now)
		return st, nil
	}
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonProxySurfaceReady}, now)
	st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
	return st, nil
}

func reconcileConnectionDocker(ctx context.Context, req reconciler.Request, cfg provider.Provider) (status.Status, error) {
	st := status.Status{}
	conn, err := connection.FromEnvelope(req.Resource)
	if err != nil {
		return st, err
	}
	proxyName := containerName(req.Provider)
	connName := req.Resource.Metadata.Name
	host := routeHost(connName, cfg)
	route := desiredRoute(connName, host, conn.Target)

	adminAddr, closeAdmin, err := providerkit.ReachableAddr(ctx, req.Runtime, proxyName, adminContainerPort)
	if err != nil {
		return st, fmt.Errorf("Connection %q: reach ingress admin API: %w", connName, err)
	}
	defer closeAdmin()
	baseURL := "http://" + adminAddr
	if err := ensureRoute(ctx, baseURL, route); err != nil {
		return st, fmt.Errorf("Connection %q: reconcile route: %w", connName, err)
	}

	// Observed binding, not intent — the shared proxy's actually-published
	// HTTP port, so the endpoint URL this reports is dialable, not guessed.
	proxyState, found, err := req.Runtime.Inspect(ctx, proxyName)
	if err != nil {
		return st, err
	}
	hostURL := ""
	if found {
		if hostAddr := proxyState.HostAddr(httpContainerPort); hostAddr != "" {
			if _, portStr, splitErr := net.SplitHostPort(hostAddr); splitErr == nil {
				hostURL = "http://" + host + ":" + portStr
			}
		}
	}

	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonRouteHealthy}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	st.ProviderState = map[string]any{
		"host":   host,
		"target": conn.Target,
		endpoint.Key: endpoint.List{
			{
				Name:          "route",
				Scheme:        "http",
				Host:          hostURL,
				Internal:      "http://" + proxyName + ":" + strconv.Itoa(httpContainerPort) + " (Host: " + host + ")",
				Insecure:      true,
				RuntimeName:   proxyName,
				ContainerPort: httpContainerPort,
				Audience:      runtime.AudienceHost,
				Network:       providerkit.Network(cfg),
			},
		}.ToState(),
	}
	return st, nil
}

func destroyConnectionDocker(ctx context.Context, req reconciler.Request, cfg provider.Provider) error {
	proxyName := containerName(req.Provider)
	connName := req.Resource.Metadata.Name
	adminAddr, closeAdmin, err := providerkit.ReachableAddr(ctx, req.Runtime, proxyName, adminContainerPort)
	if err != nil {
		// The shared proxy is already gone (e.g. its own Provider was
		// destroyed first in the same run) — nothing to delete a route
		// from.
		return nil
	}
	defer closeAdmin()
	return deleteRoute(ctx, "http://"+adminAddr, routeID(connName))
}

func probeConnectionDocker(ctx context.Context, req reconciler.Request, cfg provider.Provider) (status.Status, error) {
	st := status.Status{}
	now := time.Now()
	conn, err := connection.FromEnvelope(req.Resource)
	if err != nil {
		return st, err
	}
	proxyName := containerName(req.Provider)
	connName := req.Resource.Metadata.Name
	host := routeHost(connName, cfg)

	adminAddr, closeAdmin, err := providerkit.ReachableAddr(ctx, req.Runtime, proxyName, adminContainerPort)
	if err != nil {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonProxySurfaceDown, Message: err.Error()}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonProxySurfaceDown}, now)
		return st, nil
	}
	defer closeAdmin()
	baseURL := "http://" + adminAddr

	live, found, err := getRoute(ctx, baseURL, routeID(connName))
	if err != nil {
		return st, err
	}
	if !found {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonRouteMissing}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonRouteMissing}, now)
		return st, nil
	}
	desired := desiredRoute(connName, host, conn.Target)
	if !routesEquivalent(live, desired) {
		msg := fmt.Sprintf("live route host/upstream differs from declared Connection %q", connName)
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonRouteConfigDrift, Message: msg}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonRouteConfigDrift, Message: msg}, now)
		return st, nil
	}

	proxyAddr, closeProxy, err := providerkit.ReachableAddr(ctx, req.Runtime, proxyName, httpContainerPort)
	if err != nil {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonProxySurfaceDown, Message: err.Error()}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonProxySurfaceDown}, now)
		return st, nil
	}
	defer closeProxy()
	if err := probeThroughRoute(ctx, proxyAddr, host); err != nil {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonIngressUpstreamUnreachable, Message: err.Error()}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonIngressUpstreamUnreachable, Message: err.Error()}, now)
		return st, nil
	}

	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonRouteHealthy}, now)
	st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
	return st, nil
}

// routesEquivalent compares exactly the two fields this provider ever sets
// — Host match and upstream dial — the same "drifted names, never values"
// bar prometheus/debezium/s3sink hold for their own config-drift checks.
func routesEquivalent(live, desired caddyRoute) bool {
	if len(live.Match) != 1 || len(desired.Match) != 1 {
		return false
	}
	if len(live.Match[0].Host) != 1 || len(desired.Match[0].Host) != 1 || live.Match[0].Host[0] != desired.Match[0].Host[0] {
		return false
	}
	if len(live.Handle) != 1 || len(desired.Handle) != 1 {
		return false
	}
	if len(live.Handle[0].Upstreams) != 1 || len(desired.Handle[0].Upstreams) != 1 {
		return false
	}
	return live.Handle[0].Upstreams[0].Dial == desired.Handle[0].Upstreams[0].Dial
}
