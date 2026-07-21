// This file holds the Kubernetes realization of the ingress provider: one
// networking.k8s.io/v1 Ingress object per Connection, via the
// runtime.IngressCapableRuntime capability (internal/adapters/runtime/
// kubernetes/ingress.go). No shared container — the cluster's own ingress
// controller does the proxying, so there is no reload-without-restart
// problem to solve on this runtime (docs/adr/018 Decision 2).
package ingress

import (
	"context"
	"fmt"
	"time"

	"github.com/rezarajan/platformctl/internal/adapters/providers/providerkit"
	"github.com/rezarajan/platformctl/internal/domain/connection"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// ingressRuntime type-asserts req.Runtime against the optional
// IngressCapableRuntime capability (docs/adr/018 "Layering") — the provider
// never imports the concrete Kubernetes adapter package.
func ingressRuntime(rt runtime.ContainerRuntime) (runtime.IngressCapableRuntime, error) {
	ic, ok := rt.(runtime.IngressCapableRuntime)
	if !ok {
		return nil, fmt.Errorf("ingress provider: runtime does not implement IngressCapableRuntime (expected on a Kubernetes-runtime Provider)")
	}
	return ic, nil
}

// reconcileInstanceKubernetes anchors the shared network only — mirroring
// proxy's own Provider-level reconcile, and the fact that there is no
// central Kubernetes object of this provider's own (every Connection's
// Ingress is self-contained).
func reconcileInstanceKubernetes(ctx context.Context, req reconciler.Request, cfg provider.Provider) (status.Status, error) {
	name := containerName(req.Provider)
	labels := runtime.ManagedLabels(req.Provider.Metadata.Namespace, "Provider", name, name)
	if err := req.Runtime.EnsureNetwork(ctx, runtime.NetworkSpec{Name: providerkit.Network(cfg), Labels: labels}); err != nil {
		return status.Status{}, err
	}
	now := time.Now()
	st := status.Status{}
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonProxySurfaceReady}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	st.ProviderState = map[string]any{"network": providerkit.Network(cfg), "domain": domainSuffix(cfg)}
	return st, nil
}

func destroyInstanceKubernetes(ctx context.Context, req reconciler.Request, cfg provider.Provider) error {
	_ = req.Runtime.RemoveNetwork(ctx, providerkit.Network(cfg))
	return nil
}

func probeInstanceKubernetes(ctx context.Context, req reconciler.Request, cfg provider.Provider) (status.Status, error) {
	now := time.Now()
	st := status.Status{}
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonProxySurfaceReady}, now)
	st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
	return st, nil
}

func reconcileConnectionKubernetes(ctx context.Context, req reconciler.Request, cfg provider.Provider) (status.Status, error) {
	st := status.Status{}
	ic, err := ingressRuntime(req.Runtime)
	if err != nil {
		return st, err
	}
	conn, err := connection.FromEnvelope(req.Resource)
	if err != nil {
		return st, err
	}
	targetHost, targetPort, err := parseTarget(conn.Target)
	if err != nil {
		return st, fmt.Errorf("Connection %q: %w", req.Resource.Metadata.Name, err)
	}
	connName := req.Resource.Metadata.Name
	host := routeHost(connName, cfg)
	name := routeID(connName)
	ns := req.Resource.Metadata.Namespace
	if ns == "" {
		ns = "default"
	}
	labels := runtime.ManagedLabels(ns, "Connection", connName, connName)

	ingState, err := ic.EnsureIngress(ctx, runtime.IngressSpec{
		Name:       name,
		Namespace:  providerkit.Network(cfg),
		Host:       host,
		TargetName: targetHost,
		TargetPort: targetPort,
		Labels:     labels,
	})
	if err != nil {
		return st, fmt.Errorf("Connection %q: %w", connName, err)
	}

	hostURL := ""
	if ingState.Address != "" {
		hostURL = "http://" + host
	}
	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonRouteHealthy}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	st.ProviderState = map[string]any{
		"host":   host,
		"target": conn.Target,
		endpoint.Key: endpoint.List{
			{
				Name:     "route",
				Scheme:   "http",
				Host:     hostURL,
				Internal: fmt.Sprintf("http://%s:%d", targetHost, targetPort),
				Insecure: true,
				Network:  providerkit.Network(cfg),
			},
		}.ToState(),
	}
	return st, nil
}

func destroyConnectionKubernetes(ctx context.Context, req reconciler.Request, cfg provider.Provider) error {
	ic, err := ingressRuntime(req.Runtime)
	if err != nil {
		return err
	}
	return ic.RemoveIngress(ctx, providerkit.Network(cfg), routeID(req.Resource.Metadata.Name))
}

func probeConnectionKubernetes(ctx context.Context, req reconciler.Request, cfg provider.Provider) (status.Status, error) {
	st := status.Status{}
	now := time.Now()
	ic, err := ingressRuntime(req.Runtime)
	if err != nil {
		return st, err
	}
	conn, err := connection.FromEnvelope(req.Resource)
	if err != nil {
		return st, err
	}
	targetHost, targetPort, err := parseTarget(conn.Target)
	if err != nil {
		return st, fmt.Errorf("Connection %q: %w", req.Resource.Metadata.Name, err)
	}
	connName := req.Resource.Metadata.Name
	host := routeHost(connName, cfg)

	live, found, err := ic.GetIngress(ctx, providerkit.Network(cfg), routeID(connName))
	if err != nil {
		return st, err
	}
	if !found {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonRouteMissing}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonRouteMissing}, now)
		return st, nil
	}
	if live.Host != host || live.TargetName != targetHost || live.TargetPort != targetPort {
		msg := fmt.Sprintf("live Ingress (host=%q, backend=%s:%d) differs from declared Connection %q (host=%q, backend=%s:%d)",
			live.Host, live.TargetName, live.TargetPort, connName, host, targetHost, targetPort)
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonRouteConfigDrift, Message: msg}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonRouteConfigDrift, Message: msg}, now)
		return st, nil
	}

	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonRouteHealthy}, now)
	st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
	return st, nil
}
