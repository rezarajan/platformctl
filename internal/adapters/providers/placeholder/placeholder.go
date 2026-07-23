// Package placeholder is the Phase 1 "prove the runtime" provider: it
// reconciles a single container (plus network and volume) from a Provider
// resource's configuration, with no technology-specific behavior. Real
// providers (redpanda, postgres, ...) supersede it from Phase 2 on, but it
// remains useful for testing the runtime path in isolation.
package placeholder

import (
	"context"
	"fmt"
	"time"

	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

type Provider struct{}

func New() *Provider { return &Provider{} }

func (p *Provider) Type() string { return "container" }

func names(res resource.Envelope, cfg provider.Provider) (network, volume, container string) {
	network = "datascape"
	if n, ok := cfg.RuntimeConfig["network"].(string); ok && n != "" {
		network = n
	}
	container = naming.RuntimeObjectName(res)
	return network, container + "-data", container
}

func (p *Provider) Reconcile(ctx context.Context, req reconciler.Request) (status.Status, error) {
	res, rt := req.Resource, req.Runtime
	st := status.Status{}
	cfg, err := provider.FromEnvelope(res)
	if err != nil {
		return st, err
	}
	image, _ := cfg.Configuration["image"].(string)
	if image == "" {
		return st, fmt.Errorf("Provider %q (type: container): spec.configuration.image is required", res.Metadata.Name)
	}

	netName, volName, ctrName := names(res, cfg)
	labels := runtime.ManagedLabels(res.Metadata.Namespace, res.Kind, res.Metadata.Name, res.Metadata.Name)

	if err := rt.EnsureNetwork(ctx, runtime.NetworkSpec{Name: netName, Labels: labels}); err != nil {
		return st, err
	}
	if err := rt.EnsureVolume(ctx, runtime.VolumeSpec{Name: volName, Labels: labels, Networks: []string{netName}}); err != nil {
		return st, err
	}

	var cmd []string
	if rawCmd, ok := cfg.Configuration["cmd"].([]any); ok {
		for _, c := range rawCmd {
			if s, ok := c.(string); ok {
				cmd = append(cmd, s)
			}
		}
	}

	// spec.configuration.ports (optional, docs/planning/08 H5's cross-runtime
	// segmentation test): container-internal ports this instance actually
	// listens on. Never published to the host (AudienceInternal) — this
	// provider exists to "prove the runtime path in isolation," and on
	// Kubernetes a container with zero declared ports gets no Service at all
	// (internal/adapters/runtime/kubernetes's ensureOneService skips Service
	// creation when a spec has no ports), so a placeholder instance other
	// resources need to dial in-network must declare one. Omitted (the
	// default) keeps every existing manifest byte-for-byte unchanged — this
	// is orthogonal to domain/network-naming (docs/adr/022 Ring 1 lives
	// entirely in internal/application/engine's decorator, never here).
	var ports []runtime.PortBinding
	if rawPorts, ok := cfg.Configuration["ports"].([]any); ok {
		for _, pv := range rawPorts {
			switch v := pv.(type) {
			case int:
				ports = append(ports, runtime.PortBinding{ContainerPort: v, Audience: runtime.AudienceInternal})
			case float64:
				ports = append(ports, runtime.PortBinding{ContainerPort: int(v), Audience: runtime.AudienceInternal})
			}
		}
	}

	ctrState, err := rt.EnsureContainer(ctx, runtime.ContainerSpec{
		Name:     ctrName,
		Image:    image,
		Cmd:      cmd,
		Networks: []string{netName},
		Volumes:  []runtime.VolumeMount{{VolumeName: volName, MountPath: "/data"}},
		Ports:    ports,
		Labels:   labels,
	})
	if err != nil {
		return st, err
	}
	if err := rt.WaitHealthy(ctx, ctrName, 60*time.Second); err != nil {
		return st, err
	}

	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonHealthCheckPassed}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	st.ProviderState = map[string]any{"containerId": ctrState.ID}
	return st, nil
}

func (p *Provider) Destroy(ctx context.Context, req reconciler.Request) error {
	res, rt := req.Resource, req.Runtime
	cfg, err := provider.FromEnvelope(res)
	if err != nil {
		return err
	}
	netName, volName, ctrName := names(res, cfg)
	if err := rt.Remove(ctx, ctrName); err != nil {
		return err
	}
	if err := rt.RemoveVolume(ctx, volName); err != nil {
		return err
	}
	// The network may be shared with other providers; removal failure due to
	// active endpoints is not an error worth failing destroy over.
	_ = rt.RemoveNetwork(ctx, netName)
	return nil
}

func (p *Provider) Probe(ctx context.Context, req reconciler.Request) (status.Status, error) {
	res, rt := req.Resource, req.Runtime
	st := status.Status{}
	cfg, err := provider.FromEnvelope(res)
	if err != nil {
		return st, err
	}
	_, _, ctrName := names(res, cfg)
	ctrState, found, err := rt.Inspect(ctx, ctrName)
	if err != nil {
		return st, err
	}
	now := time.Now()
	switch {
	case !found:
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonContainerMissing}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonContainerMissing}, now)
	case !ctrState.Healthy:
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonContainerUnhealthy}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonContainerUnhealthy}, now)
	default:
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonHealthCheckPassed}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
	}
	return st, nil
}
