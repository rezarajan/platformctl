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

	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
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
	return network, res.Metadata.Name + "-data", res.Metadata.Name
}

func (p *Provider) Reconcile(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) (status.Status, error) {
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
	if err := rt.EnsureVolume(ctx, runtime.VolumeSpec{Name: volName, Labels: labels}); err != nil {
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

	ctrState, err := rt.EnsureContainer(ctx, runtime.ContainerSpec{
		Name:     ctrName,
		Image:    image,
		Cmd:      cmd,
		Networks: []string{netName},
		Volumes:  []runtime.VolumeMount{{VolumeName: volName, MountPath: "/data"}},
		Labels:   labels,
	})
	if err != nil {
		return st, err
	}
	if err := rt.WaitHealthy(ctx, ctrName, 60*time.Second); err != nil {
		return st, err
	}

	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "HealthCheckPassed"}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: "ReconcileComplete"}, now)
	st.ProviderState = map[string]any{"containerId": ctrState.ID}
	return st, nil
}

func (p *Provider) Destroy(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) error {
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

func (p *Provider) Probe(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) (status.Status, error) {
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
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: "ContainerMissing"}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: "ContainerMissing"}, now)
	case !ctrState.Healthy:
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: "ContainerUnhealthy"}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: "ContainerUnhealthy"}, now)
	default:
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "HealthCheckPassed"}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: "NoDrift"}, now)
	}
	return st, nil
}
