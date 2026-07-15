// Package openlineage reconciles a Marquez deployment (the Marquez API
// container plus its dedicated Postgres) — the OpenLineage backend that
// metadata.observers resolve endpoints against. This is the Phase 6
// "optional" provider, built in Phase 6.5; it graduates
// LineageObservability to Beta.
package openlineage

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

const (
	defaultImage   = "marquezproject/marquez:latest"
	defaultDBImage = "postgres:16"
	marquezAPIPort = 5000
)

var httpClient = &http.Client{Timeout: 10 * time.Second}

type Provider struct {
	providerRes resource.Envelope
	cfg         provider.Provider
}

func New() *Provider { return &Provider{} }

func (p *Provider) Type() string { return "openlineage" }

func (p *Provider) SetProviderResource(env resource.Envelope) {
	p.providerRes = env
	p.cfg, _ = provider.FromEnvelope(env)
}

func (p *Provider) name() string   { return p.providerRes.Metadata.Name }
func (p *Provider) dbName() string { return p.name() + "-db" }

func (p *Provider) hostPort() int {
	if v, ok := p.cfg.Configuration["apiPort"]; ok {
		switch n := v.(type) {
		case int:
			return n
		case float64:
			return int(n)
		}
	}
	return marquezAPIPort
}

func (p *Provider) network() string {
	if n, ok := p.cfg.RuntimeConfig["network"].(string); ok && n != "" {
		return n
	}
	return "datascape"
}

// marquezInternalCred is what the Marquez image's baked-in dev
// configuration (marquez.dev.yml) hardcodes for its database user, password,
// and database name — only host/port are substitutable via env. The
// metadata store is a dedicated container internal to this provider (never
// published to the host), so it carries these fixed credentials rather than
// pretending a SecretReference could change them.
const marquezInternalCred = "marquez"

func (p *Provider) apiURL() string {
	return fmt.Sprintf("http://127.0.0.1:%d/api/v1/namespaces", p.hostPort())
}

func (p *Provider) Reconcile(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) (status.Status, error) {
	st := status.Status{}
	if res.Kind != "Provider" {
		return st, fmt.Errorf("openlineage provider cannot reconcile kind %s", res.Kind)
	}
	image, _ := p.cfg.Configuration["image"].(string)
	if image == "" {
		image = defaultImage
	}
	labels := map[string]string{
		runtime.LabelManagedBy:  runtime.ManagedByValue,
		runtime.LabelGeneration: p.name(),
	}

	if err := rt.EnsureNetwork(ctx, runtime.NetworkSpec{Name: p.network(), Labels: labels}); err != nil {
		return st, err
	}
	// Marquez's metadata store: a dedicated Postgres, internal to the
	// provider (not published to the host).
	if err := rt.EnsureVolume(ctx, runtime.VolumeSpec{Name: p.dbName() + "-data", Labels: labels}); err != nil {
		return st, err
	}
	_, err := rt.EnsureContainer(ctx, runtime.ContainerSpec{
		Name:  p.dbName(),
		Image: defaultDBImage,
		Env: map[string]string{
			"POSTGRES_USER":     marquezInternalCred,
			"POSTGRES_PASSWORD": marquezInternalCred,
			"POSTGRES_DB":       "marquez",
		},
		Networks: []string{p.network()},
		Volumes:  []runtime.VolumeMount{{VolumeName: p.dbName() + "-data", MountPath: "/var/lib/postgresql/data"}},
		HealthCheck: &runtime.HealthCheck{
			Test:     []string{"CMD-SHELL", "pg_isready -h 127.0.0.1 -U " + marquezInternalCred},
			Interval: 2 * time.Second,
			Timeout:  5 * time.Second,
			Retries:  30,
		},
		Labels: labels,
	})
	if err != nil {
		return st, err
	}
	if err := rt.WaitHealthy(ctx, p.dbName(), 120*time.Second); err != nil {
		return st, err
	}

	ctrState, err := rt.EnsureContainer(ctx, runtime.ContainerSpec{
		Name:  p.name(),
		Image: image,
		Env: map[string]string{
			"MARQUEZ_PORT":       "5000",
			"MARQUEZ_ADMIN_PORT": "5001",
			"POSTGRES_HOST":      p.dbName(),
			"POSTGRES_PORT":      "5432",
			"POSTGRES_DB":        "marquez",
			"POSTGRES_USER":      marquezInternalCred,
			"POSTGRES_PASSWORD":  marquezInternalCred,
		},
		Networks: []string{p.network()},
		Ports:    []runtime.PortBinding{{HostPort: p.hostPort(), ContainerPort: marquezAPIPort}},
		Labels:   labels,
	})
	if err != nil {
		return st, err
	}
	if err := rt.WaitHealthy(ctx, p.name(), 120*time.Second); err != nil {
		return st, err
	}
	if err := waitHTTPOK(ctx, p.apiURL(), 180*time.Second); err != nil {
		return st, err
	}

	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "LineageBackendHealthy"}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: "ReconcileComplete"}, now)
	st.ProviderState = map[string]any{
		"containerId": ctrState.ID,
		// The engine resolves observers against this: the in-network base
		// URL OpenLineage transports post to.
		"url":     fmt.Sprintf("http://%s:%d", p.name(), marquezAPIPort),
		"hostApi": fmt.Sprintf("http://127.0.0.1:%d/api/v1", p.hostPort()),
	}
	return st, nil
}

func (p *Provider) Destroy(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) error {
	if res.Kind != "Provider" {
		return fmt.Errorf("openlineage provider cannot destroy kind %s", res.Kind)
	}
	if err := rt.Remove(ctx, p.name()); err != nil {
		return err
	}
	if err := rt.Remove(ctx, p.dbName()); err != nil {
		return err
	}
	if err := rt.RemoveVolume(ctx, p.dbName()+"-data"); err != nil {
		return err
	}
	_ = rt.RemoveNetwork(ctx, p.network())
	return nil
}

func (p *Provider) Probe(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) (status.Status, error) {
	st := status.Status{}
	now := time.Now()
	if res.Kind != "Provider" {
		return st, fmt.Errorf("openlineage provider cannot probe kind %s", res.Kind)
	}
	api, apiFound, err := rt.Inspect(ctx, p.name())
	if err != nil {
		return st, err
	}
	db, dbFound, err := rt.Inspect(ctx, p.dbName())
	if err != nil {
		return st, err
	}
	healthy := apiFound && api.Healthy && dbFound && db.Healthy && httpOK(ctx, p.apiURL())
	if healthy {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "LineageBackendHealthy"}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: "NoDrift"}, now)
	} else {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: "LineageBackendUnhealthy"}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: "LineageBackendUnhealthy"}, now)
	}
	return st, nil
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

func waitHTTPOK(ctx context.Context, url string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if httpOK(ctx, url) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("endpoint %s did not answer 200 within %s", url, timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}
