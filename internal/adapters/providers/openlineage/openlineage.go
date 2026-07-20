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

	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/hostport"
	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

const (
	defaultImage   = "marquezproject/marquez:0.51.1@sha256:0721c976cff17d8b14f7949d85d6dac9c7ea37cb9fe857caa19833730fcb1a50"
	defaultDBImage = "postgres:16@sha256:33f923b05f64ca54ac4401c01126a6b92afe839a0aa0a52bc5aeb5cc958e5f20"
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

func (p *Provider) name() string   { return naming.RuntimeObjectName(p.providerRes) }
func (p *Provider) dbName() string { return p.name() + "-db" }

func (p *Provider) hostPort() int {
	configured := 0
	if v, ok := p.cfg.Configuration["apiPort"]; ok {
		switch n := v.(type) {
		case int:
			configured = n
		case float64:
			configured = int(n)
		}
	}
	return hostport.Resolve(configured, p.name())
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

// reachableAPIURL returns an "http://host:port/api/v1/namespaces" this
// process can dial right now, plus a close func that must always be called
// (docs/planning/08 B8: Docker's is a cheap no-op; Kubernetes may tear down
// a port-forward tunnel opened just for this call).
func (p *Provider) reachableAPIURL(ctx context.Context, rt runtime.ContainerRuntime) (string, func() error, error) {
	addr, closeAddr, err := rt.EnsureReachable(ctx, p.name(), marquezAPIPort)
	if err != nil {
		return "", nil, err
	}
	return "http://" + addr + "/api/v1/namespaces", closeAddr, nil
}

// waitAPIReady polls the API until it answers 200, via runtime.WithReachable
// (docs/planning/09 Class 2 / F1) so every attempt gets a freshly-resolved
// tunnel rather than reusing one across the whole wait — see
// nessie.Provider.waitAPIReady's doc for why (found live against minikube:
// a tunnel opened while Marquez is still starting can end up silently dead
// even once the app comes up).
func (p *Provider) waitAPIReady(ctx context.Context, rt runtime.ContainerRuntime, timeout time.Duration) error {
	opts := runtime.ReachableOptions{Timeout: timeout, Interval: 2 * time.Second}
	err := runtime.WithReachable(ctx, rt, p.name(), marquezAPIPort, opts, func(ctx context.Context, addr string) error {
		if !httpOK(ctx, "http://"+addr+"/api/v1/namespaces") {
			return fmt.Errorf("marquez API did not answer 200")
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("marquez API did not answer 200 within %s: %w", timeout, err)
	}
	return nil
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
	labels := runtime.ManagedLabels(p.providerRes.Metadata.Namespace, "Provider", p.name(), p.name())

	if err := rt.EnsureNetwork(ctx, runtime.NetworkSpec{Name: p.network(), Labels: labels}); err != nil {
		return st, err
	}
	// Marquez's metadata store: a dedicated Postgres, internal to the
	// provider (not published to the host).
	if err := rt.EnsureVolume(ctx, runtime.VolumeSpec{Name: p.dbName() + "-data", Labels: labels, Networks: []string{p.network()}}); err != nil {
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
		// Audience: internal — no host publish (this DB is internal to the
		// provider), but the port must still be declared: the Kubernetes
		// adapter only creates a Service (and therefore a DNS name) for
		// ports present here (docs/planning/08 B8), unlike Docker's bridge
		// network, which reaches every container port by name regardless
		// of what's declared. Marquez's own connection to this DB failed
		// with UnknownHostException before this — found live against
		// minikube, not a synthetic test.
		Ports: []runtime.PortBinding{{ContainerPort: 5432, Audience: runtime.AudienceInternal}},
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
		Ports:    []runtime.PortBinding{{HostPort: p.hostPort(), ContainerPort: marquezAPIPort, Audience: runtime.AudienceHost}},
		Labels:   labels,
	})
	if err != nil {
		return st, err
	}
	if err := rt.WaitHealthy(ctx, p.name(), 120*time.Second); err != nil {
		return st, err
	}
	if err := p.waitAPIReady(ctx, rt, 180*time.Second); err != nil {
		return st, err
	}

	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "LineageBackendHealthy"}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: "ReconcileComplete"}, now)
	// Observed binding, not intent.
	hostAddr := ctrState.HostAddr(marquezAPIPort)
	hostAPI := ""
	if hostAddr != "" {
		hostAPI = "http://" + hostAddr + "/api/v1"
	}
	st.ProviderState = map[string]any{
		"containerId": ctrState.ID,
		// The engine resolves observers against this: the in-network base
		// URL OpenLineage transports post to.
		"url":     fmt.Sprintf("http://%s:%d", p.name(), marquezAPIPort),
		"hostApi": hostAPI,
		endpoint.Key: endpoint.List{
			{Name: "openlineage", Scheme: "http", Host: hostAPI, Internal: fmt.Sprintf("http://%s:%d/api/v1", p.name(), marquezAPIPort), Insecure: true},
		}.ToState(),
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
	healthy := false
	if apiFound && api.Healthy && dbFound && db.Healthy {
		if apiURL, closeAPI, err := p.reachableAPIURL(ctx, rt); err == nil {
			healthy = httpOK(ctx, apiURL)
			closeAPI()
		}
	}
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
