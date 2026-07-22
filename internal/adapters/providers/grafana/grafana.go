// Package grafana reconciles a managed Grafana container (docs/planning/08
// C9 completion): provisioned entirely via ContainerSpec.Files (Grafana's
// own file-based provisioning mechanism) with a Prometheus datasource
// resolved from the prometheus Provider's own published endpoint fact
// (reconciler.Request.PrometheusURL, ADR 015 — never constructed by this
// package) and a minimal starter dashboard. Nessie-shaped (a single-
// container instance, no dependent kind) — see
// internal/adapters/providers/nessie for the template this follows. Gated
// by the existing MonitoringStackProvider gate (docs/planning/08 C9), not a
// new one: it gates the monitoring stack as a class.
package grafana

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/rezarajan/platformctl/internal/adapters/providers/providerkit"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

const (
	// defaultImage is pinned (scripts/pinned-images.txt).
	defaultImage = "grafana/grafana:11.4.0@sha256:d8ea37798ccc41061a62ab080f2676dda6bf7815558499f901bdb0f533a456fb"
	apiPort      = 3000
	// adminPasswordPath is where the admin credential's password file is
	// mounted — GF_SECURITY_ADMIN_PASSWORD__FILE, never env
	// (docs/planning/07 Gate 1 checkbox 4). Verified live: Grafana's own
	// double-underscore "__FILE" env-var convention.
	adminPasswordPath = "/run/datascape/admin-password"
)

var httpClient = &http.Client{Timeout: 10 * time.Second}

// Provider holds no cross-call state (docs/planning/08 F5): every method
// receives what it needs via reconciler.Request.
type Provider struct{}

func New() *Provider { return &Provider{} }

func (p *Provider) Type() string { return "grafana" }

func containerName(req reconciler.Request) string { return naming.RuntimeObjectName(req.Provider) }

// adminCredential resolves the required admin SecretReference — the
// SecretReference named by configuration.adminSecretRef, or the first
// declared secretRef, mirroring postgres's superuser()/mysql's
// rootPassword() pattern.
func adminCredential(cfg provider.Provider, secrets map[string]map[string]string, name string) (user, pass string, err error) {
	creds, refName, err := providerkit.ResolveCredential(cfg, secrets, "adminSecretRef", name)
	if err != nil {
		return "", "", err
	}
	user, pass = creds["username"], creds["password"]
	if user == "" || pass == "" {
		return "", "", fmt.Errorf("Provider %q: secretRef %q must provide username and password keys", name, refName)
	}
	return user, pass, nil
}

func (p *Provider) Reconcile(ctx context.Context, req reconciler.Request) (status.Status, error) {
	if req.Resource.Kind != "Provider" {
		return status.Status{}, fmt.Errorf("grafana provider cannot reconcile kind %s", req.Resource.Kind)
	}
	rt := req.Runtime
	st := status.Status{}
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return st, err
	}
	name := containerName(req)
	image, _ := cfg.Configuration["image"].(string)
	if image == "" {
		image = defaultImage
	}
	adminUser, adminPass, err := adminCredential(cfg, req.Secrets, name)
	if err != nil {
		return st, err
	}

	files := []runtime.FileMount{
		{Path: dashboardProviderProvisioningPath, Content: dashboardProviderYAML, Mode: 0o444},
		{Path: dashboardJSONPath, Content: starterDashboardJSON, Mode: 0o444},
		{Path: adminPasswordPath, Content: []byte(adminPass), Mode: 0o444},
	}
	// req.PrometheusURL is an already-published, already-resolved endpoint
	// fact (ADR 015) — see reconciler.Request.PrometheusURL's doc comment.
	// Empty means unresolved yet (the referenced/inferred prometheus
	// Provider has not reconciled and published its endpoint, or is
	// ambiguous/absent): the datasource provisioning file is simply not
	// written this reconcile, and Probe reports ReasonPrometheusUnresolved
	// — the same "next apply converges" caveat prometheus's own C9 status
	// note already accepts for its scrape targets (no graph edge orders
	// grafana after prometheus either).
	if req.PrometheusURL != "" {
		files = append(files, runtime.FileMount{Path: datasourceProvisioningPath, Content: datasourceYAML(req.PrometheusURL), Mode: 0o444})
	}

	ctrState, err := providerkit.EnsureInstance(ctx, rt, providerkit.InstanceSpec{
		Namespace: req.Provider.Metadata.Namespace,
		Name:      name,
		Network:   providerkit.Network(cfg),
		Container: runtime.ContainerSpec{
			Image: image,
			Env: map[string]string{
				"GF_SECURITY_ADMIN_USER":           adminUser,
				"GF_SECURITY_ADMIN_PASSWORD__FILE": adminPasswordPath,
				// Explicit, not relied on as Grafana's own default
				// (docs/planning/08 C9 completion's "anonymous access off"
				// requirement).
				"GF_AUTH_ANONYMOUS_ENABLED": "false",
			},
			Files: files,
			Ports: []runtime.PortBinding{{HostPort: providerkit.HostPort(cfg, name, "port"), ContainerPort: apiPort, Audience: runtime.AudienceHost}},
			HealthCheck: &runtime.HealthCheck{
				Test:     []string{"CMD-SHELL", fmt.Sprintf("wget -q --spider http://127.0.0.1:%d/api/health || exit 1", apiPort)},
				Interval: 2 * time.Second,
				Timeout:  5 * time.Second,
				Retries:  30,
			},
		},
		WaitTimeout: 120 * time.Second,
	})
	if err != nil {
		return st, err
	}
	if err := waitAPIReady(ctx, rt, name, 120*time.Second); err != nil {
		return st, err
	}

	now := time.Now()
	if req.PrometheusURL == "" {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonPrometheusUnresolved}, now)
		st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	} else {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonInstanceHealthy}, now)
		st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	}
	hostAddr := ctrState.HostAddr(apiPort) // observed binding, not intent
	hostURL := ""
	if hostAddr != "" {
		hostURL = "http://" + hostAddr
	}
	st.ProviderState = map[string]any{
		"containerId":          ctrState.ID,
		"internalAddr":         fmt.Sprintf("http://%s:%d", name, apiPort),
		"prometheusConfigured": req.PrometheusURL != "",
		endpoint.Key: endpoint.List{
			{Name: "grafana", Scheme: "http", Host: hostURL, Internal: fmt.Sprintf("http://%s:%d", name, apiPort), Insecure: true},
		}.ToState(),
	}
	return st, nil
}

func (p *Provider) Destroy(ctx context.Context, req reconciler.Request) error {
	if req.Resource.Kind != "Provider" {
		return fmt.Errorf("grafana provider cannot destroy kind %s", req.Resource.Kind)
	}
	rt := req.Runtime
	name := containerName(req)
	if err := rt.Remove(ctx, name); err != nil {
		return err
	}
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return err
	}
	// The network may still be shared with every other provider on it;
	// RemoveNetwork refuses (and this ignores that refusal) while any
	// container remains attached — the same convention every other
	// single-container provider's Destroy follows.
	_ = rt.RemoveNetwork(ctx, providerkit.Network(cfg))
	return nil
}

func (p *Provider) Probe(ctx context.Context, req reconciler.Request) (status.Status, error) {
	if req.Resource.Kind != "Provider" {
		return status.Status{}, fmt.Errorf("grafana provider cannot probe kind %s", req.Resource.Kind)
	}
	rt := req.Runtime
	st := status.Status{}
	now := time.Now()
	name := containerName(req)

	ctrState, found, err := rt.Inspect(ctx, name)
	if err != nil {
		return st, err
	}
	if !found || !ctrState.Healthy {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonInstanceUnhealthy}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonInstanceUnhealthy}, now)
		return st, nil
	}
	if req.PrometheusURL == "" {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonPrometheusUnresolved}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonPrometheusUnresolved}, now)
		return st, nil
	}

	addr, closeAddr, err := providerkit.ReachableAddr(ctx, rt, name, apiPort)
	if err != nil {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonInstanceUnhealthy, Message: err.Error()}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonInstanceUnhealthy}, now)
		return st, nil
	}
	defer closeAddr()
	adminUser, adminPass := "", ""
	if cfg, cerr := provider.FromEnvelope(req.Provider); cerr == nil {
		adminUser, adminPass, _ = adminCredential(cfg, req.Secrets, name)
	}
	baseURL := "http://" + addr

	if !datasourceHealthy(ctx, baseURL, adminUser, adminPass) {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonDatasourceUnhealthy}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonDatasourceUnhealthy}, now)
		return st, nil
	}
	if !dashboardExists(ctx, baseURL, adminUser, adminPass) {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonDashboardMissing}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonDashboardMissing}, now)
		return st, nil
	}

	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonInstanceHealthy}, now)
	st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
	return st, nil
}

// waitAPIReady polls /api/health via runtime.WithReachable
// (docs/planning/09 Class 2 / F1) so every attempt gets a freshly-resolved
// address rather than reusing one across the whole wait — the same
// defensive pattern nessie's/openlineage's/prometheus's own waitReady
// helpers document. /api/health requires no authentication (verified
// live), so no credential is needed here — only the container itself
// answering.
func waitAPIReady(ctx context.Context, rt runtime.ContainerRuntime, name string, timeout time.Duration) error {
	opts := runtime.ReachableOptions{Timeout: timeout, Interval: 2 * time.Second}
	err := runtime.WithReachable(ctx, rt, name, apiPort, opts, func(ctx context.Context, addr string) error {
		if !httpOK(ctx, "http://"+addr+"/api/health", "", "") {
			return fmt.Errorf("grafana /api/health did not answer 200")
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("grafana did not become ready within %s: %w", timeout, err)
	}
	return nil
}

func httpOK(ctx context.Context, url, user, pass string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}
	if user != "" {
		req.SetBasicAuth(user, pass)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// datasourceHealthResponse is the subset of Grafana's
// /api/datasources/uid/:uid/health response this package reads.
type datasourceHealthResponse struct {
	Status string `json:"status"`
}

// datasourceHealthy calls Grafana's own datasource health-check API — the
// datasource genuinely reaching Prometheus, not just existing.
func datasourceHealthy(ctx context.Context, baseURL, user, pass string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/api/datasources/uid/"+datasourceUID+"/health", nil)
	if err != nil {
		return false
	}
	req.SetBasicAuth(user, pass)
	resp, err := httpClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	var hr datasourceHealthResponse
	if err := json.NewDecoder(resp.Body).Decode(&hr); err != nil {
		return false
	}
	return hr.Status == "OK"
}

// dashboardExists checks the starter dashboard's provisioning landed —
// Grafana's own /api/dashboards/uid/:uid, 200 iff present.
func dashboardExists(ctx context.Context, baseURL, user, pass string) bool {
	return httpOK(ctx, baseURL+"/api/dashboards/uid/"+dashboardUID, user, pass)
}

// ValidateSpec implements SpecValidator: the instance cannot boot without
// admin credentials, so their wiring is checked at validate — mirroring
// postgres/mysql's own superuser/root SecretReference checks.
func (p *Provider) ValidateSpec(cfg provider.Provider) error {
	if ref, _ := cfg.Configuration["adminSecretRef"].(string); ref != "" {
		if !cfg.HasSecretRef(ref) {
			return fmt.Errorf("configuration.adminSecretRef %q must also be listed in spec.secretRefs for the engine to resolve it", ref)
		}
	} else if len(cfg.SecretRefs) == 0 {
		return fmt.Errorf("spec.secretRefs must name at least one SecretReference (the admin credentials; configuration.adminSecretRef selects one explicitly)")
	}
	// configuration.prometheusRef (when set) is a nameRef, not a secretRef;
	// nothing to check here at validate time — whether it resolves to a
	// real Provider is a graph/compatibility concern, mirroring trino's own
	// catalogRef (ValidateSpec does not check that one either).
	if v, ok := cfg.Configuration["image"]; ok {
		if s, isStr := v.(string); !isStr || s == "" {
			return fmt.Errorf("spec.configuration.image must be a non-empty string, got %v", v)
		}
	}
	return nil
}
