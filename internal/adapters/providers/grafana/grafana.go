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
	"strings"
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

// liveAdminCredential reads the admin username/password currently mounted
// on the running container — mirroring postgres's liveSuperuser/mysql's
// liveRootPassword (docs/planning/08 G1). ok is false for a fresh instance
// (no such container yet) or an incomplete read.
func liveAdminCredential(ctx context.Context, rt runtime.ContainerRuntime, name string) (user, pass string, ok bool) {
	ctr, found, err := rt.Inspect(ctx, name)
	if err != nil || !found {
		return "", "", false
	}
	user = ctr.Env["GF_SECURITY_ADMIN_USER"]
	if data, ferr := rt.ReadFile(ctx, name, adminPasswordPath); ferr == nil && len(data) > 0 {
		pass = string(data)
	}
	if user == "" || pass == "" {
		return "", "", false
	}
	return user, pass, true
}

// adminCredentialChanged is I14's rotation-detection branch: true only when
// a previously observed live credential exists AND differs from the
// currently declared one — a fresh instance (hadPrevious false) or an
// unchanged SecretReference both mean "nothing to rotate."
func adminCredentialChanged(hadPrevious bool, prevUser, prevPass, desiredUser, desiredPass string) bool {
	return hadPrevious && (prevUser != desiredUser || prevPass != desiredPass)
}

// resetAdminPasswordCmd is the grafana-cli invocation this package execs
// inside the container to rotate the live admin password (docs/planning/08
// I14) — the vendor-provided fix for C9's recorded "rotation after first
// apply is a documented Grafana limitation" note. Takes no old password:
// grafana-cli's reset-admin-password subcommand operates via local
// filesystem/DB access, not an authenticated session — confirmed live
// against the pinned image. newPassword rides argv, never a logged string;
// callers must not format this command into any log/error message (ADR 013
// fingerprints-only discipline).
func resetAdminPasswordCmd(newPassword string) []string {
	return []string{"grafana-cli", "admin", "reset-admin-password", newPassword}
}

// ensureAdminCredential runs providerkit's try-desired → try-previous →
// rotate-live → retry state machine (docs/planning/08 G1) with grafana's own
// HTTP-login ping and grafana-cli-exec rotation as the callbacks — the same
// shape postgres's ensureSuperuser/mysql's ensureRootPassword use, adapted
// because Grafana's own rotation mechanism is a container exec, not a SQL
// statement over the already-open ping connection.
func ensureAdminCredential(ctx context.Context, rt runtime.ContainerRuntime, name, desiredUser, desiredPass, prevUser, prevPass string, hadPrevious bool) error {
	return providerkit.CredentialRotation{
		Runtime:               rt,
		Name:                  name,
		Port:                  apiPort,
		NoPreviousOrUnchanged: !adminCredentialChanged(hadPrevious, prevUser, prevPass, desiredUser, desiredPass),
		PingDesired: func(ctx context.Context, addr string) error {
			return pingLogin(ctx, addr, desiredUser, desiredPass)
		},
		PingPrevious: func(ctx context.Context, addr string) error {
			return pingLogin(ctx, addr, prevUser, prevPass)
		},
		Rotate: func(ctx context.Context, _ string) error {
			execRt, ok := rt.(runtime.ExecCapableRuntime)
			if !ok {
				return fmt.Errorf("grafana admin credential rotation requires a runtime that supports container exec (grafana-cli admin reset-admin-password); the current runtime does not implement it")
			}
			_, stderr, exitCode, err := execRt.ExecInContainer(ctx, name, resetAdminPasswordCmd(desiredPass))
			if err != nil {
				return fmt.Errorf("exec grafana-cli admin reset-admin-password in container %q: %w", name, err)
			}
			if exitCode != 0 {
				return fmt.Errorf("grafana-cli admin reset-admin-password in container %q exited %d: %s", name, exitCode, strings.TrimSpace(stderr))
			}
			return nil
		},
		Exhausted: func(err error) error {
			return fmt.Errorf("grafana admin credentials changed but neither the desired SecretReference nor the previous managed-container credential can log in; manual recovery is required: %w", err)
		},
	}.Run(ctx)
}

// pingLogin reports whether user/pass authenticates against addr's admin
// API — GET /api/org, any org-admin-scoped endpoint with no side effects.
func pingLogin(ctx context.Context, addr, user, pass string) error {
	if !loginOK(ctx, "http://"+addr, user, pass) {
		return fmt.Errorf("grafana login check failed")
	}
	return nil
}

// loginOK is httpOK against /api/org — the same "does this credential
// authenticate" check Probe and pingLogin both need, kept as one place so
// the endpoint choice is made once.
func loginOK(ctx context.Context, baseURL, user, pass string) bool {
	return httpOK(ctx, baseURL+"/api/org", user, pass)
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
	// Captured before EnsureInstance below, which recreates the container
	// (and its admin-password FileMount) whenever the resolved password
	// changed — the same "read the live value off the running container
	// before it's replaced" precedent as postgres's liveSuperuser/mysql's
	// liveRootPassword (docs/planning/08 G1).
	prevAdminUser, prevAdminPass, hadPrevAdmin := liveAdminCredential(ctx, rt, name)

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
	// I14: the container answering /api/health only proves it booted, not
	// that the declared admin credential is the *live* one — Grafana only
	// consumes GF_SECURITY_ADMIN_PASSWORD__FILE at first boot (initdb-shaped,
	// like postgres/mysql), so a SecretReference rotated after that first
	// apply needs the vendor-provided fix (grafana-cli admin
	// reset-admin-password) exec'd live, then a re-probe login with the new
	// credential before Ready — the same settledness bar every other
	// provider's Reconcile holds itself to.
	if err := ensureAdminCredential(ctx, rt, name, adminUser, adminPass, prevAdminUser, prevAdminPass, hadPrevAdmin); err != nil {
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

	// I14: check login with the declared credential before anything that
	// needs it (datasource/dashboard checks below) — a wrong-credential 401
	// would otherwise surface as the unrelated-sounding DatasourceUnhealthy.
	// Healed by the same ensureAdminCredential path Reconcile already runs.
	if !loginOK(ctx, baseURL, adminUser, adminPass) {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonCredentialDrift}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonCredentialDrift}, now)
		return st, nil
	}

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
