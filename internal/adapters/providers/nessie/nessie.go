// Package nessie realizes Catalog resources with Project Nessie, the
// Iceberg REST catalog orchestrators (Dagster, Spark, Trino) point their
// lakehouse writers at. The Provider resource reconciles the Nessie
// container; a Catalog(engine: nessie) reconciles catalog-level facts (the
// default branch) against its REST API. Nessie is one engine behind the
// provider-agnostic Catalog kind — implements CatalogCapableProvider.
package nessie

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/rezarajan/platformctl/internal/adapters/providers/providerkit"
	"github.com/rezarajan/platformctl/internal/domain/catalog"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

const (
	defaultImage = "ghcr.io/projectnessie/nessie:0.108.1@sha256:0b1ffbe56a1cbc1b86641ccd83465ab3447339ea4ed17a1fca42c50288e1479d"
	apiPort      = 19120
)

var httpClient = &http.Client{Timeout: 10 * time.Second}

// Provider holds no cross-call state (docs/planning/08 F5): every method
// receives what it needs via reconciler.Request.
type Provider struct{}

func New() *Provider { return &Provider{} }

func (p *Provider) Type() string { return "nessie" }

// SupportedCatalogEngines implements CatalogCapableProvider.
func (p *Provider) SupportedCatalogEngines() []string { return []string{"nessie"} }

func containerName(provEnv resource.Envelope) string { return naming.RuntimeObjectName(provEnv) }

// reachableAPIURL is providerkit.ReachableURL with the "/api/v2" base
// appended — Nessie's REST API is stateless HTTP with no broker-style
// redirect, so the resolved address can be used directly for one call.
func reachableAPIURL(ctx context.Context, rt runtime.ContainerRuntime, name string) (string, func() error, error) {
	url, closeAddr, err := providerkit.ReachableURL(ctx, rt, name, apiPort)
	if err != nil {
		return "", nil, err
	}
	return url + "/api/v2", closeAddr, nil
}

// waitAPIReady polls path until it answers 200, via runtime.WithReachable
// (docs/planning/09 Class 2 / F1) so every attempt gets a freshly-resolved
// tunnel rather than reusing one across the whole wait. Found live against
// minikube, not a synthetic test: a port-forward tunnel opened while the
// app is still starting (its very first dial racing the JVM's listen())
// can end up silently dead for the rest of its life even once the app
// comes up, while a fresh tunnel opened moments later against the same,
// by-then-ready pod works every time.
func waitAPIReady(ctx context.Context, rt runtime.ContainerRuntime, name, path string, timeout time.Duration) error {
	opts := runtime.ReachableOptions{Timeout: timeout, Interval: 2 * time.Second}
	err := runtime.WithReachable(ctx, rt, name, apiPort, opts, func(ctx context.Context, addr string) error {
		// path is relative to the /api/v2 base (matching reachableAPIURL's
		// convention) — a regression here once polled "/config" directly
		// instead of "/api/v2/config", a 404 that always failed the check
		// and burned the full timeout every apply (found live testing the
		// lakehouse example).
		if !httpOK(ctx, "http://"+addr+"/api/v2"+path) {
			return fmt.Errorf("endpoint (path %s) did not answer 200", path)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("endpoint (path %s) did not answer 200 within %s: %w", path, timeout, err)
	}
	return nil
}

func (p *Provider) Reconcile(ctx context.Context, req reconciler.Request) (status.Status, error) {
	switch req.Resource.Kind {
	case "Provider":
		return p.reconcileInstance(ctx, req)
	case "Catalog":
		return p.reconcileCatalog(ctx, req)
	default:
		return status.Status{}, fmt.Errorf("nessie provider cannot reconcile kind %s", req.Resource.Kind)
	}
}

func (p *Provider) reconcileInstance(ctx context.Context, req reconciler.Request) (status.Status, error) {
	rt := req.Runtime
	st := status.Status{}
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return st, err
	}
	name := containerName(req.Provider)
	image, _ := cfg.Configuration["image"].(string)
	if image == "" {
		image = defaultImage
	}
	// No in-container healthcheck: the distroless Quarkus image ships no
	// shell/curl. Readiness is verified from the host against the REST API.
	ctrState, err := providerkit.EnsureInstance(ctx, rt, providerkit.InstanceSpec{
		Namespace: req.Provider.Metadata.Namespace,
		Name:      name,
		Network:   providerkit.Network(cfg),
		Container: runtime.ContainerSpec{
			Image: image,
			Ports: []runtime.PortBinding{{HostPort: providerkit.HostPort(cfg, name, "port"), ContainerPort: apiPort, Audience: runtime.AudienceHost}},
		},
		WaitTimeout: 120 * time.Second,
	})
	if err != nil {
		return st, err
	}
	if err := waitAPIReady(ctx, rt, name, "/config", 120*time.Second); err != nil {
		return st, err
	}

	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonInstanceHealthy}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	// Observed binding, not intent; "" (in-network only) on runtimes
	// without host publishing.
	hostAddr := ctrState.HostAddr(apiPort)
	hostIceberg, hostAPI := "", ""
	if hostAddr != "" {
		hostIceberg = "http://" + hostAddr + "/iceberg"
		hostAPI = "http://" + hostAddr + "/api/v2"
	}
	st.ProviderState = map[string]any{
		"containerId": ctrState.ID,
		"hostApi":     hostAPI,
		"internalApi": fmt.Sprintf("http://%s:%d/api/v2", name, apiPort),
		"icebergUri":  fmt.Sprintf("http://%s:%d/iceberg", name, apiPort),
		endpoint.Key: endpoint.List{
			{Name: "iceberg-rest", Scheme: "http", Host: hostIceberg, Internal: fmt.Sprintf("http://%s:%d/iceberg", name, apiPort), Insecure: true},
			{Name: "nessie-api", Scheme: "http", Host: hostAPI, Internal: fmt.Sprintf("http://%s:%d/api/v2", name, apiPort), Insecure: true},
		}.ToState(),
	}
	return st, nil
}

// reconcileCatalog realizes a Catalog(engine: nessie): the REST API must
// answer and the declared default branch must exist (created from main when
// missing).
func (p *Provider) reconcileCatalog(ctx context.Context, req reconciler.Request) (status.Status, error) {
	rt := req.Runtime
	st := status.Status{}
	c, err := catalog.FromEnvelope(req.Resource)
	if err != nil {
		return st, err
	}
	name := containerName(req.Provider)
	if err := waitAPIReady(ctx, rt, name, "/config", 60*time.Second); err != nil {
		return st, err
	}
	apiURL, closeAPI, err := reachableAPIURL(ctx, rt, name)
	if err != nil {
		return st, err
	}
	defer closeAPI()
	branch := defaultBranch(c)
	if err := ensureBranch(ctx, apiURL, branch); err != nil {
		return st, err
	}

	// Observed host binding for the Catalog's own endpoints (the instance
	// container publishes the port; the Catalog is what tools configure
	// against, so inventory must answer from the Catalog resource —
	// docs/planning/07 §2.3: "what exact config do I paste into my tool?").
	hostIceberg, hostAPI := "", ""
	if ctr, found, err := rt.Inspect(ctx, name); err == nil && found {
		if addr := ctr.HostAddr(apiPort); addr != "" {
			hostIceberg = "http://" + addr + "/iceberg"
			hostAPI = "http://" + addr + "/api/v2"
		}
	}

	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonCatalogProvisioned}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	st.ProviderState = map[string]any{
		"engine":        "nessie",
		"defaultBranch": branch,
		"hostApi":       hostAPI,
		"internalApi":   fmt.Sprintf("http://%s:%d/api/v2", name, apiPort),
		"icebergUri":    fmt.Sprintf("http://%s:%d/iceberg", name, apiPort),
		endpoint.Key: endpoint.List{
			{Name: "iceberg-rest", Scheme: "http", Host: hostIceberg, Internal: fmt.Sprintf("http://%s:%d/iceberg", name, apiPort), Insecure: true},
			{Name: "nessie-api", Scheme: "http", Host: hostAPI, Internal: fmt.Sprintf("http://%s:%d/api/v2", name, apiPort), Insecure: true},
		}.ToState(),
	}
	return st, nil
}

func (p *Provider) Destroy(ctx context.Context, req reconciler.Request) error {
	rt := req.Runtime
	name := containerName(req.Provider)
	switch req.Resource.Kind {
	case "Provider":
		if err := rt.Remove(ctx, name); err != nil {
			return err
		}
		cfg, err := provider.FromEnvelope(req.Provider)
		if err != nil {
			return err
		}
		_ = rt.RemoveNetwork(ctx, providerkit.Network(cfg))
		return nil
	case "Catalog":
		// Deleting branches would be data loss beyond the declared contract;
		// the instance teardown (Provider destroy) removes everything anyway.
		return nil
	default:
		return fmt.Errorf("nessie provider cannot destroy kind %s", req.Resource.Kind)
	}
}

func (p *Provider) Probe(ctx context.Context, req reconciler.Request) (status.Status, error) {
	rt := req.Runtime
	st := status.Status{}
	now := time.Now()
	name := containerName(req.Provider)
	switch req.Resource.Kind {
	case "Provider":
		ctrState, found, err := rt.Inspect(ctx, name)
		if err != nil {
			return st, err
		}
		healthy := false
		if found && ctrState.Healthy {
			if apiURL, closeAPI, err := reachableAPIURL(ctx, rt, name); err == nil {
				healthy = httpOK(ctx, apiURL+"/config")
				closeAPI()
			}
		}
		if healthy {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonInstanceHealthy}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
		} else {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonInstanceUnhealthy}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonInstanceUnhealthy}, now)
		}
		return st, nil
	case "Catalog":
		c, err := catalog.FromEnvelope(req.Resource)
		if err != nil {
			return st, err
		}
		apiURL, closeAPI, err := reachableAPIURL(ctx, rt, name)
		var ok bool
		if err == nil {
			ok, err = branchExists(ctx, apiURL, defaultBranch(c))
			closeAPI()
		}
		if err != nil || !ok {
			// Selected between two fixed reasons (not interpolated), so both
			// sides stay plain constants (docs/planning/08 G4).
			reason := status.ReasonBranchMissing
			if err != nil {
				reason = status.ReasonCatalogUnreachable
			}
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: reason}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: reason}, now)
			return st, nil
		}
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonCatalogHealthy}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
		return st, nil
	default:
		return st, fmt.Errorf("nessie provider cannot probe kind %s", req.Resource.Kind)
	}
}

func defaultBranch(c catalog.Catalog) string {
	if b, ok := c.EngineConfig["defaultBranch"].(string); ok && b != "" {
		return b
	}
	return "main"
}

func branchExists(ctx context.Context, apiURL, branch string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL+"/trees/"+branch, nil)
	if err != nil {
		return false, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		msg, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("check branch %q: HTTP %d: %s", branch, resp.StatusCode, msg)
	}
}

// ensureBranch creates the branch from main's current hash when missing.
func ensureBranch(ctx context.Context, apiURL, branch string) error {
	exists, err := branchExists(ctx, apiURL, branch)
	if err != nil || exists {
		return err
	}
	// Fetch main to use as the source reference.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL+"/trees/main", nil)
	if err != nil {
		return err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var main struct {
		Reference struct {
			Name string `json:"name"`
			Hash string `json:"hash"`
			Type string `json:"type"`
		} `json:"reference"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&main); err != nil {
		return fmt.Errorf("read main reference: %w", err)
	}
	body, err := json.Marshal(main.Reference)
	if err != nil {
		return err
	}
	createReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("%s/trees?name=%s&type=BRANCH", apiURL, branch), bytes.NewReader(body))
	if err != nil {
		return err
	}
	createReq.Header.Set("Content-Type", "application/json")
	createResp, err := httpClient.Do(createReq)
	if err != nil {
		return err
	}
	defer createResp.Body.Close()
	if createResp.StatusCode >= 300 {
		msg, _ := io.ReadAll(createResp.Body)
		return fmt.Errorf("create branch %q: HTTP %d: %s", branch, createResp.StatusCode, msg)
	}
	return nil
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
