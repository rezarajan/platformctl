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

	"github.com/rezarajan/platformctl/internal/domain/catalog"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/hostport"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

const (
	defaultImage = "ghcr.io/projectnessie/nessie:0.108.1@sha256:0b1ffbe56a1cbc1b86641ccd83465ab3447339ea4ed17a1fca42c50288e1479d"
	apiPort      = 19120
)

var httpClient = &http.Client{Timeout: 10 * time.Second}

type Provider struct {
	providerRes resource.Envelope
	cfg         provider.Provider
}

func New() *Provider { return &Provider{} }

func (p *Provider) Type() string { return "nessie" }

// SupportedCatalogEngines implements CatalogCapableProvider.
func (p *Provider) SupportedCatalogEngines() []string { return []string{"nessie"} }

func (p *Provider) SetProviderResource(env resource.Envelope) {
	p.providerRes = env
	p.cfg, _ = provider.FromEnvelope(env)
}

func (p *Provider) containerName() string { return p.providerRes.Metadata.Name }

func (p *Provider) hostPort() int {
	configured := 0
	if v, ok := p.cfg.Configuration["port"]; ok {
		switch n := v.(type) {
		case int:
			configured = n
		case float64:
			configured = int(n)
		}
	}
	return hostport.Resolve(configured, p.containerName())
}

func (p *Provider) network() string {
	if n, ok := p.cfg.RuntimeConfig["network"].(string); ok && n != "" {
		return n
	}
	return "datascape"
}

// reachableAPIURL returns an "http://host:port/api/v2" this process can
// dial right now, plus a close func that must always be called
// (docs/planning/08 B8: Docker's is a cheap no-op; Kubernetes may tear down
// a port-forward tunnel opened just for this call). Nessie's REST API is
// stateless HTTP with no broker-style redirect, so the resolved address can
// be used directly for one call.
func (p *Provider) reachableAPIURL(ctx context.Context, rt runtime.ContainerRuntime) (string, func() error, error) {
	addr, closeAddr, err := rt.EnsureReachable(ctx, p.containerName(), apiPort)
	if err != nil {
		return "", nil, err
	}
	return "http://" + addr + "/api/v2", closeAddr, nil
}

func (p *Provider) Reconcile(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) (status.Status, error) {
	switch res.Kind {
	case "Provider":
		return p.reconcileInstance(ctx, rt)
	case "Catalog":
		return p.reconcileCatalog(ctx, res, rt)
	default:
		return status.Status{}, fmt.Errorf("nessie provider cannot reconcile kind %s", res.Kind)
	}
}

func (p *Provider) reconcileInstance(ctx context.Context, rt runtime.ContainerRuntime) (status.Status, error) {
	st := status.Status{}
	name := p.containerName()
	image, _ := p.cfg.Configuration["image"].(string)
	if image == "" {
		image = defaultImage
	}
	labels := runtime.ManagedLabels(p.providerRes.Metadata.Namespace, "Provider", name, name)
	if err := rt.EnsureNetwork(ctx, runtime.NetworkSpec{Name: p.network(), Labels: labels}); err != nil {
		return st, err
	}
	// No in-container healthcheck: the distroless Quarkus image ships no
	// shell/curl. Readiness is verified from the host against the REST API.
	ctrState, err := rt.EnsureContainer(ctx, runtime.ContainerSpec{
		Name:     name,
		Image:    image,
		Networks: []string{p.network()},
		Ports:    []runtime.PortBinding{{HostPort: p.hostPort(), ContainerPort: apiPort}},
		Labels:   labels,
	})
	if err != nil {
		return st, err
	}
	if err := rt.WaitHealthy(ctx, name, 120*time.Second); err != nil {
		return st, err
	}
	apiURL, closeAPI, err := p.reachableAPIURL(ctx, rt)
	if err != nil {
		return st, err
	}
	defer closeAPI()
	if err := waitHTTPOK(ctx, apiURL+"/config", 120*time.Second); err != nil {
		return st, err
	}

	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "InstanceHealthy"}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: "ReconcileComplete"}, now)
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
func (p *Provider) reconcileCatalog(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) (status.Status, error) {
	st := status.Status{}
	c, err := catalog.FromEnvelope(res)
	if err != nil {
		return st, err
	}
	apiURL, closeAPI, err := p.reachableAPIURL(ctx, rt)
	if err != nil {
		return st, err
	}
	defer closeAPI()
	if err := waitHTTPOK(ctx, apiURL+"/config", 60*time.Second); err != nil {
		return st, err
	}
	branch := defaultBranch(c)
	if err := ensureBranch(ctx, apiURL, branch); err != nil {
		return st, err
	}

	// Observed host binding for the Catalog's own endpoints (the instance
	// container publishes the port; the Catalog is what tools configure
	// against, so inventory must answer from the Catalog resource —
	// docs/planning/07 §2.3: "what exact config do I paste into my tool?").
	hostIceberg, hostAPI := "", ""
	if ctr, found, err := rt.Inspect(ctx, p.containerName()); err == nil && found {
		if addr := ctr.HostAddr(apiPort); addr != "" {
			hostIceberg = "http://" + addr + "/iceberg"
			hostAPI = "http://" + addr + "/api/v2"
		}
	}

	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "CatalogProvisioned"}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: "ReconcileComplete"}, now)
	st.ProviderState = map[string]any{
		"engine":        "nessie",
		"defaultBranch": branch,
		"hostApi":       hostAPI,
		"internalApi":   fmt.Sprintf("http://%s:%d/api/v2", p.containerName(), apiPort),
		"icebergUri":    fmt.Sprintf("http://%s:%d/iceberg", p.containerName(), apiPort),
		endpoint.Key: endpoint.List{
			{Name: "iceberg-rest", Scheme: "http", Host: hostIceberg, Internal: fmt.Sprintf("http://%s:%d/iceberg", p.containerName(), apiPort), Insecure: true},
			{Name: "nessie-api", Scheme: "http", Host: hostAPI, Internal: fmt.Sprintf("http://%s:%d/api/v2", p.containerName(), apiPort), Insecure: true},
		}.ToState(),
	}
	return st, nil
}

func (p *Provider) Destroy(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) error {
	switch res.Kind {
	case "Provider":
		if err := rt.Remove(ctx, p.containerName()); err != nil {
			return err
		}
		_ = rt.RemoveNetwork(ctx, p.network())
		return nil
	case "Catalog":
		// Deleting branches would be data loss beyond the declared contract;
		// the instance teardown (Provider destroy) removes everything anyway.
		return nil
	default:
		return fmt.Errorf("nessie provider cannot destroy kind %s", res.Kind)
	}
}

func (p *Provider) Probe(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) (status.Status, error) {
	st := status.Status{}
	now := time.Now()
	switch res.Kind {
	case "Provider":
		ctrState, found, err := rt.Inspect(ctx, p.containerName())
		if err != nil {
			return st, err
		}
		healthy := false
		if found && ctrState.Healthy {
			if apiURL, closeAPI, err := p.reachableAPIURL(ctx, rt); err == nil {
				healthy = httpOK(ctx, apiURL+"/config")
				closeAPI()
			}
		}
		if healthy {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "InstanceHealthy"}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: "NoDrift"}, now)
		} else {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: "InstanceUnhealthy"}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: "InstanceUnhealthy"}, now)
		}
		return st, nil
	case "Catalog":
		c, err := catalog.FromEnvelope(res)
		if err != nil {
			return st, err
		}
		apiURL, closeAPI, err := p.reachableAPIURL(ctx, rt)
		var ok bool
		if err == nil {
			ok, err = branchExists(ctx, apiURL, defaultBranch(c))
			closeAPI()
		}
		if err != nil || !ok {
			reason := "BranchMissing"
			if err != nil {
				reason = "CatalogUnreachable"
			}
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: reason}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: reason}, now)
			return st, nil
		}
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "CatalogHealthy"}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: "NoDrift"}, now)
		return st, nil
	default:
		return st, fmt.Errorf("nessie provider cannot probe kind %s", res.Kind)
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
