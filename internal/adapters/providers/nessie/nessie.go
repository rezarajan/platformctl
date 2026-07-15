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
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

const (
	defaultImage = "ghcr.io/projectnessie/nessie:latest"
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
	if v, ok := p.cfg.Configuration["port"]; ok {
		switch n := v.(type) {
		case int:
			return n
		case float64:
			return int(n)
		}
	}
	return apiPort
}

func (p *Provider) network() string {
	if n, ok := p.cfg.RuntimeConfig["network"].(string); ok && n != "" {
		return n
	}
	return "datascape"
}

func (p *Provider) apiURL() string {
	return fmt.Sprintf("http://127.0.0.1:%d/api/v2", p.hostPort())
}

func (p *Provider) configURL() string { return p.apiURL() + "/config" }

func (p *Provider) Reconcile(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) (status.Status, error) {
	switch res.Kind {
	case "Provider":
		return p.reconcileInstance(ctx, rt)
	case "Catalog":
		return p.reconcileCatalog(ctx, res)
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
	labels := map[string]string{
		runtime.LabelManagedBy:  runtime.ManagedByValue,
		runtime.LabelGeneration: name,
	}
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
	if err := waitHTTPOK(ctx, p.configURL(), 120*time.Second); err != nil {
		return st, err
	}

	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "InstanceHealthy"}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: "ReconcileComplete"}, now)
	st.ProviderState = map[string]any{
		"containerId": ctrState.ID,
		"hostApi":     p.apiURL(),
		"internalApi": fmt.Sprintf("http://%s:%d/api/v2", name, apiPort),
		"icebergUri":  fmt.Sprintf("http://%s:%d/iceberg", name, apiPort),
	}
	return st, nil
}

// reconcileCatalog realizes a Catalog(engine: nessie): the REST API must
// answer and the declared default branch must exist (created from main when
// missing).
func (p *Provider) reconcileCatalog(ctx context.Context, res resource.Envelope) (status.Status, error) {
	st := status.Status{}
	c, err := catalog.FromEnvelope(res)
	if err != nil {
		return st, err
	}
	if err := waitHTTPOK(ctx, p.configURL(), 60*time.Second); err != nil {
		return st, err
	}
	branch := defaultBranch(c)
	if err := ensureBranch(ctx, p.apiURL(), branch); err != nil {
		return st, err
	}

	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "CatalogProvisioned"}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: "ReconcileComplete"}, now)
	st.ProviderState = map[string]any{
		"engine":        "nessie",
		"defaultBranch": branch,
		"hostApi":       p.apiURL(),
		"internalApi":   fmt.Sprintf("http://%s:%d/api/v2", p.containerName(), apiPort),
		"icebergUri":    fmt.Sprintf("http://%s:%d/iceberg", p.containerName(), apiPort),
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
		healthy := found && ctrState.Healthy && httpOK(ctx, p.configURL())
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
		ok, err := branchExists(ctx, p.apiURL(), defaultBranch(c))
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
