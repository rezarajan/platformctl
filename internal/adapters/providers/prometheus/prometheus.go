// Package prometheus reconciles a managed Prometheus container whose scrape
// config is generated from the platform's own published metrics endpoint
// facts (docs/planning/08 C9): every provider publishes endpoint facts in
// state; this provider scrapes whatever carries a "metrics"-named one.
// Nessie-shaped (a single-container HTTP-API instance, no dependent kind) —
// see internal/adapters/providers/nessie for the template this follows.
//
// Deferred out of this slice (see docs/planning/08 C9's status note): a
// standalone `grafana` provider, postgres/mysql sidecar exporter containers
// (no native metrics endpoint to publish yet), and live Kubernetes-runtime
// verification.
// Convergence caveat (docs/planning/08 C9 status note): scrape targets are
// resolved from endpoint facts already published in state, and no graph
// edge orders this Provider after the providers it scrapes — a fresh
// single apply may configure the then-current subset and converge on the
// next apply, surfaced by this provider's own config-drift probe.
package prometheus

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rezarajan/platformctl/internal/adapters/providers/providerkit"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

const (
	// defaultImage is a pinned Prometheus LTS release (scripts/pinned-images.txt).
	defaultImage = "prom/prometheus:v2.55.1@sha256:2659f4c2ebb718e7695cb9b25ffa7d6be64db013daba13e05c875451cf51b0d3"
	apiPort      = 9090
	configPath   = "/etc/prometheus/prometheus.yml"
)

var httpClient = &http.Client{Timeout: 10 * time.Second}

// Provider holds no cross-call state (docs/planning/08 F5): every method
// receives what it needs via reconciler.Request.
type Provider struct{}

func New() *Provider { return &Provider{} }

func (p *Provider) Type() string { return "prometheus" }

func containerName(provEnv resource.Envelope) string { return naming.RuntimeObjectName(provEnv) }

func (p *Provider) Reconcile(ctx context.Context, req reconciler.Request) (status.Status, error) {
	switch req.Resource.Kind {
	case "Provider":
		return p.reconcileInstance(ctx, req)
	default:
		return status.Status{}, fmt.Errorf("prometheus provider cannot reconcile kind %s", req.Resource.Kind)
	}
}

// scrapeInterval resolves configuration.scrapeInterval (a Prometheus
// duration literal, e.g. "15s", "1m"); empty defers to
// RenderScrapeConfig/defaultScrapeInterval.
func scrapeInterval(cfg provider.Provider) string {
	v, _ := cfg.Configuration["scrapeInterval"].(string)
	return v
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

	targets := targetsFromMetrics(req.MetricsTargets)
	configYAML, err := RenderScrapeConfig(targets, scrapeInterval(cfg))
	if err != nil {
		return st, err
	}

	ctrState, err := providerkit.EnsureInstance(ctx, rt, providerkit.InstanceSpec{
		Namespace: req.Provider.Metadata.Namespace,
		Name:      name,
		Network:   providerkit.Network(cfg),
		Container: runtime.ContainerSpec{
			Image: image,
			Cmd:   []string{"--config.file=" + configPath},
			Files: []runtime.FileMount{{Path: configPath, Content: configYAML, Mode: 0o444}},
			Ports: []runtime.PortBinding{{HostPort: providerkit.HostPort(cfg, name, "port"), ContainerPort: apiPort, Audience: runtime.AudienceHost}},
			HealthCheck: &runtime.HealthCheck{
				Test:     []string{"CMD-SHELL", fmt.Sprintf("wget -q --spider http://localhost:%d/-/ready || exit 1", apiPort)},
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
	if err := waitReady(ctx, rt, name, len(targets), 120*time.Second); err != nil {
		return st, err
	}

	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonInstanceHealthy}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	hostAddr := ctrState.HostAddr(apiPort) // observed binding, not intent
	hostURL := ""
	if hostAddr != "" {
		hostURL = "http://" + hostAddr
	}
	st.ProviderState = map[string]any{
		"containerId":  ctrState.ID,
		"targetCount":  len(targets),
		"internalAddr": "http://" + name + ":" + strconv.Itoa(apiPort),
		endpoint.Key: endpoint.List{
			{Name: "prometheus", Scheme: "http", Host: hostURL, Internal: "http://" + name + ":" + strconv.Itoa(apiPort), Insecure: true},
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
		// The network may still be shared with every other provider on it;
		// RemoveNetwork refuses (and this ignores that refusal) while any
		// container remains attached — the same convention every other
		// single-container provider's Destroy follows.
		_ = rt.RemoveNetwork(ctx, providerkit.Network(cfg))
		return nil
	default:
		return fmt.Errorf("prometheus provider cannot destroy kind %s", req.Resource.Kind)
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
		if !found || !ctrState.Healthy {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonInstanceUnhealthy}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonInstanceUnhealthy}, now)
			return st, nil
		}

		desired := targetsFromMetrics(req.MetricsTargets)
		addr, closeAddr, err := providerkit.ReachableAddr(ctx, rt, name, apiPort)
		if err != nil {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonInstanceUnhealthy, Message: err.Error()}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonInstanceUnhealthy}, now)
			return st, nil
		}
		defer closeAddr()
		baseURL := "http://" + addr

		if !httpOK(ctx, baseURL+"/-/ready") {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonInstanceUnhealthy}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonInstanceUnhealthy}, now)
			return st, nil
		}

		// Ready requires every configured target to be present in
		// /api/v1/targets (activeTargets count matches) — per-target
		// up-ness is Prometheus's own concern (its own /api/v1/targets
		// "health" field), not something this Ready condition blocks on;
		// a target Prometheus can't yet reach is Prometheus's problem to
		// report, not a reason to fail the platform's own Ready gate.
		active, err := activeTargets(ctx, baseURL)
		if err != nil {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonInstanceUnhealthy, Message: err.Error()}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonInstanceUnhealthy}, now)
			return st, nil
		}
		if len(active) != len(desired) {
			msg := fmt.Sprintf("configured targets: %d, active targets reported by Prometheus: %d", len(desired), len(active))
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonScrapeTargetsIncomplete, Message: msg}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonScrapeTargetsIncomplete, Message: msg}, now)
			return st, nil
		}

		// Beyond target count (docs/planning/07 §2.1's "verify desired
		// configuration, not just liveness"): the live config must still
		// match what the currently-published metrics facts generate.
		// Drifted job *names* only — never target values (the debezium
		// connectorConfigDrift bar).
		if liveYAML, err := effectiveConfig(ctx, baseURL); err == nil {
			if live, perr := ParseScrapeConfig([]byte(liveYAML)); perr == nil {
				if drifted := diffScrapeConfig(desired, live); len(drifted) > 0 {
					msg := "scrape config differs from generated at: " + strings.Join(drifted, ", ")
					st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonScrapeConfigDrift, Message: msg}, now)
					st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonScrapeConfigDrift, Message: msg}, now)
					return st, nil
				}
			}
		}

		st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonInstanceHealthy}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
		return st, nil
	default:
		return st, fmt.Errorf("prometheus provider cannot probe kind %s", req.Resource.Kind)
	}
}

// waitReady polls /-/ready and /api/v1/targets via runtime.WithReachable
// (docs/planning/09 Class 2 / F1) so every attempt gets a freshly-resolved
// address rather than reusing one across the whole wait — the same
// defensive pattern nessie's waitAPIReady and redpanda's
// waitSchemaRegistryReady document. /-/ready alone is not enough: found
// live, not by reasoning — Prometheus's target-discovery sync (which
// populates /api/v1/targets from static_configs) lags /-/ready by a few
// seconds at startup even for a purely static config, so a Reconcile that
// returned as soon as /-/ready 200s could hand back an apply "success"
// whose Probe would immediately report ScrapeTargetsIncomplete. Waiting for
// both here is what makes Reconcile's "success" actually mean Ready (ADR
// 015 F3, "Ready means serving").
func waitReady(ctx context.Context, rt runtime.ContainerRuntime, name string, wantTargets int, timeout time.Duration) error {
	opts := runtime.ReachableOptions{Timeout: timeout, Interval: 2 * time.Second}
	err := runtime.WithReachable(ctx, rt, name, apiPort, opts, func(ctx context.Context, addr string) error {
		baseURL := "http://" + addr
		if !httpOK(ctx, baseURL+"/-/ready") {
			return fmt.Errorf("/-/ready did not answer 200")
		}
		active, err := activeTargets(ctx, baseURL)
		if err != nil {
			return fmt.Errorf("read /api/v1/targets: %w", err)
		}
		if len(active) != wantTargets {
			return fmt.Errorf("configured targets: %d, active targets so far: %d", wantTargets, len(active))
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("prometheus did not become ready within %s: %w", timeout, err)
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

// targetsAPIResponse is the subset of Prometheus's /api/v1/targets response
// this package reads: one scrapePool (job name) per active target.
type targetsAPIResponse struct {
	Data struct {
		ActiveTargets []struct {
			ScrapePool string `json:"scrapePool"`
		} `json:"activeTargets"`
	} `json:"data"`
}

// activeTargets returns the scrape pool (job) name of every entry in
// Prometheus's own /api/v1/targets activeTargets list — the count this
// package compares against the desired target list for Ready, and the
// per-target "health" field callers needing up==1 read for themselves
// (this package deliberately does not surface it — see the Probe doc
// comment on why per-target up-ness never blocks Ready).
func activeTargets(ctx context.Context, baseURL string) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/api/v1/targets", nil)
	if err != nil {
		return nil, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET /api/v1/targets: HTTP %d", resp.StatusCode)
	}
	var tr targetsAPIResponse
	if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
		return nil, fmt.Errorf("decode /api/v1/targets: %w", err)
	}
	pools := make([]string, 0, len(tr.Data.ActiveTargets))
	for _, t := range tr.Data.ActiveTargets {
		pools = append(pools, t.ScrapePool)
	}
	return pools, nil
}

// configAPIResponse is the subset of Prometheus's /api/v1/status/config
// response this package reads: the effective config file, verbatim, as
// YAML text.
type configAPIResponse struct {
	Data struct {
		YAML string `json:"yaml"`
	} `json:"data"`
}

func effectiveConfig(ctx context.Context, baseURL string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/api/v1/status/config", nil)
	if err != nil {
		return "", err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GET /api/v1/status/config: HTTP %d", resp.StatusCode)
	}
	var cr configAPIResponse
	if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil {
		return "", fmt.Errorf("decode /api/v1/status/config: %w", err)
	}
	return cr.Data.YAML, nil
}

// ValidateSpec implements reconciler.SpecValidator: a typo'd scrapeInterval
// fails at validate, never as a half-applied platform.
func (p *Provider) ValidateSpec(cfg provider.Provider) error {
	if v, ok := cfg.Configuration["scrapeInterval"]; ok {
		s, isStr := v.(string)
		if !isStr || s == "" {
			return fmt.Errorf("spec.configuration.scrapeInterval must be a non-empty duration string (e.g. \"15s\"), got %v", v)
		}
	}
	if v, ok := cfg.Configuration["image"]; ok {
		if s, isStr := v.(string); !isStr || s == "" {
			return fmt.Errorf("spec.configuration.image must be a non-empty string, got %v", v)
		}
	}
	return nil
}
