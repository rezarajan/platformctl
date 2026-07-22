package prometheus

import (
	"context"
	"strings"
	"testing"
	"time"

	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

func providerEnvelope(name string, configuration map[string]any) resource.Envelope {
	e := resource.Envelope{}
	e.APIVersion = "datascape.io/v1alpha1"
	e.Kind = "Provider"
	e.Metadata.Name = name
	e.Spec = map[string]any{
		"type":          "prometheus",
		"runtime":       map[string]any{"type": "fake"},
		"configuration": configuration,
	}
	return e
}

// TestReconcileInstanceGeneratesConfigAndPublishesPort proves the wiring
// reaches the runtime port and the ContainerSpec.Files-mounted config is
// generated from req.MetricsTargets — independent of readiness, which the
// fake runtime cannot serve real HTTP for (waitReady's retry loop otherwise
// burns its full hardcoded 120s timeout; a short context deadline makes
// runtime.WithReachable's ctx.Done() branch return almost immediately
// instead, the same pattern redpanda's
// TestReconcileBrokerRegistryEnabledPublishesPort documents).
func TestReconcileInstanceGeneratesConfigAndPublishesPort(t *testing.T) {
	rt := fakeruntime.New()
	env := providerEnvelope("local-prom", map[string]any{"scrapeInterval": "5s"})
	req := reconciler.Request{
		Resource: env,
		Provider: env,
		Runtime:  rt,
		MetricsTargets: []reconciler.MetricsTarget{
			{JobName: "local-redpanda", Endpoint: endpoint.Endpoint{Name: "metrics", Internal: "http://local-redpanda:9644/public_metrics"}},
			{JobName: "local-minio", Endpoint: endpoint.Endpoint{Name: "metrics", Internal: "http://local-minio:9000/minio/v2/metrics/cluster"}},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	p := New()
	if _, err := p.Reconcile(ctx, req); err == nil {
		t.Fatal("want an error: the fake runtime cannot serve /-/ready's real HTTP request")
	}

	ctrState, found, err := rt.Inspect(context.Background(), "local-prom")
	if err != nil || !found {
		t.Fatalf("Inspect: found=%v err=%v", found, err)
	}
	var gotAPIPort bool
	for _, port := range ctrState.Ports {
		if port.ContainerPort == apiPort {
			gotAPIPort = true
			if port.Audience != runtime.AudienceHost {
				t.Errorf("api port audience = %q, want %q", port.Audience, runtime.AudienceHost)
			}
		}
	}
	if !gotAPIPort {
		t.Fatalf("ports = %+v, want a %d entry", ctrState.Ports, apiPort)
	}

	content, err := rt.ReadFile(context.Background(), "local-prom", configPath)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", configPath, err)
	}
	got := string(content)
	if !strings.Contains(got, "scrape_interval: 5s") {
		t.Errorf("generated config missing custom scrape_interval:\n%s", got)
	}
	if !strings.Contains(got, "job_name: local-redpanda") || !strings.Contains(got, "job_name: local-minio") {
		t.Errorf("generated config missing expected job names:\n%s", got)
	}
	if !strings.Contains(got, "local-redpanda:9644") || !strings.Contains(got, "local-minio:9000") {
		t.Errorf("generated config missing expected targets:\n%s", got)
	}
}

// TestReconcileInstanceIdempotent proves a second Reconcile call with an
// unchanged metrics target list makes zero mutating runtime calls — the
// standing idempotency contract (CLAUDE.md): ContainerSpec.Files.Content
// participates in the spec hash, so RenderScrapeConfig's determinism
// (scrapeconfig_test.go) is exactly what this depends on.
func TestReconcileInstanceIdempotent(t *testing.T) {
	rt := fakeruntime.New()
	env := providerEnvelope("local-prom", map[string]any{})
	req := reconciler.Request{
		Resource: env,
		Provider: env,
		Runtime:  rt,
		MetricsTargets: []reconciler.MetricsTarget{
			{JobName: "local-redpanda", Endpoint: endpoint.Endpoint{Name: "metrics", Internal: "http://local-redpanda:9644/public_metrics"}},
		},
	}
	p := New()

	ctx1, cancel1 := context.WithTimeout(context.Background(), 200*time.Millisecond)
	_, _ = p.Reconcile(ctx1, req) // errors (no real HTTP), but still reaches EnsureContainer
	cancel1()
	after1 := rt.MutationCount

	ctx2, cancel2 := context.WithTimeout(context.Background(), 200*time.Millisecond)
	_, _ = p.Reconcile(ctx2, req)
	cancel2()
	after2 := rt.MutationCount

	if after2 != after1 {
		t.Errorf("second Reconcile with an unchanged spec mutated the runtime: MutationCount %d -> %d", after1, after2)
	}
}

func TestValidateSpec(t *testing.T) {
	cases := []struct {
		name    string
		cfg     map[string]any
		wantErr bool
	}{
		{"empty", map[string]any{}, false},
		{"valid scrapeInterval", map[string]any{"scrapeInterval": "30s"}, false},
		{"empty scrapeInterval", map[string]any{"scrapeInterval": ""}, true},
		{"wrong type scrapeInterval", map[string]any{"scrapeInterval": 30}, true},
		{"valid image", map[string]any{"image": "prom/prometheus:v2.55.1@sha256:2659f4c2ebb718e7695cb9b25ffa7d6be64db013daba13e05c875451cf51b0d3"}, false},
		{"empty image", map[string]any{"image": ""}, true},
	}
	p := New()
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := provider.Provider{Configuration: c.cfg}
			err := p.ValidateSpec(cfg)
			if (err != nil) != c.wantErr {
				t.Errorf("ValidateSpec(%v) error = %v, wantErr %v", c.cfg, err, c.wantErr)
			}
		})
	}
}

func TestType(t *testing.T) {
	if got := New().Type(); got != "prometheus" {
		t.Errorf("Type() = %q, want %q", got, "prometheus")
	}
}
