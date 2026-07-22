package ingress

import (
	"context"
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
)

func TestSupportedConnectionSchemesIsHTTPAndHTTPS(t *testing.T) {
	p := New()
	schemes := p.SupportedConnectionSchemes()
	if len(schemes) != 2 || schemes[0] != "http" || schemes[1] != "https" {
		t.Fatalf(`SupportedConnectionSchemes() = %v, want exactly ["http", "https"] (docs/planning/08 C8)`, schemes)
	}
}

func TestDomainSuffixDefaultsToLocalhost(t *testing.T) {
	if got := domainSuffix(provider.Provider{}); got != "localhost" {
		t.Errorf("domainSuffix(empty) = %q, want %q", got, "localhost")
	}
	cfg := provider.Provider{Configuration: map[string]any{"domain": "internal.example"}}
	if got := domainSuffix(cfg); got != "internal.example" {
		t.Errorf("domainSuffix(override) = %q, want %q", got, "internal.example")
	}
}

func TestRouteHostAndID(t *testing.T) {
	cfg := provider.Provider{}
	if got := routeHost("nessie", cfg); got != "nessie.localhost" {
		t.Errorf("routeHost = %q, want nessie.localhost", got)
	}
	if got := routeID("nessie"); got != "route-nessie" {
		t.Errorf("routeID = %q, want route-nessie", got)
	}
}

func TestParseTarget(t *testing.T) {
	host, port, err := parseTarget("nessie:19120")
	if err != nil {
		t.Fatalf("parseTarget: %v", err)
	}
	if host != "nessie" || port != 19120 {
		t.Errorf("parseTarget = (%q, %d), want (nessie, 19120)", host, port)
	}
	if _, _, err := parseTarget("no-port"); err == nil {
		t.Error("parseTarget(\"no-port\") should error")
	}
	if _, _, err := parseTarget("host:notaport"); err == nil {
		t.Error("parseTarget(\"host:notaport\") should error")
	}
	if _, _, err := parseTarget("host:0"); err == nil {
		t.Error("parseTarget(\"host:0\") should error (port must be 1-65535)")
	}
}

func TestIsKubernetes(t *testing.T) {
	if isKubernetes(provider.Provider{RuntimeType: "docker"}) {
		t.Error("docker runtime misclassified as kubernetes")
	}
	if !isKubernetes(provider.Provider{RuntimeType: "kubernetes"}) {
		t.Error("kubernetes runtime not classified as kubernetes")
	}
}

func TestValidateSpecRejectsEmptyImageAndDomain(t *testing.T) {
	p := New()
	if err := p.ValidateSpec(provider.Provider{Configuration: map[string]any{"image": ""}}); err == nil {
		t.Error("empty image should be rejected")
	}
	if err := p.ValidateSpec(provider.Provider{Configuration: map[string]any{"domain": ""}}); err == nil {
		t.Error("empty domain should be rejected")
	}
	if err := p.ValidateSpec(provider.Provider{Configuration: map[string]any{"domain": "example.com"}}); err != nil {
		t.Errorf("valid domain rejected: %v", err)
	}
}

func TestReconcileDestroyProbeRejectUnknownKind(t *testing.T) {
	p := New()
	req := reconciler.Request{Resource: resource.Envelope{GroupVersionKind: resource.GroupVersionKind{Kind: "Binding"}}}
	ctx := context.Background()
	if _, err := p.Reconcile(ctx, req); err == nil {
		t.Error("Reconcile should refuse an unsupported kind")
	}
	if err := p.Destroy(ctx, req); err == nil {
		t.Error("Destroy should refuse an unsupported kind")
	}
	if _, err := p.Probe(ctx, req); err == nil {
		t.Error("Probe should refuse an unsupported kind")
	}
}

func TestImageDefaultAndOverride(t *testing.T) {
	if got := image(provider.Provider{}); got != defaultImage {
		t.Errorf("image(empty) = %q, want default %q", got, defaultImage)
	}
	cfg := provider.Provider{Configuration: map[string]any{"image": "caddy:2.10.0"}}
	if got := image(cfg); got != "caddy:2.10.0" {
		t.Errorf("image(override) = %q, want caddy:2.10.0", got)
	}
}
