// Package ingress realizes managed Connection resources with spec.scheme:
// http or https — the HTTP-routing sibling of the proxy provider's tcp
// scheme, on the same Connection seam docs/adr/002 designated for it.
// Implements ConnectionCapableProvider ("http", "https").
//
// Two structurally different realizations, chosen per Provider's
// spec.runtime.type (docs/adr/018 "Layering" section — a domain-layer field
// read from provider.Provider.RuntimeType, never an adapter type-assertion):
//
//   - Docker/fake: one shared Caddy container per ingress Provider, routing
//     Host(<connection-name>.<domain>) to each Connection's spec.target.
//     Per-Connection routes reconcile via Caddy's admin API (docker.go) —
//     never through ContainerSpec.Files, which would restart the shared
//     container on every unrelated Connection's change (docs/adr/018
//     Decision 3). TLS certificates (docs/planning/08 C8, tls.go/caddy.go)
//     follow the identical discipline: loaded via the admin API, never via
//     ContainerSpec.Files.
//   - Kubernetes: one networking.k8s.io/v1 Ingress object per Connection
//     (kubernetes.go), via the runtime.IngressCapableRuntime capability. No
//     shared container: the cluster's own ingress controller does the
//     proxying. TLS is Ingress.spec.tls referencing a kubernetes.io/tls
//     Secret (provided, self-signed, or a referenced-only cert-manager
//     Secret).
//
// A Connection declaring scheme: https requires spec.tls (see
// docs/domain/connection's TLS type) and the TLSTermination feature gate
// (checked by engine.resolveRequest via registry.RequireGate — this
// provider never checks gates itself). Every endpoint this provider
// publishes is honestly Insecure: true for http, false for https —
// docs/planning/07 §2.5's standing rule.
package ingress

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
)

const (
	// defaultImage is a pinned Caddy release (scripts/pinned-images.txt).
	// Chosen over Traefik specifically for its read-write admin API — see
	// docs/adr/018 Decision 1.
	defaultImage = "caddy:2.9.1@sha256:748016f285ed8c43a9ce6e3aed6d92d3009d90ca41157950880f40beaf3ff62b"
	// defaultDomainSuffix is the Host(...) suffix every Connection's route
	// is built from unless overridden by configuration.domain — RFC 6761
	// .localhost resolves to loopback on modern resolvers with zero manual
	// setup (docs/adr/018 Decision 4); never dialed as a literal address,
	// only used to build a Host header match string.
	defaultDomainSuffix = "localhost" // archtest:allow-loopback: local-dev routing domain suffix (docs/adr/018 Decision 4), never dialed directly
)

// Provider holds no cross-call state (docs/planning/08 F5): every method
// receives what it needs via reconciler.Request.
type Provider struct{}

func New() *Provider { return &Provider{} }

func (p *Provider) Type() string { return "ingress" }

// SupportedConnectionSchemes implements ConnectionCapableProvider.
// docs/planning/08 C8 adds "https": a Connection declaring scheme: https
// terminates TLS at this provider's entrypoint (Connection.spec.tls),
// behind the TLSTermination gate (registry.RequireGate, checked by
// engine.resolveRequest — see docs/adr/018 addendum).
func (p *Provider) SupportedConnectionSchemes() []string { return []string{"http", "https"} }

func containerName(provEnv resource.Envelope) string { return naming.RuntimeObjectName(provEnv) }

func image(cfg provider.Provider) string {
	if img, ok := cfg.Configuration["image"].(string); ok && img != "" {
		return img
	}
	return defaultImage
}

// domainSuffix resolves configuration.domain, defaulting to defaultDomainSuffix.
func domainSuffix(cfg provider.Provider) string {
	if d, ok := cfg.Configuration["domain"].(string); ok && d != "" {
		return d
	}
	return defaultDomainSuffix
}

// routeHost builds the Host(...) match value for a Connection: its own
// resource name plus the Provider's domain suffix.
func routeHost(connName string, cfg provider.Provider) string {
	return connName + "." + domainSuffix(cfg)
}

// routeID is the Caddy "@id"/Kubernetes Ingress object name for a
// Connection's route — deterministic from the Connection's own name so
// reconcile is idempotent and Destroy can address it without state.
func routeID(connName string) string { return "route-" + connName }

func (p *Provider) Reconcile(ctx context.Context, req reconciler.Request) (status.Status, error) {
	switch req.Resource.Kind {
	case "Provider":
		return p.reconcileInstance(ctx, req)
	case "Connection":
		return p.reconcileConnection(ctx, req)
	default:
		return status.Status{}, fmt.Errorf("ingress provider cannot reconcile kind %s", req.Resource.Kind)
	}
}

func (p *Provider) Destroy(ctx context.Context, req reconciler.Request) error {
	switch req.Resource.Kind {
	case "Provider":
		return p.destroyInstance(ctx, req)
	case "Connection":
		return p.destroyConnection(ctx, req)
	default:
		return fmt.Errorf("ingress provider cannot destroy kind %s", req.Resource.Kind)
	}
}

func (p *Provider) Probe(ctx context.Context, req reconciler.Request) (status.Status, error) {
	switch req.Resource.Kind {
	case "Provider":
		return p.probeInstance(ctx, req)
	case "Connection":
		return p.probeConnection(ctx, req)
	default:
		return status.Status{}, fmt.Errorf("ingress provider cannot probe kind %s", req.Resource.Kind)
	}
}

// isKubernetes reports whether req.Provider is realized on the Kubernetes
// runtime — read from provider.Provider.RuntimeType, a plain domain-layer
// fact parsed from spec.runtime.type (postgres/redpanda already read
// sibling RuntimeConfig keys the same way), never an adapter
// type-assertion (docs/adr/018 "Layering").
func isKubernetes(cfg provider.Provider) bool { return cfg.RuntimeType == "kubernetes" }

// parseTarget splits a Connection's spec.target ("host:port", a straight
// passthrough of the manifest author's declared value — never constructed,
// docs/adr/018 Decision 3) into the Kubernetes Ingress backend's
// (Service name, port). Docker's Caddy path uses the raw string directly as
// a reverse_proxy dial target and never needs this split.
func parseTarget(target string) (host string, port int, err error) {
	idx := strings.LastIndex(target, ":")
	if idx < 0 {
		return "", 0, fmt.Errorf("Connection target %q must be \"host:port\"", target)
	}
	host = target[:idx]
	portStr := target[idx+1:]
	n, err := strconv.Atoi(portStr)
	if err != nil || n <= 0 || n > 65535 {
		return "", 0, fmt.Errorf("Connection target %q: invalid port %q", target, portStr)
	}
	return host, n, nil
}

// ValidateSpec implements reconciler.SpecValidator: a typo'd image/domain
// fails at validate, never as a half-applied platform.
func (p *Provider) ValidateSpec(cfg provider.Provider) error {
	if v, ok := cfg.Configuration["image"]; ok {
		if s, isStr := v.(string); !isStr || s == "" {
			return fmt.Errorf("spec.configuration.image must be a non-empty string, got %v", v)
		}
	}
	if v, ok := cfg.Configuration["domain"]; ok {
		if s, isStr := v.(string); !isStr || s == "" {
			return fmt.Errorf("spec.configuration.domain must be a non-empty string, got %v", v)
		}
	}
	return nil
}

// reconcileInstance: Docker/fake bootstrap a shared Caddy container (once —
// see docker.go); Kubernetes has no central object of its own (mirroring
// proxy's own Provider-level reconcile, which only anchors the shared
// network) — each Connection's Ingress is entirely self-contained.
func (p *Provider) reconcileInstance(ctx context.Context, req reconciler.Request) (status.Status, error) {
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return status.Status{}, err
	}
	if isKubernetes(cfg) {
		return reconcileInstanceKubernetes(ctx, req, cfg)
	}
	return reconcileInstanceDocker(ctx, req, cfg)
}

func (p *Provider) destroyInstance(ctx context.Context, req reconciler.Request) error {
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return err
	}
	if isKubernetes(cfg) {
		return destroyInstanceKubernetes(ctx, req, cfg)
	}
	return destroyInstanceDocker(ctx, req, cfg)
}

func (p *Provider) probeInstance(ctx context.Context, req reconciler.Request) (status.Status, error) {
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return status.Status{}, err
	}
	if isKubernetes(cfg) {
		return probeInstanceKubernetes(ctx, req, cfg)
	}
	return probeInstanceDocker(ctx, req, cfg)
}

func (p *Provider) reconcileConnection(ctx context.Context, req reconciler.Request) (status.Status, error) {
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return status.Status{}, err
	}
	if isKubernetes(cfg) {
		return reconcileConnectionKubernetes(ctx, req, cfg)
	}
	return reconcileConnectionDocker(ctx, req, cfg)
}

func (p *Provider) destroyConnection(ctx context.Context, req reconciler.Request) error {
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return err
	}
	if isKubernetes(cfg) {
		return destroyConnectionKubernetes(ctx, req, cfg)
	}
	return destroyConnectionDocker(ctx, req, cfg)
}

func (p *Provider) probeConnection(ctx context.Context, req reconciler.Request) (status.Status, error) {
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return status.Status{}, err
	}
	if isKubernetes(cfg) {
		return probeConnectionKubernetes(ctx, req, cfg)
	}
	return probeConnectionDocker(ctx, req, cfg)
}
