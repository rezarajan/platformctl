package s3

import (
	"strings"
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/provider"
)

// TestValidateSpecRequiresImagePullSecretRefWiring covers docs/planning/08
// A1: configuration.imagePullSecretRef, like every other secretRef the
// provider reads, must be listed in spec.secretRefs or validate must catch
// it — never a runtime surprise from an unresolved credential.
func TestValidateSpecRequiresImagePullSecretRefWiring(t *testing.T) {
	p := New()
	cfg := provider.Provider{
		Type:          "s3",
		Configuration: map[string]any{"rootSecretRef": "root", "imagePullSecretRef": "registry-creds"},
		SecretRefs:    []string{"root"}, // registry-creds deliberately not listed
	}
	err := p.ValidateSpec(cfg)
	if err == nil {
		t.Fatal("ValidateSpec accepted an imagePullSecretRef not listed in spec.secretRefs")
	}
	if !strings.Contains(err.Error(), "imagePullSecretRef") {
		t.Errorf("error does not name imagePullSecretRef: %v", err)
	}

	cfg.SecretRefs = append(cfg.SecretRefs, "registry-creds")
	if err := p.ValidateSpec(cfg); err != nil {
		t.Errorf("ValidateSpec rejected a correctly-wired imagePullSecretRef: %v", err)
	}
}

// TestImagePullAuthResolution covers the three states imagePullAuth() can
// be in: unset (nil, nil — private pulls stay opt-in), unresolved (an error
// naming the missing secretRef, not a nil-pointer surprise later), and
// resolved (username/password/registry carried through unchanged).
func TestImagePullAuthResolution(t *testing.T) {
	cfg := provider.Provider{Configuration: map[string]any{}}
	const name = "store"

	auth, err := imagePullAuth(cfg, nil, name)
	if err != nil || auth != nil {
		t.Fatalf("imagePullAuth() with no ref = %+v, %v; want nil, nil", auth, err)
	}

	cfg.Configuration["imagePullSecretRef"] = "registry-creds"
	if _, err := imagePullAuth(cfg, nil, name); err == nil {
		t.Fatal("imagePullAuth() accepted a secretRef with no resolved credentials")
	}

	secrets := map[string]map[string]string{
		"registry-creds": {"username": "u", "password": "p", "registry": "ghcr.io"},
	}
	auth, err = imagePullAuth(cfg, secrets, name)
	if err != nil {
		t.Fatalf("imagePullAuth(): %v", err)
	}
	if auth == nil || auth.Username != "u" || auth.Password != "p" || auth.Registry != "ghcr.io" {
		t.Errorf("imagePullAuth() = %+v, want {Username:u Password:p Registry:ghcr.io}", auth)
	}
}

// TestValidateSpecNodesTopology covers docs/planning/08 C4: MinIO's
// distributed erasure-coded mode has no supported topology between 1 node
// (standalone) and 4+ nodes — 2 and 3 must be refused at validate with a
// clear message, not accepted and left to fail unpredictably at container
// start.
func TestValidateSpecNodesTopology(t *testing.T) {
	p := New()
	base := provider.Provider{
		Type:          "s3",
		Configuration: map[string]any{"rootSecretRef": "root"},
		SecretRefs:    []string{"root"},
	}

	for _, n := range []float64{2, 3} {
		cfg := base
		cfg.Configuration = map[string]any{"rootSecretRef": "root", "nodes": n}
		if err := p.ValidateSpec(cfg); err == nil {
			t.Errorf("ValidateSpec accepted nodes: %v (unsupported MinIO topology)", n)
		}
	}
	for _, n := range []float64{1, 4, 5} {
		cfg := base
		cfg.Configuration = map[string]any{"rootSecretRef": "root", "nodes": n}
		if err := p.ValidateSpec(cfg); err != nil {
			t.Errorf("ValidateSpec rejected nodes: %v: %v", n, err)
		}
	}
	// nodes' own positive-integer shape (nodes: 0 and below) is now
	// schemas/v1alpha1/fragments/provider/s3.json's job (docs/planning/08
	// E5) — see cmd/platformctl's negative-test corpus.
}

// TestValidateSpecNodesRefusesPortPin mirrors redpanda's identical
// brokers+kafkaPort refusal (docs/adr/017 §a.4): every node's host port is
// auto-assigned under the StableIdentity set shape, so a fixed pin cannot
// be combined with nodes.
func TestValidateSpecNodesRefusesPortPin(t *testing.T) {
	p := New()
	cfg := provider.Provider{
		Type:          "s3",
		Configuration: map[string]any{"rootSecretRef": "root", "nodes": float64(4), "port": float64(9000)},
		SecretRefs:    []string{"root"},
	}
	err := p.ValidateSpec(cfg)
	if err == nil {
		t.Fatal("ValidateSpec accepted nodes combined with a pinned port")
	}
	if !strings.Contains(err.Error(), "port") {
		t.Errorf("error does not name the port conflict: %v", err)
	}
}

func TestNodesDeclared(t *testing.T) {
	if _, declared := nodesDeclared(provider.Provider{Configuration: map[string]any{}}); declared {
		t.Error("nodesDeclared() = true with no nodes key")
	}
	n, declared := nodesDeclared(provider.Provider{Configuration: map[string]any{"nodes": float64(4)}})
	if !declared || n != 4 {
		t.Errorf("nodesDeclared() = %d, %v; want 4, true", n, declared)
	}
	n, declared = nodesDeclared(provider.Provider{Configuration: map[string]any{"nodes": 1}})
	if !declared || n != 1 {
		t.Errorf("nodesDeclared() = %d, %v; want 1, true", n, declared)
	}
}

func TestMinioNodeURLs(t *testing.T) {
	got := minioNodeURLs("store", 4)
	want := []string{
		"http://store-0:9000/data",
		"http://store-1:9000/data",
		"http://store-2:9000/data",
		"http://store-3:9000/data",
	}
	if len(got) != len(want) {
		t.Fatalf("minioNodeURLs() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("minioNodeURLs()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}
