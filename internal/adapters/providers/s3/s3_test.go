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
	p := New()
	p.cfg = provider.Provider{Configuration: map[string]any{}}
	p.providerRes.Metadata.Name = "store"

	auth, err := p.imagePullAuth()
	if err != nil || auth != nil {
		t.Fatalf("imagePullAuth() with no ref = %+v, %v; want nil, nil", auth, err)
	}

	p.cfg.Configuration["imagePullSecretRef"] = "registry-creds"
	if _, err := p.imagePullAuth(); err == nil {
		t.Fatal("imagePullAuth() accepted a secretRef with no resolved credentials")
	}

	p.secrets = map[string]map[string]string{
		"registry-creds": {"username": "u", "password": "p", "registry": "ghcr.io"},
	}
	auth, err = p.imagePullAuth()
	if err != nil {
		t.Fatalf("imagePullAuth(): %v", err)
	}
	if auth == nil || auth.Username != "u" || auth.Password != "p" || auth.Registry != "ghcr.io" {
		t.Errorf("imagePullAuth() = %+v, want {Username:u Password:p Registry:ghcr.io}", auth)
	}
}
