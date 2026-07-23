package openziti

import (
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/provider"
)

func configOf(cfg map[string]any) provider.Provider {
	return provider.Provider{Type: "openziti", Configuration: cfg}
}

func TestProviderType(t *testing.T) {
	p := New()
	if got := p.Type(); got != "openziti" {
		t.Errorf("Type() = %q, want %q", got, "openziti")
	}
}

func TestSupportedConnectionSchemesIsTCPOnly(t *testing.T) {
	p := New()
	schemes := p.SupportedConnectionSchemes()
	if len(schemes) != 1 || schemes[0] != "tcp" {
		t.Errorf("SupportedConnectionSchemes = %v, want [tcp]", schemes)
	}
}
