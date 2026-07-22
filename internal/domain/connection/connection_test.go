package connection

import (
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

func envelope(spec map[string]any) resource.Envelope {
	return resource.Envelope{
		GroupVersionKind: resource.GroupVersionKind{Kind: "Connection"},
		Metadata:         resource.Metadata{Name: "test-conn"},
		Spec:             spec,
	}
}

func baseManagedSpec() map[string]any {
	return map[string]any{
		"providerRef": map[string]any{"name": "edge-http"},
		"port":        float64(80),
		"target":      "nessie:19120",
	}
}

func TestTLSSecretRefParsesAndValidates(t *testing.T) {
	spec := baseManagedSpec()
	spec["scheme"] = "https"
	spec["tls"] = map[string]any{"secretRef": map[string]any{"name": "nessie-tls"}}
	c, err := FromEnvelope(envelope(spec))
	if err != nil {
		t.Fatalf("FromEnvelope: %v", err)
	}
	if c.TLS == nil || c.TLS.SecretRef == nil || *c.TLS.SecretRef != "nessie-tls" {
		t.Fatalf("TLS.SecretRef = %+v, want *TLS{SecretRef: nessie-tls}", c.TLS)
	}
	if c.TLS.SelfSigned || c.TLS.SecretName != nil {
		t.Errorf("unexpected TLS fields set: %+v", c.TLS)
	}
}

func TestTLSSelfSignedParses(t *testing.T) {
	spec := baseManagedSpec()
	spec["scheme"] = "https"
	spec["tls"] = map[string]any{"selfSigned": true}
	c, err := FromEnvelope(envelope(spec))
	if err != nil {
		t.Fatalf("FromEnvelope: %v", err)
	}
	if c.TLS == nil || !c.TLS.SelfSigned {
		t.Fatalf("TLS.SelfSigned = %+v, want true", c.TLS)
	}
}

func TestTLSSecretNameParses(t *testing.T) {
	spec := baseManagedSpec()
	spec["scheme"] = "https"
	spec["tls"] = map[string]any{"secretName": "cert-manager-issued"}
	c, err := FromEnvelope(envelope(spec))
	if err != nil {
		t.Fatalf("FromEnvelope: %v", err)
	}
	if c.TLS == nil || c.TLS.SecretName == nil || *c.TLS.SecretName != "cert-manager-issued" {
		t.Fatalf("TLS.SecretName = %+v, want *TLS{SecretName: cert-manager-issued}", c.TLS)
	}
}

func TestTLSRequiresHTTPSScheme(t *testing.T) {
	spec := baseManagedSpec()
	spec["scheme"] = "http"
	spec["tls"] = map[string]any{"selfSigned": true}
	if _, err := FromEnvelope(envelope(spec)); err == nil {
		t.Fatal("expected error: spec.tls declared with scheme != https")
	}
}

func TestHTTPSSchemeRequiresTLS(t *testing.T) {
	spec := baseManagedSpec()
	spec["scheme"] = "https"
	if _, err := FromEnvelope(envelope(spec)); err == nil {
		t.Fatal("expected error: scheme https with no spec.tls")
	}
}

func TestTLSExactlyOneOf(t *testing.T) {
	cases := []map[string]any{
		{}, // none set
		{"secretRef": map[string]any{"name": "a"}, "selfSigned": true},
		{"secretRef": map[string]any{"name": "a"}, "secretName": "b"},
		{"selfSigned": true, "secretName": "b"},
	}
	for i, tls := range cases {
		spec := baseManagedSpec()
		spec["scheme"] = "https"
		spec["tls"] = tls
		if _, err := FromEnvelope(envelope(spec)); err == nil {
			t.Errorf("case %d: expected exactly-one-of error for %+v", i, tls)
		}
	}
}

func TestTLSRefusedOnExternalConnection(t *testing.T) {
	spec := map[string]any{
		"external": true,
		"host":     "warehouse.corp.internal",
		"port":     float64(9000),
		"scheme":   "https",
		"tls":      map[string]any{"selfSigned": true},
	}
	if _, err := FromEnvelope(envelope(spec)); err == nil {
		t.Fatal("expected error: spec.tls on an external connection")
	}
}

func TestPlainHTTPConnectionUnaffected(t *testing.T) {
	spec := baseManagedSpec()
	spec["scheme"] = "http"
	c, err := FromEnvelope(envelope(spec))
	if err != nil {
		t.Fatalf("FromEnvelope: %v", err)
	}
	if c.TLS != nil {
		t.Errorf("TLS = %+v, want nil for a plain http Connection", c.TLS)
	}
}
