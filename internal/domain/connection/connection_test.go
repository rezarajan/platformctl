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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
	spec := baseManagedSpec()
	spec["scheme"] = "http"
	spec["tls"] = map[string]any{"selfSigned": true}
	if _, err := FromEnvelope(envelope(spec)); err == nil {
		t.Fatal("expected error: spec.tls declared with scheme != https")
	}
}

func TestHTTPSSchemeRequiresTLS(t *testing.T) {
	t.Parallel()
	spec := baseManagedSpec()
	spec["scheme"] = "https"
	if _, err := FromEnvelope(envelope(spec)); err == nil {
		t.Fatal("expected error: scheme https with no spec.tls")
	}
}

func TestTLSExactlyOneOf(t *testing.T) {
	t.Parallel()
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

func TestManagedOnlyTLSFieldsRefusedOnExternalConnection(t *testing.T) {
	t.Parallel()
	spec := map[string]any{
		"external": true,
		"host":     "warehouse.corp.internal",
		"port":     float64(9000),
		"tls":      map[string]any{"selfSigned": true},
	}
	if _, err := FromEnvelope(envelope(spec)); err == nil {
		t.Fatal("expected error: managed-only spec.tls.selfSigned on an external connection")
	}
}

func baseExternalSpec() map[string]any {
	return map[string]any{
		"external": true,
		"host":     "rds.us-east-1.amazonaws.com",
		"port":     float64(5432),
	}
}

func TestExternalTLSModeRequireParses(t *testing.T) {
	t.Parallel()
	spec := baseExternalSpec()
	spec["tls"] = map[string]any{"mode": "require"}
	c, err := FromEnvelope(envelope(spec))
	if err != nil {
		t.Fatalf("FromEnvelope: %v", err)
	}
	if c.TLS == nil || c.TLS.Mode != "require" {
		t.Fatalf("TLS = %+v, want mode=require", c.TLS)
	}
	if c.TLS.CASecretRef != nil {
		t.Errorf("CASecretRef = %v, want nil", c.TLS.CASecretRef)
	}
}

func TestExternalTLSModeVerifyFullWithCASecretRefParses(t *testing.T) {
	t.Parallel()
	spec := baseExternalSpec()
	spec["tls"] = map[string]any{"mode": "verify-full", "caSecretRef": map[string]any{"name": "rds-ca"}}
	c, err := FromEnvelope(envelope(spec))
	if err != nil {
		t.Fatalf("FromEnvelope: %v", err)
	}
	if c.TLS == nil || c.TLS.Mode != "verify-full" {
		t.Fatalf("TLS = %+v, want mode=verify-full", c.TLS)
	}
	if c.TLS.CASecretRef == nil || *c.TLS.CASecretRef != "rds-ca" {
		t.Fatalf("CASecretRef = %v, want rds-ca", c.TLS.CASecretRef)
	}
}

func TestExternalTLSModeVerifyCAParses(t *testing.T) {
	t.Parallel()
	spec := baseExternalSpec()
	spec["tls"] = map[string]any{"mode": "verify-ca", "caSecretRef": map[string]any{"name": "rds-ca"}}
	c, err := FromEnvelope(envelope(spec))
	if err != nil {
		t.Fatalf("FromEnvelope: %v", err)
	}
	if c.TLS == nil || c.TLS.Mode != "verify-ca" {
		t.Fatalf("TLS = %+v, want mode=verify-ca", c.TLS)
	}
}

func TestExternalTLSRequiresMode(t *testing.T) {
	t.Parallel()
	spec := baseExternalSpec()
	spec["tls"] = map[string]any{"caSecretRef": map[string]any{"name": "rds-ca"}}
	if _, err := FromEnvelope(envelope(spec)); err == nil {
		t.Fatal("expected error: spec.tls declared without spec.tls.mode on an external connection")
	}
}

func TestExternalTLSRejectsUnknownMode(t *testing.T) {
	t.Parallel()
	spec := baseExternalSpec()
	spec["tls"] = map[string]any{"mode": "trust-me"}
	if _, err := FromEnvelope(envelope(spec)); err == nil {
		t.Fatal("expected error: unknown spec.tls.mode on an external connection")
	}
}

func TestExternalConnectionWithoutTLSStaysPlaintext(t *testing.T) {
	t.Parallel()
	spec := baseExternalSpec()
	c, err := FromEnvelope(envelope(spec))
	if err != nil {
		t.Fatalf("FromEnvelope: %v", err)
	}
	if c.TLS != nil {
		t.Errorf("TLS = %+v, want nil (absent spec.tls preserves plaintext back-compat)", c.TLS)
	}
}

func TestManagedTLSRejectsModeAndCASecretRef(t *testing.T) {
	t.Parallel()
	spec := baseManagedSpec()
	spec["scheme"] = "https"
	spec["tls"] = map[string]any{"selfSigned": true, "mode": "require"}
	if _, err := FromEnvelope(envelope(spec)); err == nil {
		t.Fatal("expected error: external-only spec.tls.mode on a managed connection")
	}
}

func TestPlainHTTPConnectionUnaffected(t *testing.T) {
	t.Parallel()
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
