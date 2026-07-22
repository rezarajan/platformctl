package ingress

import (
	"strings"
	"testing"
	"time"
)

func TestGenerateCAIsSelfSignedAndValid(t *testing.T) {
	certPEM, keyPEM, err := generateCA()
	if err != nil {
		t.Fatalf("generateCA: %v", err)
	}
	cert, err := parseCertPEM(certPEM)
	if err != nil {
		t.Fatalf("parseCertPEM: %v", err)
	}
	if !cert.IsCA {
		t.Error("generated CA certificate has IsCA = false")
	}
	if cert.Subject.CommonName != caCommonName {
		t.Errorf("CommonName = %q, want %q", cert.Subject.CommonName, caCommonName)
	}
	if _, _, err := parseCAKeyPair(certPEM, keyPEM); err != nil {
		t.Errorf("generated CA key does not parse/match its cert: %v", err)
	}
}

func TestGenerateLeafCertChainsToCA(t *testing.T) {
	caCert, caKey, err := generateCA()
	if err != nil {
		t.Fatalf("generateCA: %v", err)
	}
	leafCert, _, err := generateLeafCert(caCert, caKey, "nessie.localhost")
	if err != nil {
		t.Fatalf("generateLeafCert: %v", err)
	}
	now := time.Now()
	if err := certChainsToCA(leafCert, caCert, "nessie.localhost", now); err != nil {
		t.Errorf("leaf cert does not chain to its own CA: %v", err)
	}
	// A different host name must fail hostname verification even though the
	// chain itself is valid.
	if err := certChainsToCA(leafCert, caCert, "other.localhost", now); err == nil {
		t.Error("expected hostname mismatch error for a different host")
	}
}

func TestLeafCertDoesNotChainToADifferentCA(t *testing.T) {
	caCert, caKey, err := generateCA()
	if err != nil {
		t.Fatalf("generateCA: %v", err)
	}
	otherCACert, _, err := generateCA()
	if err != nil {
		t.Fatalf("generateCA (other): %v", err)
	}
	leafCert, _, err := generateLeafCert(caCert, caKey, "nessie.localhost")
	if err != nil {
		t.Fatalf("generateLeafCert: %v", err)
	}
	if err := certChainsToCA(leafCert, otherCACert, "nessie.localhost", time.Now()); err == nil {
		t.Error("expected chain-verification failure against an unrelated CA")
	}
}

func TestCertValidForHostRejectsExpiry(t *testing.T) {
	caCert, caKey, err := generateCA()
	if err != nil {
		t.Fatalf("generateCA: %v", err)
	}
	leafCert, _, err := generateLeafCert(caCert, caKey, "nessie.localhost")
	if err != nil {
		t.Fatalf("generateLeafCert: %v", err)
	}
	// A "now" far in the future is past NotAfter — must be rejected.
	future := time.Now().Add(leafValidity * 2)
	if err := certValidForHost(leafCert, "nessie.localhost", future); err == nil {
		t.Error("expected expiry rejection for a far-future 'now'")
	}
}

func TestCertValidForHostRejectsUnparsable(t *testing.T) {
	if err := certValidForHost([]byte("not a cert"), "x", time.Now()); err == nil {
		t.Error("expected parse error for garbage PEM")
	}
}

func TestCertMatchesSecret(t *testing.T) {
	a := []byte("-----BEGIN CERTIFICATE-----\nabc\n-----END CERTIFICATE-----\n")
	b := []byte("-----BEGIN CERTIFICATE-----\nabc\n-----END CERTIFICATE-----\n")
	c := []byte("-----BEGIN CERTIFICATE-----\ndifferent\n-----END CERTIFICATE-----\n")
	if !certMatchesSecret(a, b) {
		t.Error("identical PEM content should match")
	}
	if certMatchesSecret(a, c) {
		t.Error("different PEM content should not match")
	}
}

func TestGeneratedCertsArePEMEncoded(t *testing.T) {
	certPEM, keyPEM, err := generateCA()
	if err != nil {
		t.Fatalf("generateCA: %v", err)
	}
	if !strings.Contains(string(certPEM), "BEGIN CERTIFICATE") {
		t.Error("cert PEM does not look PEM-encoded")
	}
	if !strings.Contains(string(keyPEM), "BEGIN EC PRIVATE KEY") {
		t.Error("key PEM does not look PEM-encoded")
	}
}
