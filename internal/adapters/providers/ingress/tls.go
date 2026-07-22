// This file holds the local-CA/leaf-certificate generation and structural
// validation helpers behind Connection.spec.tls: {selfSigned: true}
// (docs/planning/08 C8, docs/adr/018 addendum). Runtime-agnostic — both
// docker.go (Caddy admin API) and kubernetes.go (a kubernetes.io/tls
// Secret) call into these; only where the resulting PEM material *lands*
// differs per runtime.
package ingress

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"time"
)

const (
	// caValidity is generous for a dev-only local CA — it only needs to
	// outlive the platform instance it was generated for, and a long
	// validity avoids "my CA expired mid-project" surprise.
	caValidity = 10 * 365 * 24 * time.Hour
	// leafValidity is short (well under the ~398 day CA/Browser Forum
	// public-CA cap, though this CA is private and not bound by it) —
	// leaf certs regenerate cheaply on the next apply if ever needed, so
	// there's no cost to keeping them short-lived.
	leafValidity   = 90 * 24 * time.Hour
	caCommonName   = "platformctl local dev CA"
	certPEMType    = "CERTIFICATE"
	privKeyPEMType = "EC PRIVATE KEY"
)

// generateCA creates a new self-signed CA keypair, PEM-encoded. Called only
// when no existing CA was found (the read-before-regenerate pattern in
// docker.go/kubernetes.go) — a fresh CA on every apply would force every
// tool trusting the previous one to re-trust it.
func generateCA() (certPEM, keyPEM []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generate CA key: %w", err)
	}
	serial, err := randomSerial()
	if err != nil {
		return nil, nil, err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: caCommonName},
		NotBefore:             now.Add(-5 * time.Minute), // clock-skew slack
		NotAfter:              now.Add(caValidity),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, nil, fmt.Errorf("create CA certificate: %w", err)
	}
	certPEM, err = encodeCertPEM(der)
	if err != nil {
		return nil, nil, err
	}
	keyPEM, err = encodeECKeyPEM(key)
	if err != nil {
		return nil, nil, err
	}
	return certPEM, keyPEM, nil
}

// generateLeafCert creates a new leaf certificate for host, signed by the
// CA named by caCertPEM/caKeyPEM. PEM-encoded, ready to load into Caddy's
// admin API or a kubernetes.io/tls Secret.
func generateLeafCert(caCertPEM, caKeyPEM []byte, host string) (certPEM, keyPEM []byte, err error) {
	caCert, caKey, err := parseCAKeyPair(caCertPEM, caKeyPEM)
	if err != nil {
		return nil, nil, fmt.Errorf("parse CA for leaf cert %q: %w", host, err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generate leaf key for %q: %w", host, err)
	}
	serial, err := randomSerial()
	if err != nil {
		return nil, nil, err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: host},
		DNSNames:     []string{host},
		NotBefore:    now.Add(-5 * time.Minute),
		NotAfter:     now.Add(leafValidity),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		return nil, nil, fmt.Errorf("create leaf certificate for %q: %w", host, err)
	}
	certPEM, err = encodeCertPEM(der)
	if err != nil {
		return nil, nil, err
	}
	keyPEM, err = encodeECKeyPEM(key)
	if err != nil {
		return nil, nil, err
	}
	return certPEM, keyPEM, nil
}

func randomSerial() (*big.Int, error) {
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, limit)
	if err != nil {
		return nil, fmt.Errorf("generate certificate serial: %w", err)
	}
	return serial, nil
}

func encodeCertPEM(der []byte) ([]byte, error) {
	return pem.EncodeToMemory(&pem.Block{Type: certPEMType, Bytes: der}), nil
}

func encodeECKeyPEM(key *ecdsa.PrivateKey) ([]byte, error) {
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("marshal private key: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: privKeyPEMType, Bytes: der}), nil
}

// parseCertPEM decodes a single PEM-encoded certificate.
func parseCertPEM(certPEM []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return nil, fmt.Errorf("no PEM-encoded certificate found")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse certificate: %w", err)
	}
	return cert, nil
}

// parseCAKeyPair decodes a CA's cert+key PEM pair for use as a signer.
func parseCAKeyPair(certPEM, keyPEM []byte) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	cert, err := parseCertPEM(certPEM)
	if err != nil {
		return nil, nil, err
	}
	block, _ := pem.Decode(keyPEM)
	if block == nil {
		return nil, nil, fmt.Errorf("no PEM-encoded private key found")
	}
	key, err := x509.ParseECPrivateKey(block.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("parse CA private key: %w", err)
	}
	return cert, key, nil
}

// certValidForHost is the structural drift/idempotency check both Reconcile
// (read-before-regenerate) and Probe use: does the given cert PEM parse,
// remain unexpired with margin, and cover host — without comparing byte
// content, which would flag drift on every apply purely from a freshly
// generated cert's different serial number/timestamps (docs/adr/018
// addendum's "drifted names, not values" bar, extended to structural
// validity for a non-deterministically-regenerated artifact).
func certValidForHost(certPEM []byte, host string, now time.Time) error {
	cert, err := parseCertPEM(certPEM)
	if err != nil {
		return err
	}
	// A margin before expiry so a cert doesn't go invalid between an apply
	// declaring it healthy and a client actually connecting.
	if now.Add(24 * time.Hour).After(cert.NotAfter) {
		return fmt.Errorf("certificate for %q expires %s (within 24h or already expired)", host, cert.NotAfter)
	}
	if now.Before(cert.NotBefore) {
		return fmt.Errorf("certificate for %q is not yet valid (NotBefore %s)", host, cert.NotBefore)
	}
	if err := cert.VerifyHostname(host); err != nil {
		return fmt.Errorf("certificate for %q: %w", host, err)
	}
	return nil
}

// certChainsToCA additionally verifies the certificate was signed by
// caCertPEM — the self-signed-mode check that catches a leaf cert issued by
// a *previous* (rotated/regenerated) CA, which would otherwise pass
// certValidForHost alone.
func certChainsToCA(certPEM, caCertPEM []byte, host string, now time.Time) error {
	if err := certValidForHost(certPEM, host, now); err != nil {
		return err
	}
	cert, err := parseCertPEM(certPEM)
	if err != nil {
		return err
	}
	caCert, err := parseCertPEM(caCertPEM)
	if err != nil {
		return fmt.Errorf("parse CA certificate: %w", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	if _, err := cert.Verify(x509.VerifyOptions{DNSName: host, Roots: pool, CurrentTime: now}); err != nil {
		return fmt.Errorf("certificate for %q does not chain to the current CA: %w", host, err)
	}
	return nil
}

// validateKeyPair confirms certPEM/keyPEM parse and the key actually
// corresponds to the certificate's public key — an operator-provided
// secretRef cert/key that don't match must fail clearly at reconcile, never
// load silently into Caddy as a cert nothing can complete a handshake
// with. Reuses crypto/tls's own loader rather than re-implementing the
// check.
func validateKeyPair(certPEM, keyPEM []byte) error {
	if _, err := tls.X509KeyPair(certPEM, keyPEM); err != nil {
		return fmt.Errorf("cert/key do not form a valid pair: %w", err)
	}
	return nil
}

// certMatchesSecret reports whether liveCertPEM is byte-identical to
// desiredCertPEM — the provided-secretRef mode's drift check, valid here
// (unlike the self-signed leaf-cert case) because a provided cert's desired
// content is fully deterministic: whatever the SecretReference currently
// holds.
func certMatchesSecret(liveCertPEM, desiredCertPEM []byte) bool {
	return string(liveCertPEM) == string(desiredCertPEM)
}
