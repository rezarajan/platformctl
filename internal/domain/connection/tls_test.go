package connection

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"testing"
	"time"
)

// generateTestCA/generateTestLeaf build a throwaway CA + leaf certificate
// pair for these tests — deliberately minimal (no reuse of the ingress
// provider's generator, which lives in an adapter package domain must not
// import).
func generateTestCA(t *testing.T) (caCertPEM []byte, ca *x509.Certificate, caKey *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), cert, key
}

func generateTestLeaf(t *testing.T, caCert *x509.Certificate, caKey *ecdsa.PrivateKey, host string) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: host},
		DNSNames:     []string{host},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create leaf cert: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal leaf key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("X509KeyPair: %v", err)
	}
	return cert
}

// tlsEchoServer starts a TLS listener presenting leaf on 127.0.0.1, serving
// exactly one accepted connection before closing (enough for a client
// handshake attempt), returning its address.
func tlsEchoServer(t *testing.T, leaf tls.Certificate) string {
	t.Helper()
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{leaf}})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.(*tls.Conn).Handshake()
	}()
	return ln.Addr().String()
}

func dialWithConfig(t *testing.T, addr string, cfg *tls.Config) error {
	t.Helper()
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split addr: %v", err)
	}
	_ = host
	conn, err := tls.Dial("tcp", addr, cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	return nil
}

func TestClientTLSConfigRequireAcceptsAnyCertificate(t *testing.T) {
	caCertPEM, ca, caKey := generateTestCA(t)
	_ = caCertPEM
	leaf := generateTestLeaf(t, ca, caKey, "db.example.internal")
	addr := tlsEchoServer(t, leaf)

	cfg, err := ClientTLSConfig(TLSModeRequire, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("ClientTLSConfig: %v", err)
	}
	if !cfg.InsecureSkipVerify {
		t.Fatalf("require mode must skip verification")
	}
	if err := dialWithConfig(t, addr, cfg); err != nil {
		t.Fatalf("dial under require mode with no CA should succeed (no verification): %v", err)
	}
}

func TestClientTLSConfigVerifyFullSucceedsWithCorrectCAAndHostname(t *testing.T) {
	caCertPEM, ca, caKey := generateTestCA(t)
	leaf := generateTestLeaf(t, ca, caKey, "db.example.internal")
	addr := tlsEchoServer(t, leaf)

	cfg, err := ClientTLSConfig(TLSModeVerifyFull, caCertPEM, "db.example.internal")
	if err != nil {
		t.Fatalf("ClientTLSConfig: %v", err)
	}
	if err := dialWithConfig(t, addr, cfg); err != nil {
		t.Fatalf("dial under verify-full with the correct CA should succeed: %v", err)
	}
}

func TestClientTLSConfigVerifyFullFailsWithWrongCA(t *testing.T) {
	_, ca, caKey := generateTestCA(t)
	leaf := generateTestLeaf(t, ca, caKey, "db.example.internal")
	addr := tlsEchoServer(t, leaf)

	wrongCAPEM, _, _ := generateTestCA(t)
	cfg, err := ClientTLSConfig(TLSModeVerifyFull, wrongCAPEM, "db.example.internal")
	if err != nil {
		t.Fatalf("ClientTLSConfig: %v", err)
	}
	if err := dialWithConfig(t, addr, cfg); err == nil {
		t.Fatal("dial under verify-full with the wrong CA should fail")
	}
}

func TestClientTLSConfigVerifyCAFailsWithWrongCA(t *testing.T) {
	_, ca, caKey := generateTestCA(t)
	leaf := generateTestLeaf(t, ca, caKey, "db.example.internal")
	addr := tlsEchoServer(t, leaf)

	wrongCAPEM, _, _ := generateTestCA(t)
	cfg, err := ClientTLSConfig(TLSModeVerifyCA, wrongCAPEM, "db.example.internal")
	if err != nil {
		t.Fatalf("ClientTLSConfig: %v", err)
	}
	if err := dialWithConfig(t, addr, cfg); err == nil {
		t.Fatal("dial under verify-ca with the wrong CA should fail")
	}
}

func TestClientTLSConfigVerifyCASucceedsRegardlessOfHostname(t *testing.T) {
	caCertPEM, ca, caKey := generateTestCA(t)
	// The leaf covers "some-other-name", not "127.0.0.1" — verify-ca must
	// still succeed (it does not check the hostname), unlike verify-full.
	leaf := generateTestLeaf(t, ca, caKey, "some-other-name")
	addr := tlsEchoServer(t, leaf)

	cfg, err := ClientTLSConfig(TLSModeVerifyCA, caCertPEM, "db.example.internal")
	if err != nil {
		t.Fatalf("ClientTLSConfig: %v", err)
	}
	if err := dialWithConfig(t, addr, cfg); err != nil {
		t.Fatalf("dial under verify-ca should ignore hostname mismatch: %v", err)
	}
}

func TestClientTLSConfigInvalidCAPEM(t *testing.T) {
	if _, err := ClientTLSConfig(TLSModeVerifyFull, []byte("not a certificate"), "host"); err == nil {
		t.Fatal("expected error for an unparseable CA bundle")
	}
}

func TestClientTLSConfigUnknownMode(t *testing.T) {
	if _, err := ClientTLSConfig("trust-me", nil, "host"); err == nil {
		t.Fatal("expected error for an unknown TLS mode")
	}
}
