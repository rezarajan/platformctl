package connection

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
)

// ClientTLSConfig builds the *tls.Config a Go-side database driver (pgx,
// go-sql-driver/mysql) dials with for an external Connection's outbound TLS
// posture (docs/planning/08 I2). Pure stdlib — no I/O, no adapter
// dependency — so it is usable from any layer (domain has no import
// restriction on the standard library).
//
// mode is one of TLSModeRequire/TLSModeVerifyCA/TLSModeVerifyFull; caPEM is
// the already-resolved CA bundle (nil means "trust the process's system
// root CAs" for verify-ca/verify-full — libpq's own documented sslmode
// default, reused here rather than inventing a different one); serverName
// is the host being dialed, used by verify-full's hostname check only.
//
//   - require: encrypts the transport, verifies nothing (the caller sends
//     credentials over an unauthenticated channel — still strictly better
//     than plaintext, but does not defend against an on-path attacker).
//   - verify-ca: verifies the presented chain against a trusted CA, but not
//     the certificate's hostname. Go's tls.Config has no direct toggle for
//     "verify chain, skip hostname" — this is done by hand in a
//     VerifyPeerCertificate callback with the package's own verification
//     disabled (InsecureSkipVerify), matching libpq's sslmode=verify-ca.
//   - verify-full: verifies the chain AND the hostname — plain RootCAs +
//     ServerName, Go's own default verification behavior.
func ClientTLSConfig(mode string, caPEM []byte, serverName string) (*tls.Config, error) {
	var pool *x509.CertPool
	if len(caPEM) > 0 {
		pool = x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("CA bundle does not contain a valid PEM-encoded certificate")
		}
	}
	cfg := &tls.Config{ServerName: serverName, MinVersion: tls.VersionTLS12}
	switch mode {
	case TLSModeRequire:
		cfg.InsecureSkipVerify = true
	case TLSModeVerifyCA:
		cfg.InsecureSkipVerify = true
		cfg.VerifyPeerCertificate = verifyChainOnly(pool)
	case TLSModeVerifyFull:
		cfg.RootCAs = pool
	default:
		return nil, fmt.Errorf("unknown TLS mode %q (must be %s, %s, or %s)", mode, TLSModeRequire, TLSModeVerifyCA, TLSModeVerifyFull)
	}
	return cfg, nil
}

// verifyChainOnly builds a VerifyPeerCertificate callback that verifies the
// presented chain against pool (nil pool means the process's system root
// CAs) without checking the certificate's hostname — TLSModeVerifyCA's
// semantics, which crypto/tls has no built-in toggle for.
func verifyChainOnly(pool *x509.CertPool) func([][]byte, [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return fmt.Errorf("server presented no certificate")
		}
		cert, err := x509.ParseCertificate(rawCerts[0])
		if err != nil {
			return fmt.Errorf("parse server certificate: %w", err)
		}
		intermediates := x509.NewCertPool()
		for _, raw := range rawCerts[1:] {
			if ic, err := x509.ParseCertificate(raw); err == nil {
				intermediates.AddCert(ic)
			}
		}
		if _, err := cert.Verify(x509.VerifyOptions{Roots: pool, Intermediates: intermediates}); err != nil {
			return fmt.Errorf("certificate chain does not verify against the trusted CA: %w", err)
		}
		return nil
	}
}
