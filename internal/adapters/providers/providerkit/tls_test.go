package providerkit

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/rezarajan/platformctl/internal/domain/connection"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
)

func TestCAFilePathDeterministic(t *testing.T) {
	t.Parallel()
	if got, want := CAFilePath("rds-ca"), CATrustDir+"/rds-ca.ca.pem"; got != want {
		t.Errorf("CAFilePath = %q, want %q", got, want)
	}
	// Same input, same output — the connector-property side (built at
	// Binding-reconcile time) and the file-mount side (built at
	// Provider-reconcile time) must agree without coordination.
	if CAFilePath("rds-ca") != CAFilePath("rds-ca") { //nolint:staticcheck // SA4000: deliberate same-input-twice determinism check, not a copy-paste bug
		t.Error("CAFilePath must be deterministic")
	}
}

func TestCATrustFileMountsOnlyMountsSecretsCarryingCA(t *testing.T) {
	t.Parallel()
	cfg := provider.Provider{SecretRefs: []string{"db-creds", "rds-ca", "unresolved-ref"}}
	secrets := map[string]map[string]string{
		"db-creds": {"username": "u", "password": "p"}, // no "ca" key
		"rds-ca":   {"ca": "-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----\n"},
		// "unresolved-ref" absent from secrets entirely
	}
	mounts := CATrustFileMounts(cfg, secrets)
	if len(mounts) != 1 {
		t.Fatalf("mounts = %+v, want exactly 1 (only rds-ca carries a \"ca\" key)", mounts)
	}
	if mounts[0].Path != CAFilePath("rds-ca") {
		t.Errorf("mount path = %q, want %q", mounts[0].Path, CAFilePath("rds-ca"))
	}
	if string(mounts[0].Content) != secrets["rds-ca"]["ca"] {
		t.Errorf("mount content = %q, want the resolved CA PEM verbatim", mounts[0].Content)
	}
}

func TestCATrustFileMountsEmptyWhenNoSecretCarriesCA(t *testing.T) {
	t.Parallel()
	cfg := provider.Provider{SecretRefs: []string{"db-creds"}}
	secrets := map[string]map[string]string{"db-creds": {"username": "u", "password": "p"}}
	if mounts := CATrustFileMounts(cfg, secrets); len(mounts) != 0 {
		t.Errorf("mounts = %+v, want none", mounts)
	}
}

func TestResolveDatabaseTLSNilWhenNoConnectionTLS(t *testing.T) {
	t.Parallel()
	req := reconciler.Request{}
	cfg := provider.Provider{}
	posture, err := ResolveDatabaseTLS(req, cfg, EndpointResolution{})
	if err != nil {
		t.Fatalf("ResolveDatabaseTLS: %v", err)
	}
	if posture != nil {
		t.Errorf("posture = %+v, want nil", posture)
	}
}

func TestResolveDatabaseTLSRequiresCASecretRefInProviderSecretRefs(t *testing.T) {
	t.Parallel()
	caRef := "rds-ca"
	ep := EndpointResolution{TLS: &connection.TLS{Mode: "verify-full", CASecretRef: &caRef}}
	req := reconciler.Request{Secrets: map[string]map[string]string{"rds-ca": {"ca": "pem"}}}
	cfg := provider.Provider{} // rds-ca deliberately not declared in spec.secretRefs
	if _, err := ResolveDatabaseTLS(req, cfg, ep); err == nil {
		t.Fatal("expected error: caSecretRef not listed in the Provider's own spec.secretRefs")
	}
}

func TestResolveDatabaseTLSRequiresCAKey(t *testing.T) {
	t.Parallel()
	caRef := "rds-ca"
	ep := EndpointResolution{TLS: &connection.TLS{Mode: "verify-full", CASecretRef: &caRef}}
	cfg := provider.Provider{SecretRefs: []string{"rds-ca"}}
	req := reconciler.Request{Secrets: map[string]map[string]string{"rds-ca": {"username": "u"}}} // no "ca" key
	if _, err := ResolveDatabaseTLS(req, cfg, ep); err == nil {
		t.Fatal("expected error: SecretReference missing its \"ca\" key")
	}
}

func TestResolveDatabaseTLSSuccess(t *testing.T) {
	t.Parallel()
	caRef := "rds-ca"
	ep := EndpointResolution{TLS: &connection.TLS{Mode: "require", CASecretRef: &caRef}}
	cfg := provider.Provider{SecretRefs: []string{"rds-ca"}}
	req := reconciler.Request{Secrets: map[string]map[string]string{"rds-ca": {"ca": "pem-bytes"}}}
	posture, err := ResolveDatabaseTLS(req, cfg, ep)
	if err != nil {
		t.Fatalf("ResolveDatabaseTLS: %v", err)
	}
	if posture.Mode != "require" || string(posture.CAPEM) != "pem-bytes" || posture.CASecretRefName != "rds-ca" {
		t.Errorf("posture = %+v, want Mode=require CAPEM=pem-bytes CASecretRefName=rds-ca", posture)
	}
}

func TestResolveDatabaseTLSNoCASecretRefResolvesModeOnly(t *testing.T) {
	t.Parallel()
	ep := EndpointResolution{TLS: &connection.TLS{Mode: "require"}}
	posture, err := ResolveDatabaseTLS(reconciler.Request{}, provider.Provider{}, ep)
	if err != nil {
		t.Fatalf("ResolveDatabaseTLS: %v", err)
	}
	if posture.Mode != "require" || posture.CAPEM != nil || posture.CASecretRefName != "" {
		t.Errorf("posture = %+v, want Mode=require with no CA material", posture)
	}
}

// closedPort returns a "127.0.0.1:<port>" address nothing is listening on
// — a TCP dial there fails immediately with connection-refused, letting
// these tests assert VerifyDatabaseConnection surfaces the real dial error
// (ADR 011: never swallowed) without needing a live TLS-speaking server.
func closedPort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func TestVerifyDatabaseConnectionPostgresSurfacesRealError(t *testing.T) {
	t.Parallel()
	addr := closedPort(t)
	host, portText, _ := net.SplitHostPort(addr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := VerifyDatabaseConnection(ctx, "postgres", host, mustAtoi(t, portText), "db", "u", "p", nil)
	if err == nil {
		t.Fatal("expected a connection error against a closed port")
	}
}

func TestVerifyDatabaseConnectionPostgresInvalidCA(t *testing.T) {
	t.Parallel()
	addr := closedPort(t)
	host, portText, _ := net.SplitHostPort(addr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	posture := &DatabaseTLS{Mode: "verify-full", CAPEM: []byte("not a certificate")}
	err := VerifyDatabaseConnection(ctx, "postgres", host, mustAtoi(t, portText), "db", "u", "p", posture)
	if err == nil || !strings.Contains(err.Error(), "CA bundle") {
		t.Fatalf("err = %v, want an error naming the invalid CA bundle", err)
	}
}

func TestVerifyDatabaseConnectionMySQLSurfacesRealError(t *testing.T) {
	t.Parallel()
	addr := closedPort(t)
	host, portText, _ := net.SplitHostPort(addr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := VerifyDatabaseConnection(ctx, "mysql", host, mustAtoi(t, portText), "db", "u", "p", nil)
	if err == nil {
		t.Fatal("expected a connection error against a closed port")
	}
}

func TestVerifyDatabaseConnectionMySQLInvalidCA(t *testing.T) {
	t.Parallel()
	addr := closedPort(t)
	host, portText, _ := net.SplitHostPort(addr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	posture := &DatabaseTLS{Mode: "require", CAPEM: []byte("not a certificate")}
	err := VerifyDatabaseConnection(ctx, "mysql", host, mustAtoi(t, portText), "db", "u", "p", posture)
	if err == nil || !strings.Contains(err.Error(), "CA bundle") {
		t.Fatalf("err = %v, want an error naming the invalid CA bundle", err)
	}
}

func TestVerifyDatabaseConnectionUnknownEngineNoop(t *testing.T) {
	t.Parallel()
	if err := VerifyDatabaseConnection(context.Background(), "mongodb", "h", 1, "db", "u", "p", nil); err != nil {
		t.Errorf("unknown engine should no-op (mirrors the pre-I2 default case), got: %v", err)
	}
}

func mustAtoi(t *testing.T, s string) int {
	t.Helper()
	var n int
	for _, c := range s {
		if c < '0' || c > '9' {
			t.Fatalf("not a number: %q", s)
		}
		n = n*10 + int(c-'0')
	}
	return n
}
