//go:build integration

package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	dockerruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/docker"
)

const (
	tlsdbConnectURL = "http://localhost:18795"
	// tlsdbSubnet/tlsdbIP: the out-of-band Postgres gets a fixed IP on a
	// fixed-subnet Docker network. Linux Docker routes a user-defined
	// bridge network's subnet directly on the host (no -p publish needed
	// to reach a container's own bridge IP from the host machine that
	// runs dockerd) — so this ONE address is reachable identically from
	// both vantage points the engine's own external-Connection reachability
	// check dials (docs/planning/08 C10): the platformctl CLI process
	// itself (native on the host, not containerized — the same process
	// go test runs in) and a throwaway probe container on
	// datascape-tlsdb-net (the debezium worker's own network, which the
	// registered connector also dials from). This mirrors the one real
	// address a genuine cloud-managed database has — reachable the same
	// way from anywhere — where a purely local docker-network fixture
	// would otherwise need two (host-published vs. in-network) addresses
	// a single spec.host cannot carry both of at once.
	tlsdbSubnet = "172.28.61.0/24"
	tlsdbIP     = "172.28.61.10"
)

// TestExternalDatabaseTLSEndToEnd covers docs/planning/08 I2's accept
// criteria: a real TLS-required Postgres (server cert from a throwaway test
// CA, ssl=on, no plaintext pg_hba.conf rule — refuses a non-TLS connection
// outright, the way a cloud-managed engine like RDS/Cloud SQL does) —
//
//   - no spec.tls declared: the preflight dial is refused, the real
//     server-side error surfaced (never swallowed into a generic timeout).
//   - spec.tls.mode: verify-full with the WRONG caSecretRef: the preflight
//     TLS handshake completes but certificate verification fails — a named
//     reason at preflight (ADR 011: never mid-apply, no connector ever
//     registered).
//   - spec.tls.mode: verify-full with the correct caSecretRef: apply
//     succeeds and the CDC connector reaches RUNNING against the TLS DB.
func TestExternalDatabaseTLSEndToEnd(t *testing.T) {
	rt, err := dockerruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to Docker: %v", err)
	}
	ctx := context.Background()

	correctCAPEM, wrongCAPEM, buildDir := generateTLSDBFixtures(t)
	buildImage(t, "datascape-tlsdb-pg:test", buildDir)

	cleanupPG := func() {
		_ = exec.Command("docker", "rm", "-f", "datascape-tlsdb-outofband-pg").Run()
		_ = exec.Command("docker", "network", "rm", "datascape-tlsdb-net").Run()
	}
	cleanupPG()
	t.Cleanup(cleanupPG)

	if out, err := exec.Command("docker", "network", "create",
		"--label", "io.datascape.managed-by=platformctl",
		"--subnet", tlsdbSubnet, "datascape-tlsdb-net").CombinedOutput(); err != nil {
		t.Fatalf("create network: %v\n%s", err, out)
	}
	if out, err := exec.Command("docker", "run", "-d", "--name", "datascape-tlsdb-outofband-pg",
		"--network", "datascape-tlsdb-net", "--ip", tlsdbIP,
		"-e", "POSTGRES_USER=tlsuser", "-e", "POSTGRES_PASSWORD=tlspw", "-e", "POSTGRES_DB=attendance",
		"datascape-tlsdb-pg:test",
		"postgres", "-c", "wal_level=logical", "-c", "ssl=on",
		"-c", "ssl_cert_file=/certs/server.crt", "-c", "ssl_key_file=/certs/server.key",
		"-c", "hba_file=/certs/pg_hba.conf").CombinedOutput(); err != nil {
		t.Fatalf("out-of-band docker run: %v\n%s", err, out)
	}
	waitTLSDBReady(t)

	cleanupInfra := func() {
		for _, c := range []string{"datascape-tlsdb-dbz", "datascape-tlsdb-rp"} {
			_ = rt.Remove(ctx, c)
		}
		_ = rt.RemoveVolume(ctx, "datascape-tlsdb-rp-data")
	}

	t.Run("no-tls-refused", func(t *testing.T) {
		cleanupInfra()
		t.Cleanup(cleanupInfra)
		t.Setenv("DATASCAPE_SECRET_TLSDB_CONN_USERNAME", "tlsuser")
		t.Setenv("DATASCAPE_SECRET_TLSDB_CONN_PASSWORD", "tlspw")

		stateFile := filepath.Join(t.TempDir(), "state.json")
		out, err, code := run(t, "apply", "testdata/external-db-tls-scenario/no-tls", "--state-file", stateFile, "--auto-approve")
		if err == nil && code == 0 {
			t.Fatalf("apply against an SSL-required database with no spec.tls should have failed:\n%s", out)
		}
		lower := strings.ToLower(out)
		if !strings.Contains(lower, "ssl") && !strings.Contains(lower, "encrypt") {
			t.Errorf("error does not appear to name the real TLS refusal (want \"ssl\"/\"encrypt\" mentioned):\n%s", out)
		}
	})

	t.Run("wrong-ca-verify-fails", func(t *testing.T) {
		cleanupInfra()
		t.Cleanup(cleanupInfra)
		t.Setenv("DATASCAPE_SECRET_TLSDB_CONN_USERNAME", "tlsuser")
		t.Setenv("DATASCAPE_SECRET_TLSDB_CONN_PASSWORD", "tlspw")
		t.Setenv("DATASCAPE_SECRET_TLSDB_CA_CA", string(wrongCAPEM))

		stateFile := filepath.Join(t.TempDir(), "state.json")
		out, err, code := run(t, "apply", "testdata/external-db-tls-scenario/tls", "--state-file", stateFile, "--auto-approve")
		if err == nil && code == 0 {
			t.Fatalf("apply with the wrong CA should have failed certificate verification:\n%s", out)
		}
		lower := strings.ToLower(out)
		if !strings.Contains(lower, "certificate") && !strings.Contains(lower, "x509") && !strings.Contains(lower, "unknown authority") {
			t.Errorf("error does not appear to name a certificate-verification failure:\n%s", out)
		}
	})

	t.Run("verify-full-succeeds", func(t *testing.T) {
		cleanupInfra()
		t.Cleanup(cleanupInfra)
		t.Setenv("DATASCAPE_SECRET_TLSDB_CONN_USERNAME", "tlsuser")
		t.Setenv("DATASCAPE_SECRET_TLSDB_CONN_PASSWORD", "tlspw")
		t.Setenv("DATASCAPE_SECRET_TLSDB_CA_CA", string(correctCAPEM))

		stateFile := filepath.Join(t.TempDir(), "state.json")
		manifests := "testdata/external-db-tls-scenario/tls"

		start := time.Now()
		out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve")
		if err != nil || code != 0 {
			t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
		}
		t.Logf("apply from empty state took %s", time.Since(start).Round(time.Second))

		out, err, code = run(t, "status", manifests, "--state-file", stateFile)
		if err != nil || code != 0 {
			t.Fatalf("status failed (code %d): %v\n%s", code, err, out)
		}
		for _, line := range strings.Split(strings.TrimSpace(out), "\n")[1:] {
			if !strings.Contains(line, "True") {
				t.Errorf("resource not Ready after apply: %s", line)
			}
		}
		if state := tlsdbConnectorStatus(t, "tlsdb-students-to-events"); state != "RUNNING" {
			t.Errorf("connector state = %q, want RUNNING", state)
		}

		// Idempotent re-apply — the standing per-task bar every other e2e
		// suite in this repo enforces.
		out, err, code = run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve")
		if err != nil || code != 0 {
			t.Fatalf("re-apply failed (code %d): %v\n%s", code, err, out)
		}
		if !strings.Contains(out, "no changes") {
			t.Errorf("re-apply is not a no-op:\n%s", out)
		}
	})
}

func tlsdbConnectorStatus(t *testing.T, name string) string {
	t.Helper()
	var body struct {
		Connector struct {
			State string `json:"state"`
		} `json:"connector"`
	}
	resp, err := http.Get(fmt.Sprintf("%s/connectors/%s/status", tlsdbConnectURL, name))
	if err != nil {
		t.Fatalf("get connector status: %v", err)
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode connector status: %v", err)
	}
	return body.Connector.State
}

// waitTLSDBReady polls the published TLS port until it accepts a raw TCP
// connection — the out-of-band container has no healthcheck (mirrors
// phase5's own out-of-band fixtures), and a bare sleep is a known flake
// source (docs/planning/08 I3/NFR-11); poll instead.
func waitTLSDBReady(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", net.JoinHostPort(tlsdbIP, "5432"), time.Second)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("out-of-band TLS Postgres never accepted a TCP connection within 30s")
}

// generateTLSDBFixtures mints two independent throwaway CAs (correct/wrong)
// plus a server leaf certificate signed by the correct one, covering
// tlsdbIP — the one address both TLS-checking vantage points dial (see
// tlsdbIP's own doc comment). Returns both CA PEMs (the test sets whichever
// one it wants as the SecretReference's env value per case) and the Docker
// build context directory (Dockerfile + certs + pg_hba.conf) for the
// custom postgres image buildImage builds — a plain bind mount cannot
// place these files with the ownership/permissions postgres requires for
// ssl_key_file (must be owned by the postgres server user, world/group-
// unreadable), so they are baked into the image at build time instead
// (root COPY + chown, unlike a bind mount which preserves the host's own
// uid/gid).
func generateTLSDBFixtures(t *testing.T) (correctCAPEM, wrongCAPEM []byte, buildDir string) {
	t.Helper()
	correctCAPEM, correctCACert, correctCAKey := generateThrowawayCA(t)
	wrongCAPEM, _, _ = generateThrowawayCA(t)
	serverCertPEM, serverKeyPEM := generateServerLeaf(t, correctCACert, correctCAKey)

	buildDir = t.TempDir()
	writeFixtureFile(t, buildDir, "server.crt", serverCertPEM)
	writeFixtureFile(t, buildDir, "server.key", serverKeyPEM)
	writeFixtureFile(t, buildDir, "pg_hba.conf", []byte("local all all trust\nhostssl all all all scram-sha-256\n"))
	dockerfile := `FROM postgres:16@sha256:33f923b05f64ca54ac4401c01126a6b92afe839a0aa0a52bc5aeb5cc958e5f20
COPY server.crt server.key pg_hba.conf /certs/
RUN chown postgres:postgres /certs/server.crt /certs/server.key /certs/pg_hba.conf && \
    chmod 600 /certs/server.key && chmod 644 /certs/server.crt /certs/pg_hba.conf
`
	writeFixtureFile(t, buildDir, "Dockerfile", []byte(dockerfile))
	return correctCAPEM, wrongCAPEM, buildDir
}

func writeFixtureFile(t *testing.T, dir, name string, content []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), content, 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

func generateThrowawayCA(t *testing.T) (certPEM []byte, cert *x509.Certificate, key *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: "datascape I2 test CA"},
		NotBefore:             time.Now().Add(-5 * time.Minute),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA certificate: %v", err)
	}
	cert, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA certificate: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), cert, key
}

func generateServerLeaf(t *testing.T, caCert *x509.Certificate, caKey *ecdsa.PrivateKey) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate server key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: "datascape-tlsdb-outofband-pg"},
		// tlsdbIP is the one address both TLS-checking vantage points dial
		// (see its own doc comment) — a single IP SAN covers both.
		IPAddresses: []net.IP{net.ParseIP(tlsdbIP)},
		NotBefore:   time.Now().Add(-5 * time.Minute),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create server certificate: %v", err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal server key: %v", err)
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM
}
