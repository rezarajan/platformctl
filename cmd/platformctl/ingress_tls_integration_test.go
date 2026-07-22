//go:build integration

package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

const (
	ingressTLSHTTPSAddr = "127.0.0.1:19722"
	ingressTLSAdminAddr = "127.0.0.1:19723"
)

// --- self-contained CA/cert generation (deliberately independent of the
// ingress provider's own internal tls.go — an integration test proves the
// provider against material it did not itself generate, the same way the
// provided-secretRef accept criterion asks for an operator-supplied cert)

func genTestCA(t *testing.T) (certPEM, keyPEM []byte, cert *x509.Certificate, key *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "ingress-tls-integration-test-ca"},
		NotBefore:             time.Now().Add(-5 * time.Minute),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	cert, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal CA key: %v", err)
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM, cert, key
}

func genTestLeafCert(t *testing.T, caCert *x509.Certificate, caKey *ecdsa.PrivateKey, host string) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: host},
		DNSNames:     []string{host},
		NotBefore:    time.Now().Add(-5 * time.Minute),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create leaf cert for %q: %v", host, err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal leaf key: %v", err)
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM
}

// getThroughEdgeTLS dials the shared ingress proxy's published HTTPS port
// with SNI and the HTTP Host header both set to host, verifying the served
// chain against caPool — exactly the accept criterion's "Go tls.Config
// with the CA pool" mechanism. Dialing the fixed address directly (rather
// than relying on the runner's DNS resolving *.localhost) keeps the test
// deterministic, mirroring getThroughEdge's identical rationale for HTTP.
func getThroughEdgeTLS(t *testing.T, host, path string, caPool *x509.CertPool) *http.Response {
	t.Helper()
	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			DialTLSContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
				d := &tls.Dialer{Config: &tls.Config{RootCAs: caPool, ServerName: host}}
				return d.DialContext(ctx, network, ingressTLSHTTPSAddr)
			},
		},
	}
	req, err := http.NewRequest(http.MethodGet, "https://"+host+path, nil) //nolint:noctx
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Host = host
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET %s (Host/SNI: %s): %v", path, host, err)
	}
	return resp
}

func mangleTLSRouteOutOfBand(t *testing.T, routeID, wrongHost string) {
	t.Helper()
	body := []byte(`{"@id":"` + routeID + `","match":[{"host":["` + wrongHost + `"]}],"handle":[{"handler":"reverse_proxy","upstreams":[{"dial":"nowhere:1"}]}]}`)
	req, err := http.NewRequest(http.MethodPatch, "http://"+ingressTLSAdminAddr+"/id/"+routeID, bytes.NewReader(body)) //nolint:noctx
	if err != nil {
		t.Fatalf("build mangle request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("mangle route %q: %v", routeID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("mangle route %q: HTTP %d: %s", routeID, resp.StatusCode, string(b))
	}
}

type inventoryJSON struct {
	CertificateAuthorities []struct {
		Provider string `json:"provider"`
		CACert   string `json:"caCert"`
	} `json:"certificateAuthorities"`
}

// TestIngressTLSEndToEnd covers docs/planning/08 C8's accept criteria live
// on Docker: a provided-secretRef https endpoint verifies against the
// test's own CA; the self-signed path works and inventory names the CA
// location; the plaintext upstream is unreachable from outside when TLS
// mode is on (its port is not host-published); drift on a mangled TLS
// route heals; idempotent re-apply; clean destroy.
func TestIngressTLSEndToEnd(t *testing.T) {
	// Operator-provided cert+key (option 1): generated against a CA this
	// test holds, so the served chain can be verified against it exactly
	// like a real operator's own PKI would be.
	_, _, caCert, caKey := genTestCA(t)
	leafCertPEM, leafKeyPEM := genTestLeafCert(t, caCert, caKey, "nessie-provided.localhost")
	t.Setenv("DATASCAPE_SECRET_ING_TLS_PROVIDED_CERT_CERT", string(leafCertPEM))
	t.Setenv("DATASCAPE_SECRET_ING_TLS_PROVIDED_CERT_KEY", string(leafKeyPEM))
	testCAPool := x509.NewCertPool()
	testCAPool.AddCert(caCert)

	rt := requireDocker(t)
	ctx := context.Background()
	containers := []string{"ing-tls-nessie", "ing-tls-edge", "ingtls-internal-upstream"}
	cleanup := registerDockerCleanup(t, rt, containers, nil, "datascape-ingtls-net")
	cleanup()

	// The internal-only upstream (accept: "plaintext upstream is NOT
	// reachable from outside when TLS mode is on") — created out-of-band
	// with Audience: internal, so no host port is ever published for it.
	// caddy's own "respond" subcommand is a zero-config static 200
	// responder — the same already-pinned image the ingress provider
	// itself uses, no new pinned-image entry needed.
	if err := rt.EnsureNetwork(ctx, runtime.NetworkSpec{Name: "datascape-ingtls-net"}); err != nil {
		t.Fatalf("ensure network: %v", err)
	}
	upstreamState, err := rt.EnsureContainer(ctx, runtime.ContainerSpec{
		Name:     "ingtls-internal-upstream",
		Image:    "caddy:2.9.1@sha256:748016f285ed8c43a9ce6e3aed6d92d3009d90ca41157950880f40beaf3ff62b",
		Cmd:      []string{"caddy", "respond", "--listen", ":80", "internal-upstream-ok"},
		Networks: []string{"datascape-ingtls-net"},
		Ports:    []runtime.PortBinding{{ContainerPort: 80, Audience: runtime.AudienceInternal}},
		Labels:   runtime.ManagedLabels("default", "helper", "ingtls-internal-upstream", "ingtls-internal-upstream"),
	})
	if err != nil {
		t.Fatalf("create internal-only upstream: %v", err)
	}
	if hostAddr := upstreamState.HostAddr(80); hostAddr != "" {
		t.Fatalf("internal-only upstream unexpectedly has a host-published address: %s", hostAddr)
	}

	stateFile := filepath.Join(t.TempDir(), "state.json")
	manifests := "testdata/ingress-tls-scenario"
	gates := []string{"--feature-gates", "IngressProvider=true,TLSTermination=true"}

	applyArgs := append([]string{"apply", manifests, "--state-file", stateFile, "--auto-approve"}, gates...)
	out, err, code := run(t, applyArgs...)
	if err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}

	// Accept: provided-secretRef https endpoint verifies against the
	// test's own CA (a Go tls.Config with that CA pool, per the accept
	// criterion's own wording).
	resp := getThroughEdgeTLS(t, "nessie-provided.localhost", "/api/v2/config", testCAPool)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET /api/v2/config via nessie-provided.localhost (provided cert): HTTP %d: %s", resp.StatusCode, string(b))
	}
	if resp.TLS == nil || len(resp.TLS.VerifiedChains) == 0 {
		t.Fatal("response has no verified TLS chain — the cert did not actually verify against the test CA")
	}

	// Accept: the self-signed path works — verify against the CA
	// *published in providerState* (via inventory), not a CA the test
	// invented, proving the provider's own self-signed CA is the one
	// actually serving.
	invOut, err, code := run(t, append([]string{"inventory", manifests, "--state-file", stateFile, "-o", "json"}, gates...)...)
	if err != nil || code != 0 {
		t.Fatalf("inventory -o json failed (code %d): %v\n%s", code, err, invOut)
	}
	var inv inventoryJSON
	if err := json.NewDecoder(strings.NewReader(invOut)).Decode(&inv); err != nil {
		t.Fatalf("decode inventory json: %v\n%s", err, invOut)
	}
	if len(inv.CertificateAuthorities) == 0 {
		t.Fatalf("inventory -o json published no certificateAuthorities:\n%s", invOut)
	}
	selfSignedPool := x509.NewCertPool()
	found := false
	for _, ca := range inv.CertificateAuthorities {
		if strings.Contains(ca.Provider, "ing-tls-edge") {
			if !selfSignedPool.AppendCertsFromPEM([]byte(ca.CACert)) {
				t.Fatalf("published CA cert for %s did not parse as PEM", ca.Provider)
			}
			found = true
		}
	}
	if !found {
		t.Fatalf("inventory did not publish a CA for the ing-tls-edge Provider:\n%s", invOut)
	}
	respSS := getThroughEdgeTLS(t, "nessie-selfsigned.localhost", "/api/v2/config", selfSignedPool)
	defer respSS.Body.Close()
	if respSS.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(respSS.Body)
		t.Fatalf("GET /api/v2/config via nessie-selfsigned.localhost (self-signed): HTTP %d: %s", respSS.StatusCode, string(b))
	}

	// Accept: inventory names the CA location in the human-readable path
	// too (never the raw PEM inline there).
	invHuman, err, code := run(t, append([]string{"inventory", manifests, "--state-file", stateFile}, gates...)...)
	if err != nil || code != 0 {
		t.Fatalf("inventory (human) failed (code %d): %v\n%s", code, err, invHuman)
	}
	if !strings.Contains(invHuman, "self-signed CA for") || !strings.Contains(invHuman, "ing-tls-edge") {
		t.Errorf("human-readable inventory does not name the self-signed CA location:\n%s", invHuman)
	}

	// Accept: the plaintext upstream is not reachable from outside when
	// TLS mode is on — reaching it THROUGH the entrypoint (the only route)
	// works, its own container port is not host-published (already
	// asserted above via HostAddr), and a re-inspect after apply confirms
	// apply didn't add one.
	respInternal := getThroughEdgeTLS(t, "internal-upstream.localhost", "/", selfSignedPool)
	defer respInternal.Body.Close()
	if respInternal.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(respInternal.Body)
		t.Fatalf("GET / via internal-upstream.localhost (through the entrypoint): HTTP %d: %s", respInternal.StatusCode, string(b))
	}
	body, _ := io.ReadAll(respInternal.Body)
	if !strings.Contains(string(body), "internal-upstream-ok") {
		t.Errorf("response body %q did not come from the internal-only upstream", string(body))
	}
	reInspected, foundCtr, err := rt.Inspect(ctx, "ingtls-internal-upstream")
	if err != nil || !foundCtr {
		t.Fatalf("re-inspect internal upstream: found=%v err=%v", foundCtr, err)
	}
	if hostAddr := reInspected.HostAddr(80); hostAddr != "" {
		t.Fatalf("internal-only upstream gained a host-published address after apply: %s", hostAddr)
	}

	// Accept: idempotent re-apply — the shared proxy container is never
	// recreated by a no-op re-apply.
	edgeBefore, foundEdge, err := rt.Inspect(ctx, "ing-tls-edge")
	if err != nil || !foundEdge {
		t.Fatalf("Inspect edge before re-apply: found=%v err=%v", foundEdge, err)
	}
	out, err, code = run(t, applyArgs...)
	if err != nil || code != 0 {
		t.Fatalf("re-apply failed (code %d): %v\n%s", code, err, out)
	}
	if !strings.Contains(out, "no changes") {
		t.Errorf("re-apply did not report 'no changes':\n%s", out)
	}
	edgeAfter, foundEdge, err := rt.Inspect(ctx, "ing-tls-edge")
	if err != nil || !foundEdge {
		t.Fatalf("Inspect edge after re-apply: found=%v err=%v", foundEdge, err)
	}
	if edgeAfter.ID != edgeBefore.ID {
		t.Errorf("shared proxy container was recreated on a no-op re-apply (ID %s -> %s) — a per-Connection cert/route change must never restart it", edgeBefore.ID, edgeAfter.ID)
	}

	// Accept: drift on a mangled TLS route heals. Mangle the self-signed
	// Connection's route directly against Caddy's admin API.
	mangleTLSRouteOutOfBand(t, "route-nessie-selfsigned", "mangled-tls.localhost")
	drift, driftCode := runDrift(t, manifests, stateFile, gates...)
	if driftCode == 0 {
		t.Fatalf("drift exit code = 0, want nonzero (drift present):\n%+v", drift)
	}
	ssDrift, ok := drift["Connection/nessie-selfsigned"]
	if !ok {
		t.Fatalf("drift report missing Connection/nessie-selfsigned: %+v", drift)
	}
	if ssDrift.Drift != "True" {
		t.Errorf("Connection/nessie-selfsigned drift = %+v, want Drift=\"True\"", ssDrift)
	}

	out, err, code = run(t, applyArgs...)
	if err != nil || code != 0 {
		t.Fatalf("heal apply failed (code %d): %v\n%s", code, err, out)
	}
	healed := getThroughEdgeTLS(t, "nessie-selfsigned.localhost", "/api/v2/config", selfSignedPool)
	defer healed.Body.Close()
	if healed.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(healed.Body)
		t.Fatalf("nessie-selfsigned.localhost still not routed correctly after heal: HTTP %d: %s", healed.StatusCode, string(b))
	}

	driftAfterHeal, driftCode2 := runDrift(t, manifests, stateFile, gates...)
	if driftCode2 != 0 {
		t.Errorf("drift after heal: exit code %d, want 0 (clean): %+v", driftCode2, driftAfterHeal)
	}

	// Accept: clean destroy.
	destroyArgs := append([]string{"destroy", manifests, "--state-file", stateFile, "--auto-approve"}, gates...)
	out, err, code = run(t, destroyArgs...)
	if err != nil || code != 0 {
		t.Fatalf("destroy failed (code %d): %v\n%s", code, err, out)
	}
	for _, c := range []string{"ing-tls-nessie", "ing-tls-edge"} {
		if _, foundAfter, _ := rt.Inspect(ctx, c); foundAfter {
			t.Errorf("container %q still present after destroy", c)
		}
	}
}
