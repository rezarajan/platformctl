package ingress

import (
	"context"
	"strings"
	"testing"
	"time"

	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	"github.com/rezarajan/platformctl/internal/domain/connection"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

func TestTLSServerDispatch(t *testing.T) {
	t.Parallel()
	server, isTLS := tlsServer(connection.Connection{})
	if server != serverName || isTLS {
		t.Errorf("plain connection: server=%q isTLS=%v, want %q/false", server, isTLS, serverName)
	}
	ref := "x"
	server, isTLS = tlsServer(connection.Connection{TLS: &connection.TLS{SecretRef: &ref}})
	if server != httpsServerName || !isTLS {
		t.Errorf("tls connection: server=%q isTLS=%v, want %q/true", server, isTLS, httpsServerName)
	}
}

func TestEnsureLocalCAGeneratesOnceThenReuses(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	rt := fakeruntime.New()

	// No container yet: ReadFile fails, so a fresh CA is generated.
	cert1, key1, err := ensureLocalCA(ctx, rt, "edge")
	if err != nil {
		t.Fatalf("ensureLocalCA (first): %v", err)
	}
	if _, _, err := parseCAKeyPair(cert1, key1); err != nil {
		t.Fatalf("generated CA does not parse: %v", err)
	}

	// Simulate the Provider-level bootstrap placing those exact bytes via
	// ContainerSpec.Files (what reconcileInstanceDocker does after calling
	// ensureLocalCA) — the read-before-regenerate pattern this function
	// implements.
	if _, err := rt.EnsureContainer(ctx, runtime.ContainerSpec{
		Name: "edge",
		Files: []runtime.FileMount{
			{Path: caCertPath, Content: cert1},
			{Path: caKeyPath, Content: key1},
		},
	}); err != nil {
		t.Fatalf("seed container: %v", err)
	}

	cert2, key2, err := ensureLocalCA(ctx, rt, "edge")
	if err != nil {
		t.Fatalf("ensureLocalCA (second): %v", err)
	}
	if string(cert1) != string(cert2) || string(key1) != string(key2) {
		t.Error("ensureLocalCA regenerated a new CA even though a valid one already existed — every apply would force tools to re-trust a new CA")
	}
}

func TestEnsureLocalCARegeneratesWhenExistingFilesAreGarbage(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	rt := fakeruntime.New()
	if _, err := rt.EnsureContainer(ctx, runtime.ContainerSpec{
		Name: "edge",
		Files: []runtime.FileMount{
			{Path: caCertPath, Content: []byte("not a cert")},
			{Path: caKeyPath, Content: []byte("not a key")},
		},
	}); err != nil {
		t.Fatalf("seed container: %v", err)
	}
	cert, key, err := ensureLocalCA(ctx, rt, "edge")
	if err != nil {
		t.Fatalf("ensureLocalCA: %v", err)
	}
	if _, _, err := parseCAKeyPair(cert, key); err != nil {
		t.Fatalf("fallback-generated CA does not parse: %v", err)
	}
}

func TestResolveCertDockerSecretRefValidatesPair(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	rt := fakeruntime.New()
	refName := "nessie-tls"

	caCert, caKey, err := generateCA()
	if err != nil {
		t.Fatalf("generateCA: %v", err)
	}
	leafCert, leafKey, err := generateLeafCert(caCert, caKey, "nessie.localhost")
	if err != nil {
		t.Fatalf("generateLeafCert: %v", err)
	}

	// Matching pair: accepted verbatim.
	req := reconciler.Request{Runtime: rt, Secrets: map[string]map[string]string{
		refName: {"cert": string(leafCert), "key": string(leafKey)},
	}}
	tls := &connection.TLS{SecretRef: &refName}
	gotCert, gotKey, err := resolveCertDocker(ctx, req, "edge", "http://unused", "nessie", "nessie.localhost", tls)
	if err != nil {
		t.Fatalf("resolveCertDocker: %v", err)
	}
	if string(gotCert) != string(leafCert) || string(gotKey) != string(leafKey) {
		t.Error("resolveCertDocker did not pass through the provided secretRef cert/key verbatim")
	}

	// Missing from req.Secrets: the ingress Provider didn't list it in its
	// own spec.secretRefs — clear error naming the fix.
	emptyReq := reconciler.Request{Runtime: rt}
	if _, _, err := resolveCertDocker(ctx, emptyReq, "edge", "http://unused", "nessie", "nessie.localhost", tls); err == nil {
		t.Error("expected error when tls.secretRef is not resolved in req.Secrets")
	} else if !strings.Contains(err.Error(), "spec.secretRefs") {
		t.Errorf("error = %q, want it to name spec.secretRefs as the fix", err.Error())
	}

	// Mismatched cert/key pair: rejected.
	otherCACert, otherCAKey, _ := generateCA()
	_, mismatchedKey, _ := generateLeafCert(otherCACert, otherCAKey, "nessie.localhost")
	badReq := reconciler.Request{Runtime: rt, Secrets: map[string]map[string]string{
		refName: {"cert": string(leafCert), "key": string(mismatchedKey)},
	}}
	if _, _, err := resolveCertDocker(ctx, badReq, "edge", "http://unused", "nessie", "nessie.localhost", tls); err == nil {
		t.Error("expected error for a cert/key that do not form a valid pair")
	}
}

func TestResolveCertDockerSecretNameIsKubernetesOnly(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	rt := fakeruntime.New()
	name := "cert-manager-issued"
	req := reconciler.Request{Runtime: rt}
	tls := &connection.TLS{SecretName: &name}
	if _, _, err := resolveCertDocker(ctx, req, "edge", "http://unused", "nessie", "nessie.localhost", tls); err == nil {
		t.Fatal("expected tls.secretName to be refused on Docker")
	} else if !strings.Contains(err.Error(), "Kubernetes-only") {
		t.Errorf("error = %q, want it to say Kubernetes-only", err.Error())
	}
}

func TestResolveCertDockerSelfSignedRequiresProvisionedCA(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	rt := fakeruntime.New()
	tlsSpec := &connection.TLS{SelfSigned: true}
	req := reconciler.Request{Runtime: rt}
	if _, _, err := resolveCertDocker(ctx, req, "edge", "http://unused", "nessie", "nessie.localhost", tlsSpec); err == nil {
		t.Fatal("expected error when the ingress Provider has not provisioned a CA yet")
	} else if !strings.Contains(err.Error(), "apply the Provider first") {
		t.Errorf("error = %q, want it to suggest applying the Provider first", err.Error())
	}
}

func TestResolveCertDockerSelfSignedGeneratesAndReuses(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	rt := fakeruntime.New()
	caCert, caKey, err := generateCA()
	if err != nil {
		t.Fatalf("generateCA: %v", err)
	}
	if _, err := rt.EnsureContainer(ctx, runtime.ContainerSpec{
		Name: "edge",
		Files: []runtime.FileMount{
			{Path: caCertPath, Content: caCert},
			{Path: caKeyPath, Content: caKey},
		},
	}); err != nil {
		t.Fatalf("seed CA container: %v", err)
	}

	admin := newFakeCaddyAdmin()
	defer admin.Close()

	req := reconciler.Request{Runtime: rt}
	tlsSpec := &connection.TLS{SelfSigned: true}

	// First resolve: no cert loaded yet — generates a fresh leaf cert
	// chaining to the CA.
	cert1, key1, err := resolveCertDocker(ctx, req, "edge", admin.URL, "nessie", "nessie.localhost", tlsSpec)
	if err != nil {
		t.Fatalf("resolveCertDocker (first): %v", err)
	}
	if err := certChainsToCA(cert1, caCert, "nessie.localhost", time.Now()); err != nil {
		t.Fatalf("generated leaf cert does not chain to the CA: %v", err)
	}

	// Load it (what reconcileConnectionDocker does next) then resolve
	// again: must reuse the already-loaded, still-valid cert rather than
	// generating a new one every apply.
	if err := ensureCert(ctx, admin.URL, caddyPEMCert{ID: certID("nessie"), Certificate: string(cert1), Key: string(key1)}); err != nil {
		t.Fatalf("ensureCert: %v", err)
	}
	cert2, key2, err := resolveCertDocker(ctx, req, "edge", admin.URL, "nessie", "nessie.localhost", tlsSpec)
	if err != nil {
		t.Fatalf("resolveCertDocker (second): %v", err)
	}
	if string(cert1) != string(cert2) || string(key1) != string(key2) {
		t.Error("resolveCertDocker regenerated a new leaf cert even though a valid one was already loaded")
	}
}
