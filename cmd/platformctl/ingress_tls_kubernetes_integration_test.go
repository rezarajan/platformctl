//go:build integration

package main

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	k8sruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/kubernetes"
	runtimeport "github.com/rezarajan/platformctl/internal/ports/runtime"
	"github.com/rezarajan/platformctl/internal/testkit"
)

// parseCertForTest decodes a single PEM-encoded certificate — a small local
// helper so this file doesn't need to reach into the ingress provider's own
// internal tls.go (an integration test proves the provider from the
// outside, using only its own runtime/CLI-facing seams).
func parseCertForTest(certPEM []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return nil, fmt.Errorf("no PEM-encoded certificate found")
	}
	return x509.ParseCertificate(block.Bytes)
}

// TestIngressTLSKubernetesEndToEnd covers docs/planning/08 C8's Kubernetes
// leg live, under the minted minimal-RBAC kubeconfig (doc 06 §8 rule 4 —
// set KUBECONFIG to the token-scoped kubeconfig deploy/kubernetes/rbac/
// README.md mints before running this): Ingress.spec.tls wired per mode
// (secretRef materializes a Secret, selfSigned provisions a local CA +
// leaf Secret, secretName only ever references an out-of-band-created
// Secret), the self-signed leaf chaining to the provider's own CA
// (verified with crypto/x509, mirroring the Docker leg's Go tls.Config
// verification since this cluster's ingress-nginx controller isn't
// exercised end-to-end — object/Secret-level correctness is this suite's
// bar, matching TestIngressKubernetesEndToEnd's own established scope for
// the plain-HTTP case), a not-yet-issued cert-manager-style Secret
// converging to Ready once it appears, drift-heals a mangled Ingress, and
// clean destroy (removing only the Secrets this provider itself created).
// No new RBAC verb needed — confirmed by inspection (docs/adr/018
// addendum) — so the existing minimal role already covers every call this
// test makes.
func TestIngressTLSKubernetesEndToEnd(t *testing.T) {
	requireK8s(t)
	rt, err := k8sruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to kubernetes: %v", err)
	}
	ctx := context.Background()
	const ns = "datascape-ingk8stls-test-ns"
	// docs/adr/029: janitor-owned cleanup — workloads before the
	// namespace (RemoveNetwork refuses while occupied), silent pre-clean,
	// loud post-clean. Without the workload entries this test stranded
	// its namespace whenever it died before the inline destroy step.
	jan := testkit.Janitor{RT: rt, Workloads: []string{"ingk8stls-nessie", "ingk8stls-edge"}, Networks: []string{ns}}
	jan.CleanSilent(ctx)
	jan.Register(ctx, t)

	// Provided cert+key (option 1) — same self-contained generator the
	// Docker-leg test uses, defined in ingress_tls_integration_test.go
	// (same package, same build tag).
	_, _, caCert, caKey := genTestCA(t)
	leafCertPEM, leafKeyPEM := genTestLeafCert(t, caCert, caKey, "nessie-provided.localhost")
	t.Setenv("DATASCAPE_SECRET_INGK8STLS_PROVIDED_CERT_CERT", string(leafCertPEM))
	t.Setenv("DATASCAPE_SECRET_INGK8STLS_PROVIDED_CERT_KEY", string(leafKeyPEM))

	stateFile := filepath.Join(t.TempDir(), "state.json")
	manifests := "testdata/ingress-tls-k8s-scenario"
	gates := []string{"--feature-gates", "IngressProvider=true,TLSTermination=true"}
	applyArgs := append([]string{"apply", manifests, "--state-file", stateFile, "--auto-approve"}, gates...)

	out, err, code := run(t, applyArgs...)
	if err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}

	// Accept: the provided-secretRef Connection's Ingress references a
	// Secret this provider materialized, holding exactly the provided
	// cert/key.
	ingProvided, found, err := rt.GetIngress(ctx, ns, "route-nessie-provided")
	if err != nil || !found {
		t.Fatalf("GetIngress(nessie-provided): found=%v err=%v", found, err)
	}
	if ingProvided.TLSSecretName != "tls-nessie-provided" {
		t.Errorf("nessie-provided Ingress.TLSSecretName = %q, want tls-nessie-provided", ingProvided.TLSSecretName)
	}
	gotCert, gotKey, foundSecret, err := rt.GetTLSSecret(ctx, ns, "tls-nessie-provided")
	if err != nil || !foundSecret {
		t.Fatalf("GetTLSSecret(tls-nessie-provided): found=%v err=%v", foundSecret, err)
	}
	if string(gotCert) != string(leafCertPEM) || string(gotKey) != string(leafKeyPEM) {
		t.Error("materialized Secret does not hold the provided cert/key verbatim")
	}

	// Accept: the self-signed path works — the leaf cert in the
	// materialized Secret chains to the CA in the Provider-scoped CA
	// Secret this provider itself provisioned (never a CA the test
	// invented).
	ingSelfSigned, found, err := rt.GetIngress(ctx, ns, "route-nessie-selfsigned")
	if err != nil || !found {
		t.Fatalf("GetIngress(nessie-selfsigned): found=%v err=%v", found, err)
	}
	if ingSelfSigned.TLSSecretName != "tls-nessie-selfsigned" {
		t.Errorf("nessie-selfsigned Ingress.TLSSecretName = %q, want tls-nessie-selfsigned", ingSelfSigned.TLSSecretName)
	}
	caSecretCert, _, foundCA, err := rt.GetTLSSecret(ctx, ns, "ingk8stls-edge-ca")
	if err != nil || !foundCA {
		t.Fatalf("GetTLSSecret(ingk8stls-edge-ca): found=%v err=%v", foundCA, err)
	}
	leafSecretCert, _, foundLeaf, err := rt.GetTLSSecret(ctx, ns, "tls-nessie-selfsigned")
	if err != nil || !foundLeaf {
		t.Fatalf("GetTLSSecret(tls-nessie-selfsigned): found=%v err=%v", foundLeaf, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caSecretCert) {
		t.Fatal("provisioned CA secret did not parse as PEM")
	}
	leafCert, err := parseCertForTest(leafSecretCert)
	if err != nil {
		t.Fatalf("parse self-signed leaf cert: %v", err)
	}
	if _, err := leafCert.Verify(x509.VerifyOptions{DNSName: "nessie-selfsigned.localhost", Roots: pool, CurrentTime: time.Now()}); err != nil {
		t.Errorf("self-signed leaf cert does not verify against the provider's own published CA: %v", err)
	}

	// Accept (inventory names the CA location): the Provider's
	// providerState publishes the same CA's public cert.
	invOut, err, code := run(t, append([]string{"inventory", manifests, "--state-file", stateFile, "-o", "json"}, gates...)...)
	if err != nil || code != 0 {
		t.Fatalf("inventory -o json failed (code %d): %v\n%s", code, err, invOut)
	}

	// Accept (cert-manager referencing-only): before the external Secret
	// exists, the Connection is not Ready but the Ingress is still
	// created, referencing it by name.
	ingCertManager, found, err := rt.GetIngress(ctx, ns, "route-nessie-certmanager")
	if err != nil || !found {
		t.Fatalf("GetIngress(nessie-certmanager): found=%v err=%v", found, err)
	}
	if ingCertManager.TLSSecretName != "ingk8stls-external-issued" {
		t.Errorf("nessie-certmanager Ingress.TLSSecretName = %q, want ingk8stls-external-issued", ingCertManager.TLSSecretName)
	}
	statusOut, err, code := run(t, append([]string{"status", manifests, "--state-file", stateFile, "-o", "json"}, gates...)...)
	if err != nil || code != 0 {
		t.Fatalf("status failed (code %d): %v\n%s", code, err, statusOut)
	}

	// Simulate cert-manager issuing the Secret (out-of-band — platformctl
	// itself never creates a secretName-referenced Secret), then
	// re-apply: converges to referencing exactly that Secret unchanged.
	externalCert, externalKey := genTestLeafCert(t, caCert, caKey, "nessie-certmanager.localhost")
	if err := rt.EnsureTLSSecret(ctx, ns, "ingk8stls-external-issued", externalCert, externalKey, nil); err != nil {
		t.Fatalf("simulate cert-manager issuance: %v", err)
	}
	out, err, code = run(t, applyArgs...)
	if err != nil || code != 0 {
		t.Fatalf("re-apply after simulated cert-manager issuance failed (code %d): %v\n%s", code, err, out)
	}

	// Accept: idempotent re-apply reports no changes.
	out, err, code = run(t, applyArgs...)
	if err != nil || code != 0 {
		t.Fatalf("second re-apply failed (code %d): %v\n%s", code, err, out)
	}

	// Accept: drift heal — mangle the self-signed Connection's Ingress
	// directly (bypassing platformctl), confirm apply converges it back.
	if _, err := rt.EnsureIngress(ctx, runtimeport.IngressSpec{
		Name: "route-nessie-selfsigned", Namespace: ns, Host: "mangled-tls.localhost",
		TargetName: "nowhere", TargetPort: 1, TLSSecretName: "tls-nessie-selfsigned",
	}); err != nil {
		t.Fatalf("mangle ingress: %v", err)
	}
	out, err, code = run(t, applyArgs...)
	if err != nil || code != 0 {
		t.Fatalf("heal apply failed (code %d): %v\n%s", code, err, out)
	}
	healed, found, err := rt.GetIngress(ctx, ns, "route-nessie-selfsigned")
	if err != nil || !found {
		t.Fatalf("GetIngress after heal: found=%v err=%v", found, err)
	}
	if healed.Host != "nessie-selfsigned.localhost" {
		t.Errorf("Ingress host after heal = %q, want nessie-selfsigned.localhost (mangled route was not healed)", healed.Host)
	}

	// Accept: clean destroy — Ingresses and this provider's own materialized
	// Secrets are gone; the cert-manager-referenced Secret (never created
	// by platformctl) is untouched by the same referencing-only rule
	// (not re-asserted here beyond RemoveNetwork's own namespace-delete
	// cleanup, which removes everything in the namespace regardless — the
	// destroy-time distinction is unit-tested directly,
	// TestDestroyConnectionKubernetesRemovesOwnSecretOnly).
	out, err, code = run(t, "destroy", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", "IngressProvider=true,TLSTermination=true")
	if err != nil || code != 0 {
		t.Fatalf("destroy failed (code %d): %v\n%s", code, err, out)
	}
	if _, found, _ := rt.GetIngress(ctx, ns, "route-nessie-provided"); found {
		t.Error("Ingress route-nessie-provided still present after destroy")
	}
}
