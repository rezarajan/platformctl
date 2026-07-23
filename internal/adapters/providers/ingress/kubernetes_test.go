package ingress

import (
	"context"
	"strings"
	"testing"
	"time"

	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// fakeIngressRuntime is a local test double implementing
// runtime.IngressCapableRuntime over the fake ContainerRuntime — this
// package is an adapter (internal/adapters/providers/ingress), not
// internal/application, so CLAUDE.md's narrower test-double allowlist
// (fake/localfile/env/noop only) doesn't apply here; a package-local fake
// of the exact capability under test is the normal pattern (mirrors
// registry_test.go's ingressCapableFake, one layer down).
type fakeIngressRuntime struct {
	*fakeruntime.Runtime
	ingresses map[string]runtime.IngressSpec
	secrets   map[string][2][]byte // namespace/name -> [cert, key]
}

func newFakeIngressRuntime() *fakeIngressRuntime {
	return &fakeIngressRuntime{
		Runtime:   fakeruntime.New(),
		ingresses: map[string]runtime.IngressSpec{},
		secrets:   map[string][2][]byte{},
	}
}

func (f *fakeIngressRuntime) EnsureIngress(_ context.Context, spec runtime.IngressSpec) (runtime.IngressState, error) {
	f.ingresses[spec.Namespace+"/"+spec.Name] = spec
	return runtime.IngressState{Host: spec.Host, TargetName: spec.TargetName, TargetPort: spec.TargetPort, TLSSecretName: spec.TLSSecretName, Address: "127.0.0.1"}, nil
}

func (f *fakeIngressRuntime) GetIngress(_ context.Context, namespace, name string) (runtime.IngressState, bool, error) {
	spec, ok := f.ingresses[namespace+"/"+name]
	if !ok {
		return runtime.IngressState{}, false, nil
	}
	return runtime.IngressState{Host: spec.Host, TargetName: spec.TargetName, TargetPort: spec.TargetPort, TLSSecretName: spec.TLSSecretName, Address: "127.0.0.1"}, true, nil
}

func (f *fakeIngressRuntime) RemoveIngress(_ context.Context, namespace, name string) error {
	delete(f.ingresses, namespace+"/"+name)
	return nil
}

func (f *fakeIngressRuntime) EnsureTLSSecret(_ context.Context, namespace, name string, cert, key []byte, _ map[string]string) error {
	f.secrets[namespace+"/"+name] = [2][]byte{cert, key}
	return nil
}

func (f *fakeIngressRuntime) GetTLSSecret(_ context.Context, namespace, name string) ([]byte, []byte, bool, error) {
	v, ok := f.secrets[namespace+"/"+name]
	if !ok {
		return nil, nil, false, nil
	}
	return v[0], v[1], true, nil
}

func (f *fakeIngressRuntime) RemoveTLSSecret(_ context.Context, namespace, name string) error {
	delete(f.secrets, namespace+"/"+name)
	return nil
}

func kubernetesConnEnvelope(name string, spec map[string]any) resource.Envelope {
	e := resource.Envelope{}
	e.APIVersion = "datascape.io/v1alpha1"
	e.Kind = "Connection"
	e.Metadata.Name = name
	e.Spec = spec
	return e
}

func kubernetesProviderEnvelope(name string) resource.Envelope {
	e := resource.Envelope{}
	e.APIVersion = "datascape.io/v1alpha1"
	e.Kind = "Provider"
	e.Metadata.Name = name
	e.Spec = map[string]any{"type": "ingress", "runtime": map[string]any{"type": "kubernetes", "network": "datascape"}}
	return e
}

func TestReconcileConnectionKubernetesSelfSigned(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	frt := newFakeIngressRuntime()
	provEnv := kubernetesProviderEnvelope("edge-http")
	connEnv := kubernetesConnEnvelope("nessie", map[string]any{
		"providerRef": map[string]any{"name": "edge-http"},
		"scheme":      "https",
		"port":        float64(443),
		"target":      "nessie:19120",
		"tls":         map[string]any{"selfSigned": true},
	})
	req := reconciler.Request{Runtime: frt, Provider: provEnv, Resource: connEnv}

	st, err := reconcileConnectionKubernetes(ctx, req, providerCfg(t, provEnv))
	if err != nil {
		t.Fatalf("reconcileConnectionKubernetes: %v", err)
	}
	if !st.IsReady() {
		c, _ := st.Condition(status.Ready)
		t.Fatalf("expected Ready, got %+v", c)
	}
	ing, ok := frt.ingresses["datascape/route-nessie"]
	if !ok {
		t.Fatal("Ingress was not created")
	}
	if ing.TLSSecretName != "tls-nessie" {
		t.Errorf("Ingress.TLSSecretName = %q, want tls-nessie", ing.TLSSecretName)
	}
	if _, _, found, _ := frt.GetTLSSecret(ctx, "datascape", "edge-http-ca"); !found {
		t.Error("local CA secret was not provisioned")
	}
	leafCert, _, found, _ := frt.GetTLSSecret(ctx, "datascape", "tls-nessie")
	if !found {
		t.Fatal("leaf cert secret was not created")
	}
	caCert, _, _, _ := frt.GetTLSSecret(ctx, "datascape", "edge-http-ca")
	if err := certChainsToCA(leafCert, caCert, "nessie.localhost", time.Now()); err != nil {
		t.Errorf("leaf cert does not chain to the provisioned CA: %v", err)
	}

	// Re-reconcile: the leaf cert must be reused verbatim, not regenerated.
	st2, err := reconcileConnectionKubernetes(ctx, req, providerCfg(t, provEnv))
	if err != nil {
		t.Fatalf("re-reconcile: %v", err)
	}
	if !st2.IsReady() {
		t.Fatal("re-reconcile should stay Ready")
	}
	leafCert2, _, _, _ := frt.GetTLSSecret(ctx, "datascape", "tls-nessie")
	if string(leafCert) != string(leafCert2) {
		t.Error("leaf cert was regenerated on re-reconcile even though the existing one was still valid")
	}
}

func TestReconcileConnectionKubernetesSecretRef(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	frt := newFakeIngressRuntime()
	provEnv := kubernetesProviderEnvelope("edge-http")
	connEnv := kubernetesConnEnvelope("nessie", map[string]any{
		"providerRef": map[string]any{"name": "edge-http"},
		"scheme":      "https",
		"port":        float64(443),
		"target":      "nessie:19120",
		"tls":         map[string]any{"secretRef": map[string]any{"name": "nessie-tls"}},
	})
	caCert, caKey, _ := generateCA()
	leafCert, leafKey, _ := generateLeafCert(caCert, caKey, "nessie.localhost")
	req := reconciler.Request{
		Runtime:  frt,
		Provider: provEnv,
		Resource: connEnv,
		Secrets:  map[string]map[string]string{"nessie-tls": {"cert": string(leafCert), "key": string(leafKey)}},
	}
	st, err := reconcileConnectionKubernetes(ctx, req, providerCfg(t, provEnv))
	if err != nil {
		t.Fatalf("reconcileConnectionKubernetes: %v", err)
	}
	if !st.IsReady() {
		c, _ := st.Condition(status.Ready)
		t.Fatalf("expected Ready, got %+v", c)
	}
	gotCert, gotKey, found, _ := frt.GetTLSSecret(ctx, "datascape", "tls-nessie")
	if !found || string(gotCert) != string(leafCert) || string(gotKey) != string(leafKey) {
		t.Error("provided secretRef cert/key were not materialized verbatim into the tls-nessie Secret")
	}
}

func TestReconcileConnectionKubernetesSecretRefMissingFromRequestSecrets(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	frt := newFakeIngressRuntime()
	provEnv := kubernetesProviderEnvelope("edge-http")
	connEnv := kubernetesConnEnvelope("nessie", map[string]any{
		"providerRef": map[string]any{"name": "edge-http"},
		"scheme":      "https",
		"port":        float64(443),
		"target":      "nessie:19120",
		"tls":         map[string]any{"secretRef": map[string]any{"name": "nessie-tls"}},
	})
	req := reconciler.Request{Runtime: frt, Provider: provEnv, Resource: connEnv}
	st, err := reconcileConnectionKubernetes(ctx, req, providerCfg(t, provEnv))
	if err != nil {
		t.Fatalf("reconcileConnectionKubernetes should not hard-error: %v", err)
	}
	if st.IsReady() {
		t.Fatal("expected Ready: false when tls.secretRef has no resolved credentials")
	}
	c, ok := st.Condition(status.Ready)
	if !ok || !strings.Contains(c.Message, "spec.secretRefs") {
		t.Errorf("condition = %+v, want a message pointing at spec.secretRefs", c)
	}
}

func TestReconcileConnectionKubernetesSecretNameNotYetIssued(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	frt := newFakeIngressRuntime()
	provEnv := kubernetesProviderEnvelope("edge-http")
	connEnv := kubernetesConnEnvelope("nessie", map[string]any{
		"providerRef": map[string]any{"name": "edge-http"},
		"scheme":      "https",
		"port":        float64(443),
		"target":      "nessie:19120",
		"tls":         map[string]any{"secretName": "cert-manager-issued"},
	})
	req := reconciler.Request{Runtime: frt, Provider: provEnv, Resource: connEnv}
	st, err := reconcileConnectionKubernetes(ctx, req, providerCfg(t, provEnv))
	if err != nil {
		t.Fatalf("reconcileConnectionKubernetes: %v", err)
	}
	if st.IsReady() {
		t.Fatal("expected Ready: false while the cert-manager Secret has not been issued yet")
	}
	// The Ingress is still created, referencing the not-yet-existing
	// Secret — cert-manager-style flows commonly issue only after seeing
	// the reference.
	ing, ok := frt.ingresses["datascape/route-nessie"]
	if !ok || ing.TLSSecretName != "cert-manager-issued" {
		t.Errorf("Ingress = %+v, want it to reference cert-manager-issued even before issuance", ing)
	}

	// Once the Secret appears (simulating cert-manager), re-reconcile
	// converges to Ready — platformctl never created it.
	caCert, caKey, _ := generateCA()
	leafCert, leafKey, _ := generateLeafCert(caCert, caKey, "nessie.localhost")
	if err := frt.EnsureTLSSecret(ctx, "datascape", "cert-manager-issued", leafCert, leafKey, nil); err != nil {
		t.Fatalf("simulate cert-manager issuance: %v", err)
	}
	st2, err := reconcileConnectionKubernetes(ctx, req, providerCfg(t, provEnv))
	if err != nil {
		t.Fatalf("re-reconcile: %v", err)
	}
	if !st2.IsReady() {
		c, _ := st2.Condition(status.Ready)
		t.Fatalf("expected Ready once the cert-manager Secret exists, got %+v", c)
	}
}

func TestDestroyConnectionKubernetesRemovesOwnSecretOnly(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	frt := newFakeIngressRuntime()
	provEnv := kubernetesProviderEnvelope("edge-http")
	connEnv := kubernetesConnEnvelope("nessie", map[string]any{
		"providerRef": map[string]any{"name": "edge-http"},
		"scheme":      "https",
		"port":        float64(443),
		"target":      "nessie:19120",
		"tls":         map[string]any{"selfSigned": true},
	})
	req := reconciler.Request{Runtime: frt, Provider: provEnv, Resource: connEnv}
	if _, err := reconcileConnectionKubernetes(ctx, req, providerCfg(t, provEnv)); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if _, _, found, _ := frt.GetTLSSecret(ctx, "datascape", "tls-nessie"); !found {
		t.Fatal("precondition: leaf secret should exist before destroy")
	}
	if err := destroyConnectionKubernetes(ctx, req, providerCfg(t, provEnv)); err != nil {
		t.Fatalf("destroy: %v", err)
	}
	if _, _, found, _ := frt.GetTLSSecret(ctx, "datascape", "tls-nessie"); found {
		t.Error("leaf secret still present after destroy")
	}
	// The CA secret is Provider-scoped, not Connection-scoped — destroying
	// one Connection must never remove it (another self-signed Connection
	// on the same Provider still needs it).
	if _, _, found, _ := frt.GetTLSSecret(ctx, "datascape", "edge-http-ca"); !found {
		t.Error("destroying a Connection must not remove the Provider-scoped CA secret")
	}
}

func TestProbeConnectionKubernetesDetectsCertDrift(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	frt := newFakeIngressRuntime()
	provEnv := kubernetesProviderEnvelope("edge-http")
	connEnv := kubernetesConnEnvelope("nessie", map[string]any{
		"providerRef": map[string]any{"name": "edge-http"},
		"scheme":      "https",
		"port":        float64(443),
		"target":      "nessie:19120",
		"tls":         map[string]any{"selfSigned": true},
	})
	req := reconciler.Request{Runtime: frt, Provider: provEnv, Resource: connEnv}
	if _, err := reconcileConnectionKubernetes(ctx, req, providerCfg(t, provEnv)); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	st, err := probeConnectionKubernetes(ctx, req, providerCfg(t, provEnv))
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if !st.IsReady() {
		c, _ := st.Condition(status.Ready)
		t.Fatalf("expected Ready right after reconcile, got %+v", c)
	}

	// Hand-edit the leaf secret out-of-band with a cert signed by an
	// unrelated CA — the drift-detection case.
	otherCA, otherKey, _ := generateCA()
	mangledCert, mangledKey, _ := generateLeafCert(otherCA, otherKey, "nessie.localhost")
	if err := frt.EnsureTLSSecret(ctx, "datascape", "tls-nessie", mangledCert, mangledKey, nil); err != nil {
		t.Fatalf("mangle secret: %v", err)
	}
	st2, err := probeConnectionKubernetes(ctx, req, providerCfg(t, provEnv))
	if err != nil {
		t.Fatalf("probe after mangle: %v", err)
	}
	if st2.IsReady() {
		t.Fatal("expected Ready: false after the loaded cert stopped chaining to the Provider's CA")
	}
	c, ok := st2.Condition(status.Ready)
	if !ok || c.Reason != status.ReasonCertInvalid {
		t.Errorf("condition = %+v, want Reason=%s", c, status.ReasonCertInvalid)
	}
}

func TestProbeConnectionKubernetesCertMissing(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	frt := newFakeIngressRuntime()
	provEnv := kubernetesProviderEnvelope("edge-http")
	connEnv := kubernetesConnEnvelope("nessie", map[string]any{
		"providerRef": map[string]any{"name": "edge-http"},
		"scheme":      "https",
		"port":        float64(443),
		"target":      "nessie:19120",
		"tls":         map[string]any{"selfSigned": true},
	})
	req := reconciler.Request{Runtime: frt, Provider: provEnv, Resource: connEnv}
	if _, err := reconcileConnectionKubernetes(ctx, req, providerCfg(t, provEnv)); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := frt.RemoveTLSSecret(ctx, "datascape", "tls-nessie"); err != nil {
		t.Fatalf("remove secret: %v", err)
	}
	st, err := probeConnectionKubernetes(ctx, req, providerCfg(t, provEnv))
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	c, ok := st.Condition(status.Ready)
	if st.IsReady() || !ok || c.Reason != status.ReasonCertMissing {
		t.Errorf("condition = %+v (ready=%v), want Ready:false Reason=%s", c, st.IsReady(), status.ReasonCertMissing)
	}
}

// providerCfg parses the standing provider.Provider config this package's
// Docker/Kubernetes functions all take alongside reconciler.Request —
// mirrors what ingress.go's own dispatch (reconcileConnection etc.) does
// via provider.FromEnvelope before calling into the per-runtime file.
func providerCfg(t *testing.T, provEnv resource.Envelope) provider.Provider {
	t.Helper()
	cfg, err := provider.FromEnvelope(provEnv)
	if err != nil {
		t.Fatalf("parse provider config: %v", err)
	}
	return cfg
}
