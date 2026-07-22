package grafana

import (
	"context"
	"strings"
	"testing"
	"time"

	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
)

func grafanaEnvelope(name string, configuration map[string]any) resource.Envelope {
	e := resource.Envelope{}
	e.APIVersion = "datascape.io/v1alpha1"
	e.Kind = "Provider"
	e.Metadata.Name = name
	e.Spec = map[string]any{
		"type":          "grafana",
		"runtime":       map[string]any{"type": "fake"},
		"configuration": configuration,
		"secretRefs":    []any{"grafana-admin"},
	}
	return e
}

func TestValidateSpecRequiresAdminSecret(t *testing.T) {
	p := New()
	if err := p.ValidateSpec(provider.Provider{Configuration: map[string]any{}}); err == nil {
		t.Fatal("ValidateSpec accepted a spec with no secretRefs")
	}
	cfg := provider.Provider{Configuration: map[string]any{}, SecretRefs: []string{"grafana-admin"}}
	if err := p.ValidateSpec(cfg); err != nil {
		t.Fatalf("ValidateSpec rejected a valid spec: %v", err)
	}
	cfg2 := provider.Provider{
		Configuration: map[string]any{"adminSecretRef": "missing"},
		SecretRefs:    []string{"grafana-admin"},
	}
	if err := p.ValidateSpec(cfg2); err == nil {
		t.Fatal("ValidateSpec accepted adminSecretRef not listed in secretRefs")
	}
}

// TestDatasourceProvisioningSkippedWithoutPrometheusURL: with
// Request.PrometheusURL empty (unresolved), Reconcile does not write the
// datasource provisioning file — it must never construct a substitute
// address (ADR 015). waitAPIReady itself always fails against the fake
// runtime (no real HTTP server behind the fabricated address), so a short
// context deadline is used and the resulting error is expected — mirroring
// redpanda_test.go's TestReconcileBrokerRegistryEnabledPublishesPort
// pattern — but EnsureInstance's EnsureContainer call (which records
// Files) has already run by then, so the fake runtime's ReadFile lets this
// test verify the datasource file was skipped while the dashboard file
// (which never depends on PrometheusURL) was still written.
func TestDatasourceProvisioningSkippedWithoutPrometheusURL(t *testing.T) {
	rt := fakeruntime.New()
	env := grafanaEnvelope("graf-test", map[string]any{})
	secrets := map[string]map[string]string{"grafana-admin": {"username": "admin", "password": "adminpass"}}
	p := New()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err := p.Reconcile(ctx, reconciler.Request{Resource: env, Provider: env, Runtime: rt, Secrets: secrets})
	if err == nil {
		t.Fatal("want an error: the fake runtime cannot serve /api/health's real HTTP request")
	}
	if _, ferr := rt.ReadFile(context.Background(), "graf-test", datasourceProvisioningPath); ferr == nil {
		t.Error("datasource provisioning file was written despite an unresolved PrometheusURL")
	}
	if _, ferr := rt.ReadFile(context.Background(), "graf-test", dashboardJSONPath); ferr != nil {
		t.Errorf("dashboard JSON file (metrics-independent) was not written: %v", ferr)
	}
}

func TestDatasourceYAMLNeverConstructsAddress(t *testing.T) {
	// datasourceYAML only formats what it's given — it takes no
	// runtime/naming dependency, proving by construction it cannot
	// re-derive an address itself (ADR 015).
	yaml := string(datasourceYAML("http://prom-test:9090"))
	if !strings.Contains(yaml, "http://prom-test:9090") {
		t.Errorf("datasourceYAML output missing the given URL: %s", yaml)
	}
	if !strings.Contains(yaml, "uid: "+datasourceUID) {
		t.Errorf("datasourceYAML output missing pinned uid: %s", yaml)
	}
}

func TestStarterDashboardHasPinnedUID(t *testing.T) {
	if !strings.Contains(string(starterDashboardJSON), `"uid": "`+dashboardUID+`"`) {
		t.Errorf("starterDashboardJSON missing pinned uid %q", dashboardUID)
	}
}
