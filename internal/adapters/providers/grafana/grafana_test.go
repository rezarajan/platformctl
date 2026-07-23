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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
	if !strings.Contains(string(starterDashboardJSON), `"uid": "`+dashboardUID+`"`) {
		t.Errorf("starterDashboardJSON missing pinned uid %q", dashboardUID)
	}
}

// TestResetAdminPasswordCmdArgs pins the exact grafana-cli invocation I14
// execs to rotate the live admin password — verified live against the
// pinned image (docs/planning/08 I14): no old password argument, exactly
// this argv, so a review of the command construction alone (never a running
// container) is enough to confirm the vendor-documented, no-old-password-
// needed mechanism is what actually runs.
func TestResetAdminPasswordCmdArgs(t *testing.T) {
	got := resetAdminPasswordCmd("new-secret-pw")
	want := []string{"grafana-cli", "admin", "reset-admin-password", "new-secret-pw"}
	if len(got) != len(want) {
		t.Fatalf("resetAdminPasswordCmd() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("resetAdminPasswordCmd() = %v, want %v", got, want)
		}
	}
}

// TestAdminCredentialChanged covers I14's rotation-detection branch: a
// rotation is only attempted when a previously observed live credential
// exists AND differs from what's currently declared — a fresh instance or
// an unchanged SecretReference must never trigger a live grafana-cli exec.
func TestAdminCredentialChanged(t *testing.T) {
	cases := []struct {
		name                     string
		hadPrevious              bool
		prevUser, prevPass       string
		desiredUser, desiredPass string
		want                     bool
	}{
		{"fresh instance: no previous credential observed", false, "", "", "admin", "newpass", false},
		{"unchanged: previous equals desired", true, "admin", "samepass", "admin", "samepass", false},
		{"password rotated", true, "admin", "oldpass", "admin", "newpass", true},
		{"username changed only", true, "olduser", "samepass", "newuser", "samepass", true},
		{"both changed", true, "olduser", "oldpass", "newuser", "newpass", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := adminCredentialChanged(tc.hadPrevious, tc.prevUser, tc.prevPass, tc.desiredUser, tc.desiredPass)
			if got != tc.want {
				t.Errorf("adminCredentialChanged(%v, %q, %q, %q, %q) = %v, want %v",
					tc.hadPrevious, tc.prevUser, tc.prevPass, tc.desiredUser, tc.desiredPass, got, tc.want)
			}
		})
	}
}

// TestLiveAdminCredentialFreshInstance: against a fake runtime with no such
// container yet, liveAdminCredential reports ok=false — the
// NoPreviousOrUnchanged path ensureAdminCredential takes for a first-ever
// apply, never attempting rotation against an instance that never had a
// prior credential to rotate away from.
func TestLiveAdminCredentialFreshInstance(t *testing.T) {
	rt := fakeruntime.New()
	_, _, ok := liveAdminCredential(context.Background(), rt, "graf-does-not-exist")
	if ok {
		t.Fatal("liveAdminCredential reported ok=true for a container that was never created")
	}
}
