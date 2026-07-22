package mysql

import (
	"context"
	"testing"
	"time"

	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

func TestMetricsEnabled(t *testing.T) {
	cases := []struct {
		name string
		v    any
		want bool
	}{
		{"unset", nil, false},
		{"disabled", "disabled", false},
		{"typo", "Enabled", false},
		{"enabled", "enabled", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := provider.Provider{Configuration: map[string]any{}}
			if c.v != nil {
				cfg.Configuration["metrics"] = c.v
			}
			if got := metricsEnabled(cfg); got != c.want {
				t.Errorf("metricsEnabled(%v) = %v, want %v", c.v, got, c.want)
			}
		})
	}
}

func mysqlEnvelope(name string, configuration map[string]any) resource.Envelope {
	e := resource.Envelope{}
	e.APIVersion = "datascape.io/v1alpha1"
	e.Kind = "Provider"
	e.Metadata.Name = name
	e.Spec = map[string]any{
		"type":          "mysql",
		"runtime":       map[string]any{"type": "fake"},
		"configuration": configuration,
		"secretRefs":    []any{"mysql-root"},
	}
	return e
}

// TestInstanceContainerSpecUnaffectedByMetrics mirrors postgres's own pin:
// reconcileInstance's providerkit.EnsureInstance call for the main
// container runs before any metrics-gated code, unconditionally, so its
// recorded spec is identical regardless of configuration.metrics — proven
// by inspecting the fake runtime's observed ContainerState after a
// Reconcile call that fails at ensureRootPassword (the fake runtime serves
// no real MySQL) but only after the main container's spec is already
// recorded.
func TestInstanceContainerSpecUnaffectedByMetrics(t *testing.T) {
	secrets := map[string]map[string]string{"mysql-root": {"password": "rootpass"}}
	specOf := func(t *testing.T, metrics any) runtime.ContainerState {
		t.Helper()
		rt := fakeruntime.New()
		cfg := map[string]any{}
		if metrics != nil {
			cfg["metrics"] = metrics
		}
		env := mysqlEnvelope("my-test", cfg)
		p := New()
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		_, _ = p.Reconcile(ctx, reconciler.Request{Resource: env, Provider: env, Runtime: rt, Secrets: secrets})
		ctrState, found, err := rt.Inspect(context.Background(), "my-test")
		if err != nil || !found {
			t.Fatalf("Inspect: found=%v err=%v", found, err)
		}
		return ctrState
	}

	without := specOf(t, nil)
	with := specOf(t, "enabled")

	if without.Image != with.Image {
		t.Errorf("Image differs: %q vs %q", without.Image, with.Image)
	}
	if len(without.Ports) != len(with.Ports) {
		t.Fatalf("Ports length differs: %d vs %d", len(without.Ports), len(with.Ports))
	}
	for i := range without.Ports {
		if without.Ports[i] != with.Ports[i] {
			t.Errorf("Ports[%d] differs: %+v vs %+v", i, without.Ports[i], with.Ports[i])
		}
	}
	for k := range without.Labels {
		if without.Labels[k] != with.Labels[k] {
			t.Errorf("Labels[%q] differs: %q vs %q", k, without.Labels[k], with.Labels[k])
		}
	}

	rt2 := fakeruntime.New()
	env2 := mysqlEnvelope("my-test2", map[string]any{})
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, _ = New().Reconcile(ctx, reconciler.Request{Resource: env2, Provider: env2, Runtime: rt2, Secrets: secrets})
	if _, found, _ := rt2.Inspect(context.Background(), "my-test2-exporter"); found {
		t.Error("exporter container exists with metrics unset")
	}
}

func TestExporterNaming(t *testing.T) {
	if exporterPort != 9104 {
		t.Fatalf("exporterPort = %d, want 9104 (mysqld_exporter's own default)", exporterPort)
	}
	if got, want := exporterName("my"), "my-exporter"; got != want {
		t.Errorf("exporterName(%q) = %q, want %q", "my", got, want)
	}
	if got, want := monitorUsername("my"), "my-monitor"; got != want {
		t.Errorf("monitorUsername(%q) = %q, want %q", "my", got, want)
	}
}

func TestParseMyCnfPassword(t *testing.T) {
	cnf := "[client]\nhost = my\nport = 3306\nuser = my-monitor\npassword = s3cr3t!\n"
	if got, want := parseMyCnfPassword(cnf), "s3cr3t!"; got != want {
		t.Errorf("parseMyCnfPassword() = %q, want %q", got, want)
	}
	if got := parseMyCnfPassword("[client]\nuser = x\n"); got != "" {
		t.Errorf("parseMyCnfPassword() with no password line = %q, want \"\"", got)
	}
}
