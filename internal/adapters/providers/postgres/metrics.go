// Metrics sidecar (docs/planning/08 C9 completion): an opt-in
// (spec.configuration.metrics: enabled) postgres_exporter container,
// mirroring openlineage's Marquez+db two-container shape (docs/adr/004 —
// this is a SECOND, independent container per Provider, never a replica of
// the instance container) rather than modifying the instance's own
// ContainerSpec at all. Absent/disabled leaves reconcileInstance's existing
// EnsureInstance call byte-for-byte unchanged — see metrics_test.go's
// TestInstanceContainerSpecUnaffectedByMetrics.
package postgres

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/rezarajan/platformctl/internal/adapters/providers/providerkit"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

const (
	// exporterImage is pinned (scripts/pinned-images.txt).
	exporterImage = "quay.io/prometheuscommunity/postgres-exporter:v0.17.1@sha256:38606faa38c54787525fb0ff2fd6b41b4cfb75d455c1df294927c5f611699b17"
	exporterPort  = 9187
	// exporterPasswordPath is where the monitoring role's password file is
	// mounted — DATA_SOURCE_PASS_FILE, never env (docs/planning/07 Gate 1
	// checkbox 4). Verified live: postgres_exporter's own file-based
	// credential support (DATA_SOURCE_URI/_USER stay in env — neither is a
	// secret value — DATA_SOURCE_PASS_FILE carries the password).
	exporterPasswordPath = "/run/datascape/monitor-password"
)

func exporterName(name string) string { return name + "-exporter" }

// monitorUsername is the dedicated least-privilege monitoring role's name —
// never the admin/superuser credential (docs/planning/08 C9 completion:
// "a dedicated least-privilege monitoring user created at reconcile").
func monitorUsername(name string) string { return name + "-monitor" }

// metricsEnabled resolves spec.configuration.metrics — the same
// "enabled"|"disabled" string-enum convention redpanda's schemaRegistry
// already established. Any value other than the literal "enabled" (unset,
// "disabled", a typo) leaves the sidecar off — zero behavior change for
// every manifest that predates this field.
func metricsEnabled(cfg provider.Provider) bool {
	v, _ := cfg.Configuration["metrics"].(string)
	return v == "enabled"
}

// randomMonitorPassword generates a fresh monitoring-role password —
// platform-internal, never a user-declared SecretReference, so no rotation
// state machine is needed (docs/planning/08 G1's CredentialRotation exists
// for credentials the platform does NOT control end-to-end; here the admin
// connection can simply (re)set it unconditionally).
func randomMonitorPassword() (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate monitoring password: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// liveMonitorPassword reads back the exporter container's own previously
// mounted password file (mirrors liveSuperuser/liveRootPassword's
// read-back-for-idempotency pattern) so a re-apply reuses the same
// credential instead of rotating it — and recreating the exporter
// container via ContainerSpec.Files' content-participates-in-the-hash rule
// — on every reconcile.
func liveMonitorPassword(ctx context.Context, rt runtime.ContainerRuntime, name string) (string, bool) {
	data, err := rt.ReadFile(ctx, exporterName(name), exporterPasswordPath)
	if err != nil || len(data) == 0 {
		return "", false
	}
	return string(data), true
}

// ensureExporter reconciles the postgres_exporter sidecar: resolves (or
// mints) the monitoring role's password, ensures the SQL role via the
// already-authenticated admin connection (adminAddr/adminUser/adminPass —
// resolved by the caller exactly like ensureSuperuser's own admin
// connection), then ensures the exporter container itself — a second,
// independent EnsureContainer call on the instance's own network, sharing
// none of the main ContainerSpec's fields (ADR 004: a sidecar, not a
// replica). The exporter's port is Audience: internal — no host publish;
// only prometheus (same network) ever needs to reach it. Returns the
// "metrics" endpoint fact to publish alongside the instance's own
// endpoints, or an error.
func ensureExporter(ctx context.Context, rt runtime.ContainerRuntime, namespace, network, name, adminUser, adminPass string) (endpoint.Endpoint, error) {
	expName := exporterName(name)
	monitorUser := monitorUsername(name)
	monitorPass, ok := liveMonitorPassword(ctx, rt, name)
	if !ok {
		p, err := randomMonitorPassword()
		if err != nil {
			return endpoint.Endpoint{}, err
		}
		monitorPass = p
	}

	addr, closeAddr, err := providerkit.ReachableAddr(ctx, rt, name, 5432)
	if err != nil {
		return endpoint.Endpoint{}, err
	}
	adminConn := connStringAddr(addr, adminUser, adminPass, "postgres", nil)
	closeErr := closeAddr()
	if err := ensureMonitoringUser(ctx, adminConn, monitorUser, monitorPass); err != nil {
		return endpoint.Endpoint{}, err
	}
	if closeErr != nil {
		return endpoint.Endpoint{}, closeErr
	}

	// Same top-level Provider name/generation as the instance container
	// (openlineage's two-container precedent, not this exporter's own
	// literal name) — both containers are one logical managed resource for
	// GC/destroy purposes.
	labels := runtime.ManagedLabels(namespace, "Provider", name, name)
	_, err = rt.EnsureContainer(ctx, runtime.ContainerSpec{
		Name:  expName,
		Image: exporterImage,
		Env: map[string]string{
			// Non-secret: host/port/db/sslmode and the username, mirroring
			// the main instance's own POSTGRES_USER-in-env convention.
			"DATA_SOURCE_URI":       fmt.Sprintf("%s:5432/postgres?sslmode=disable", name),
			"DATA_SOURCE_USER":      monitorUser,
			"DATA_SOURCE_PASS_FILE": exporterPasswordPath,
		},
		Files:    []runtime.FileMount{{Path: exporterPasswordPath, Content: []byte(monitorPass)}},
		Networks: []string{network},
		Ports:    []runtime.PortBinding{{ContainerPort: exporterPort, Audience: runtime.AudienceInternal}},
		HealthCheck: &runtime.HealthCheck{
			Test:     []string{"CMD-SHELL", fmt.Sprintf("wget -q --spider http://127.0.0.1:%d/metrics || exit 1", exporterPort)},
			Interval: 2 * time.Second,
			Timeout:  5 * time.Second,
			Retries:  30,
		},
		Labels: labels,
	})
	if err != nil {
		return endpoint.Endpoint{}, err
	}
	if err := rt.WaitHealthy(ctx, expName, 60*time.Second); err != nil {
		return endpoint.Endpoint{}, err
	}
	return endpoint.Endpoint{
		Name: "metrics", Scheme: "http",
		Internal:      fmt.Sprintf("http://%s:%d/metrics", expName, exporterPort),
		Insecure:      true,
		RuntimeName:   expName,
		ContainerPort: exporterPort,
		Audience:      runtime.AudienceInternal,
		Network:       network,
	}, nil
}

// probeExporter reports whether the exporter container is present and
// healthy — the same shape as the instance's own Probe check.
func probeExporter(ctx context.Context, rt runtime.ContainerRuntime, name string) bool {
	ctrState, found, err := rt.Inspect(ctx, exporterName(name))
	return err == nil && found && ctrState.Healthy
}
