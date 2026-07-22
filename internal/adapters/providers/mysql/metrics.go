// Metrics sidecar (docs/planning/08 C9 completion): an opt-in
// (spec.configuration.metrics: enabled) mysqld_exporter container,
// mirroring openlineage's Marquez+db two-container shape (docs/adr/004 —
// this is a SECOND, independent container per Provider, never a replica of
// the instance container) rather than modifying the instance's own
// ContainerSpec at all. Absent/disabled leaves reconcileInstance's existing
// EnsureInstance call byte-for-byte unchanged.
package mysql

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
	exporterImage = "prom/mysqld-exporter:v0.15.1@sha256:7211a617ec657701ca819aa0ba28e1d5750f5bf2c1391b755cc4a48cc360b0fa"
	exporterPort  = 9104
	// exporterMyCnfPath is where the monitoring role's my.cnf is mounted —
	// mysqld_exporter's own --config.my-cnf file-based credential support
	// (verified live), keeping BOTH the username and password out of env
	// entirely (docs/planning/07 Gate 1 checkbox 4) — a stricter posture
	// than postgres_exporter's DSN-in-env-plus-file-password shape, since
	// mysqld_exporter offers a fully file-based path and there is no
	// reason not to use it.
	exporterMyCnfPath = "/run/datascape/exporter-my.cnf"
)

func exporterName(name string) string { return name + "-exporter" }

// monitorUsername is the dedicated least-privilege monitoring user's
// name — never the root credential (docs/planning/08 C9 completion: "a
// dedicated least-privilege monitoring user created at reconcile").
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

// randomMonitorPassword generates a fresh monitoring-user password —
// platform-internal, never a user-declared SecretReference, so no
// rotation state machine is needed (the root connection can simply
// (re)set it unconditionally).
func randomMonitorPassword() (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate monitoring password: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// liveMonitorPassword reads back the exporter container's own previously
// mounted my.cnf (mirrors liveRootPassword's read-back-for-idempotency
// pattern) so a re-apply reuses the same credential instead of rotating it
// — and recreating the exporter container via ContainerSpec.Files'
// content-participates-in-the-hash rule — on every reconcile. Parses the
// password back out of the [client] stanza this package itself writes.
func liveMonitorPassword(ctx context.Context, rt runtime.ContainerRuntime, name string) (string, bool) {
	data, err := rt.ReadFile(ctx, exporterName(name), exporterMyCnfPath)
	if err != nil || len(data) == 0 {
		return "", false
	}
	pass := parseMyCnfPassword(string(data))
	if pass == "" {
		return "", false
	}
	return pass, true
}

// ensureExporter reconciles the mysqld_exporter sidecar: resolves (or
// mints) the monitoring user's password, ensures the SQL user via the
// already-authenticated root connection (rootPass — resolved by the
// caller exactly like ensureRootPassword's own connection), then ensures
// the exporter container itself — a second, independent EnsureContainer
// call on the instance's own network, sharing none of the main
// ContainerSpec's fields (ADR 004: a sidecar, not a replica). The
// exporter's port is Audience: internal — no host publish; only
// prometheus (same network) ever needs to reach it. Returns the "metrics"
// endpoint fact to publish alongside the instance's own endpoints, or an
// error.
func ensureExporter(ctx context.Context, rt runtime.ContainerRuntime, namespace, network, name, rootPass string) (endpoint.Endpoint, error) {
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

	addr, closeAddr, err := providerkit.ReachableAddr(ctx, rt, name, 3306)
	if err != nil {
		return endpoint.Endpoint{}, err
	}
	adminConn := dsnAddr(addr, "root", rootPass, "")
	closeErr := closeAddr()
	if err := ensureMonitoringUser(ctx, adminConn, monitorUser, monitorPass); err != nil {
		return endpoint.Endpoint{}, err
	}
	if closeErr != nil {
		return endpoint.Endpoint{}, closeErr
	}

	myCnf := fmt.Sprintf("[client]\nhost = %s\nport = 3306\nuser = %s\npassword = %s\n", name, monitorUser, monitorPass)

	// Same top-level Provider name/generation as the instance container
	// (openlineage's two-container precedent, not this exporter's own
	// literal name) — both containers are one logical managed resource for
	// GC/destroy purposes.
	labels := runtime.ManagedLabels(namespace, "Provider", name, name)
	_, err = rt.EnsureContainer(ctx, runtime.ContainerSpec{
		Name:     expName,
		Image:    exporterImage,
		Cmd:      []string{"--config.my-cnf=" + exporterMyCnfPath},
		Files:    []runtime.FileMount{{Path: exporterMyCnfPath, Content: []byte(myCnf)}},
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
