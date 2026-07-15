// Package mysql reconciles a MySQL or MariaDB container (the provider is
// registered under both types — same protocol, per-type image and binlog
// flags) and provisions databases and replication-capable users from
// SecretReference-sourced credentials (Phase 6.5).
package mysql

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/source"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/domain/versionprofile"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

type Provider struct {
	providerRes resource.Envelope
	cfg         provider.Provider
	secrets     map[string]map[string]string
}

func New() *Provider { return &Provider{} }

func (p *Provider) Type() string { return "mysql" }

func (p *Provider) SetProviderResource(env resource.Envelope) {
	p.providerRes = env
	p.cfg, _ = provider.FromEnvelope(env)
}

func (p *Provider) SetSecrets(secrets map[string]map[string]string) { p.secrets = secrets }

func (p *Provider) containerName() string { return p.providerRes.Metadata.Name }

// mariadb reports whether this Provider resource was declared type: mariadb.
func (p *Provider) mariadb() bool { return p.cfg.Type == "mariadb" }

// mysqlCatalog / mariadbCatalog pin each engine's supported versions. Both
// store data at /var/lib/mysql across these versions; the catalog still
// pins image↔internals so a future version whose datadir moves cannot be run
// with a stale mount.
var mysqlCatalog = versionprofile.Catalog{
	Default: "8.4",
	Profiles: map[string]versionprofile.Profile{
		"8.0": {Version: "8.0", Image: "mysql:8.0", DataMount: "/var/lib/mysql"},
		"8.4": {Version: "8.4", Image: "mysql:8.4", DataMount: "/var/lib/mysql"},
	},
}

var mariadbCatalog = versionprofile.Catalog{
	Default: "11",
	Profiles: map[string]versionprofile.Profile{
		"10.11": {Version: "10.11", Image: "mariadb:10.11", DataMount: "/var/lib/mysql"},
		"11":    {Version: "11", Image: "mariadb:11", DataMount: "/var/lib/mysql"},
	},
}

func (p *Provider) catalog() versionprofile.Catalog {
	if p.mariadb() {
		return mariadbCatalog
	}
	return mysqlCatalog
}

// VersionCatalog implements reconciler.VersionedProvider.
func (p *Provider) VersionCatalog() versionprofile.Catalog { return p.catalog() }

func (p *Provider) profile() (versionprofile.Profile, error) {
	version, _ := p.cfg.Configuration["version"].(string)
	prof, err := p.catalog().Resolve(version)
	if err != nil {
		return prof, err
	}
	if img, _ := p.cfg.Configuration["image"].(string); img != "" {
		prof.Image = img
	}
	return prof, nil
}

func (p *Provider) hostPort() int {
	if v, ok := p.cfg.Configuration["port"]; ok {
		switch n := v.(type) {
		case int:
			return n
		case float64:
			return int(n)
		}
	}
	return 3306
}

func (p *Provider) network() string {
	if n, ok := p.cfg.RuntimeConfig["network"].(string); ok && n != "" {
		return n
	}
	return "datascape"
}

// rootPassword returns the server root password: the "password" key of the
// SecretReference named by configuration.rootSecretRef, or the first
// declared secretRef.
func (p *Provider) rootPassword() (string, error) {
	refName, _ := p.cfg.Configuration["rootSecretRef"].(string)
	if refName == "" && len(p.cfg.SecretRefs) > 0 {
		refName = p.cfg.SecretRefs[0]
	}
	creds, ok := p.secrets[refName]
	if !ok {
		return "", fmt.Errorf("Provider %q (type: %s): no resolved credentials for secretRef %q", p.containerName(), p.cfg.Type, refName)
	}
	if creds["password"] == "" {
		return "", fmt.Errorf("Provider %q: secretRef %q must provide a password key", p.containerName(), refName)
	}
	return creds["password"], nil
}

func (p *Provider) Reconcile(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) (status.Status, error) {
	switch res.Kind {
	case "Provider":
		return p.reconcileInstance(ctx, rt)
	case "Source":
		return p.reconcileSource(ctx, res)
	default:
		return status.Status{}, fmt.Errorf("%s provider cannot reconcile kind %s", p.cfg.Type, res.Kind)
	}
}

func (p *Provider) reconcileInstance(ctx context.Context, rt runtime.ContainerRuntime) (status.Status, error) {
	st := status.Status{}
	name := p.containerName()
	prof, err := p.profile()
	if err != nil {
		return st, fmt.Errorf("Provider %q (type: %s): %w", name, p.cfg.Type, err)
	}
	rootPass, err := p.rootPassword()
	if err != nil {
		return st, err
	}
	labels := map[string]string{
		runtime.LabelManagedBy:  runtime.ManagedByValue,
		runtime.LabelGeneration: name,
	}

	if err := rt.EnsureNetwork(ctx, runtime.NetworkSpec{Name: p.network(), Labels: labels}); err != nil {
		return st, err
	}
	if err := rt.EnsureVolume(ctx, runtime.VolumeSpec{Name: name + "-data", Labels: labels}); err != nil {
		return st, err
	}
	env := map[string]string{"MYSQL_ROOT_PASSWORD": rootPass}
	if p.mariadb() {
		env["MARIADB_ROOT_PASSWORD"] = rootPass
	}
	// MySQL 8.x has row-format binlog on by default; MariaDB needs it asked
	// for. Setting it explicitly on both keeps CDC-readiness uniform.
	cmd := []string{"--log-bin=binlog", "--binlog-format=ROW", "--server-id=1"}
	adminTool := "mysqladmin"
	if p.mariadb() {
		adminTool = "mariadb-admin"
	}
	ctrState, err := rt.EnsureContainer(ctx, runtime.ContainerSpec{
		Name:     name,
		Image:    prof.Image,
		Cmd:      cmd,
		Env:      env,
		Networks: []string{p.network()},
		Volumes:  []runtime.VolumeMount{{VolumeName: name + "-data", MountPath: prof.DataMount}},
		Ports:    []runtime.PortBinding{{HostPort: p.hostPort(), ContainerPort: 3306}},
		HealthCheck: &runtime.HealthCheck{
			// TCP ping, no credentials required for liveness.
			Test:     []string{"CMD-SHELL", adminTool + " ping -h 127.0.0.1 --silent"},
			Interval: 2 * time.Second,
			Timeout:  5 * time.Second,
			Retries:  45,
		},
		Labels: labels,
	})
	if err != nil {
		return st, err
	}
	if err := rt.WaitHealthy(ctx, name, 180*time.Second); err != nil {
		return st, err
	}
	// Health says the server answers; make sure root auth over TCP works
	// before declaring Ready (the images run an init phase like postgres's).
	if err := waitReady(ctx, dsn("127.0.0.1", p.hostPort(), "root", rootPass, ""), 60*time.Second); err != nil {
		return st, err
	}

	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "InstanceHealthy"}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: "ReconcileComplete"}, now)
	st.ProviderState = map[string]any{
		"containerId":  ctrState.ID,
		"hostAddr":     "127.0.0.1:" + strconv.Itoa(p.hostPort()),
		"internalAddr": name + ":3306",
		endpoint.Key: endpoint.List{
			{Name: "mysql", Scheme: "mysql", Host: "127.0.0.1:" + strconv.Itoa(p.hostPort()), Internal: name + ":3306"},
		}.ToState(),
	}
	return st, nil
}

// reconcileSource ensures the declared database exists, a replication-capable
// user is provisioned from configuration.replicationSecretRef, and row-format
// binary logging is active (what Debezium's MySQL/MariaDB connector needs).
func (p *Provider) reconcileSource(ctx context.Context, res resource.Envelope) (status.Status, error) {
	st := status.Status{}
	src, err := source.FromEnvelope(res)
	if err != nil {
		return st, err
	}
	dbName, _ := src.EngineConfig["database"].(string)
	if dbName == "" {
		return st, fmt.Errorf("Source %q: spec.%s.database is required", res.Metadata.Name, src.Engine)
	}
	rootPass, err := p.rootPassword()
	if err != nil {
		return st, err
	}

	admin := dsn("127.0.0.1", p.hostPort(), "root", rootPass, "")
	if err := waitReady(ctx, admin, 30*time.Second); err != nil {
		return st, err
	}
	if err := ensureDatabase(ctx, admin, dbName); err != nil {
		return st, err
	}
	replRefName, _ := p.cfg.Configuration["replicationSecretRef"].(string)
	replUser := ""
	if replRefName != "" {
		creds, ok := p.secrets[replRefName]
		if !ok {
			return st, fmt.Errorf("Source %q: no resolved credentials for replicationSecretRef %q", res.Metadata.Name, replRefName)
		}
		replUser = creds["username"]
		if err := ensureReplicationUser(ctx, admin, creds["username"], creds["password"]); err != nil {
			return st, err
		}
	}
	if err := verifyBinlog(ctx, admin); err != nil {
		return st, err
	}

	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "SourceProvisioned"}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: "ReconcileComplete"}, now)
	st.ProviderState = map[string]any{"database": dbName, "replicationUser": replUser}
	return st, nil
}

func (p *Provider) Destroy(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) error {
	switch res.Kind {
	case "Provider":
		name := p.containerName()
		if err := rt.Remove(ctx, name); err != nil {
			return err
		}
		if err := rt.RemoveVolume(ctx, name+"-data"); err != nil {
			return err
		}
		_ = rt.RemoveNetwork(ctx, p.network())
		return nil
	case "Source":
		// Dropping the database would be data loss beyond the declared
		// contract; instance teardown removes everything anyway.
		return nil
	default:
		return fmt.Errorf("%s provider cannot destroy kind %s", p.cfg.Type, res.Kind)
	}
}

func (p *Provider) Probe(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) (status.Status, error) {
	st := status.Status{}
	now := time.Now()
	switch res.Kind {
	case "Provider":
		ctrState, found, err := rt.Inspect(ctx, p.containerName())
		if err != nil {
			return st, err
		}
		if !found || !ctrState.Healthy {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: "InstanceUnhealthy"}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: "InstanceUnhealthy"}, now)
			return st, nil
		}
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "InstanceHealthy"}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: "NoDrift"}, now)
		return st, nil
	case "Source":
		src, err := source.FromEnvelope(res)
		if err != nil {
			return st, err
		}
		dbName, _ := src.EngineConfig["database"].(string)
		rootPass, err := p.rootPassword()
		if err != nil {
			return st, err
		}
		exists, err := databaseExists(ctx, dsn("127.0.0.1", p.hostPort(), "root", rootPass, ""), dbName)
		if err != nil {
			return st, err
		}
		if !exists {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: "DatabaseMissing"}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: "DatabaseMissing"}, now)
		} else {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "SourceHealthy"}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: "NoDrift"}, now)
		}
		return st, nil
	default:
		return st, fmt.Errorf("%s provider cannot probe kind %s", p.cfg.Type, res.Kind)
	}
}

// ValidateSpec implements SpecValidator: the instance cannot boot without
// root credentials, so their wiring is checked at validate.
func (p *Provider) ValidateSpec(cfg provider.Provider) error {
	if ref, _ := cfg.Configuration["rootSecretRef"].(string); ref != "" {
		if !cfg.HasSecretRef(ref) {
			return fmt.Errorf("configuration.rootSecretRef %q must also be listed in spec.secretRefs for the engine to resolve it", ref)
		}
	} else if len(cfg.SecretRefs) == 0 {
		return fmt.Errorf("spec.secretRefs must name at least one SecretReference (the root credentials; configuration.rootSecretRef selects one explicitly)")
	}
	// ValidateSpec runs on a fresh instance (SetProviderResource not called),
	// so pick the catalog from the passed spec's type, not p.cfg.
	cat := mysqlCatalog
	if cfg.Type == "mariadb" {
		cat = mariadbCatalog
	}
	return cat.ValidateConfig(cfg.Configuration)
}
