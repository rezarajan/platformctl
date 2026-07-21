// Package mysql reconciles a MySQL or MariaDB container (the provider is
// registered under both types — same protocol, per-type image and binlog
// flags) and provisions databases and replication-capable users from
// SecretReference-sourced credentials (Phase 6.5).
package mysql

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/rezarajan/platformctl/internal/adapters/providers/providerkit"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/source"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/domain/versionprofile"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// Provider holds no cross-call state (docs/planning/08 F5): every method
// receives what it needs via reconciler.Request.
type Provider struct{}

func New() *Provider { return &Provider{} }

func (p *Provider) Type() string { return "mysql" }

// mariadb reports whether this Provider resource was declared type: mariadb.
func mariadb(cfg provider.Provider) bool { return cfg.Type == "mariadb" }

// mysqlCatalog / mariadbCatalog pin each engine's supported versions. Both
// store data at /var/lib/mysql across these versions; the catalog still
// pins image↔internals so a future version whose datadir moves cannot be run
// with a stale mount.
var mysqlCatalog = versionprofile.Catalog{
	Default: "8.4",
	Profiles: map[string]versionprofile.Profile{
		"8.0": {Version: "8.0", Image: "mysql:8.0@sha256:7dcddc01f13bab2f15cde676d44d01f61fc9f99fe7785e86196dfc07d358ae2b", DataMount: "/var/lib/mysql"},
		"8.4": {Version: "8.4", Image: "mysql:8.4@sha256:c592c15aaf4a1961e15d82eb31ea5987dda862d1c4b1e93424438c0e91dc1f8d", DataMount: "/var/lib/mysql"},
	},
}

var mariadbCatalog = versionprofile.Catalog{
	Default: "11",
	Profiles: map[string]versionprofile.Profile{
		"10.11": {Version: "10.11", Image: "mariadb:10.11@sha256:be981e4113326ada8d6004174dd09eeaefc03094037f811182a52d4f2e737350", DataMount: "/var/lib/mysql"},
		"11":    {Version: "11", Image: "mariadb:11@sha256:efb4959ef2c835cd735dbc388eb9ad6aab0c78dd64febcd51bc17481111890c4", DataMount: "/var/lib/mysql"},
	},
}

func catalogFor(cfg provider.Provider) versionprofile.Catalog {
	if mariadb(cfg) {
		return mariadbCatalog
	}
	return mysqlCatalog
}

// VersionCatalog implements reconciler.VersionedProvider.
func (p *Provider) VersionCatalog(cfg provider.Provider) versionprofile.Catalog {
	return catalogFor(cfg)
}

func profile(cfg provider.Provider) (versionprofile.Profile, error) {
	version, _ := cfg.Configuration["version"].(string)
	prof, err := catalogFor(cfg).Resolve(version)
	if err != nil {
		return prof, err
	}
	if img, _ := cfg.Configuration["image"].(string); img != "" {
		prof.Image = img
	}
	return prof, nil
}

// rootPassword returns the server root password: the "password" key of the
// SecretReference named by configuration.rootSecretRef, or the first
// declared secretRef.
func rootPassword(cfg provider.Provider, secrets map[string]map[string]string, name string) (string, error) {
	creds, refName, err := providerkit.ResolveCredential(cfg, secrets, "rootSecretRef", name)
	if err != nil {
		return "", err
	}
	if creds["password"] == "" {
		return "", fmt.Errorf("Provider %q: secretRef %q must provide a password key", name, refName)
	}
	return creds["password"], nil
}

func (p *Provider) Reconcile(ctx context.Context, req reconciler.Request) (status.Status, error) {
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return status.Status{}, err
	}
	switch req.Resource.Kind {
	case "Provider":
		return p.reconcileInstance(ctx, req)
	case "Source":
		return p.reconcileSource(ctx, req)
	default:
		return status.Status{}, fmt.Errorf("%s provider cannot reconcile kind %s", cfg.Type, req.Resource.Kind)
	}
}

func (p *Provider) reconcileInstance(ctx context.Context, req reconciler.Request) (status.Status, error) {
	rt := req.Runtime
	st := status.Status{}
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return st, err
	}
	name := naming.RuntimeObjectName(req.Provider)
	prof, err := profile(cfg)
	if err != nil {
		return st, fmt.Errorf("Provider %q (type: %s): %w", name, cfg.Type, err)
	}
	rootPass, err := rootPassword(cfg, req.Secrets, name)
	if err != nil {
		return st, err
	}
	oldRootPass, _ := liveRootPassword(ctx, rt, name)

	// The password rides a file mount, not env — env is readable by anyone
	// with `docker inspect` access (docs/planning/07 Gate 1 checkbox 4);
	// both official images consume *_FILE natively.
	env := map[string]string{"MYSQL_ROOT_PASSWORD_FILE": rootPasswordPath}
	if mariadb(cfg) {
		env["MARIADB_ROOT_PASSWORD_FILE"] = rootPasswordPath
	}
	files := []runtime.FileMount{{Path: rootPasswordPath, Content: []byte(rootPass)}}
	// MySQL 8.x has row-format binlog on by default; MariaDB needs it asked
	// for. Setting it explicitly on both keeps CDC-readiness uniform.
	cmd := []string{"--log-bin=binlog", "--binlog-format=ROW", "--server-id=1"}
	adminTool := "mysqladmin"
	if mariadb(cfg) {
		adminTool = "mariadb-admin"
	}
	ctrState, err := providerkit.EnsureInstance(ctx, rt, providerkit.InstanceSpec{
		Namespace: req.Provider.Metadata.Namespace,
		Name:      name,
		Network:   providerkit.Network(cfg),
		Volume:    &providerkit.InstanceVolume{Name: name + "-data", MountPath: prof.DataMount},
		Container: runtime.ContainerSpec{
			Image: prof.Image,
			Cmd:   cmd,
			Env:   env,
			Files: files,
			Ports: []runtime.PortBinding{{HostPort: providerkit.HostPort(cfg, name, "port"), ContainerPort: 3306, Audience: runtime.AudienceHost}},
			HealthCheck: &runtime.HealthCheck{
				// TCP ping, no credentials required for liveness.
				Test:     []string{"CMD-SHELL", adminTool + " ping -h 127.0.0.1 --silent"},
				Interval: 2 * time.Second,
				Timeout:  5 * time.Second,
				Retries:  45,
			},
		},
		WaitTimeout: 180 * time.Second,
	})
	if err != nil {
		return st, err
	}
	// Health says the server answers; make sure root auth over TCP works
	// before declaring Ready (the images run an init phase like postgres's).
	if err := ensureRootPassword(ctx, rt, name, rootPass, oldRootPass); err != nil {
		return st, err
	}

	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonInstanceHealthy}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	hostAddr := ctrState.HostAddr(3306) // observed binding, not intent
	st.ProviderState = map[string]any{
		"containerId":  ctrState.ID,
		"hostAddr":     hostAddr,
		"internalAddr": name + ":3306",
		endpoint.Key: endpoint.List{
			{Name: "mysql", Scheme: "mysql", Host: hostAddr, Internal: name + ":3306", Insecure: true},
		}.ToState(),
	}
	return st, nil
}

// rootPasswordPath is where the bootstrap password file is mounted.
const rootPasswordPath = "/run/datascape/root-password"

func liveRootPassword(ctx context.Context, rt runtime.ContainerRuntime, name string) (string, bool) {
	ctr, found, err := rt.Inspect(ctx, name)
	if err != nil || !found {
		return "", false
	}
	if data, err := rt.ReadFile(ctx, name, rootPasswordPath); err == nil && len(data) > 0 {
		return string(data), true
	}
	// Containers created before the file-mount change carried the password
	// in env; keep rotation working across the upgrade.
	if pass := ctr.Env["MYSQL_ROOT_PASSWORD"]; pass != "" {
		return pass, true
	}
	if pass := ctr.Env["MARIADB_ROOT_PASSWORD"]; pass != "" {
		return pass, true
	}
	return "", false
}

// ensureRootPassword runs providerkit's try-desired → try-previous-bootstrap
// → rotate-live → retry state machine (docs/planning/08 G1) with mysql's
// database/sql-backed ping and ALTER USER rotation as the callbacks.
func ensureRootPassword(ctx context.Context, rt runtime.ContainerRuntime, name, desiredPass, previousPass string) error {
	return providerkit.CredentialRotation{
		Runtime:               rt,
		Name:                  name,
		Port:                  3306,
		NoPreviousOrUnchanged: previousPass == "" || previousPass == desiredPass,
		PingDesired: func(ctx context.Context, addr string) error {
			return ping(ctx, dsnAddr(addr, "root", desiredPass, ""))
		},
		PingPrevious: func(ctx context.Context, addr string) error {
			return ping(ctx, dsnAddr(addr, "root", previousPass, ""))
		},
		Rotate: func(ctx context.Context, addr string) error {
			return rotateRootPassword(ctx, dsnAddr(addr, "root", previousPass, ""), desiredPass)
		},
		Exhausted: func(err error) error {
			return fmt.Errorf("mysql root credentials changed but neither the desired SecretReference nor the previous managed-container environment password can authenticate; manual recovery is required: %w", err)
		},
	}.Run(ctx)
}

// reconcileSource ensures the declared database exists, a replication-capable
// user is provisioned from configuration.replicationSecretRef, and row-format
// binary logging is active (what Debezium's MySQL/MariaDB connector needs).
func (p *Provider) reconcileSource(ctx context.Context, req reconciler.Request) (status.Status, error) {
	res, rt := req.Resource, req.Runtime
	st := status.Status{}
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return st, err
	}
	name := naming.RuntimeObjectName(req.Provider)
	src, err := source.FromEnvelope(res)
	if err != nil {
		return st, err
	}
	dbName, _ := src.EngineConfig["database"].(string)
	if dbName == "" {
		return st, fmt.Errorf("Source %q: spec.%s.database is required", res.Metadata.Name, src.Engine)
	}
	rootPass, err := rootPassword(cfg, req.Secrets, name)
	if err != nil {
		return st, err
	}

	if err := waitReadyReachable(ctx, rt, name, 3306, func(addr string) string {
		return dsnAddr(addr, "root", rootPass, "")
	}, 30*time.Second); err != nil {
		return st, err
	}
	addr, closeAddr, err := providerkit.ReachableAddr(ctx, rt, name, 3306)
	if err != nil {
		return st, err
	}
	defer closeAddr()
	admin := dsnAddr(addr, "root", rootPass, "")
	if err := ensureDatabase(ctx, admin, dbName); err != nil {
		return st, err
	}
	replRefName, _ := cfg.Configuration["replicationSecretRef"].(string)
	replUser := ""
	if replRefName != "" {
		creds, ok := req.Secrets[replRefName]
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
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonSourceProvisioned}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	st.ProviderState = map[string]any{"database": dbName, "replicationUser": replUser}
	return st, nil
}

func (p *Provider) Destroy(ctx context.Context, req reconciler.Request) error {
	res, rt := req.Resource, req.Runtime
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return err
	}
	name := naming.RuntimeObjectName(req.Provider)
	switch res.Kind {
	case "Provider":
		if err := rt.Remove(ctx, name); err != nil {
			return err
		}
		if err := rt.RemoveVolume(ctx, name+"-data"); err != nil {
			return err
		}
		_ = rt.RemoveNetwork(ctx, providerkit.Network(cfg))
		return nil
	case "Source":
		// deletionPolicy governs the data (docs/planning/07 §2.2): retain
		// (the default) forgets the record and keeps the database; only an
		// explicit `deletionPolicy: delete` drops it. External sources are
		// engine-guarded before this is ever reached (NFR-3).
		src, err := source.FromEnvelope(res)
		if err != nil {
			return err
		}
		if src.DeletionPolicy != source.DeletionDelete || src.External {
			return nil
		}
		dbName, _ := src.EngineConfig["database"].(string)
		if dbName == "" {
			return nil
		}
		if ctr, found, ierr := rt.Inspect(ctx, name); ierr != nil || !found || !ctr.Running {
			return ierr
		}
		rootPass, err := rootPassword(cfg, req.Secrets, name)
		if err != nil {
			return err
		}
		addr, closeAddr, err := providerkit.ReachableAddr(ctx, rt, name, 3306)
		if err != nil {
			return err
		}
		defer closeAddr()
		return dropDatabase(ctx, dsnAddr(addr, "root", rootPass, ""), dbName)
	default:
		return fmt.Errorf("%s provider cannot destroy kind %s", cfg.Type, res.Kind)
	}
}

func (p *Provider) Probe(ctx context.Context, req reconciler.Request) (status.Status, error) {
	res, rt := req.Resource, req.Runtime
	st := status.Status{}
	now := time.Now()
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return st, err
	}
	name := naming.RuntimeObjectName(req.Provider)
	switch res.Kind {
	case "Provider":
		ctrState, found, err := rt.Inspect(ctx, name)
		if err != nil {
			return st, err
		}
		if !found || !ctrState.Healthy {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonInstanceUnhealthy}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonInstanceUnhealthy}, now)
			return st, nil
		}
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonInstanceHealthy}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
		return st, nil
	case "Source":
		src, err := source.FromEnvelope(res)
		if err != nil {
			return st, err
		}
		dbName, _ := src.EngineConfig["database"].(string)
		rootPass, err := rootPassword(cfg, req.Secrets, name)
		if err != nil {
			return st, err
		}
		addr, closeAddr, err := providerkit.ReachableAddr(ctx, rt, name, 3306)
		if err != nil {
			return st, err
		}
		defer closeAddr()
		admin := dsnAddr(addr, "root", rootPass, "")
		// Full desired configuration, not just liveness (docs/planning/07
		// §2.1): database exists, binlog is row-format (the CDC-readiness
		// this provider declares), and the replication user's declared
		// credentials still authenticate.
		exists, err := databaseExists(ctx, admin, dbName)
		if err != nil {
			return st, err
		}
		if !exists {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonDatabaseMissing}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonDatabaseMissing}, now)
			return st, nil
		}
		if format, err := globalVariable(ctx, admin, "binlog_format"); err != nil {
			return st, err
		} else if !strings.EqualFold(format, "ROW") {
			msg := fmt.Sprintf("binlog_format is %q, want ROW", format)
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonBinlogNotRowFormat, Message: msg}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonBinlogNotRowFormat, Message: msg}, now)
			return st, nil
		}
		if replRefName, _ := cfg.Configuration["replicationSecretRef"].(string); replRefName != "" {
			if creds, ok := req.Secrets[replRefName]; ok {
				if err := ping(ctx, dsnAddr(addr, creds["username"], creds["password"], "")); err != nil {
					msg := fmt.Sprintf("replication credentials (%s) no longer authenticate", replRefName)
					st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonReplicationCredentialsInvalid, Message: msg}, now)
					st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonReplicationCredentialsInvalid, Message: msg}, now)
					return st, nil
				}
			}
		}
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonSourceHealthy}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
		return st, nil
	default:
		return st, fmt.Errorf("%s provider cannot probe kind %s", cfg.Type, res.Kind)
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
	return catalogFor(cfg).ValidateConfig(cfg.Configuration)
}
