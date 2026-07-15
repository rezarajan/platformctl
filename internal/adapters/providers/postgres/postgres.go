// Package postgres reconciles a Postgres container with logical replication
// enabled (wal_level=logical) and provisions databases and replication users
// from SecretReference-sourced credentials (Phase 3).
package postgres

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
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

const defaultImage = "postgres:16"

type Provider struct {
	providerRes resource.Envelope
	cfg         provider.Provider
	secrets     map[string]map[string]string
}

func New() *Provider { return &Provider{} }

func (p *Provider) Type() string { return "postgres" }

func (p *Provider) SetProviderResource(env resource.Envelope) {
	p.providerRes = env
	p.cfg, _ = provider.FromEnvelope(env)
}

func (p *Provider) SetSecrets(secrets map[string]map[string]string) { p.secrets = secrets }

func (p *Provider) containerName() string { return p.providerRes.Metadata.Name }

func (p *Provider) hostPort() int {
	if v, ok := p.cfg.Configuration["port"]; ok {
		switch n := v.(type) {
		case int:
			return n
		case float64:
			return int(n)
		}
	}
	return 5432
}

func (p *Provider) network() string {
	if n, ok := p.cfg.RuntimeConfig["network"].(string); ok && n != "" {
		return n
	}
	return "datascape"
}

// superuser returns the bootstrap credentials: the SecretReference named by
// configuration.superuserSecretRef, or the first declared secretRef.
func (p *Provider) superuser() (user, pass string, err error) {
	refName, _ := p.cfg.Configuration["superuserSecretRef"].(string)
	if refName == "" && len(p.cfg.SecretRefs) > 0 {
		refName = p.cfg.SecretRefs[0]
	}
	creds, ok := p.secrets[refName]
	if !ok {
		return "", "", fmt.Errorf("Provider %q (type: postgres): no resolved credentials for secretRef %q", p.containerName(), refName)
	}
	user, pass = creds["username"], creds["password"]
	if user == "" || pass == "" {
		return "", "", fmt.Errorf("Provider %q: secretRef %q must provide username and password keys", p.containerName(), refName)
	}
	return user, pass, nil
}

func (p *Provider) Reconcile(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) (status.Status, error) {
	switch res.Kind {
	case "Provider":
		return p.reconcileInstance(ctx, rt)
	case "Source":
		return p.reconcileSource(ctx, res)
	default:
		return status.Status{}, fmt.Errorf("postgres provider cannot reconcile kind %s", res.Kind)
	}
}

func (p *Provider) reconcileInstance(ctx context.Context, rt runtime.ContainerRuntime) (status.Status, error) {
	st := status.Status{}
	name := p.containerName()
	image, _ := p.cfg.Configuration["image"].(string)
	if image == "" {
		image = defaultImage
	}
	user, pass, err := p.superuser()
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
	ctrState, err := rt.EnsureContainer(ctx, runtime.ContainerSpec{
		Name:  name,
		Image: image,
		Cmd:   []string{"postgres", "-c", "wal_level=logical"},
		Env: map[string]string{
			"POSTGRES_USER":     user,
			"POSTGRES_PASSWORD": pass,
		},
		Networks: []string{p.network()},
		Volumes:  []runtime.VolumeMount{{VolumeName: name + "-data", MountPath: "/var/lib/postgresql/data"}},
		Ports:    []runtime.PortBinding{{HostPort: p.hostPort(), ContainerPort: 5432}},
		HealthCheck: &runtime.HealthCheck{
			// Force a TCP check: the plain unix-socket pg_isready answers
			// during the image's initdb temp-server phase, before the real
			// server listens on TCP — reporting healthy while connections
			// from the host are still refused.
			Test:     []string{"CMD-SHELL", "pg_isready -h 127.0.0.1 -U " + user},
			Interval: 2 * time.Second,
			Timeout:  5 * time.Second,
			Retries:  30,
		},
		Labels: labels,
	})
	if err != nil {
		return st, err
	}
	if err := rt.WaitHealthy(ctx, name, 120*time.Second); err != nil {
		return st, err
	}

	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "InstanceHealthy"}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: "ReconcileComplete"}, now)
	hostAddr := "127.0.0.1:" + strconv.Itoa(p.hostPort())
	internalAddr := name + ":5432"
	st.ProviderState = map[string]any{
		"containerId":  ctrState.ID,
		"hostAddr":     hostAddr,
		"internalAddr": internalAddr,
		endpoint.Key: endpoint.List{
			{Name: "postgres", Scheme: "postgres", Host: hostAddr, Internal: internalAddr},
		}.ToState(),
	}
	return st, nil
}

// reconcileSource ensures the declared database exists, logical replication
// is active (wal_level=logical is set at instance level), and the replication
// role from configuration.replicationSecretRef exists with REPLICATION LOGIN.
func (p *Provider) reconcileSource(ctx context.Context, res resource.Envelope) (status.Status, error) {
	st := status.Status{}
	src, err := source.FromEnvelope(res)
	if err != nil {
		return st, err
	}
	dbName, _ := src.EngineConfig["database"].(string)
	if dbName == "" {
		return st, fmt.Errorf("Source %q: spec.postgres.database is required", res.Metadata.Name)
	}
	suUser, suPass, err := p.superuser()
	if err != nil {
		return st, err
	}

	replRefName, _ := p.cfg.Configuration["replicationSecretRef"].(string)
	var replUser, replPass string
	if replRefName != "" {
		creds, ok := p.secrets[replRefName]
		if !ok {
			return st, fmt.Errorf("Source %q: no resolved credentials for replicationSecretRef %q", res.Metadata.Name, replRefName)
		}
		replUser, replPass = creds["username"], creds["password"]
	}

	admin := connString("127.0.0.1", p.hostPort(), suUser, suPass, "postgres")
	if err := waitReady(ctx, admin, 30*time.Second); err != nil {
		return st, err
	}
	if err := ensureDatabase(ctx, admin, dbName); err != nil {
		return st, err
	}
	if replUser != "" {
		if err := ensureReplicationRole(ctx, admin, replUser, replPass); err != nil {
			return st, err
		}
	}
	// The publication lives in the source database itself, created by the
	// superuser so the replication role never needs table ownership.
	// "dbz_publication" is Debezium's default publication.name.
	if err := ensurePublication(ctx, connString("127.0.0.1", p.hostPort(), suUser, suPass, dbName), "dbz_publication"); err != nil {
		return st, err
	}
	if err := verifyLogicalWAL(ctx, admin); err != nil {
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
		// Dropping the database on Source destroy would be data loss beyond
		// the declared contract; the instance teardown (Provider destroy)
		// removes everything anyway.
		return nil
	default:
		return fmt.Errorf("postgres provider cannot destroy kind %s", res.Kind)
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
		suUser, suPass, err := p.superuser()
		if err != nil {
			return st, err
		}
		exists, err := databaseExists(ctx, connString("127.0.0.1", p.hostPort(), suUser, suPass, "postgres"), dbName)
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
		return st, fmt.Errorf("postgres provider cannot probe kind %s", res.Kind)
	}
}

// ValidateSpec implements SpecValidator: the instance cannot boot without
// superuser credentials, so their wiring is checked at validate.
func (p *Provider) ValidateSpec(cfg provider.Provider) error {
	if ref, _ := cfg.Configuration["superuserSecretRef"].(string); ref != "" {
		if !cfg.HasSecretRef(ref) {
			return fmt.Errorf("configuration.superuserSecretRef %q must also be listed in spec.secretRefs for the engine to resolve it", ref)
		}
	} else if len(cfg.SecretRefs) == 0 {
		return fmt.Errorf("spec.secretRefs must name at least one SecretReference (the superuser credentials; configuration.superuserSecretRef selects one explicitly)")
	}
	if ref, _ := cfg.Configuration["replicationSecretRef"].(string); ref != "" && !cfg.HasSecretRef(ref) {
		return fmt.Errorf("configuration.replicationSecretRef %q must also be listed in spec.secretRefs for the engine to resolve it", ref)
	}
	return nil
}
