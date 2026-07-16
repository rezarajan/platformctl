// Package s3 reconciles an S3-API-compatible object store (MinIO is the
// reference target): instance lifecycle on the container runtime plus Dataset
// (bucket/prefix) reconciliation via the S3 API (Phase 4).
package s3

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/rezarajan/platformctl/internal/domain/dataset"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/hostport"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

const (
	defaultImage = "minio/minio:latest"
	apiPort      = 9000
	// rootPasswordPath is where the bootstrap password file is mounted.
	rootPasswordPath = "/run/datascape/root-password"
)

type Provider struct {
	providerRes resource.Envelope
	cfg         provider.Provider
	secrets     map[string]map[string]string
}

func New() *Provider { return &Provider{} }

func (p *Provider) Type() string { return "s3" }

func (p *Provider) SetProviderResource(env resource.Envelope) {
	p.providerRes = env
	p.cfg, _ = provider.FromEnvelope(env)
}

func (p *Provider) SetSecrets(secrets map[string]map[string]string) { p.secrets = secrets }

func (p *Provider) containerName() string { return p.providerRes.Metadata.Name }

func (p *Provider) hostPort() int {
	configured := 0
	if v, ok := p.cfg.Configuration["port"]; ok {
		switch n := v.(type) {
		case int:
			configured = n
		case float64:
			configured = int(n)
		}
	}
	return hostport.Resolve(configured, p.containerName())
}

func (p *Provider) network() string {
	if n, ok := p.cfg.RuntimeConfig["network"].(string); ok && n != "" {
		return n
	}
	return "datascape"
}

// rootCredentials returns the MinIO root credentials: the SecretReference
// named by configuration.rootSecretRef, or the first declared secretRef.
func (p *Provider) rootCredentials() (user, pass string, err error) {
	refName, _ := p.cfg.Configuration["rootSecretRef"].(string)
	if refName == "" && len(p.cfg.SecretRefs) > 0 {
		refName = p.cfg.SecretRefs[0]
	}
	creds, ok := p.secrets[refName]
	if !ok {
		return "", "", fmt.Errorf("Provider %q (type: s3): no resolved credentials for secretRef %q", p.containerName(), refName)
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
	case "Dataset":
		return p.reconcileDataset(ctx, res)
	default:
		return status.Status{}, fmt.Errorf("s3 provider cannot reconcile kind %s", res.Kind)
	}
}

func (p *Provider) reconcileInstance(ctx context.Context, rt runtime.ContainerRuntime) (status.Status, error) {
	st := status.Status{}
	name := p.containerName()
	image, _ := p.cfg.Configuration["image"].(string)
	if image == "" {
		image = defaultImage
	}
	user, pass, err := p.rootCredentials()
	if err != nil {
		return st, err
	}
	labels := runtime.ManagedLabels(p.providerRes.Metadata.Namespace, "Provider", name, name)

	if err := rt.EnsureNetwork(ctx, runtime.NetworkSpec{Name: p.network(), Labels: labels}); err != nil {
		return st, err
	}
	if err := rt.EnsureVolume(ctx, runtime.VolumeSpec{Name: name + "-data", Labels: labels, Networks: []string{p.network()}}); err != nil {
		return st, err
	}
	ctrState, err := rt.EnsureContainer(ctx, runtime.ContainerSpec{
		Name:  name,
		Image: image,
		Cmd:   []string{"server", "/data"},
		// The password rides a file mount, not env — env is readable by
		// anyone with `docker inspect` access (docs/planning/07 Gate 1
		// checkbox 4); MinIO's entrypoint consumes *_FILE natively.
		Env: map[string]string{
			"MINIO_ROOT_USER":          user,
			"MINIO_ROOT_PASSWORD_FILE": rootPasswordPath,
		},
		Files:    []runtime.FileMount{{Path: rootPasswordPath, Content: []byte(pass)}},
		Networks: []string{p.network()},
		Volumes:  []runtime.VolumeMount{{VolumeName: name + "-data", MountPath: "/data"}},
		Ports:    []runtime.PortBinding{{HostPort: p.hostPort(), ContainerPort: apiPort}},
		HealthCheck: &runtime.HealthCheck{
			Test:     []string{"CMD-SHELL", fmt.Sprintf("curl -sf http://localhost:%d/minio/health/live || exit 1", apiPort)},
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
	hostAddr := ctrState.HostAddr(apiPort) // observed binding, not intent
	hostURL := ""
	if hostAddr != "" {
		hostURL = "http://" + hostAddr
	}
	st.ProviderState = map[string]any{
		"containerId":  ctrState.ID,
		"hostEndpoint": hostAddr,
		"internalUrl":  "http://" + name + ":" + strconv.Itoa(apiPort),
		endpoint.Key: endpoint.List{
			{Name: "s3", Scheme: "http", Host: hostURL, Internal: "http://" + name + ":" + strconv.Itoa(apiPort)},
		}.ToState(),
	}
	return st, nil
}

func (p *Provider) reconcileDataset(ctx context.Context, res resource.Envelope) (status.Status, error) {
	st := status.Status{}
	ds, err := dataset.FromEnvelope(res)
	if err != nil {
		return st, err
	}
	user, pass, err := p.rootCredentials()
	if err != nil {
		return st, err
	}
	cl, err := newClient("127.0.0.1:"+strconv.Itoa(p.hostPort()), user, pass)
	if err != nil {
		return st, err
	}
	if err := ensureBucket(ctx, cl, ds.Bucket); err != nil {
		return st, err
	}

	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "DatasetProvisioned"}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: "ReconcileComplete"}, now)
	st.ProviderState = map[string]any{"bucket": ds.Bucket, "prefix": ds.Prefix, "format": ds.Format}
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
	case "Dataset":
		ds, err := dataset.FromEnvelope(res)
		if err != nil {
			return err
		}
		// deletionPolicy governs the data (docs/planning/07 §2.2): retain
		// (the default) forgets the record and keeps every object —
		// destroying the platform's bookkeeping must not destroy data.
		// Only an explicit `deletionPolicy: delete` wipes bucket/prefix.
		if ds.DeletionPolicy != dataset.DeletionDelete {
			return nil
		}
		// If the backing store is already gone (killed out-of-band), its
		// data went with it — nothing left to remove, and failing here
		// would strand the Dataset in state forever.
		if ctr, found, err := rt.Inspect(ctx, p.containerName()); err != nil || !found || !ctr.Running {
			return err
		}
		user, pass, err := p.rootCredentials()
		if err != nil {
			return err
		}
		cl, err := newClient("127.0.0.1:"+strconv.Itoa(p.hostPort()), user, pass)
		if err != nil {
			return err
		}
		return removeDataset(ctx, cl, ds.Bucket, ds.Prefix)
	default:
		return fmt.Errorf("s3 provider cannot destroy kind %s", res.Kind)
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
	case "Dataset":
		ds, err := dataset.FromEnvelope(res)
		if err != nil {
			return st, err
		}
		user, pass, err := p.rootCredentials()
		if err != nil {
			return st, err
		}
		cl, err := newClient("127.0.0.1:"+strconv.Itoa(p.hostPort()), user, pass)
		if err != nil {
			return st, err
		}
		exists, err := bucketExists(ctx, cl, ds.Bucket)
		if err != nil {
			return st, err
		}
		if !exists {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: "BucketMissing"}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: "BucketMissing"}, now)
		} else {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "DatasetHealthy"}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: "NoDrift"}, now)
		}
		return st, nil
	default:
		return st, fmt.Errorf("s3 provider cannot probe kind %s", res.Kind)
	}
}

// ValidateSpec implements SpecValidator: the store cannot boot without root
// credentials, so their wiring is checked at validate.
func (p *Provider) ValidateSpec(cfg provider.Provider) error {
	if ref, _ := cfg.Configuration["rootSecretRef"].(string); ref != "" {
		if !cfg.HasSecretRef(ref) {
			return fmt.Errorf("configuration.rootSecretRef %q must also be listed in spec.secretRefs for the engine to resolve it", ref)
		}
	} else if len(cfg.SecretRefs) == 0 {
		return fmt.Errorf("spec.secretRefs must name at least one SecretReference (the root credentials; configuration.rootSecretRef selects one explicitly)")
	}
	return nil
}
