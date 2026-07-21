// Package s3 reconciles an S3-API-compatible object store (MinIO is the
// reference target): instance lifecycle on the container runtime plus Dataset
// (bucket/prefix) reconciliation via the S3 API (Phase 4).
package s3

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/rezarajan/platformctl/internal/adapters/providers/providerkit"
	"github.com/rezarajan/platformctl/internal/domain/dataset"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

const (
	defaultImage = "minio/minio:RELEASE.2025-04-22T22-12-26Z@sha256:a1ea29fa28355559ef137d71fc570e508a214ec84ff8083e39bc5428980b015e"
	apiPort      = 9000
	// rootPasswordPath is where the bootstrap password file is mounted.
	rootPasswordPath = "/run/datascape/root-password"
)

// Provider holds no cross-call state (docs/planning/08 F5): every method
// receives what it needs via reconciler.Request.
type Provider struct{}

func New() *Provider { return &Provider{} }

func (p *Provider) Type() string { return "s3" }

// reachableAddr returns an address this process can dial right now to reach
// the store's S3 API port, plus a close func that must always be called
// (docs/planning/08 B8: Docker's is a cheap no-op; Kubernetes may tear down
// a port-forward tunnel opened just for this call).
func reachableAddr(ctx context.Context, rt runtime.ContainerRuntime, name string) (string, func() error, error) {
	return providerkit.ReachableAddr(ctx, rt, name, apiPort)
}

// rootCredentials returns the MinIO root credentials: the SecretReference
// named by configuration.rootSecretRef, or the first declared secretRef.
func rootCredentials(cfg provider.Provider, secrets map[string]map[string]string, name string) (user, pass string, err error) {
	creds, refName, err := providerkit.ResolveCredential(cfg, secrets, "rootSecretRef", name)
	if err != nil {
		return "", "", err
	}
	user, pass = creds["username"], creds["password"]
	if user == "" || pass == "" {
		return "", "", fmt.Errorf("Provider %q: secretRef %q must provide username and password keys", name, refName)
	}
	return user, pass, nil
}

func (p *Provider) Reconcile(ctx context.Context, req reconciler.Request) (status.Status, error) {
	switch req.Resource.Kind {
	case "Provider":
		return p.reconcileInstance(ctx, req)
	case "Dataset":
		return p.reconcileDataset(ctx, req)
	default:
		return status.Status{}, fmt.Errorf("s3 provider cannot reconcile kind %s", req.Resource.Kind)
	}
}

// imagePullAuth resolves configuration.imagePullSecretRef (docs/planning/07
// §1.1 deferral, docs/planning/08 A1) into runtime credentials, or returns
// nil when unset — private-image pulls stay opt-in, and the runtime's
// ambient/daemon-level credentials keep working unchanged either way.
func imagePullAuth(cfg provider.Provider, secrets map[string]map[string]string, name string) (*runtime.ImagePullAuth, error) {
	refName, _ := cfg.Configuration["imagePullSecretRef"].(string)
	if refName == "" {
		return nil, nil
	}
	creds, ok := secrets[refName]
	if !ok {
		return nil, fmt.Errorf("Provider %q: no resolved credentials for imagePullSecretRef %q", name, refName)
	}
	if creds["username"] == "" || creds["password"] == "" {
		return nil, fmt.Errorf("Provider %q: imagePullSecretRef %q must provide username and password keys", name, refName)
	}
	return &runtime.ImagePullAuth{Username: creds["username"], Password: creds["password"], Registry: creds["registry"]}, nil
}

func (p *Provider) reconcileInstance(ctx context.Context, req reconciler.Request) (status.Status, error) {
	rt := req.Runtime
	st := status.Status{}
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return st, err
	}
	name := naming.RuntimeObjectName(req.Provider)
	image, _ := cfg.Configuration["image"].(string)
	if image == "" {
		image = defaultImage
	}
	user, pass, err := rootCredentials(cfg, req.Secrets, name)
	if err != nil {
		return st, err
	}
	pullAuth, err := imagePullAuth(cfg, req.Secrets, name)
	if err != nil {
		return st, err
	}
	ctrState, err := providerkit.EnsureInstance(ctx, rt, providerkit.InstanceSpec{
		Namespace: req.Provider.Metadata.Namespace,
		Name:      name,
		Network:   providerkit.Network(cfg),
		Volume:    &providerkit.InstanceVolume{Name: name + "-data", MountPath: "/data"},
		Container: runtime.ContainerSpec{
			Image:         image,
			ImagePullAuth: pullAuth,
			Cmd:           []string{"server", "/data"},
			// The password rides a file mount, not env — env is readable by
			// anyone with `docker inspect` access (docs/planning/07 Gate 1
			// checkbox 4); MinIO's entrypoint consumes *_FILE natively.
			Env: map[string]string{
				"MINIO_ROOT_USER":          user,
				"MINIO_ROOT_PASSWORD_FILE": rootPasswordPath,
			},
			Files: []runtime.FileMount{{Path: rootPasswordPath, Content: []byte(pass)}},
			Ports: []runtime.PortBinding{{HostPort: providerkit.HostPort(cfg, name, "port"), ContainerPort: apiPort, Audience: runtime.AudienceHost}},
			HealthCheck: &runtime.HealthCheck{
				Test:     []string{"CMD-SHELL", fmt.Sprintf("curl -sf http://localhost:%d/minio/health/live || exit 1", apiPort)},
				Interval: 2 * time.Second,
				Timeout:  5 * time.Second,
				Retries:  30,
			},
		},
		WaitTimeout: 120 * time.Second,
	})
	if err != nil {
		return st, err
	}

	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonInstanceHealthy}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
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
			// RuntimeName/ContainerPort/Audience/Network are the F4 facts a
			// caller resolving a backup.Location from this Dataset's
			// providerRef needs to reach this store both from inside the
			// runtime (Internal, joining Network) and from the CLI host
			// itself (via ContainerRuntime.EnsureReachable on RuntimeName/
			// ContainerPort — docs/planning/08 F4, C6 review findings 2/3;
			// docs/adr/007-backup-restore.md). Internal is a bare
			// "host:port" (no scheme), matching every other provider's
			// convention (see the package doc on endpoint.Endpoint.Internal)
			// — a consumer composing a URL prepends Scheme itself.
			{
				Name:          "s3",
				Scheme:        "http",
				Host:          hostURL,
				Internal:      name + ":" + strconv.Itoa(apiPort),
				Insecure:      true,
				RuntimeName:   name,
				ContainerPort: apiPort,
				Audience:      runtime.AudienceHost,
				Network:       providerkit.Network(cfg),
			},
		}.ToState(),
	}
	return st, nil
}

func (p *Provider) reconcileDataset(ctx context.Context, req reconciler.Request) (status.Status, error) {
	st := status.Status{}
	ds, err := dataset.FromEnvelope(req.Resource)
	if err != nil {
		return st, err
	}
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return st, err
	}
	name := naming.RuntimeObjectName(req.Provider)
	user, pass, err := rootCredentials(cfg, req.Secrets, name)
	if err != nil {
		return st, err
	}
	if err := ensureBucket(ctx, req.Runtime, name, apiPort, user, pass, ds.Bucket); err != nil {
		return st, err
	}

	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonDatasetProvisioned}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	st.ProviderState = map[string]any{"bucket": ds.Bucket, "prefix": ds.Prefix, "format": ds.Format}
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
		if ctr, found, err := rt.Inspect(ctx, name); err != nil || !found || !ctr.Running {
			return err
		}
		user, pass, err := rootCredentials(cfg, req.Secrets, name)
		if err != nil {
			return err
		}
		addr, closeAddr, err := reachableAddr(ctx, rt, name)
		if err != nil {
			return err
		}
		defer closeAddr()
		cl, err := newClient(addr, user, pass)
		if err != nil {
			return err
		}
		return removeDataset(ctx, cl, ds.Bucket, ds.Prefix)
	default:
		return fmt.Errorf("s3 provider cannot destroy kind %s", res.Kind)
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
	case "Dataset":
		ds, err := dataset.FromEnvelope(res)
		if err != nil {
			return st, err
		}
		user, pass, err := rootCredentials(cfg, req.Secrets, name)
		if err != nil {
			return st, err
		}
		addr, closeAddr, err := reachableAddr(ctx, rt, name)
		if err != nil {
			return st, err
		}
		defer closeAddr()
		cl, err := newClient(addr, user, pass)
		if err != nil {
			return st, err
		}
		exists, err := bucketExists(ctx, cl, ds.Bucket)
		if err != nil {
			return st, err
		}
		if !exists {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonBucketMissing}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonBucketMissing}, now)
			return st, nil
		}
		// Beyond existence (docs/planning/07 §2.1): the prefix must be
		// listable with the declared credentials — a permissions/policy
		// change that breaks readers is drift, not health.
		if err := prefixListable(ctx, cl, ds.Bucket, ds.Prefix); err != nil {
			msg := "bucket exists but prefix is not listable: " + err.Error()
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonPrefixUnlistable, Message: msg}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonPrefixUnlistable, Message: msg}, now)
			return st, nil
		}
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonDatasetHealthy}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
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
	if ref, _ := cfg.Configuration["imagePullSecretRef"].(string); ref != "" && !cfg.HasSecretRef(ref) {
		return fmt.Errorf("configuration.imagePullSecretRef %q must also be listed in spec.secretRefs for the engine to resolve it", ref)
	}
	return nil
}
