// Package s3 reconciles an S3-API-compatible object store (MinIO is the
// reference target): instance lifecycle on the container runtime plus Dataset
// (bucket/prefix) reconciliation via the S3 API (Phase 4).
package s3

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/rezarajan/platformctl/internal/adapters/providers/providerkit"
	"github.com/rezarajan/platformctl/internal/domain/connection"
	"github.com/rezarajan/platformctl/internal/domain/dataset"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

const (
	defaultImage = "minio/minio:RELEASE.2025-04-22T22-12-26Z@sha256:a1ea29fa28355559ef137d71fc570e508a214ec84ff8083e39bc5428980b015e"
	apiPort      = 9000
	// consolePort is MinIO's web console — bound but not published unless a
	// caller asks for it; needed only so the distributed-mode start command
	// has an explicit, non-conflicting console address (MinIO auto-picks one
	// otherwise, which is fine for a single node but ambiguous across
	// identical per-ordinal commands).
	consolePort = 9001
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

// metricsURL appends MinIO's cluster metrics path to an already-resolved
// "scheme://host:port" base, or returns "" unchanged when base is empty
// (no host binding published yet).
func metricsURL(base string) string {
	if base == "" {
		return ""
	}
	return base + "/minio/v2/metrics/cluster"
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

// nodesDeclared reads spec.configuration.nodes — the s3/minio mirror of
// redpanda's brokersDeclared (docs/adr/017 §a.1's same deliberate
// asymmetry): declared=false (the key absent) keeps the pre-C4
// single-container shape byte-for-byte; declared=true (any value >= 1,
// bounds validated by ValidateSpec) opts into the StableIdentity
// ordinal-set shape (docs/planning/08 C4) — a distributed, erasure-coded
// MinIO cluster when n >= 4, or a single ordinal still using the set shape
// when n == 1 (same "1 uses the set shape too" amendment C2 made for
// brokers).
func nodesDeclared(cfg provider.Provider) (int, bool) {
	v, ok := cfg.Configuration["nodes"]
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case int:
		return n, true
	case float64:
		return int(n), true
	}
	return 0, true
}

// minioNodeURLs is the distributed-mode node list every ordinal's start
// command carries identically (docs/planning/08 C4): unlike redpanda's
// seed-and-join protocol, MinIO distributed mode requires every node to be
// started with the full peer list up front, computed here from manifest
// facts alone (name + declared count + the fixed API port), never from
// runtime observation (ADR 015 F4) — ordinal DNS names are deterministic on
// both adapters.
func minioNodeURLs(name string, n int) []string {
	urls := make([]string, n)
	for i := range urls {
		urls[i] = fmt.Sprintf("http://%s:%d/data", runtime.OrdinalName(name, i), apiPort)
	}
	return urls
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

	// configuration.nodes declared (any N >= 1) opts into the StableIdentity
	// ordinal-set shape (docs/planning/08 C4); undeclared keeps the
	// pre-C4 single-container path below, byte-for-byte.
	if n, declared := nodesDeclared(cfg); declared {
		if n < 1 {
			return st, fmt.Errorf("spec.configuration.nodes must be a positive integer, got %v", cfg.Configuration["nodes"])
		}
		return p.reconcileInstanceSet(ctx, req, cfg, name, image, user, pass, pullAuth, n)
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
				// "public" makes /minio/v2/metrics/cluster scrapable with no
				// bearer token — the metrics endpoint fact published below
				// exists specifically so a Prometheus (managed or BYO,
				// docs/planning/08 C9) can scrape it; MinIO's default ("jwt")
				// would otherwise 403 every unauthenticated scrape.
				"MINIO_PROMETHEUS_AUTH_TYPE": "public",
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
			// "metrics" is the /minio/v2/metrics/cluster fact
			// (docs/planning/08 C9) — zero extra containers, the same
			// apiPort already published above. Unlike "s3"'s bare
			// "host:port" above, Internal/Host carry a full URL (scheme +
			// path): the prometheus provider parses this directly as a
			// scrape target (internal/adapters/providers/prometheus).
			{
				Name:          "metrics",
				Scheme:        "http",
				Host:          metricsURL(hostURL),
				Internal:      metricsURL("http://" + name + ":" + strconv.Itoa(apiPort)),
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

// reconcileInstanceSet is reconcileInstance's distributed-MinIO counterpart
// (docs/planning/08 C4): n ordinal nodes via ContainerSpec.Replicas +
// StableIdentity — the same pattern C2/ADR 017 established for redpanda's
// brokers, but simpler: MinIO's node list is identical on every ordinal and
// fully known at spec-build time (no per-ordinal seed logic, no Entrypoint
// override), unlike Kafka's join protocol. It does not use
// providerkit.EnsureInstance: the runtime owns the entire per-ordinal
// volume lifecycle for a StableIdentity set (docs/adr/004), so the
// single-volume skeleton doesn't fit.
func (p *Provider) reconcileInstanceSet(ctx context.Context, req reconciler.Request, cfg provider.Provider, name, image, user, pass string, pullAuth *runtime.ImagePullAuth, n int) (status.Status, error) {
	rt := req.Runtime
	st := status.Status{}

	// Scale-down refusal (mirrors docs/adr/017 §a.5 exactly): an observed
	// ordinal at or beyond the desired count means the set was last applied
	// larger — pruning it would discard that node's share of the
	// erasure-coded data.
	observed := n
	for {
		_, found, err := rt.Inspect(ctx, runtime.OrdinalName(name, observed))
		if err != nil {
			return st, err
		}
		if !found {
			break
		}
		observed++
	}
	if observed > n {
		return st, fmt.Errorf("scaling spec.configuration.nodes down from %d to %d risks data loss (this node's share of the erasure-coded pool would be discarded) and is refused; restore nodes: %d, or destroy and recreate the Provider to shrink the cluster", observed, n, observed)
	}

	labels := runtime.ManagedLabels(req.Provider.Metadata.Namespace, "Provider", name, name)
	network := providerkit.Network(cfg)
	if err := rt.EnsureNetwork(ctx, runtime.NetworkSpec{Name: network, Labels: labels}); err != nil {
		return st, err
	}
	cmd := append([]string{"server"}, minioNodeURLs(name, n)...)
	cmd = append(cmd, "--console-address", fmt.Sprintf(":%d", consolePort))
	ctrState, err := rt.EnsureContainer(ctx, runtime.ContainerSpec{
		Name:          name,
		Image:         image,
		ImagePullAuth: pullAuth,
		Cmd:           cmd,
		Networks:      []string{network},
		Env: map[string]string{
			"MINIO_ROOT_USER":            user,
			"MINIO_ROOT_PASSWORD_FILE":   rootPasswordPath,
			"MINIO_PROMETHEUS_AUTH_TYPE": "public",
		},
		Files: []runtime.FileMount{{Path: rootPasswordPath, Content: []byte(pass)}},
		// The runtime suffixes this volume per ordinal and owns its
		// lifecycle (docs/adr/004): "<name>-data-<i>" on Docker,
		// volumeClaimTemplates on Kubernetes.
		Volumes: []runtime.VolumeMount{{VolumeName: name + "-data", MountPath: "/data"}},
		Ports: []runtime.PortBinding{
			// Host port auto-assigned per ordinal (HostPort 0): a fixed pin
			// cannot be combined with Replicas > 1 (docs/adr/004 known
			// limitation; ValidateSpec refuses the pin below).
			{ContainerPort: apiPort, Audience: runtime.AudienceHost},
		},
		HealthCheck: &runtime.HealthCheck{
			Test:     []string{"CMD-SHELL", fmt.Sprintf("curl -sf http://localhost:%d/minio/health/live || exit 1", apiPort)},
			Interval: 2 * time.Second,
			Timeout:  5 * time.Second,
			Retries:  30,
		},
		Labels:         labels,
		Replicas:       n,
		StableIdentity: true,
	})
	if err != nil {
		return st, err
	}
	// WaitHealthy returns at one-ordinal-healthy (docs/adr/004's deliberate
	// at-least-one rule) — a distributed MinIO node doesn't finish quorum
	// negotiation with its peers at the same instant, so confirm the S3 API
	// is actually serving requests (implies quorum was reached) before
	// declaring Ready.
	if err := rt.WaitHealthy(ctx, name, 240*time.Second); err != nil {
		return st, err
	}
	if err := waitNodeSetServing(ctx, rt, name, user, pass, 180*time.Second); err != nil {
		return st, err
	}

	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonInstanceHealthy}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	st.ProviderState = instanceSetProviderState(ctx, rt, name, n, ctrState.ID, cfg)
	return st, nil
}

// waitNodeSetServing polls the S3 API through ordinal 0 (runtime.WithReachable
// re-resolves a dialable address on every attempt, docs/planning/09 F1) until
// a ListBuckets call succeeds — MinIO's distributed nodes block serving until
// enough peers have joined, so this is the real "the cluster formed" signal,
// analogous to redpanda's waitClusterFormed.
func waitNodeSetServing(ctx context.Context, rt runtime.ContainerRuntime, name, user, pass string, timeout time.Duration) error {
	ord0 := runtime.OrdinalName(name, 0)
	opts := runtime.ReachableOptions{Timeout: timeout, Interval: 2 * time.Second}
	err := runtime.WithReachable(ctx, rt, ord0, apiPort, opts, func(ctx context.Context, addr string) error {
		cl, err := newClient(addr, user, pass, false)
		if err != nil {
			return err
		}
		_, err = cl.ListBuckets(ctx)
		return err
	})
	if err != nil {
		return fmt.Errorf("MinIO node set %q did not start serving within %s: %w", name, timeout, err)
	}
	return nil
}

// instanceSetProviderState assembles the set shape's published facts
// (mirrors redpanda's brokerSetProviderState, docs/adr/017 question b): the
// aggregate "s3"/"metrics" endpoints (ordinal 0's observed host binding —
// any node serves the whole namespace, so ordinal 0 is as good as any for a
// single published address) plus one per-ordinal "s3-<i>" fact each, the
// only per-ordinal information that legitimately belongs in state (a
// runtime-allocated host binding cannot be re-derived, ADR 015 "publish,
// don't construct").
func instanceSetProviderState(ctx context.Context, rt runtime.ContainerRuntime, name string, n int, containerID string, cfg provider.Provider) map[string]any {
	network := providerkit.Network(cfg)
	endpoints := endpoint.List{}
	var s3Host0, metricsHost0 string
	for i := 0; i < n; i++ {
		ord := runtime.OrdinalName(name, i)
		ordState, found, err := rt.Inspect(ctx, ord)
		host := ""
		if err == nil && found {
			host = ordState.HostAddr(apiPort) // observed binding, not intent
		}
		hostURL := ""
		if host != "" {
			hostURL = "http://" + host
		}
		if i == 0 {
			s3Host0 = hostURL
			metricsHost0 = metricsURL(hostURL)
		}
		endpoints = append(endpoints, endpoint.Endpoint{
			Name: fmt.Sprintf("s3-%d", i), Scheme: "http", Host: hostURL, Internal: ord + ":" + strconv.Itoa(apiPort),
			Insecure: true, RuntimeName: ord, ContainerPort: apiPort, Audience: runtime.AudienceHost, Network: network,
		})
	}
	ord0 := runtime.OrdinalName(name, 0)
	endpoints = append(endpoint.List{
		{
			Name: "s3", Scheme: "http", Host: s3Host0, Internal: ord0 + ":" + strconv.Itoa(apiPort),
			Insecure: true, RuntimeName: ord0, ContainerPort: apiPort, Audience: runtime.AudienceHost, Network: network,
		},
		{
			Name: "metrics", Scheme: "http", Host: metricsHost0, Internal: metricsURL("http://" + ord0 + ":" + strconv.Itoa(apiPort)),
			Insecure: true, RuntimeName: ord0, ContainerPort: apiPort, Audience: runtime.AudienceHost, Network: network,
		},
	}, endpoints...)
	return map[string]any{
		"containerId":  containerID,
		"hostEndpoint": s3Host0,
		"nodes":        n,
		endpoint.Key:   endpoints.ToState(),
	}
}

// anyReachableOrdinal returns a dialable address for a StableIdentity node
// set's S3 API: MinIO's distributed mode serves the full bucket/object
// namespace from *any* live node (unlike redpanda's per-broker partition
// ownership), so the first node this process can currently reach is enough
// — tolerating up to n-1 nodes being down (docs/planning/08 C4's "4-node
// MinIO survives one node kill with sink traffic flowing" accept
// criterion).
func anyReachableOrdinal(ctx context.Context, rt runtime.ContainerRuntime, name string, n int) (string, func() error, error) {
	var lastErr error
	for i := 0; i < n; i++ {
		addr, closeAddr, err := rt.EnsureReachable(ctx, runtime.OrdinalName(name, i), apiPort)
		if err == nil {
			return addr, closeAddr, nil
		}
		lastErr = err
	}
	return "", nil, fmt.Errorf("no node of %q (%d ordinals) is currently reachable: %w", name, n, lastErr)
}

// externalStoreDial resolves an external s3 Provider's own spec.connectionRef
// (a Connection or bare SecretReference, resolved from req.Resources —
// mirrors exactly how debezium.buildDesiredConnector resolves an external
// Source's connectionRef) into a dialable address, resolved credentials, and
// whether the endpoint is TLS (docs/planning/08 C4). A no-op closer: there is
// nothing runtime-side to release for a static external address.
func externalStoreDial(req reconciler.Request, cfg provider.Provider, name string) (addr string, closeAddr func() error, user, pass string, secure bool, err error) {
	closeAddr = func() error { return nil }
	if cfg.ConnectionRef == nil {
		err = fmt.Errorf("Provider %q: external s3 provider requires spec.connectionRef", name)
		return
	}
	connRef := resource.RefFromSpec(req.Provider.Spec, "connectionRef")
	connEnv, ok := req.Resources[connRef.Key(req.Provider.Metadata.Namespace, "Connection")]
	if !ok {
		err = fmt.Errorf("Provider %q: connectionRef %q not found", name, *cfg.ConnectionRef)
		return
	}
	conn, cerr := connection.FromEnvelope(connEnv)
	if cerr != nil {
		err = fmt.Errorf("Provider %q: %w", name, cerr)
		return
	}
	host, port := conn.Endpoint(naming.RuntimeObjectName(connEnv))
	addr = net.JoinHostPort(host, strconv.Itoa(port))
	secure = conn.Scheme == "https"
	secretRefName := ""
	if conn.SecretRef != nil {
		secretRefName = *conn.SecretRef
	}
	creds, ok := req.Secrets[secretRefName]
	if !ok {
		err = fmt.Errorf("Provider %q: no resolved credentials for Connection %q's secretRef %q (list it in this Provider's spec.secretRefs)", name, connRef.Name, secretRefName)
		return
	}
	user, pass = creds["username"], creds["password"]
	if user == "" || pass == "" {
		err = fmt.Errorf("Provider %q: Connection %q's secretRef %q must provide username and password keys", name, connRef.Name, secretRefName)
	}
	return
}

// resolveDatasetDial resolves a currently-dialable address + credentials +
// TLS flag for req.Provider's S3 API, covering all three docs/planning/08
// C4 shapes: the legacy single container, a StableIdentity node set (any
// reachable ordinal), and an external Provider's own Connection. A single
// attempt — no boot-race wait; ensureBucket (called separately, managed
// shapes only) owns that.
func resolveDatasetDial(ctx context.Context, req reconciler.Request, cfg provider.Provider, name string) (addr string, closeAddr func() error, user, pass string, secure bool, err error) {
	if cfg.External {
		return externalStoreDial(req, cfg, name)
	}
	user, pass, err = rootCredentials(cfg, req.Secrets, name)
	if err != nil {
		return
	}
	if n, declared := nodesDeclared(cfg); declared && n >= 1 {
		addr, closeAddr, err = anyReachableOrdinal(ctx, req.Runtime, name, n)
		return
	}
	addr, closeAddr, err = reachableAddr(ctx, req.Runtime, name)
	return
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

	// Managed shapes (legacy single container, StableIdentity node set) get
	// the boot-race-tolerant wait; an external store is assumed already up
	// (docs/planning/08 C4).
	if !cfg.External {
		user, pass, err := rootCredentials(cfg, req.Secrets, name)
		if err != nil {
			return st, err
		}
		dialName := name
		if n, declared := nodesDeclared(cfg); declared && n >= 1 {
			dialName = runtime.OrdinalName(name, 0)
		}
		if err := ensureBucket(ctx, req.Runtime, dialName, apiPort, user, pass, ds.Bucket); err != nil {
			return st, err
		}
	}

	addr, closeAddr, user, pass, secure, err := resolveDatasetDial(ctx, req, cfg, name)
	if err != nil {
		return st, err
	}
	defer closeAddr()
	cl, err := newClient(addr, user, pass, secure)
	if err != nil {
		return st, err
	}
	if cfg.External {
		if err := ensureBucketAt(ctx, cl, ds.Bucket); err != nil {
			return st, err
		}
	}
	ruleID := lifecycleRuleID(req.Resource.Metadata.Name)
	if err := ensureLifecycle(ctx, cl, ds, ruleID); err != nil {
		return st, err
	}

	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonDatasetProvisioned}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	providerState := map[string]any{"bucket": ds.Bucket, "prefix": ds.Prefix, "format": ds.Format}
	if !ds.Lifecycle.Empty() {
		providerState["lifecycle"] = map[string]any{
			"expireAfterDays": ds.Lifecycle.ExpireAfterDays,
			"versioning":      ds.Lifecycle.Versioning,
		}
	}
	st.ProviderState = providerState
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
		if cfg.External {
			// Nothing was ever created for an external Provider (docs/planning/08
			// C4/§3.3) — this branch should be unreachable (the engine's
			// no-provider external path never calls Destroy), but stays a
			// safe no-op rather than an error if ever reached.
			return nil
		}
		if err := rt.Remove(ctx, name); err != nil {
			return err
		}
		if _, declared := nodesDeclared(cfg); declared {
			// A StableIdentity set's per-ordinal volumes are runtime-owned
			// and adapter-named (mirrors redpanda's Destroy, docs/adr/017
			// §a.1) — reclaim them through the labeled-volume listing
			// rather than guessing adapter naming.
			vols, err := rt.ListManagedVolumes(ctx)
			if err != nil {
				return err
			}
			prefix := name + "-data"
			for _, v := range vols {
				if v.Name == prefix || strings.HasPrefix(v.Name, prefix+"-") {
					if err := rt.RemoveVolume(ctx, v.Name); err != nil {
						return err
					}
				}
			}
		} else if err := rt.RemoveVolume(ctx, name+"-data"); err != nil {
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
		// If the backing store is a managed instance that's already gone
		// (killed out-of-band), its data went with it — nothing left to
		// remove, and failing here would strand the Dataset in state
		// forever. An external store has no container to have "gone".
		if !cfg.External {
			if ctr, found, err := rt.Inspect(ctx, name); err != nil || !found || !ctr.Running {
				return err
			}
		}
		addr, closeAddr, user, pass, secure, err := resolveDatasetDial(ctx, req, cfg, name)
		if err != nil {
			return err
		}
		defer closeAddr()
		cl, err := newClient(addr, user, pass, secure)
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
		if n, declared := nodesDeclared(cfg); declared && n >= 1 {
			return probeInstanceSet(ctx, rt, name, n)
		}
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
		addr, closeAddr, user, pass, secure, err := resolveDatasetDial(ctx, req, cfg, name)
		if err != nil {
			return st, err
		}
		defer closeAddr()
		cl, err := newClient(addr, user, pass, secure)
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
		// D7: lifecycle/versioning drift, only when spec.lifecycle declares
		// either — rule names/values only, never secrets.
		if !ds.Lifecycle.Empty() {
			drift, reason, err := probeLifecycleDrift(ctx, cl, ds, lifecycleRuleID(res.Metadata.Name))
			if err != nil {
				return st, err
			}
			if drift {
				st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: reason}, now)
				st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: reason}, now)
				return st, nil
			}
		}
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonDatasetHealthy}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
		return st, nil
	default:
		return st, fmt.Errorf("s3 provider cannot probe kind %s", res.Kind)
	}
}

// probeInstanceSet is the Provider probe for the distributed-MinIO shape
// (docs/planning/08 C4), mirroring redpanda's probeBrokerSet: per-ordinal
// container presence first (a missing/stopped ordinal is drift the runtime
// can report even with the whole cluster otherwise reachable), then S3 API
// reachability through any surviving node.
func probeInstanceSet(ctx context.Context, rt runtime.ContainerRuntime, name string, n int) (status.Status, error) {
	st := status.Status{}
	now := time.Now()
	var missing []string
	for i := 0; i < n; i++ {
		ord := runtime.OrdinalName(name, i)
		ordState, found, err := rt.Inspect(ctx, ord)
		if err != nil {
			return st, err
		}
		if !found || !ordState.Running {
			missing = append(missing, ord)
		}
	}
	if len(missing) > 0 {
		reason := fmt.Sprintf("%s(%s)", status.ReasonNodeMissing, strings.Join(missing, ","))
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: reason}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: reason}, now)
		return st, nil
	}
	_, closeAddr, err := anyReachableOrdinal(ctx, rt, name, n)
	if err != nil {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonNodeUnreachable}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonNodeUnreachable}, now)
		return st, nil //nolint:nilerr // the unreachability IS the probe finding, reported as drift
	}
	defer closeAddr()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonInstanceHealthy}, now)
	st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
	return st, nil
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
	if v, declared := cfg.Configuration["nodes"]; declared {
		// nodes' own positive-integer shape (docs/planning/08 E5) is now
		// schemas/v1alpha1/fragments/provider/s3.json's job, composed into
		// manifest.Validate ahead of this method in every real CLI path
		// (ADR 011's loadAndValidate order); a non-numeric nodes value
		// slipping past that layer leaves n at its zero value (-1) below,
		// which matches neither the 2/3 refusal nor any positive count —
		// still caught by the mutual-exclusion/HighAvailability paths a
		// real declared value would need to clear.
		n := -1
		switch t := v.(type) {
		case int:
			n = t
		case float64:
			if t == float64(int(t)) {
				n = int(t)
			}
		}
		// MinIO's erasure-coded distributed mode has no supported topology
		// between "1 node, no erasure coding" and "4+ nodes, erasure
		// coding" — 2-3 nodes is refused with a clear message rather than
		// silently accepted and failing unpredictably at container start
		// (docs/planning/08 C4).
		if n == 2 || n == 3 {
			return fmt.Errorf("spec.configuration.nodes: %d is not a supported MinIO topology (2-3 nodes has no erasure-coding scheme); use nodes: 1 (standalone) or nodes: 4 or more (distributed, erasure-coded)", n)
		}
		// Host-port pins cannot be combined with the ordinal-set shape:
		// every ordinal's host port is auto-assigned (docs/adr/004 known
		// limitation, same as redpanda's brokers — docs/adr/017 §a.4).
		if _, pinned := cfg.Configuration["port"]; pinned {
			return fmt.Errorf("spec.configuration.port cannot be combined with spec.configuration.nodes: each node's host port is auto-assigned")
		}
	}
	return nil
}
