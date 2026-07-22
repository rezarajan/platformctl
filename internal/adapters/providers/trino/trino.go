// Package trino realizes Provider(type: trino): a coordinator container plus
// N worker containers (via C1/C2's replica primitive, StableIdentity: false
// — workers are pure compute, no durable per-replica state) fronting the
// platform's Iceberg REST catalog. See docs/adr/006-compute-engines.md for
// the design decision and docs/planning/08 D10 for the task spec this
// package implements.
package trino

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/rezarajan/platformctl/internal/adapters/providers/providerkit"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

const (
	apiPort = 8080
	// defaultImage is a pinned Trino release (scripts/pinned-images.txt).
	defaultImage = "trinodb/trino:482@sha256:90b35b7c603eaa1f889bf03981a62b75f998ee6c0f851d9f4e341b49a57022b6"

	nodePropertiesPath   = "/etc/trino/node.properties"
	configPropertiesPath = "/etc/trino/config.properties"
	// nodeEnvironment must be identical across every node (coordinator and
	// every worker) of one Trino cluster — Trino refuses to let a node with
	// a different node.environment join.
	nodeEnvironment = "datascape"
)

var httpClient = &http.Client{Timeout: 10 * time.Second}

// Provider holds no cross-call state (docs/planning/08 F5): every method
// receives what it needs via reconciler.Request.
type Provider struct{}

func New() *Provider { return &Provider{} }

func (p *Provider) Type() string { return "trino" }

// coordinatorName/workerSetName derive the two runtime-object names this
// provider owns from the Provider resource's own name (naming.
// RuntimeObjectName is the single authority for "what's this resource's own
// runtime object called" — coordinator/worker are two *additional* objects a
// single Provider resource realizes, so this package suffixes that name
// rather than asking the naming package to know about a shape it has no
// reason to: see naming's package doc for why "the realizing resource" is
// deliberately the only convention it owns).
func coordinatorName(provEnv resource.Envelope) string {
	return naming.RuntimeObjectName(provEnv) + "-coordinator"
}

func workerSetName(provEnv resource.Envelope) string {
	return naming.RuntimeObjectName(provEnv) + "-worker"
}

// workerCount reads spec.configuration.workers (default 1) — a positive
// integer, validated by ValidateSpec; this reader stays defensive (clamped
// to 1) so a caller invoking Reconcile/Probe directly in a test without
// going through validate first still gets sane behavior, the same posture
// redpanda's brokersDeclared documents.
func workerCount(cfg provider.Provider) (int, error) {
	v, ok := cfg.Configuration["workers"]
	if !ok {
		return 1, nil
	}
	n := 0
	switch t := v.(type) {
	case int:
		n = t
	case float64:
		n = int(t)
	default:
		return 0, fmt.Errorf("spec.configuration.workers must be an integer, got %v", v)
	}
	if n < 1 {
		return 0, fmt.Errorf("spec.configuration.workers must be >= 1, got %d", n)
	}
	return n, nil
}

func (p *Provider) Reconcile(ctx context.Context, req reconciler.Request) (status.Status, error) {
	switch req.Resource.Kind {
	case "Provider":
		return p.reconcileInstance(ctx, req)
	default:
		return status.Status{}, fmt.Errorf("trino provider cannot reconcile kind %s", req.Resource.Kind)
	}
}

// catalogFile renders the desired etc/catalog/lakehouse.properties content
// for this request, or nil when configuration.catalogRef is unset/not yet
// resolvable. It resolves the warehouse Provider's credential *values* from
// req.Secrets, keyed by req.CatalogFacts.S3SecretRef (a graph fact, not a
// value — see reconciler.CatalogFacts's doc comment): that name must also
// appear in this Provider's own spec.secretRefs for the engine to have
// resolved it, exactly the s3 provider's own configuration.rootSecretRef
// convention ("must also be listed in spec.secretRefs").
func catalogFile(req reconciler.Request, name string) ([]byte, error) {
	if req.CatalogFacts == nil {
		return nil, nil
	}
	facts := req.CatalogFacts
	creds, ok := req.Secrets[facts.S3SecretRef]
	if !ok {
		return nil, fmt.Errorf("Provider %q (type: trino): spec.configuration.catalogRef resolved a warehouse SecretReference %q, but it is not listed in this Provider's own spec.secretRefs for the engine to resolve — add it there", name, facts.S3SecretRef)
	}
	return renderCatalogConfig(facts.RestInternal, facts.S3Internal, creds["username"], creds["password"]), nil
}

func nodeProperties() []byte {
	return []byte(fmt.Sprintf("node.environment=%s\nnode.data-dir=/data/trino\n", nodeEnvironment))
}

func coordinatorConfigProperties() []byte {
	return []byte(fmt.Sprintf(`coordinator=true
node-scheduler.include-coordinator=false
http-server.http.port=%d
discovery.uri=http://localhost:%d
`, apiPort, apiPort))
}

func workerConfigProperties(coordName string) []byte {
	return []byte(fmt.Sprintf(`coordinator=false
http-server.http.port=%d
discovery.uri=http://%s:%d
`, apiPort, coordName, apiPort))
}

func healthCheck() *runtime.HealthCheck {
	return &runtime.HealthCheck{
		Test:     []string{"CMD-SHELL", fmt.Sprintf("curl -sf http://localhost:%d/v1/info || exit 1", apiPort)},
		Interval: 3 * time.Second,
		Timeout:  5 * time.Second,
		Retries:  60,
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
	coordName := coordinatorName(req.Provider)
	workerName := workerSetName(req.Provider)
	image, _ := cfg.Configuration["image"].(string)
	if image == "" {
		image = defaultImage
	}
	n, err := workerCount(cfg)
	if err != nil {
		return st, err
	}
	network := providerkit.Network(cfg)

	catFile, err := catalogFile(req, name)
	if err != nil {
		return st, err
	}

	// Drift-heal (docs/planning/08 D10): etc/catalog/lakehouse.properties is
	// laid down only at container-create time (ContainerRuntime has no
	// write-into-a-running-container primitive, only ReadFile) —
	// EnsureContainer's own spec-hash idempotency cannot see an out-of-band
	// edit to the *live* filesystem, because the desired hash never changes
	// when the underlying facts don't. Reconcile therefore reads the live
	// file itself and forces a recreate (Remove, so EnsureContainer's
	// create path re-copies Files content) when it has drifted — the
	// config-file analogue of debezium/s3sink's "PUT the connector config
	// again every Reconcile" self-heal, adapted to a file-mounted config
	// with no REST endpoint of its own to re-PUT against.
	if catFile != nil {
		if drifted, checked := coordinatorCatalogDrifted(ctx, rt, coordName, catFile); checked && drifted {
			_ = rt.Remove(ctx, coordName)
		}
	}

	coordFiles := []runtime.FileMount{
		{Path: nodePropertiesPath, Content: nodeProperties(), Mode: 0o444},
		{Path: configPropertiesPath, Content: coordinatorConfigProperties(), Mode: 0o444},
	}
	if catFile != nil {
		coordFiles = append(coordFiles, runtime.FileMount{Path: lakehouseCatalogPath, Content: catFile, Mode: 0o444})
	}

	// Both containers are CREATED before either readiness wait below: a
	// worker only needs the coordinator's container (hence DNS name) to
	// exist to start up and announce itself — not for the coordinator to
	// have already finished its own internal startup — so there is no
	// correctness reason to serialize worker creation behind the
	// coordinator's full /v1/info readiness, and doing the two
	// EnsureContainer calls back-to-back lets both start warming up in
	// parallel instead of strictly sequentially.
	ctrState, err := providerkit.EnsureInstance(ctx, rt, providerkit.InstanceSpec{
		Namespace: req.Provider.Metadata.Namespace,
		Name:      coordName,
		Network:   network,
		Container: runtime.ContainerSpec{
			Image:       image,
			Files:       coordFiles,
			Ports:       []runtime.PortBinding{{HostPort: providerkit.HostPort(cfg, coordName, "port"), ContainerPort: apiPort, Audience: runtime.AudienceHost}},
			HealthCheck: healthCheck(),
		},
		WaitTimeout: 180 * time.Second,
	})
	if err != nil {
		return st, err
	}

	workerFiles := []runtime.FileMount{
		{Path: nodePropertiesPath, Content: nodeProperties(), Mode: 0o444},
		{Path: configPropertiesPath, Content: workerConfigProperties(coordName), Mode: 0o444},
	}
	if catFile != nil {
		// Every node (not just the coordinator) needs the identical catalog
		// config: workers execute query fragments against the Iceberg
		// connector directly. Byte-identical across every ordinal, which is
		// what ADR 004's "one identical spec for every ordinal" requires.
		workerFiles = append(workerFiles, runtime.FileMount{Path: lakehouseCatalogPath, Content: catFile, Mode: 0o444})
	}
	labels := runtime.ManagedLabels(req.Provider.Metadata.Namespace, "Provider", workerName, workerName)
	if err := rt.EnsureNetwork(ctx, runtime.NetworkSpec{Name: network, Labels: labels}); err != nil {
		return st, err
	}
	if _, err := rt.EnsureContainer(ctx, runtime.ContainerSpec{
		Name: workerName,
		// Networks: unlike the coordinator (providerkit.EnsureInstance sets
		// this from InstanceSpec.Network automatically), this path calls
		// EnsureContainer directly and must join the shared network itself
		// — live-caught omission (docs/adr/006's Implementation notes): a
		// worker set with no Networks landed on Docker's default "bridge"
		// network instead, unable to resolve the coordinator's DNS name at
		// all (discovery announcement failed silently forever, no query
		// ever left QUEUED — indistinguishable from the outside from the
		// S3-region hang this same session already found and fixed).
		Networks:       []string{network},
		Image:          image,
		Files:          workerFiles,
		Ports:          []runtime.PortBinding{{ContainerPort: apiPort, Audience: runtime.AudienceInternal}},
		HealthCheck:    healthCheck(),
		Labels:         labels,
		Replicas:       n,
		StableIdentity: false,
	}); err != nil {
		return st, err
	}

	if err := waitCoordinatorReady(ctx, rt, coordName, 180*time.Second); err != nil {
		return st, err
	}
	if err := rt.WaitHealthy(ctx, workerName, 180*time.Second); err != nil {
		return st, err
	}

	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonCoordinatorHealthy}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	hostAddr := ctrState.HostAddr(apiPort) // observed binding, not intent
	hostURL := ""
	if hostAddr != "" {
		hostURL = "http://" + hostAddr
	}
	st.ProviderState = map[string]any{
		"containerId":  ctrState.ID,
		"workers":      n,
		"internalAddr": fmt.Sprintf("http://%s:%d", coordName, apiPort),
		endpoint.Key: endpoint.List{
			{
				Name: "trino", Scheme: "http", Host: hostURL, Internal: fmt.Sprintf("%s:%d", coordName, apiPort),
				Insecure: true, RuntimeName: coordName, ContainerPort: apiPort, Audience: runtime.AudienceHost, Network: network,
			},
		}.ToState(),
	}
	return st, nil
}

// coordinatorCatalogDrifted reads the live lakehouse.properties (if the
// coordinator container already exists) and diffs it against desired,
// returning checked=false when there is nothing to compare yet (no
// container, or the file was never written) — the caller must not treat
// that as "drifted".
func coordinatorCatalogDrifted(ctx context.Context, rt runtime.ContainerRuntime, coordName string, desired []byte) (drifted bool, checked bool) {
	if _, found, err := rt.Inspect(ctx, coordName); err != nil || !found {
		return false, false
	}
	live, err := rt.ReadFile(ctx, coordName, lakehouseCatalogPath)
	if err != nil {
		return false, false
	}
	d := diffCatalogConfig(parseProperties(desired), parseProperties(live))
	return len(d) > 0, true
}

// waitCoordinatorReady polls /v1/info via runtime.WithReachable
// (docs/planning/09 Class 2 / F1) until it answers 200 with starting: false
// — the same defensive re-resolve-per-attempt pattern nessie's
// waitAPIReady/prometheus's waitReady document. A healthy container
// HealthCheck alone is not enough: Trino's HTTP server can answer 200 on
// /v1/info while the query engine itself is still initializing
// ("starting": true), the same class of gap prometheus's waitReady closes
// for /-/ready vs /api/v1/targets.
func waitCoordinatorReady(ctx context.Context, rt runtime.ContainerRuntime, name string, timeout time.Duration) error {
	opts := runtime.ReachableOptions{Timeout: timeout, Interval: 2 * time.Second}
	err := runtime.WithReachable(ctx, rt, name, apiPort, opts, func(ctx context.Context, addr string) error {
		info, ok := fetchInfo(ctx, "http://"+addr)
		if !ok {
			return fmt.Errorf("/v1/info did not answer 200")
		}
		if info.Starting {
			return fmt.Errorf("coordinator reports starting: true")
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("trino coordinator did not become ready within %s: %w", timeout, err)
	}
	return nil
}

// infoResponse is the subset of Trino's /v1/info response this package
// reads (docs/planning/08 D10's Probe contract: "reachable and starting:
// false").
type infoResponse struct {
	Starting    bool `json:"starting"`
	Coordinator bool `json:"coordinator"`
}

func fetchInfo(ctx context.Context, baseURL string) (infoResponse, bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/v1/info", nil)
	if err != nil {
		return infoResponse{}, false
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return infoResponse{}, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return infoResponse{}, false
	}
	var info infoResponse
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return infoResponse{}, false
	}
	return info, true
}

func (p *Provider) Destroy(ctx context.Context, req reconciler.Request) error {
	rt := req.Runtime
	switch req.Resource.Kind {
	case "Provider":
		coordName := coordinatorName(req.Provider)
		workerName := workerSetName(req.Provider)
		if err := rt.Remove(ctx, coordName); err != nil {
			return err
		}
		if err := rt.Remove(ctx, workerName); err != nil {
			return err
		}
		cfg, err := provider.FromEnvelope(req.Provider)
		if err != nil {
			return err
		}
		// The network may still be shared with every other provider on it;
		// RemoveNetwork refuses (and this ignores that refusal) while any
		// container remains attached — the same convention every other
		// single-container provider's Destroy follows.
		_ = rt.RemoveNetwork(ctx, providerkit.Network(cfg))
		return nil
	default:
		return fmt.Errorf("trino provider cannot destroy kind %s", req.Resource.Kind)
	}
}

func (p *Provider) Probe(ctx context.Context, req reconciler.Request) (status.Status, error) {
	rt := req.Runtime
	st := status.Status{}
	now := time.Now()
	switch req.Resource.Kind {
	case "Provider":
		cfg, err := provider.FromEnvelope(req.Provider)
		if err != nil {
			return st, err
		}
		n, err := workerCount(cfg)
		if err != nil {
			return st, err
		}
		coordName := coordinatorName(req.Provider)
		workerName := workerSetName(req.Provider)

		coordState, found, err := rt.Inspect(ctx, coordName)
		if err != nil {
			return st, err
		}
		if !found || !coordState.Healthy {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonCoordinatorUnhealthy}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonCoordinatorUnhealthy}, now)
			return st, nil
		}

		workerState, _, err := rt.Inspect(ctx, workerName)
		if err != nil {
			return st, err
		}
		if workerState.ReadyReplicas != n {
			msg := fmt.Sprintf("declared workers: %d, ready replicas observed: %d", n, workerState.ReadyReplicas)
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonWorkerCountMismatch, Message: msg}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonWorkerCountMismatch, Message: msg}, now)
			return st, nil
		}

		addr, closeAddr, err := providerkit.ReachableAddr(ctx, rt, coordName, apiPort)
		if err != nil {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonCoordinatorUnhealthy, Message: err.Error()}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonCoordinatorUnhealthy}, now)
			return st, nil
		}
		info, ok := fetchInfo(ctx, "http://"+addr)
		closeAddr()
		if !ok || info.Starting {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonCoordinatorUnhealthy}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonCoordinatorUnhealthy}, now)
			return st, nil
		}

		// Catalog config drift (docs/planning/08 D10's "debezium bar" —
		// regenerated on probe, diffed by keys): only checked when
		// catalogRef is declared at all (spec.configuration.catalogRef).
		catalogRef := resource.RefFromSpec(cfg.Configuration, "catalogRef")
		if catalogRef.Name != "" {
			desired, err := catalogFile(req, req.Provider.Metadata.Name)
			if err != nil || desired == nil {
				msg := "catalogRef declared but its facts are not yet resolvable (has the referenced Catalog been applied?)"
				if err != nil {
					msg = err.Error()
				}
				st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonCatalogConfigMissing, Message: msg}, now)
				st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonCatalogConfigMissing, Message: msg}, now)
				return st, nil
			}
			live, err := rt.ReadFile(ctx, coordName, lakehouseCatalogPath)
			if err != nil {
				st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonCatalogConfigMissing, Message: err.Error()}, now)
				st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonCatalogConfigMissing}, now)
				return st, nil
			}
			if drifted := diffCatalogConfig(parseProperties(desired), parseProperties(live)); len(drifted) > 0 {
				msg := "etc/catalog/lakehouse.properties differs from generated at: " + joinKeys(drifted)
				st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonCatalogConfigDrift, Message: msg}, now)
				st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonCatalogConfigDrift, Message: msg}, now)
				return st, nil
			}
		}

		st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonCoordinatorHealthy}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
		return st, nil
	default:
		return st, fmt.Errorf("trino provider cannot probe kind %s", req.Resource.Kind)
	}
}

func joinKeys(keys []string) string {
	out := ""
	for i, k := range keys {
		if i > 0 {
			out += ", "
		}
		out += k
	}
	return out
}

// ValidateSpec implements reconciler.SpecValidator: an out-of-range workers
// count or a malformed image fails at validate, never as a half-applied
// platform.
func (p *Provider) ValidateSpec(cfg provider.Provider) error {
	if _, err := workerCount(cfg); err != nil {
		return err
	}
	if v, ok := cfg.Configuration["image"]; ok {
		if s, isStr := v.(string); !isStr || s == "" {
			return fmt.Errorf("spec.configuration.image must be a non-empty string, got %v", v)
		}
	}
	return nil
}
