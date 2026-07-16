// Package s3sink reconciles a Kafka Connect worker carrying an S3 sink
// connector plugin (Aiven's s3-connector-for-apache-kafka is the reference)
// and registers/updates sink connectors realizing Binding(mode: sink):
// EventStream topics land as objects under a Dataset's bucket/prefix.
// Implements SinkCapableProvider (Phase 4).
package s3sink

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/rezarajan/platformctl/internal/adapters/kafkaconnect"
	"github.com/rezarajan/platformctl/internal/domain/binding"
	"github.com/rezarajan/platformctl/internal/domain/dataset"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/hostport"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// The stock Debezium/Connect images ship no S3 sink plugin, so there is no
// usable default image — spec.configuration.image is required and must
// contain the Aiven S3 sink connector on the plugin path.
const connectorClass = "io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector"

type Provider struct {
	providerRes resource.Envelope
	cfg         provider.Provider
	secrets     map[string]map[string]string
	resources   map[resource.Key]resource.Envelope
}

func New() *Provider { return &Provider{} }

func (p *Provider) Type() string { return "s3sink" }

// SupportedSinkFormats implements SinkCapableProvider. These are the output
// formats of the Aiven S3 sink connector; parquet additionally requires
// schema-carrying records at runtime.
func (p *Provider) SupportedSinkFormats() []string {
	return []string{"json", "jsonl", "csv", "parquet"}
}

func (p *Provider) SetProviderResource(env resource.Envelope) {
	p.providerRes = env
	p.cfg, _ = provider.FromEnvelope(env)
}

func (p *Provider) SetSecrets(secrets map[string]map[string]string) { p.secrets = secrets }

func (p *Provider) SetResourceSet(byKey map[resource.Key]resource.Envelope) { p.resources = byKey }

func (p *Provider) containerName() string { return p.providerRes.Metadata.Name }

func (p *Provider) connectPort() int {
	configured := 0
	if v, ok := p.cfg.Configuration["connectPort"]; ok {
		switch n := v.(type) {
		case int:
			configured = n
		case float64:
			configured = int(n)
		}
	}
	return hostport.Resolve(configured, p.containerName())
}

func (p *Provider) connectURL() string { return "http://127.0.0.1:" + strconv.Itoa(p.connectPort()) }

func (p *Provider) network() string {
	if n, ok := p.cfg.RuntimeConfig["network"].(string); ok && n != "" {
		return n
	}
	return "datascape"
}

func (p *Provider) Reconcile(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) (status.Status, error) {
	switch res.Kind {
	case "Provider":
		return p.reconcileWorker(ctx, rt)
	case "Binding":
		return p.reconcileConnector(ctx, res)
	default:
		return status.Status{}, fmt.Errorf("s3sink provider cannot reconcile kind %s", res.Kind)
	}
}

func (p *Provider) reconcileWorker(ctx context.Context, rt runtime.ContainerRuntime) (status.Status, error) {
	st := status.Status{}
	name := p.containerName()
	image, _ := p.cfg.Configuration["image"].(string)
	if image == "" {
		return st, fmt.Errorf("Provider %q (type: s3sink): spec.configuration.image is required (a Connect image carrying the S3 sink plugin)", name)
	}
	bootstrap, _ := p.cfg.Configuration["bootstrapServers"].(string)
	if bootstrap == "" {
		return st, fmt.Errorf("Provider %q (type: s3sink): spec.configuration.bootstrapServers is required", name)
	}
	labels := runtime.ManagedLabels(p.providerRes.Metadata.Namespace, "Provider", name, name)

	if err := rt.EnsureNetwork(ctx, runtime.NetworkSpec{Name: p.network(), Labels: labels}); err != nil {
		return st, err
	}
	ctrState, err := rt.EnsureContainer(ctx, runtime.ContainerSpec{
		Name:  name,
		Image: image,
		Env: map[string]string{
			"BOOTSTRAP_SERVERS":                      bootstrap,
			"GROUP_ID":                               name,
			"CONFIG_STORAGE_TOPIC":                   name + "-configs",
			"OFFSET_STORAGE_TOPIC":                   name + "-offsets",
			"STATUS_STORAGE_TOPIC":                   name + "-status",
			"CONFIG_STORAGE_REPLICATION_FACTOR":      "1",
			"OFFSET_STORAGE_REPLICATION_FACTOR":      "1",
			"STATUS_STORAGE_REPLICATION_FACTOR":      "1",
			"KEY_CONVERTER":                          "org.apache.kafka.connect.json.JsonConverter",
			"VALUE_CONVERTER":                        "org.apache.kafka.connect.json.JsonConverter",
			"CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE":   "false",
			"CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE": "false",
			// Sink files are written on offset commit; the 60s default makes
			// every reconcile-and-verify cycle glacial.
			"OFFSET_FLUSH_INTERVAL_MS": "5000",
			// topics.regex subscriptions only discover topics created after
			// connector registration on consumer metadata refresh — the 5min
			// default would stall CDC topics that appear on first table event.
			"CONNECT_CONSUMER_METADATA_MAX_AGE_MS": "10000",
		},
		Networks: []string{p.network()},
		Ports:    []runtime.PortBinding{{HostPort: p.connectPort(), ContainerPort: 8083}},
		HealthCheck: &runtime.HealthCheck{
			Test:     []string{"CMD-SHELL", "curl -sf http://localhost:8083/connectors || exit 1"},
			Interval: 3 * time.Second,
			Timeout:  5 * time.Second,
			Retries:  40,
		},
		Labels: labels,
	})
	if err != nil {
		return st, err
	}
	if err := rt.WaitHealthy(ctx, name, 180*time.Second); err != nil {
		return st, err
	}

	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "ConnectWorkerHealthy"}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: "ReconcileComplete"}, now)
	// Observed binding, not intent (connectURL() stays intent-based for the
	// provider's own REST calls, a documented Docker-mode assumption).
	hostAddr := ctrState.HostAddr(8083)
	hostURL := ""
	if hostAddr != "" {
		hostURL = "http://" + hostAddr
	}
	st.ProviderState = map[string]any{
		"connectUrl": p.connectURL(),
		endpoint.Key: endpoint.List{
			{Name: "connect-rest", Scheme: "http", Host: hostURL, Internal: fmt.Sprintf("http://%s:8083", p.containerName())},
		}.ToState(),
	}
	return st, nil
}

// desiredConnectorConfig builds the manifest-derived connector config —
// shared by reconcile (to register) and Probe (to diff against the live
// config; docs/planning/07 §2.1: RUNNING with the wrong bucket, topic
// filter, or credentials is drift, not health).
func (p *Provider) desiredConnectorConfig(res resource.Envelope) (string, map[string]string, error) {
	b, err := binding.FromEnvelope(res)
	if err != nil {
		return "", nil, err
	}
	if b.Mode != binding.ModeSink {
		return "", nil, fmt.Errorf("Binding %q: s3sink realizes mode \"sink\" only, got %q", res.Metadata.Name, b.Mode)
	}

	sourceRef := resource.RefFromSpec(res.Spec, "sourceRef")
	if _, ok := p.resources[sourceRef.Key(res.Metadata.Namespace, "EventStream")]; !ok {
		return "", nil, fmt.Errorf("Binding %q: sourceRef %q not found", res.Metadata.Name, b.SourceRef)
	}
	targetRef := resource.RefFromSpec(res.Spec, "targetRef")
	dsEnv, ok := p.resources[targetRef.Key(res.Metadata.Namespace, "Dataset")]
	if !ok {
		return "", nil, fmt.Errorf("Binding %q: targetRef %q not found", res.Metadata.Name, b.TargetRef)
	}
	ds, err := dataset.FromEnvelope(dsEnv)
	if err != nil {
		return "", nil, err
	}

	endpoint, err := p.objectStoreEndpoint(dsEnv, ds, b)
	if err != nil {
		return "", nil, fmt.Errorf("Binding %q: %w", res.Metadata.Name, err)
	}

	credsRefName, _ := p.cfg.Configuration["credentialsSecretRef"].(string)
	creds, ok := p.secrets[credsRefName]
	if !ok {
		return "", nil, fmt.Errorf("Binding %q: s3sink Provider %q needs configuration.credentialsSecretRef naming a declared secretRef", res.Metadata.Name, p.containerName())
	}

	config := map[string]string{
		"connector.class":       connectorClass,
		"tasks.max":             "1",
		"aws.access.key.id":     creds["username"],
		"aws.secret.access.key": creds["password"],
		"aws.s3.bucket.name":    ds.Bucket,
		"aws.s3.endpoint":       endpoint,
		"aws.s3.region":         "us-east-1",
		// CDC traffic arrives on per-table topics prefixed with the
		// EventStream name (<stream>.<schema>.<table>); match the stream's own
		// topic and any prefixed ones. The name is regex-quoted so a topic
		// name containing regex metacharacters (e.g. a '.') matches
		// literally instead of as a wildcard (docs/planning/07 §2.2).
		"topics.regex":                   "^" + regexp.QuoteMeta(b.SourceRef) + "(\\..*)?$",
		"format.output.type":             ds.Format,
		"format.output.fields":           "value",
		"file.compression.type":          "none",
		"key.converter":                  "org.apache.kafka.connect.json.JsonConverter",
		"value.converter":                "org.apache.kafka.connect.json.JsonConverter",
		"key.converter.schemas.enable":   "false",
		"value.converter.schemas.enable": "false",
	}
	if ds.Prefix != "" {
		config["aws.s3.prefix"] = ds.Prefix
	}
	return res.Metadata.Name, config, nil
}

// reconcileConnector registers or updates the S3 sink connector realizing a
// Binding(mode: sink), then verifies it reaches RUNNING.
func (p *Provider) reconcileConnector(ctx context.Context, res resource.Envelope) (status.Status, error) {
	st := status.Status{}
	connectorName, config, err := p.desiredConnectorConfig(res)
	if err != nil {
		return st, err
	}

	if err := kafkaconnect.PutConnectorConfig(ctx, p.connectURL(), connectorName, config); err != nil {
		return st, err
	}

	state, err := kafkaconnect.WaitConnectorRunning(ctx, p.connectURL(), connectorName, 90*time.Second)
	now := time.Now()
	if err != nil {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: "ConnectorNotRunning", Message: err.Error()}, now)
		st.SetCondition(status.Condition{Type: status.Degraded, Status: status.True, Reason: "ConnectorState" + state}, now)
		return st, err
	}
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "ConnectorRunning"}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: "ReconcileComplete"}, now)
	st.ProviderState = map[string]any{"connector": connectorName, "state": state}
	return st, nil
}

// objectStoreEndpoint resolves the S3 endpoint reachable from the Connect
// worker container: an explicit options.endpoint wins (external stores),
// otherwise the Dataset's Provider container on the shared network.
func (p *Provider) objectStoreEndpoint(dsEnv resource.Envelope, ds dataset.Dataset, b binding.Binding) (string, error) {
	if ep, ok := b.Options["endpoint"].(string); ok && ep != "" {
		return ep, nil
	}
	if ds.ProviderRef == "" {
		return "", fmt.Errorf("cannot determine object-store endpoint (no providerRef on Dataset and no options.endpoint)")
	}
	providerRef := resource.RefFromSpec(dsEnv.Spec, "providerRef")
	if _, ok := p.resources[providerRef.Key(dsEnv.Metadata.Namespace, "Provider")]; !ok {
		return "", fmt.Errorf("Dataset providerRef %q not found", ds.ProviderRef)
	}
	// The s3 provider always serves the S3 API on 9000 inside the network.
	return "http://" + ds.ProviderRef + ":9000", nil
}

func (p *Provider) Destroy(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) error {
	switch res.Kind {
	case "Provider":
		if err := rt.Remove(ctx, p.containerName()); err != nil {
			return err
		}
		_ = rt.RemoveNetwork(ctx, p.network())
		return nil
	case "Binding":
		// A dead Connect worker takes its connectors with it; requiring a
		// live REST API here would make destroy unable to converge after
		// out-of-band failures.
		if ctr, found, err := rt.Inspect(ctx, p.containerName()); err != nil || !found || !ctr.Running {
			return err
		}
		return kafkaconnect.DeleteConnector(ctx, p.connectURL(), res.Metadata.Name)
	default:
		return fmt.Errorf("s3sink provider cannot destroy kind %s", res.Kind)
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
		if found && ctrState.Healthy {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "ConnectWorkerHealthy"}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: "NoDrift"}, now)
		} else {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: "ConnectWorkerUnhealthy"}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: "ConnectWorkerUnhealthy"}, now)
		}
		return st, nil
	case "Binding":
		state, err := kafkaconnect.ConnectorState(ctx, p.connectURL(), res.Metadata.Name)
		if err != nil {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: "ConnectorMissing", Message: err.Error()}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: "ConnectorMissing"}, now)
			return st, nil
		}
		if state != "RUNNING" {
			// Declared state is a RUNNING connector; anything else is drift.
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: "ConnectorState" + state}, now)
			st.SetCondition(status.Condition{Type: status.Degraded, Status: status.True, Reason: "ConnectorState" + state}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: "ConnectorState" + state}, now)
			return st, nil
		}
		// RUNNING is not enough (docs/planning/07 §2.1): the live config
		// must still match the manifest-derived one. Drifted key *names*
		// only — values may carry credentials and must never leak into
		// conditions.
		if drifted := p.connectorConfigDrift(ctx, res); len(drifted) > 0 {
			msg := "connector config differs from manifest at: " + strings.Join(drifted, ", ")
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: "ConnectorConfigDrift", Message: msg}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: "ConnectorConfigDrift", Message: msg}, now)
			return st, nil
		}
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "ConnectorRunning"}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: "NoDrift"}, now)
		return st, nil
	default:
		return st, fmt.Errorf("s3sink provider cannot probe kind %s", res.Kind)
	}
}

// connectorConfigDrift diffs the live connector config against the
// manifest-derived one and returns the drifted key names (sorted), or nil
// when equivalent. Extra live keys beyond the desired set are Connect-added
// defaults, not drift.
func (p *Provider) connectorConfigDrift(ctx context.Context, res resource.Envelope) []string {
	name, desired, err := p.desiredConnectorConfig(res)
	if err != nil {
		return []string{"(desired config unresolvable: " + err.Error() + ")"}
	}
	actual, err := kafkaconnect.GetConnectorConfig(ctx, p.connectURL(), name)
	if err != nil {
		return []string{"(live config unreadable: " + err.Error() + ")"}
	}
	var drifted []string
	for k, want := range desired {
		if actual[k] != want {
			drifted = append(drifted, k)
		}
	}
	sort.Strings(drifted)
	return drifted
}

// ValidateSpec implements SpecValidator: this provider exists only to run
// sink connectors, so everything a connector registration needs is required
// up front — at validate, never as a half-applied platform.
func (p *Provider) ValidateSpec(cfg provider.Provider) error {
	if v, _ := cfg.Configuration["image"].(string); v == "" {
		return fmt.Errorf("spec.configuration.image is required (a Connect image carrying the S3 sink plugin; no stock image ships one)")
	}
	if v, _ := cfg.Configuration["bootstrapServers"].(string); v == "" {
		return fmt.Errorf("spec.configuration.bootstrapServers is required (the Kafka address the Connect worker joins)")
	}
	ref, _ := cfg.Configuration["credentialsSecretRef"].(string)
	if ref == "" {
		return fmt.Errorf("spec.configuration.credentialsSecretRef is required (the SecretReference carrying object-store credentials)")
	}
	if !cfg.HasSecretRef(ref) {
		return fmt.Errorf("configuration.credentialsSecretRef %q must also be listed in spec.secretRefs for the engine to resolve it", ref)
	}
	return nil
}

// ValidateBindingOptions implements reconciler.BindingOptionsValidator: the
// sink endpoint override must be a well-formed URL at validate time, not an
// apply-time connector failure.
func (p *Provider) ValidateBindingOptions(_ string, options map[string]any) error {
	if v, ok := options["endpoint"]; ok {
		ep, _ := v.(string)
		if ep == "" {
			return fmt.Errorf("options.endpoint must be a non-empty URL when set")
		}
		u, err := url.Parse(ep)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return fmt.Errorf("options.endpoint %q is not a valid URL (need scheme://host[:port])", ep)
		}
	}
	return nil
}
