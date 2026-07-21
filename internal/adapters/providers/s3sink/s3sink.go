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
	"strings"
	"time"

	"github.com/rezarajan/platformctl/internal/adapters/kafkaconnect"
	"github.com/rezarajan/platformctl/internal/adapters/providers/providerkit"
	"github.com/rezarajan/platformctl/internal/domain/binding"
	"github.com/rezarajan/platformctl/internal/domain/dataset"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// The stock Debezium/Connect images ship no S3 sink plugin, so there is no
// usable default image — spec.configuration.image is required and must
// contain the Aiven S3 sink connector on the plugin path.
const connectorClass = "io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector"

// Provider holds no cross-call state (docs/planning/08 F5): every method
// receives what it needs via reconciler.Request.
type Provider struct{}

func New() *Provider { return &Provider{} }

func (p *Provider) Type() string { return "s3sink" }

// SupportedSinkFormats implements SinkCapableProvider. These are the output
// formats of the Aiven S3 sink connector; parquet additionally requires
// schema-carrying records at runtime.
func (p *Provider) SupportedSinkFormats() []string {
	return []string{"json", "jsonl", "csv", "parquet"}
}

func connectPort(cfg provider.Provider, name string) int {
	return providerkit.HostPort(cfg, name, "connectPort")
}

// reachableURL returns an "http://host:port" this process can dial right
// now for the Connect worker's REST API, plus a close func that must always
// be called. Kafka Connect's REST API is stateless HTTP with no
// broker-style redirect protocol, so the resolved address can be used
// directly for one call.
func reachableURL(ctx context.Context, rt runtime.ContainerRuntime, name string) (string, func() error, error) {
	return providerkit.ReachableURL(ctx, rt, name, 8083)
}

func (p *Provider) Reconcile(ctx context.Context, req reconciler.Request) (status.Status, error) {
	switch req.Resource.Kind {
	case "Provider":
		return p.reconcileWorker(ctx, req)
	case "Binding":
		return p.reconcileConnector(ctx, req)
	default:
		return status.Status{}, fmt.Errorf("s3sink provider cannot reconcile kind %s", req.Resource.Kind)
	}
}

func (p *Provider) reconcileWorker(ctx context.Context, req reconciler.Request) (status.Status, error) {
	rt := req.Runtime
	st := status.Status{}
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return st, err
	}
	name := naming.RuntimeObjectName(req.Provider)
	image, _ := cfg.Configuration["image"].(string)
	if image == "" {
		return st, fmt.Errorf("Provider %q (type: s3sink): spec.configuration.image is required (a Connect image carrying the S3 sink plugin)", name)
	}
	bootstrap, _ := cfg.Configuration["bootstrapServers"].(string)
	if bootstrap == "" {
		// Graph-inferred (docs/planning/08 E2): the engine already resolved
		// this from the Binding(s) wired to this worker's target/source
		// EventStream, when unambiguous — req.KafkaBootstrapServers.
		bootstrap = req.KafkaBootstrapServers
	}
	if bootstrap == "" {
		return st, fmt.Errorf("Provider %q (type: s3sink): spec.configuration.bootstrapServers is required (declare it, or wire a Binding on this Provider to an EventStream whose Provider publishes a Kafka bootstrap address)", name)
	}
	ctrState, err := providerkit.EnsureInstance(ctx, rt, providerkit.InstanceSpec{
		Namespace: req.Provider.Metadata.Namespace,
		Name:      name,
		Network:   providerkit.Network(cfg),
		Container: runtime.ContainerSpec{
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
			Ports: []runtime.PortBinding{{HostPort: connectPort(cfg, name), ContainerPort: 8083, Audience: runtime.AudienceHost}},
			HealthCheck: &runtime.HealthCheck{
				Test:     []string{"CMD-SHELL", "curl -sf http://localhost:8083/connectors || exit 1"},
				Interval: 3 * time.Second,
				Timeout:  5 * time.Second,
				Retries:  40,
			},
		},
		WaitTimeout: 180 * time.Second,
	})
	if err != nil {
		return st, err
	}

	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonConnectWorkerHealthy}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	// Observed binding, not configured intent (docs/planning/09 F1: no
	// domain-layer address guess is kept around for this).
	hostAddr := ctrState.HostAddr(8083)
	hostURL := ""
	if hostAddr != "" {
		hostURL = "http://" + hostAddr
	}
	st.ProviderState = map[string]any{
		endpoint.Key: endpoint.List{
			{Name: "connect-rest", Scheme: "http", Host: hostURL, Internal: fmt.Sprintf("http://%s:8083", name), Insecure: true},
		}.ToState(),
		// The effective bootstrapServers this worker was actually started
		// with — declared or graph-inferred (docs/planning/08 E2) — so an
		// inferred default is as visible as an explicit one via
		// `platformctl state inspect`, not silently baked in.
		"bootstrapServers": bootstrap,
	}
	return st, nil
}

// desiredConnectorConfig builds the manifest-derived connector config —
// shared by reconcile (to register) and Probe (to diff against the live
// config; docs/planning/07 §2.1: RUNNING with the wrong bucket, topic
// filter, or credentials is drift, not health).
func desiredConnectorConfig(req reconciler.Request) (string, map[string]string, error) {
	res := req.Resource
	b, err := binding.FromEnvelope(res)
	if err != nil {
		return "", nil, err
	}
	if b.Mode != binding.ModeSink {
		return "", nil, fmt.Errorf("Binding %q: s3sink realizes mode \"sink\" only, got %q", res.Metadata.Name, b.Mode)
	}

	sourceRef := resource.RefFromSpec(res.Spec, "sourceRef")
	if _, ok := req.Resources[sourceRef.Key(res.Metadata.Namespace, "EventStream")]; !ok {
		return "", nil, fmt.Errorf("Binding %q: sourceRef %q not found", res.Metadata.Name, b.SourceRef)
	}
	targetRef := resource.RefFromSpec(res.Spec, "targetRef")
	dsEnv, ok := req.Resources[targetRef.Key(res.Metadata.Namespace, "Dataset")]
	if !ok {
		return "", nil, fmt.Errorf("Binding %q: targetRef %q not found", res.Metadata.Name, b.TargetRef)
	}
	ds, err := dataset.FromEnvelope(dsEnv)
	if err != nil {
		return "", nil, err
	}

	objectStoreEP, err := objectStoreEndpoint(req, dsEnv, ds, b)
	if err != nil {
		return "", nil, fmt.Errorf("Binding %q: %w", res.Metadata.Name, err)
	}

	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return "", nil, err
	}
	credsRefName, _ := cfg.Configuration["credentialsSecretRef"].(string)
	creds, ok := req.Secrets[credsRefName]
	if !ok {
		return "", nil, fmt.Errorf("Binding %q: s3sink Provider %q needs configuration.credentialsSecretRef naming a declared secretRef", res.Metadata.Name, naming.RuntimeObjectName(req.Provider))
	}

	config := map[string]string{
		"connector.class":       connectorClass,
		"tasks.max":             "1",
		"aws.access.key.id":     creds["username"],
		"aws.secret.access.key": creds["password"],
		"aws.s3.bucket.name":    ds.Bucket,
		"aws.s3.endpoint":       objectStoreEP,
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
func (p *Provider) reconcileConnector(ctx context.Context, req reconciler.Request) (status.Status, error) {
	st := status.Status{}
	connectorName, config, err := desiredConnectorConfig(req)
	if err != nil {
		return st, err
	}

	name := naming.RuntimeObjectName(req.Provider)
	url, closeURL, err := reachableURL(ctx, req.Runtime, name)
	if err != nil {
		return st, err
	}
	defer closeURL()
	if err := kafkaconnect.PutConnectorConfig(ctx, url, connectorName, config); err != nil {
		return st, err
	}

	state, err := kafkaconnect.WaitConnectorRunning(ctx, url, connectorName, 90*time.Second)
	now := time.Now()
	if err != nil {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonConnectorNotRunning, Message: err.Error()}, now)
		// ReasonConnectorState is a prefix: the observed live connector
		// state is appended so the reason names the exact state without a
		// separate Message (docs/planning/08 G4).
		st.SetCondition(status.Condition{Type: status.Degraded, Status: status.True, Reason: status.ReasonConnectorState + state}, now)
		return st, err
	}
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonConnectorRunning}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	st.ProviderState = map[string]any{"connector": connectorName, "state": state}
	return st, nil
}

// objectStoreEndpoint resolves the S3 endpoint reachable from the Connect
// worker container: an explicit options.endpoint wins (external stores),
// otherwise the Dataset's Provider container on the shared network.
func objectStoreEndpoint(req reconciler.Request, dsEnv resource.Envelope, ds dataset.Dataset, b binding.Binding) (string, error) {
	if ep, ok := b.Options["endpoint"].(string); ok && ep != "" {
		return ep, nil
	}
	if ds.ProviderRef == "" {
		return "", fmt.Errorf("cannot determine object-store endpoint (no providerRef on Dataset and no options.endpoint)")
	}
	providerRef := resource.RefFromSpec(dsEnv.Spec, "providerRef")
	if _, ok := req.Resources[providerRef.Key(dsEnv.Metadata.Namespace, "Provider")]; !ok {
		return "", fmt.Errorf("Dataset providerRef %q not found", ds.ProviderRef)
	}
	// The s3 provider always serves the S3 API on 9000 inside the network.
	return "http://" + ds.ProviderRef + ":9000", nil
}

func (p *Provider) Destroy(ctx context.Context, req reconciler.Request) error {
	res, rt := req.Resource, req.Runtime
	name := naming.RuntimeObjectName(req.Provider)
	switch res.Kind {
	case "Provider":
		cfg, err := provider.FromEnvelope(req.Provider)
		if err != nil {
			return err
		}
		if err := rt.Remove(ctx, name); err != nil {
			return err
		}
		_ = rt.RemoveNetwork(ctx, providerkit.Network(cfg))
		return nil
	case "Binding":
		// A dead Connect worker takes its connectors with it; requiring a
		// live REST API here would make destroy unable to converge after
		// out-of-band failures.
		if ctr, found, err := rt.Inspect(ctx, name); err != nil || !found || !ctr.Running {
			return err
		}
		url, closeURL, err := reachableURL(ctx, rt, name)
		if err != nil {
			return err
		}
		defer closeURL()
		return kafkaconnect.DeleteConnector(ctx, url, res.Metadata.Name)
	default:
		return fmt.Errorf("s3sink provider cannot destroy kind %s", res.Kind)
	}
}

func (p *Provider) Probe(ctx context.Context, req reconciler.Request) (status.Status, error) {
	res, rt := req.Resource, req.Runtime
	st := status.Status{}
	now := time.Now()
	name := naming.RuntimeObjectName(req.Provider)
	switch res.Kind {
	case "Provider":
		ctrState, found, err := rt.Inspect(ctx, name)
		if err != nil {
			return st, err
		}
		if found && ctrState.Healthy {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonConnectWorkerHealthy}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
		} else {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonConnectWorkerUnhealthy}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonConnectWorkerUnhealthy}, now)
		}
		return st, nil
	case "Binding":
		url, closeURL, err := reachableURL(ctx, rt, name)
		if err != nil {
			return st, err
		}
		defer closeURL()
		state, err := kafkaconnect.ConnectorState(ctx, url, res.Metadata.Name)
		if err != nil {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonConnectorMissing, Message: err.Error()}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonConnectorMissing}, now)
			return st, nil
		}
		if state != "RUNNING" {
			// Declared state is a RUNNING connector; anything else is drift.
			// ReasonConnectorState is a prefix: the observed live connector
			// state is appended so the reason names the exact state without
			// a separate Message (docs/planning/08 G4).
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonConnectorState + state}, now)
			st.SetCondition(status.Condition{Type: status.Degraded, Status: status.True, Reason: status.ReasonConnectorState + state}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonConnectorState + state}, now)
			return st, nil
		}
		// RUNNING is not enough (docs/planning/07 §2.1): the live config
		// must still match the manifest-derived one. Drifted key *names*
		// only — values may carry credentials and must never leak into
		// conditions.
		if drifted := connectorConfigDrift(ctx, req, url); len(drifted) > 0 {
			msg := "connector config differs from manifest at: " + strings.Join(drifted, ", ")
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonConnectorConfigDrift, Message: msg}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonConnectorConfigDrift, Message: msg}, now)
			return st, nil
		}
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonConnectorRunning}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
		return st, nil
	default:
		return st, fmt.Errorf("s3sink provider cannot probe kind %s", res.Kind)
	}
}

// connectorConfigDrift diffs the live connector config against the
// manifest-derived one and returns the drifted key names (sorted), or nil
// when equivalent. Extra live keys beyond the desired set are Connect-added
// defaults, not drift.
func connectorConfigDrift(ctx context.Context, req reconciler.Request, url string) []string {
	name, desired, err := desiredConnectorConfig(req)
	if err != nil {
		return []string{"(desired config unresolvable: " + err.Error() + ")"}
	}
	actual, err := kafkaconnect.GetConnectorConfig(ctx, url, name)
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
