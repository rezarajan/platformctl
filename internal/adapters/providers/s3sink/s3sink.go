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
	"github.com/rezarajan/platformctl/internal/adapters/providers/providerkit"
	"github.com/rezarajan/platformctl/internal/domain/binding"
	"github.com/rezarajan/platformctl/internal/domain/dataset"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/eventstream"
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
// formats of the Aiven S3 sink connector. parquet requires schema-carrying
// records: since docs/planning/08 D2 the connector consumes them via
// registry-backed Avro converters (compatibility.Check refuses a parquet
// Dataset at validate time unless the EventStream's Provider exposes a
// schema registry supporting avro), so listing parquet here is honest —
// json/jsonl/csv remain the schemaless paths.
func (p *Provider) SupportedSinkFormats() []string {
	return []string{"json", "jsonl", "csv", "parquet"}
}

func connectPort(cfg provider.Provider, name string) int {
	return providerkit.HostPort(cfg, name, "connectPort")
}

// connectPorts builds the worker container's Ports declaration: workers <=
// 1 (undeclared) keeps the exact pre-C3 single-container behavior — a
// concrete, deterministically-derived (or pinned) HostPort via
// connectPort. workers > 1 deliberately leaves HostPort unset (0) so
// Docker/Kubernetes auto-assign a *distinct* port per ordinal — mirroring
// redpanda's reconcileBrokerSet and debezium's identical helper (see its
// doc comment for the full ADR 004/017 reasoning). ValidateSpec refuses a
// connectPort pin combined with workers.
func connectPorts(cfg provider.Provider, name string, workers int) []runtime.PortBinding {
	if workers > 1 {
		return []runtime.PortBinding{{ContainerPort: 8083, Audience: runtime.AudienceHost}}
	}
	return []runtime.PortBinding{{HostPort: connectPort(cfg, name), ContainerPort: 8083, Audience: runtime.AudienceHost}}
}

// workersDeclared reads spec.configuration.workers (docs/planning/08 C3).
// declared=false (the key absent) selects the pre-C3 single-container
// shape, byte-for-byte; declared=true (any value >= 1, validated by
// ValidateSpec) opts into the `ContainerSpec.Replicas: N, StableIdentity:
// false` shape — Connect workers are natively distributed (group.id +
// internal topics) and hold no per-worker durable state, so unlike
// redpanda's brokers (docs/adr/017) no stable per-ordinal identity is
// needed. See debezium.go's identical helper — not shared via providerkit
// because it is one three-line map read, well under G1's "more parameters
// than lines saved" bar for an extraction.
func workersDeclared(cfg provider.Provider) (int, bool) {
	v, ok := cfg.Configuration["workers"]
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

// workerURLs resolves every currently-reachable Connect worker's REST base
// URL for this Provider (docs/planning/08 C3) — the input to
// kafkaconnect's multi-address failover. workers <= 1 (undeclared) is the
// pre-C3 single-address reachableURL path, unchanged; workers > 1 iterates
// ordinals via providerkit.ReachableURLs, skipping any that don't currently
// resolve (a killed worker just isn't offered as a failover candidate).
func workerURLs(ctx context.Context, rt runtime.ContainerRuntime, name string, cfg provider.Provider) ([]string, func() error, error) {
	n, _ := workersDeclared(cfg)
	return providerkit.ReachableURLs(ctx, rt, name, 8083, n)
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
	workers, workersDecl := workersDeclared(cfg)
	if workersDecl && workers < 1 {
		return st, fmt.Errorf("Provider %q (type: s3sink): spec.configuration.workers must be a positive integer, got %v", name, cfg.Configuration["workers"])
	}
	ctrState, err := providerkit.EnsureInstance(ctx, rt, providerkit.InstanceSpec{
		Namespace: req.Provider.Metadata.Namespace,
		Name:      name,
		Network:   providerkit.Network(cfg),
		Container: runtime.ContainerSpec{
			Image: image,
			// Replicas/StableIdentity (docs/planning/08 C3, docs/adr/004):
			// workers undeclared -> ReplicaCount()==1, byte-for-byte the
			// pre-C3 single-container shape. StableIdentity is always
			// false — see debezium.go's identical reasoning.
			Replicas: workers,
			Env: map[string]string{
				"BOOTSTRAP_SERVERS": bootstrap,
				"GROUP_ID":          name,
				// Fast task failover (docs/planning/08 C3/I7; found live at
				// the wave-3 gate, doc 11): Kafka Connect's incremental
				// cooperative rebalancing parks a departed worker's tasks
				// UNASSIGNED for scheduled.rebalance.max.delay.ms awaiting
				// its return — default FIVE MINUTES, tuned for rolling
				// upgrades, not failover. C3's promise is "the Binding
				// keeps RUNNING through the loss of one worker", and these
				// workers' restarts are reconcile-driven (heal re-creates
				// them), so a short delay is the right posture: tasks
				// reassign to survivors in seconds; a healed worker simply
				// triggers one more (cooperative, incremental) rebalance.
				"CONNECT_SCHEDULED_REBALANCE_MAX_DELAY_MS": "10000",
				"CONFIG_STORAGE_TOPIC":                     name + "-configs",
				"OFFSET_STORAGE_TOPIC":                     name + "-offsets",
				"STATUS_STORAGE_TOPIC":                     name + "-status",
				"CONFIG_STORAGE_REPLICATION_FACTOR":        "1",
				"OFFSET_STORAGE_REPLICATION_FACTOR":        "1",
				"STATUS_STORAGE_REPLICATION_FACTOR":        "1",
				"KEY_CONVERTER":                            "org.apache.kafka.connect.json.JsonConverter",
				"VALUE_CONVERTER":                          "org.apache.kafka.connect.json.JsonConverter",
				"CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE":     "false",
				"CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE":   "false",
				// Sink files are written on offset commit; the 60s default makes
				// every reconcile-and-verify cycle glacial.
				"OFFSET_FLUSH_INTERVAL_MS": "5000",
				// topics.regex subscriptions only discover topics created after
				// connector registration on consumer metadata refresh — the 5min
				// default would stall CDC topics that appear on first table event.
				"CONNECT_CONSUMER_METADATA_MAX_AGE_MS": "10000",
			},
			Ports: connectPorts(cfg, name, workers),
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
	providerState := map[string]any{
		endpoint.Key: endpoint.List{
			{Name: "connect-rest", Scheme: "http", Host: hostURL, Internal: fmt.Sprintf("http://%s:8083", name), Insecure: true},
		}.ToState(),
		// The effective bootstrapServers this worker was actually started
		// with — declared or graph-inferred (docs/planning/08 E2) — so an
		// inferred default is as visible as an explicit one via
		// `platformctl state inspect`, not silently baked in.
		"bootstrapServers": bootstrap,
	}
	if workersDecl {
		// Echoed for operators (docs/planning/08 C3), mirroring redpanda's
		// "brokers" providerState field.
		providerState["workers"] = workers
	}
	st.ProviderState = providerState
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
		"topics.regex":          "^" + regexp.QuoteMeta(b.SourceRef) + "(\\..*)?$",
		"format.output.type":    ds.Format,
		"format.output.fields":  "value",
		"file.compression.type": "none",
	}
	converterOverride, _ := b.Options["converter"].(string)
	if err := applyConverterConfig(config, streamFormat(b, ds), converterOverride, req.SchemaRegistryURL); err != nil {
		return "", nil, fmt.Errorf("Binding %q: %w", res.Metadata.Name, err)
	}
	if ds.Prefix != "" {
		config["aws.s3.prefix"] = ds.Prefix
	}
	if b.DeadLetter != nil {
		applyDeadLetterConfig(config, b.DeadLetter, req, res.Metadata.Namespace)
	}
	return res.Metadata.Name, config, nil
}

// applyDeadLetterConfig translates Binding.spec.options.deadLetter
// (docs/planning/08 D6) into the Aiven S3 sink connector's error-handling
// config: errors.tolerance is a direct pass-through of dl.Tolerance
// (Kafka Connect's own values, all|none); errors.deadletterqueue.topic.name
// is dl.Stream itself — an EventStream's resource name IS its Kafka topic
// name (the same convention redpanda.reconcileTopic uses), so no separate
// lookup is needed to know the topic to route to.
// errors.deadletterqueue.topic.replication.factor is resolved from the
// named EventStream's own spec.replication when it is present in
// req.Resources (always true once compatibility.Check's existence check
// has passed — Resources is the full validated manifest set regardless of
// reconcile order, see compatibility.go's ordering-story comment), else
// "1" (compatible with every single-broker dev deployment, Kafka Connect's
// own framework default of 3 would otherwise fail topic creation against
// them). errors.deadletterqueue.context.headers.enable is always "true"
// when a DLQ is declared: the original topic/partition/offset/exception
// ride along as record headers, the only way to make a landed poison
// record diagnosable after the fact.
func applyDeadLetterConfig(config map[string]string, dl *binding.DeadLetter, req reconciler.Request, namespace string) {
	config["errors.tolerance"] = dl.Tolerance
	config["errors.deadletterqueue.topic.name"] = dl.Stream
	config["errors.deadletterqueue.context.headers.enable"] = "true"
	replication := 1
	dlqKey := resource.Key{Namespace: resource.NormalizeNamespace(namespace), Kind: "EventStream", Name: dl.Stream}
	if dlqEnv, ok := req.Resources[dlqKey]; ok {
		if es, err := eventstream.FromEnvelope(dlqEnv); err == nil {
			replication = es.ReplicationFactor()
		}
	}
	config["errors.deadletterqueue.topic.replication.factor"] = strconv.Itoa(replication)
}

// streamFormat resolves the serialization format of the records this sink
// reads off the stream: an explicit Binding spec.options.format wins
// (docs/planning/03 §7.3 — the compatibility layer already validated it
// against the EventStream's Provider); otherwise a parquet Dataset implies
// avro, because the Aiven parquet writer requires schema-carrying Connect
// records and Avro-via-registry is how this platform's CDC leg produces
// them (docs/planning/08 D2). Everything else stays on the schemaless JSON
// path, byte-for-byte the pre-D2 connector config.
func streamFormat(b binding.Binding, ds dataset.Dataset) string {
	if f, _ := b.Options["format"].(string); f != "" {
		return f
	}
	if ds.Format == "parquet" {
		return "avro"
	}
	return ""
}

// applyConverterConfig sets the key/value converter config for the resolved
// stream format — the sink half of debezium's applyConverterConfig
// (docs/planning/08 D1/D2): json (default) keeps the schemaless JSON
// converters; avro/protobuf require registryURL, resolved by the engine
// from the EventStream's own realizing Provider — compatibility.Check
// already refused schema-carrying formats (and parquet Datasets) against a
// registry-less provider chain at validate time, so an empty registryURL
// reaching here means the upstream Provider hasn't reconciled yet in this
// run (defensive, not expected given dependency-graph ordering).
// converterOverride is the advanced escape hatch of docs/planning/03 §7.3:
// an explicit converter class wins over the format-derived default for both
// key and value converters.
func applyConverterConfig(config map[string]string, format, converterOverride, registryURL string) error {
	switch format {
	case "", "json":
		class := "org.apache.kafka.connect.json.JsonConverter"
		if converterOverride != "" {
			class = converterOverride
		}
		config["key.converter"] = class
		config["value.converter"] = class
		config["key.converter.schemas.enable"] = "false"
		config["value.converter.schemas.enable"] = "false"
	case "avro", "protobuf":
		if registryURL == "" {
			return fmt.Errorf("stream format %q requires a schema registry endpoint, but none was resolved from the EventStream's Provider (has it been applied since configuration.schemaRegistry: enabled was set?)", format)
		}
		class := defaultConverterClass(format)
		if converterOverride != "" {
			class = converterOverride
		}
		config["key.converter"] = class
		config["value.converter"] = class
		config["key.converter.schema.registry.url"] = registryURL
		config["value.converter.schema.registry.url"] = registryURL
	default:
		return fmt.Errorf("options.format %q is not supported (must be one of: json, avro, protobuf)", format)
	}
	return nil
}

// defaultConverterClass maps a schema-carrying format to the Confluent
// Connect converter class Redpanda's built-in registry is compatible with —
// the jars must be present in the worker image (docs/planning/03 §7.3's
// worker-image requirement; see testdata/s3sink-image's Dockerfile).
func defaultConverterClass(format string) string {
	switch format {
	case "avro":
		return "io.confluent.connect.avro.AvroConverter"
	case "protobuf":
		return "io.confluent.connect.protobuf.ProtobufConverter"
	default:
		return ""
	}
}

// reconcileConnector registers or updates the S3 sink connector realizing a
// Binding(mode: sink), then verifies it reaches RUNNING.
func (p *Provider) reconcileConnector(ctx context.Context, req reconciler.Request) (status.Status, error) {
	st := status.Status{}
	connectorName, config, err := desiredConnectorConfig(req)
	if err != nil {
		return st, err
	}
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return st, err
	}

	name := naming.RuntimeObjectName(req.Provider)
	urls, closeURLs, err := workerURLs(ctx, req.Runtime, name, cfg)
	if err != nil {
		return st, err
	}
	defer closeURLs()
	if err := kafkaconnect.PutConnectorConfig(ctx, urls, connectorName, config); err != nil {
		return st, err
	}

	state, err := kafkaconnect.WaitConnectorRunning(ctx, urls, connectorName, 90*time.Second)
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
		// out-of-band failures. Inspect(name) covers both shapes: the
		// literal container (workers undeclared) or the replica-set
		// aggregate (docs/adr/004) — "at least one member running" for the
		// latter.
		if ctr, found, err := rt.Inspect(ctx, name); err != nil || !found || !ctr.Running {
			return err
		}
		cfg, err := provider.FromEnvelope(req.Provider)
		if err != nil {
			return err
		}
		urls, closeURLs, err := workerURLs(ctx, rt, name, cfg)
		if err != nil {
			return err
		}
		defer closeURLs()
		return kafkaconnect.DeleteConnector(ctx, urls, res.Metadata.Name)
	default:
		return fmt.Errorf("s3sink provider cannot destroy kind %s", res.Kind)
	}
}

func (p *Provider) Probe(ctx context.Context, req reconciler.Request) (status.Status, error) {
	res, rt := req.Resource, req.Runtime
	st := status.Status{}
	now := time.Now()
	name := naming.RuntimeObjectName(req.Provider)
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return st, err
	}
	switch res.Kind {
	case "Provider":
		if n, declared := workersDeclared(cfg); declared && n > 1 {
			return providerkit.ProbeConnectWorkerSet(ctx, rt, name, n, now)
		}
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
		urls, closeURLs, err := workerURLs(ctx, rt, name, cfg)
		if err != nil {
			return st, err
		}
		defer closeURLs()
		state, err := kafkaconnect.ConnectorState(ctx, urls, res.Metadata.Name)
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
		// must still match the manifest-derived one — including the DLQ
		// keys applyDeadLetterConfig added when spec.options.deadLetter is
		// declared (docs/planning/08 D6). Drifted key *names* only — values
		// may carry credentials and must never leak into conditions.
		if drifted := connectorConfigDrift(ctx, req, urls); len(drifted) > 0 {
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
func connectorConfigDrift(ctx context.Context, req reconciler.Request, urls []string) []string {
	name, desired, err := desiredConnectorConfig(req)
	if err != nil {
		return []string{"(desired config unresolvable: " + err.Error() + ")"}
	}
	actual, err := kafkaconnect.GetConnectorConfig(ctx, urls, name)
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
// ValidateSpec implements reconciler.SpecValidator. image and
// credentialsSecretRef's own required-ness (docs/planning/08 E5) are now
// schemas/v1alpha1/fragments/provider/s3sink.json's job, composed into
// manifest.Validate ahead of this method in every real CLI path (ADR 011's
// loadAndValidate order) — bootstrapServers stays a Go-side check
// (graph-inferable from an in-manifest redpanda Provider, docs/planning/08
// E2, so it must NOT be schema-required), and credentialsSecretRef's
// spec.secretRefs membership plus the connectPort/workers mutual exclusion
// remain cross-field checks a static JSON Schema fragment cannot express.
func (p *Provider) ValidateSpec(cfg provider.Provider) error {
	if v, _ := cfg.Configuration["bootstrapServers"].(string); v == "" {
		return fmt.Errorf("spec.configuration.bootstrapServers is required (the Kafka address the Connect worker joins)")
	}
	if ref, _ := cfg.Configuration["credentialsSecretRef"].(string); ref != "" && !cfg.HasSecretRef(ref) {
		return fmt.Errorf("configuration.credentialsSecretRef %q must also be listed in spec.secretRefs for the engine to resolve it", ref)
	}
	if _, declared := cfg.Configuration["workers"]; declared {
		// Host-port pin cannot be combined with the replica-set shape:
		// every ordinal's host port is auto-assigned (connectPorts,
		// mirroring docs/adr/017 §a.4's identical refusal for redpanda's
		// brokers).
		if _, pinned := cfg.Configuration["connectPort"]; pinned {
			return fmt.Errorf("spec.configuration.connectPort cannot be combined with spec.configuration.workers: each worker's host port is auto-assigned")
		}
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
	// format/converter (docs/planning/03 §7.3, docs/planning/08 D2): shape
	// only — whether avro/protobuf actually has a registry to talk to is a
	// compatibility.Check concern (it needs the EventStream's Provider
	// resolved), not this provider's own option-shape validation.
	if v, ok := options["format"]; ok {
		format, _ := v.(string)
		switch format {
		case "json", "avro", "protobuf":
		default:
			return fmt.Errorf("options.format %q is not supported (must be one of: json, avro, protobuf)", format)
		}
	}
	if v, ok := options["converter"]; ok {
		if s, _ := v.(string); s == "" {
			return fmt.Errorf("options.converter must be a non-empty string when set")
		}
	}
	return nil
}
