// Package s3source reconciles a Kafka Connect worker carrying Aiven's S3
// source connector plugin and registers/updates source connectors realizing
// Binding(mode: ingest, sourceRef: Dataset, targetRef: EventStream): objects
// under a Dataset's bucket/prefix are replayed into an EventStream's topic —
// the ingest pairing ADR 001 modeled and ADR 009 left as a seam with no
// shipped provider. Implements IngestCapableProvider (docs/planning/08 D4).
//
// The connector: Aiven's s3-source-connector-for-apache-kafka
// (io.aiven.kafka.connect.s3.source.S3SourceConnector), from the
// Aiven-Open/cloud-storage-connectors-for-apache-kafka repository — the
// successor to Aiven-Open/s3-connector-for-apache-kafka (s3sink's plugin
// source), which was archived 2024-09-11 with development moved to the new
// repo. Reference build: cmd/platformctl/testdata/s3source-image/Dockerfile
// (version-pinned, mirrors s3sink's required-image pattern).
//
// Dataset endpoint resolution mirrors s3sink.objectStoreEndpoint verbatim —
// both resolve a Dataset's providerRef to the s3/minio Provider's in-network
// S3 API address, directionally symmetric for sink (write) vs ingest (read).
package s3source

import (
	"context"
	"fmt"
	"net/url"
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

// The stock Debezium/Connect images ship no S3 source plugin, so there is no
// usable default image — spec.configuration.image is required and must
// contain the Aiven S3 source connector on the plugin path.
const connectorClass = "io.aiven.kafka.connect.s3.source.S3SourceConnector"

// Provider holds no cross-call state (docs/planning/08 F5): every method
// receives what it needs via reconciler.Request.
type Provider struct{}

func New() *Provider { return &Provider{} }

func (p *Provider) Type() string { return "s3source" }

// SupportedIngestFormats implements IngestCapableProvider. These are the
// input formats the Aiven S3 source connector's input.format config
// cleanly supports (avro, parquet, jsonl, bytes per the connector's own
// README) mapped onto this platform's Dataset.spec.format vocabulary.
// Deliberately does NOT include the literal "json" value: the connector
// reads S3 objects as one-record-per-line files (or, for parquet/avro,
// multi-record container files) — a whole-file JSON *array* (what
// Dataset.spec.format: "json" means elsewhere in this model, e.g. the old
// Aiven S3 SINK connector's format.output.type: json) has no clean
// line-by-line reader in this connector, only "jsonl" (JSON Lines) does.
// This is a deliberate, documented deviation from a literal "json" support
// claim — see docs/planning/03 §7.2's ingest row and this task's final
// report. "bytes" is excluded: it carries no structure this platform's
// EventStream consumers could decode generically.
func (p *Provider) SupportedIngestFormats() []string {
	return []string{"jsonl", "avro", "parquet"}
}

func connectPort(cfg provider.Provider, name string) int {
	return providerkit.HostPort(cfg, name, "connectPort")
}

// connectPorts mirrors s3sink's/debezium's identical helper — see either's
// doc comment for the full ADR 004/017 reasoning.
func connectPorts(cfg provider.Provider, name string, workers int) []runtime.PortBinding {
	if workers > 1 {
		return []runtime.PortBinding{{ContainerPort: 8083, Audience: runtime.AudienceHost}}
	}
	return []runtime.PortBinding{{HostPort: connectPort(cfg, name), ContainerPort: 8083, Audience: runtime.AudienceHost}}
}

// workersDeclared mirrors s3sink's/debezium's identical helper (docs/planning/08 C3).
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

// workerURLs mirrors s3sink's/debezium's identical helper.
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
		return status.Status{}, fmt.Errorf("s3source provider cannot reconcile kind %s", req.Resource.Kind)
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
		return st, fmt.Errorf("Provider %q (type: s3source): spec.configuration.image is required (a Connect image carrying the S3 source plugin)", name)
	}
	bootstrap, _ := cfg.Configuration["bootstrapServers"].(string)
	if bootstrap == "" {
		// Graph-inferred (docs/planning/08 E2), mirroring debezium/s3sink.
		bootstrap = req.KafkaBootstrapServers
	}
	if bootstrap == "" {
		return st, fmt.Errorf("Provider %q (type: s3source): spec.configuration.bootstrapServers is required (declare it, or wire a Binding on this Provider to an EventStream whose Provider publishes a Kafka bootstrap address)", name)
	}
	workers, workersDecl := workersDeclared(cfg)
	if workersDecl && workers < 1 {
		return st, fmt.Errorf("Provider %q (type: s3source): spec.configuration.workers must be a positive integer, got %v", name, cfg.Configuration["workers"])
	}
	ctrState, err := providerkit.EnsureInstance(ctx, rt, providerkit.InstanceSpec{
		Namespace: req.Provider.Metadata.Namespace,
		Name:      name,
		Network:   providerkit.Network(cfg),
		Container: runtime.ContainerSpec{
			Image:    image,
			Replicas: workers,
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
				"OFFSET_FLUSH_INTERVAL_MS":               "5000",
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
	hostAddr := ctrState.HostAddr(8083)
	hostURL := ""
	if hostAddr != "" {
		hostURL = "http://" + hostAddr
	}
	providerState := map[string]any{
		endpoint.Key: endpoint.List{
			{Name: "connect-rest", Scheme: "http", Host: hostURL, Internal: fmt.Sprintf("http://%s:8083", name), Insecure: true},
		}.ToState(),
		"bootstrapServers": bootstrap,
	}
	if workersDecl {
		providerState["workers"] = workers
	}
	st.ProviderState = providerState
	return st, nil
}

// inputFormatFor maps Dataset.spec.format onto the connector's own
// input.format enum — a 1:1 mapping today (see SupportedIngestFormats' doc
// comment for why "json" is deliberately excluded).
func inputFormatFor(datasetFormat string) (string, bool) {
	switch datasetFormat {
	case "jsonl", "avro", "parquet":
		return datasetFormat, true
	default:
		return "", false
	}
}

// desiredConnectorConfig builds the manifest-derived connector config —
// shared by reconcile (to register) and Probe (to diff against the live
// config; docs/planning/07 §2.1: RUNNING with the wrong bucket/topic is
// drift, not health). Mirrors s3sink.desiredConnectorConfig's shape and
// Dataset-resolution logic, directionally inverted: here the Dataset is the
// Binding's sourceRef (the origin being read), and the EventStream is the
// targetRef (what gets filled).
func desiredConnectorConfig(req reconciler.Request) (string, map[string]string, error) {
	res := req.Resource
	b, err := binding.FromEnvelope(res)
	if err != nil {
		return "", nil, err
	}
	if b.Mode != binding.ModeIngest {
		return "", nil, fmt.Errorf("Binding %q: s3source realizes mode \"ingest\" only, got %q", res.Metadata.Name, b.Mode)
	}

	srcRef := resource.RefFromSpec(res.Spec, "sourceRef")
	dsEnv, ok := req.Resources[srcRef.Key(res.Metadata.Namespace, "Dataset")]
	if !ok {
		return "", nil, fmt.Errorf("Binding %q: sourceRef %q not found", res.Metadata.Name, b.SourceRef)
	}
	ds, err := dataset.FromEnvelope(dsEnv)
	if err != nil {
		return "", nil, err
	}

	tgtRef := resource.RefFromSpec(res.Spec, "targetRef")
	if _, ok := req.Resources[tgtRef.Key(res.Metadata.Namespace, "EventStream")]; !ok {
		return "", nil, fmt.Errorf("Binding %q: targetRef %q not found", res.Metadata.Name, b.TargetRef)
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
		return "", nil, fmt.Errorf("Binding %q: s3source Provider %q needs configuration.credentialsSecretRef naming a declared secretRef", res.Metadata.Name, naming.RuntimeObjectName(req.Provider))
	}

	inputFormat, ok := inputFormatFor(ds.Format)
	if !ok {
		// Defensive: compatibility.Check's SupportedIngestFormats gate
		// already refuses this at validate time.
		return "", nil, fmt.Errorf("Binding %q: Dataset %q format %q has no s3source input.format mapping", res.Metadata.Name, b.SourceRef, ds.Format)
	}

	config := map[string]string{
		"connector.class":       connectorClass,
		"tasks.max":             "1",
		"aws.access.key.id":     creds["username"],
		"aws.secret.access.key": creds["password"],
		"aws.s3.bucket.name":    ds.Bucket,
		"aws.s3.endpoint":       objectStoreEP,
		"aws.s3.region":         "us-east-1",
		// The Kafka topic to fill: set directly (the "topic" config entry,
		// the connector's own first-preference lookup) rather than via a
		// {{topic}} file.name.template placeholder — an EventStream's
		// resource name IS its Kafka topic name (the same convention
		// redpanda.reconcileTopic uses), so no separate lookup is needed.
		"topic": b.TargetRef,
		// Ordering/offset semantics (docs/planning/03, additive): the
		// connector lists objects under the bucket/prefix via S3's
		// ListObjectsV2 API in lexicographical key order and tracks
		// progress with a startAfter-style cursor persisted to its own
		// offsets topic — an object already processed is never
		// reprocessed, and object naming that isn't lexicographically
		// monotonic with upload order can be read out of arrival order
		// (see the connector's own README "How the AWS S3 API works").
		// distribution.type: object_hash (the connector's own default,
		// set explicitly for drift-diff visibility, mirroring s3sink's
		// explicit file.compression.type: none) allows file.name.template
		// to be a plain regex instead of requiring {{topic}}/{{partition}}/
		// {{start_offset}} placeholders — ".*" matches every object key
		// under the Dataset's prefix regardless of which sink connector (or
		// process) wrote it, the natural "replay everything under this
		// bucket/prefix" semantics an ingest/backfill Binding wants.
		"distribution.type":  "object_hash",
		"file.name.template": ".*",
		// This connector always produces a String key (the source object's
		// own key/partition info) — a fixed, connector-mandated converter,
		// not a format-derived choice (see its README).
		"key.converter": "org.apache.kafka.connect.storage.StringConverter",
		"input.format":  inputFormat,
	}
	if ds.Prefix != "" {
		config["aws.s3.prefix"] = ds.Prefix
	}
	converterOverride, _ := b.Options["converter"].(string)
	if err := applyValueConverterConfig(config, inputFormat, converterOverride, req.SchemaRegistryURL); err != nil {
		return "", nil, fmt.Errorf("Binding %q: %w", res.Metadata.Name, err)
	}
	return res.Metadata.Name, config, nil
}

// applyValueConverterConfig sets only the VALUE converter (key.converter is
// fixed to StringConverter above — a connector-mandated choice, unlike
// debezium's/s3sink's applyConverterConfig, which sets both symmetrically;
// this is the one genuine divergence from verbatim reuse, so it is a local,
// differently-shaped function rather than a call to their shared pattern —
// see the package doc comment). jsonl needs no schema registry (schemaless
// JSON output, mirroring every other provider's json-format default);
// avro/parquet are schema-carrying and require registryURL, resolved by the
// engine from the EventStream endpoint's own realizing Provider — mirroring
// debezium's/s3sink's identical registry-resolution rationale.
func applyValueConverterConfig(config map[string]string, format, converterOverride, registryURL string) error {
	switch format {
	case "jsonl":
		class := "org.apache.kafka.connect.json.JsonConverter"
		if converterOverride != "" {
			class = converterOverride
		}
		config["value.converter"] = class
		config["value.converter.schemas.enable"] = "false"
	case "avro", "parquet":
		if registryURL == "" {
			return fmt.Errorf("input format %q requires a schema registry endpoint, but none was resolved from the EventStream's Provider (has it been applied since configuration.schemaRegistry: enabled was set?)", format)
		}
		class := "io.confluent.connect.avro.AvroConverter"
		if converterOverride != "" {
			class = converterOverride
		}
		config["value.converter"] = class
		config["value.converter.schema.registry.url"] = registryURL
	default:
		return fmt.Errorf("input format %q is not supported (must be one of: jsonl, avro, parquet)", format)
	}
	return nil
}

// reconcileConnector registers or updates the S3 source connector realizing
// a Binding(mode: ingest), then verifies it reaches RUNNING. No preflight
// dial (mirrors s3sink's reconcileConnector, which likewise registers
// directly): an object store's reachability is a Connect-task-level concern
// here, same as it is for the sink direction.
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
		st.SetCondition(status.Condition{Type: status.Degraded, Status: status.True, Reason: status.ReasonConnectorState + state}, now)
		return st, err
	}
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonConnectorRunning}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	st.ProviderState = map[string]any{"connector": connectorName, "state": state}
	return st, nil
}

// objectStoreEndpoint mirrors s3sink.objectStoreEndpoint verbatim (see the
// package doc comment): an explicit options.endpoint wins (external
// stores), otherwise the Dataset's Provider container on the shared
// network.
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
		return fmt.Errorf("s3source provider cannot destroy kind %s", res.Kind)
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
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonConnectorState + state}, now)
			st.SetCondition(status.Condition{Type: status.Degraded, Status: status.True, Reason: status.ReasonConnectorState + state}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonConnectorState + state}, now)
			return st, nil
		}
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
		return st, fmt.Errorf("s3source provider cannot probe kind %s", res.Kind)
	}
}

// connectorConfigDrift mirrors s3sink's identical pattern.
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

// ValidateSpec implements SpecValidator: mirrors s3sink's identical
// unconditional-credentialsSecretRef requirement — a Dataset, unlike a
// Source, has no Connection/secretRef of its own, so this provider is the
// only possible credential source.
func (p *Provider) ValidateSpec(cfg provider.Provider) error {
	if v, _ := cfg.Configuration["image"].(string); v == "" {
		return fmt.Errorf("spec.configuration.image is required (a Connect image carrying the S3 source plugin; no stock image ships one)")
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
	if v, declared := cfg.Configuration["workers"]; declared {
		n, ok := -1, false
		switch t := v.(type) {
		case int:
			n, ok = t, true
		case float64:
			if t == float64(int(t)) {
				n, ok = int(t), true
			}
		}
		if !ok || n < 1 {
			return fmt.Errorf("spec.configuration.workers must be a positive integer, got %v", v)
		}
		if _, pinned := cfg.Configuration["connectPort"]; pinned {
			return fmt.Errorf("spec.configuration.connectPort cannot be combined with spec.configuration.workers: each worker's host port is auto-assigned")
		}
	}
	return nil
}

// ValidateBindingOptions implements reconciler.BindingOptionsValidator: the
// origin endpoint override must be a well-formed URL at validate time.
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
	if v, ok := options["converter"]; ok {
		if s, _ := v.(string); s == "" {
			return fmt.Errorf("options.converter must be a non-empty string when set")
		}
	}
	return nil
}
