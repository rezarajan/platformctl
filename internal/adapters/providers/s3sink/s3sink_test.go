package s3sink

import (
	"context"
	"testing"

	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

func workerEnvelope(name string, configuration map[string]any) resource.Envelope {
	e := resource.Envelope{}
	e.APIVersion = "datascape.io/v1alpha1"
	e.Kind = "Provider"
	e.Metadata.Name = name
	e.Spec = map[string]any{
		"type":          "s3sink",
		"runtime":       map[string]any{"type": "fake"},
		"configuration": configuration,
	}
	return e
}

// TestReconcileWorkerBootstrapServersInferred mirrors debezium's coverage
// of docs/planning/08 E2: an omitted spec.configuration.bootstrapServers
// falls back to the engine's graph-inferred req.KafkaBootstrapServers, and
// the effective value is published into providerState for visibility.
func TestReconcileWorkerBootstrapServersInferred(t *testing.T) {
	rt := fakeruntime.New()
	env := workerEnvelope("sink", map[string]any{
		"image":                "datascape-s3sink-connect:local",
		"credentialsSecretRef": "creds",
	})
	p := New()
	req := reconciler.Request{
		Resource:              env,
		Provider:              env,
		Runtime:               rt,
		KafkaBootstrapServers: "broker:29092",
	}
	st, err := p.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := st.ProviderState["bootstrapServers"]; got != "broker:29092" {
		t.Errorf("providerState[bootstrapServers] = %v, want %q", got, "broker:29092")
	}
	ctrState, found, err := rt.Inspect(context.Background(), "sink")
	if err != nil || !found {
		t.Fatalf("Inspect: found=%v err=%v", found, err)
	}
	if got := ctrState.Env["BOOTSTRAP_SERVERS"]; got != "broker:29092" {
		t.Errorf("container BOOTSTRAP_SERVERS = %q, want %q", got, "broker:29092")
	}
}

// TestReconcileWorkerBootstrapServersRequiredWithoutInference: no declared
// value and nothing inferable fails clearly rather than starting with an
// empty Kafka address.
func TestReconcileWorkerBootstrapServersRequiredWithoutInference(t *testing.T) {
	rt := fakeruntime.New()
	env := workerEnvelope("sink", map[string]any{
		"image":                "datascape-s3sink-connect:local",
		"credentialsSecretRef": "creds",
	})
	p := New()
	req := reconciler.Request{Resource: env, Provider: env, Runtime: rt}
	if _, err := p.Reconcile(context.Background(), req); err == nil {
		t.Fatal("want an error when bootstrapServers is neither declared nor inferable")
	}
}

func simpleEnvelope(kind, name string, spec map[string]any) resource.Envelope {
	e := resource.Envelope{}
	e.APIVersion = "datascape.io/v1alpha1"
	e.Kind = kind
	e.Metadata.Name = name
	e.Spec = spec
	return e
}

// sinkRequest builds the Binding-kind Request desiredConnectorConfig
// consumes, mirroring the engine's construction: Resources indexed by
// (kind, namespace, name), secrets resolved, SchemaRegistryURL as given.
func sinkRequest(datasetFormat, registryURL string, options map[string]any) reconciler.Request {
	worker := workerEnvelope("sink-worker", map[string]any{
		"image":                "datascape-s3sink-connect:local",
		"bootstrapServers":     "broker:29092",
		"credentialsSecretRef": "creds",
	})
	es := simpleEnvelope("EventStream", "events", map[string]any{
		"providerRef": map[string]any{"name": "broker"},
	})
	ds := simpleEnvelope("Dataset", "lake", map[string]any{
		"providerRef": map[string]any{"name": "store"},
		"bucket":      "raw",
		"format":      datasetFormat,
	})
	store := simpleEnvelope("Provider", "store", map[string]any{
		"type":    "minio",
		"runtime": map[string]any{"type": "fake"},
	})
	bindingSpec := map[string]any{
		"mode":        "sink",
		"sourceRef":   map[string]any{"name": "events"},
		"targetRef":   map[string]any{"name": "lake"},
		"providerRef": map[string]any{"name": "sink-worker"},
	}
	if options != nil {
		bindingSpec["options"] = options
	}
	b := simpleEnvelope("Binding", "events-to-lake", bindingSpec)
	return reconciler.Request{
		Resource: b,
		Provider: worker,
		Resources: map[resource.Key]resource.Envelope{
			es.Key():     es,
			ds.Key():     ds,
			store.Key():  store,
			worker.Key(): worker,
		},
		Secrets:           map[string]map[string]string{"creds": {"username": "u", "password": "p"}},
		SchemaRegistryURL: registryURL,
	}
}

// TestConnectorConfigJSONStaysSchemaless: the pre-D2 default — a json
// Dataset keeps the schemaless JSON converters, byte-for-byte.
func TestConnectorConfigJSONStaysSchemaless(t *testing.T) {
	_, cfg, err := desiredConnectorConfig(sinkRequest("json", "", nil))
	if err != nil {
		t.Fatalf("desiredConnectorConfig: %v", err)
	}
	if got := cfg["format.output.type"]; got != "json" {
		t.Errorf("format.output.type = %q, want json", got)
	}
	for _, k := range []string{"key.converter", "value.converter"} {
		if got, want := cfg[k], "org.apache.kafka.connect.json.JsonConverter"; got != want {
			t.Errorf("%s = %q, want %q", k, got, want)
		}
	}
	for _, k := range []string{"key.converter.schemas.enable", "value.converter.schemas.enable"} {
		if got := cfg[k]; got != "false" {
			t.Errorf("%s = %q, want false", k, got)
		}
	}
	if _, ok := cfg["value.converter.schema.registry.url"]; ok {
		t.Error("json path must not carry a schema registry URL")
	}
}

// TestConnectorConfigParquetWiresAvroConverters covers docs/planning/08 D2:
// a parquet Dataset implies Avro converters wired to the engine-resolved
// registry URL — the schema-carrying records the parquet writer requires.
func TestConnectorConfigParquetWiresAvroConverters(t *testing.T) {
	_, cfg, err := desiredConnectorConfig(sinkRequest("parquet", "http://broker:8081", nil))
	if err != nil {
		t.Fatalf("desiredConnectorConfig: %v", err)
	}
	if got := cfg["format.output.type"]; got != "parquet" {
		t.Errorf("format.output.type = %q, want parquet", got)
	}
	for _, k := range []string{"key.converter", "value.converter"} {
		if got, want := cfg[k], "io.confluent.connect.avro.AvroConverter"; got != want {
			t.Errorf("%s = %q, want %q", k, got, want)
		}
	}
	for _, k := range []string{"key.converter.schema.registry.url", "value.converter.schema.registry.url"} {
		if got, want := cfg[k], "http://broker:8081"; got != want {
			t.Errorf("%s = %q, want %q", k, got, want)
		}
	}
	for _, k := range []string{"key.converter.schemas.enable", "value.converter.schemas.enable"} {
		if _, ok := cfg[k]; ok {
			t.Errorf("%s must not be set on the avro path", k)
		}
	}
}

// TestConnectorConfigParquetWithoutRegistryFails: defensive — compatibility
// refused this at validate, so an empty registry URL here means the
// upstream Provider hasn't reconciled; fail with a pointed message instead
// of registering a connector that cannot decode its records.
func TestConnectorConfigParquetWithoutRegistryFails(t *testing.T) {
	if _, _, err := desiredConnectorConfig(sinkRequest("parquet", "", nil)); err == nil {
		t.Fatal("want an error for parquet without a resolved registry URL")
	}
}

// TestConnectorConfigExplicitFormatAndConverterOverride: an explicit
// Binding options.format wins over the Dataset-derived default, and
// options.converter overrides the converter class for both key and value
// (docs/planning/03 §7.3's escape hatch).
func TestConnectorConfigExplicitFormatAndConverterOverride(t *testing.T) {
	_, cfg, err := desiredConnectorConfig(sinkRequest("parquet", "http://broker:8081", map[string]any{
		"format":    "avro",
		"converter": "io.example.CustomConverter",
	}))
	if err != nil {
		t.Fatalf("desiredConnectorConfig: %v", err)
	}
	for _, k := range []string{"key.converter", "value.converter"} {
		if got, want := cfg[k], "io.example.CustomConverter"; got != want {
			t.Errorf("%s = %q, want %q", k, got, want)
		}
	}
}

// TestValidateBindingOptionsFormatShape: option-shape validation mirrors
// debezium's — a bogus format or empty converter fails at validate.
func TestValidateBindingOptionsFormatShape(t *testing.T) {
	p := New()
	if err := p.ValidateBindingOptions("sink", map[string]any{"format": "xml"}); err == nil {
		t.Error("bogus options.format accepted")
	}
	if err := p.ValidateBindingOptions("sink", map[string]any{"converter": ""}); err == nil {
		t.Error("empty options.converter accepted")
	}
	if err := p.ValidateBindingOptions("sink", map[string]any{"format": "avro"}); err != nil {
		t.Errorf("valid options.format rejected: %v", err)
	}
}

// TestDeadLetterConfigTranslation covers docs/planning/08 D6: a declared
// options.deadLetter translates to the Aiven connector's error-handling
// config, defaulting the DLQ topic's replication factor to 1 when the
// named EventStream isn't present in req.Resources (e.g. a stub Request in
// a unit test, or a not-yet-graph-resolved DLQ stream).
func TestDeadLetterConfigTranslation(t *testing.T) {
	req := sinkRequest("json", "", map[string]any{
		"deadLetter": map[string]any{"stream": "dlq-events"},
	})
	_, cfg, err := desiredConnectorConfig(req)
	if err != nil {
		t.Fatalf("desiredConnectorConfig: %v", err)
	}
	want := map[string]string{
		"errors.tolerance":                                "all",
		"errors.deadletterqueue.topic.name":               "dlq-events",
		"errors.deadletterqueue.topic.replication.factor": "1",
		"errors.deadletterqueue.context.headers.enable":   "true",
	}
	for k, v := range want {
		if got := cfg[k]; got != v {
			t.Errorf("%s = %q, want %q", k, got, v)
		}
	}
}

// TestDeadLetterConfigReplicationFromEventStream: when the named DLQ
// EventStream is present in req.Resources (the normal engine-resolved
// case), its own spec.replication wins over the "1" fallback.
func TestDeadLetterConfigReplicationFromEventStream(t *testing.T) {
	req := sinkRequest("json", "", map[string]any{
		"deadLetter": map[string]any{"stream": "dlq-events", "tolerance": "none"},
	})
	dlq := simpleEnvelope("EventStream", "dlq-events", map[string]any{
		"providerRef": map[string]any{"name": "broker"},
		"replication": 3,
	})
	req.Resources[dlq.Key()] = dlq
	_, cfg, err := desiredConnectorConfig(req)
	if err != nil {
		t.Fatalf("desiredConnectorConfig: %v", err)
	}
	if got := cfg["errors.tolerance"]; got != "none" {
		t.Errorf("errors.tolerance = %q, want none", got)
	}
	if got := cfg["errors.deadletterqueue.topic.replication.factor"]; got != "3" {
		t.Errorf("errors.deadletterqueue.topic.replication.factor = %q, want 3 (from the DLQ EventStream's own spec.replication)", got)
	}
}

// TestDeadLetterAbsentNoConfig: zero behavior change when deadLetter is
// unset — none of the errors.* keys appear at all.
func TestDeadLetterAbsentNoConfig(t *testing.T) {
	_, cfg, err := desiredConnectorConfig(sinkRequest("json", "", nil))
	if err != nil {
		t.Fatalf("desiredConnectorConfig: %v", err)
	}
	for k := range cfg {
		if len(k) >= 7 && k[:7] == "errors." {
			t.Errorf("unexpected %s in config when deadLetter is unset", k)
		}
	}
}

// TestReconcileWorkerWorkersReplicaSet mirrors debezium's coverage of
// docs/planning/08 C3: configuration.workers: N fans the Connect worker out
// to N ordinals.
func TestReconcileWorkerWorkersReplicaSet(t *testing.T) {
	rt := fakeruntime.New()
	env := workerEnvelope("sink", map[string]any{
		"image":                "datascape-s3sink-connect:local",
		"credentialsSecretRef": "creds",
		"bootstrapServers":     "broker:29092",
		"workers":              2,
	})
	p := New()
	req := reconciler.Request{Resource: env, Provider: env, Runtime: rt}
	st, err := p.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := st.ProviderState["workers"]; got != 2 {
		t.Errorf("providerState[workers] = %v, want 2", got)
	}
	for i := 0; i < 2; i++ {
		ord := runtime.OrdinalName("sink", i)
		if _, found, err := rt.Inspect(context.Background(), ord); err != nil || !found {
			t.Errorf("ordinal %s not found after reconcile: found=%v err=%v", ord, found, err)
		}
	}
}

// TestReconcileWorkerWorkersUndeclaredIsSingleContainer guards the zero-
// behavior-change bar, mirroring debezium's identical test.
func TestReconcileWorkerWorkersUndeclaredIsSingleContainer(t *testing.T) {
	rt := fakeruntime.New()
	env := workerEnvelope("sink", map[string]any{
		"image":                "datascape-s3sink-connect:local",
		"credentialsSecretRef": "creds",
		"bootstrapServers":     "broker:29092",
	})
	p := New()
	req := reconciler.Request{Resource: env, Provider: env, Runtime: rt}
	if _, err := p.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if _, found, _ := rt.Inspect(context.Background(), "sink-0"); found {
		t.Error("ordinal \"sink-0\" must not exist when workers is undeclared")
	}
}

// TestValidateSpecWorkers mirrors debezium's identical coverage.
func TestValidateSpecWorkers(t *testing.T) {
	p := New()
	base := map[string]any{
		"image":                "datascape-s3sink-connect:local",
		"bootstrapServers":     "broker:29092",
		"credentialsSecretRef": "creds",
	}
	for _, v := range []any{1, 2, float64(3)} {
		cfg := provider.Provider{
			Configuration: mergeConfig(base, "workers", v),
			SecretRefs:    []string{"creds"},
		}
		if err := p.ValidateSpec(cfg); err != nil {
			t.Errorf("workers %v rejected: %v", v, err)
		}
	}
	for _, v := range []any{0, -1, "two", 1.5} {
		cfg := provider.Provider{
			Configuration: mergeConfig(base, "workers", v),
			SecretRefs:    []string{"creds"},
		}
		if err := p.ValidateSpec(cfg); err == nil {
			t.Errorf("workers %v (%T) accepted, want an error", v, v)
		}
	}
}

// TestValidateSpecWorkersRefusesConnectPortPin mirrors debezium's identical
// coverage of the real Docker port-collision bug a pinned connectPort
// combined with workers > 1 would hit.
func TestValidateSpecWorkersRefusesConnectPortPin(t *testing.T) {
	p := New()
	cfg := provider.Provider{
		Configuration: map[string]any{
			"image":                "datascape-s3sink-connect:local",
			"bootstrapServers":     "broker:29092",
			"credentialsSecretRef": "creds",
			"workers":              2,
			"connectPort":          18186,
		},
		SecretRefs: []string{"creds"},
	}
	if err := p.ValidateSpec(cfg); err == nil {
		t.Fatal("want an error when connectPort is pinned alongside workers")
	}
}

func mergeConfig(base map[string]any, key string, value any) map[string]any {
	out := make(map[string]any, len(base)+1)
	for k, v := range base {
		out[k] = v
	}
	out[key] = value
	return out
}
