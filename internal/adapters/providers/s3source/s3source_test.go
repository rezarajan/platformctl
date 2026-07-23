package s3source

import (
	"context"
	"testing"

	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// Interface assertions: s3source must implement the capability +
// validation interfaces docs/planning/08 D4 requires (ADR 009).
var (
	_ reconciler.IngestCapableProvider   = (*Provider)(nil)
	_ reconciler.SpecValidator           = (*Provider)(nil)
	_ reconciler.BindingOptionsValidator = (*Provider)(nil)
)

func workerEnvelope(name string, configuration map[string]any) resource.Envelope {
	e := resource.Envelope{}
	e.APIVersion = "datascape.io/v1alpha1"
	e.Kind = "Provider"
	e.Metadata.Name = name
	e.Spec = map[string]any{
		"type":          "s3source",
		"runtime":       map[string]any{"type": "fake"},
		"configuration": configuration,
	}
	return e
}

func simpleEnvelope(kind, name string, spec map[string]any) resource.Envelope {
	e := resource.Envelope{}
	e.APIVersion = "datascape.io/v1alpha1"
	e.Kind = kind
	e.Metadata.Name = name
	e.Spec = spec
	return e
}

func TestSupportedIngestFormats(t *testing.T) {
	t.Parallel()
	p := New()
	got := p.SupportedIngestFormats()
	want := map[string]bool{"jsonl": true, "avro": true, "parquet": true}
	if len(got) != len(want) {
		t.Fatalf("SupportedIngestFormats = %v, want exactly %v", got, want)
	}
	for _, f := range got {
		if !want[f] {
			t.Errorf("unexpected format %q in SupportedIngestFormats", f)
		}
	}
	// "json" (whole-file array) is deliberately unsupported — see the
	// package doc comment / SupportedIngestFormats' own doc comment.
	for _, f := range got {
		if f == "json" {
			t.Error("literal \"json\" must not be listed (the connector has no whole-file-array reader)")
		}
	}
}

func TestReconcileWorkerBootstrapServersInferred(t *testing.T) {
	t.Parallel()
	rt := fakeruntime.New()
	env := workerEnvelope("s3src", map[string]any{
		"image":                "datascape-s3source-connect:local",
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
}

func TestReconcileWorkerBootstrapServersRequiredWithoutInference(t *testing.T) {
	t.Parallel()
	rt := fakeruntime.New()
	env := workerEnvelope("s3src", map[string]any{
		"image":                "datascape-s3source-connect:local",
		"credentialsSecretRef": "creds",
	})
	p := New()
	req := reconciler.Request{Resource: env, Provider: env, Runtime: rt}
	if _, err := p.Reconcile(context.Background(), req); err == nil {
		t.Fatal("want an error when bootstrapServers is neither declared nor inferable")
	}
}

// ingestRequest builds the Binding-kind Request desiredConnectorConfig
// consumes: sourceRef -> Dataset (the origin), targetRef -> EventStream
// (what gets filled) — mirroring sinkRequest's construction in
// s3sink_test.go, directionally inverted.
func ingestRequest(datasetFormat, registryURL string, options map[string]any) reconciler.Request {
	worker := workerEnvelope("s3src-worker", map[string]any{
		"image":                "datascape-s3source-connect:local",
		"bootstrapServers":     "broker:29092",
		"credentialsSecretRef": "creds",
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
	es := simpleEnvelope("EventStream", "replayed-events", map[string]any{
		"providerRef": map[string]any{"name": "broker"},
	})
	bindingSpec := map[string]any{
		"mode":        "ingest",
		"sourceRef":   map[string]any{"name": "lake"},
		"targetRef":   map[string]any{"name": "replayed-events"},
		"providerRef": map[string]any{"name": "s3src-worker"},
	}
	if options != nil {
		bindingSpec["options"] = options
	}
	b := simpleEnvelope("Binding", "lake-to-events", bindingSpec)
	return reconciler.Request{
		Resource: b,
		Provider: worker,
		Resources: map[resource.Key]resource.Envelope{
			ds.Key():     ds,
			store.Key():  store,
			es.Key():     es,
			worker.Key(): worker,
		},
		Secrets:           map[string]map[string]string{"creds": {"username": "u", "password": "p"}},
		SchemaRegistryURL: registryURL,
	}
}

func TestDesiredConnectorConfigJSONLNoRegistry(t *testing.T) {
	t.Parallel()
	_, cfg, err := desiredConnectorConfig(ingestRequest("jsonl", "", nil))
	if err != nil {
		t.Fatalf("desiredConnectorConfig: %v", err)
	}
	if got, want := cfg["input.format"], "jsonl"; got != want {
		t.Errorf("input.format = %q, want %q", got, want)
	}
	if got, want := cfg["value.converter"], "org.apache.kafka.connect.json.JsonConverter"; got != want {
		t.Errorf("value.converter = %q, want %q", got, want)
	}
	if got, want := cfg["key.converter"], "org.apache.kafka.connect.storage.StringConverter"; got != want {
		t.Errorf("key.converter = %q, want %q (connector-mandated, not format-derived)", got, want)
	}
	if got, want := cfg["topic"], "replayed-events"; got != want {
		t.Errorf("topic = %q, want %q", got, want)
	}
	if got, want := cfg["aws.s3.bucket.name"], "raw"; got != want {
		t.Errorf("aws.s3.bucket.name = %q, want %q", got, want)
	}
	if got, want := cfg["distribution.type"], "object_hash"; got != want {
		t.Errorf("distribution.type = %q, want %q", got, want)
	}
	if got, want := cfg["file.name.template"], ".*"; got != want {
		t.Errorf("file.name.template = %q, want %q", got, want)
	}
	if _, ok := cfg["value.converter.schema.registry.url"]; ok {
		t.Error("jsonl path must not carry a schema registry URL")
	}
}

func TestDesiredConnectorConfigAvroWiresRegistry(t *testing.T) {
	t.Parallel()
	_, cfg, err := desiredConnectorConfig(ingestRequest("avro", "http://broker:8081", nil))
	if err != nil {
		t.Fatalf("desiredConnectorConfig: %v", err)
	}
	if got, want := cfg["input.format"], "avro"; got != want {
		t.Errorf("input.format = %q, want %q", got, want)
	}
	if got, want := cfg["value.converter"], "io.confluent.connect.avro.AvroConverter"; got != want {
		t.Errorf("value.converter = %q, want %q", got, want)
	}
	if got, want := cfg["value.converter.schema.registry.url"], "http://broker:8081"; got != want {
		t.Errorf("value.converter.schema.registry.url = %q, want %q", got, want)
	}
}

func TestDesiredConnectorConfigParquetWithoutRegistryFails(t *testing.T) {
	t.Parallel()
	if _, _, err := desiredConnectorConfig(ingestRequest("parquet", "", nil)); err == nil {
		t.Fatal("want an error for parquet without a resolved registry URL")
	}
}

func TestDesiredConnectorConfigConverterOverride(t *testing.T) {
	t.Parallel()
	_, cfg, err := desiredConnectorConfig(ingestRequest("avro", "http://broker:8081", map[string]any{
		"converter": "io.example.CustomConverter",
	}))
	if err != nil {
		t.Fatalf("desiredConnectorConfig: %v", err)
	}
	if got, want := cfg["value.converter"], "io.example.CustomConverter"; got != want {
		t.Errorf("value.converter = %q, want %q", got, want)
	}
}

func TestDesiredConnectorConfigPrefix(t *testing.T) {
	t.Parallel()
	worker := workerEnvelope("s3src-worker", map[string]any{
		"image":                "datascape-s3source-connect:local",
		"bootstrapServers":     "broker:29092",
		"credentialsSecretRef": "creds",
	})
	ds := simpleEnvelope("Dataset", "lake", map[string]any{
		"providerRef": map[string]any{"name": "store"},
		"bucket":      "raw",
		"prefix":      "attendance/",
		"format":      "jsonl",
	})
	store := simpleEnvelope("Provider", "store", map[string]any{"type": "minio", "runtime": map[string]any{"type": "fake"}})
	es := simpleEnvelope("EventStream", "replayed-events", map[string]any{"providerRef": map[string]any{"name": "broker"}})
	b := simpleEnvelope("Binding", "lake-to-events", map[string]any{
		"mode":        "ingest",
		"sourceRef":   map[string]any{"name": "lake"},
		"targetRef":   map[string]any{"name": "replayed-events"},
		"providerRef": map[string]any{"name": "s3src-worker"},
	})
	req := reconciler.Request{
		Resource: b,
		Provider: worker,
		Resources: map[resource.Key]resource.Envelope{
			ds.Key(): ds, store.Key(): store, es.Key(): es, worker.Key(): worker,
		},
		Secrets: map[string]map[string]string{"creds": {"username": "u", "password": "p"}},
	}
	_, cfg, err := desiredConnectorConfig(req)
	if err != nil {
		t.Fatalf("desiredConnectorConfig: %v", err)
	}
	if got, want := cfg["aws.s3.prefix"], "attendance/"; got != want {
		t.Errorf("aws.s3.prefix = %q, want %q", got, want)
	}
}

func TestDesiredConnectorConfigMissingCredentialsFails(t *testing.T) {
	t.Parallel()
	req := ingestRequest("jsonl", "", nil)
	req.Secrets = nil
	if _, _, err := desiredConnectorConfig(req); err == nil {
		t.Fatal("want an error when no credentials resolve")
	}
}

func TestValidateBindingOptionsEndpointShape(t *testing.T) {
	t.Parallel()
	p := New()
	if err := p.ValidateBindingOptions("ingest", map[string]any{"endpoint": "not-a-url"}); err == nil {
		t.Error("malformed options.endpoint accepted")
	}
	if err := p.ValidateBindingOptions("ingest", map[string]any{"endpoint": "http://minio:9000"}); err != nil {
		t.Errorf("valid options.endpoint rejected: %v", err)
	}
	if err := p.ValidateBindingOptions("ingest", map[string]any{"converter": ""}); err == nil {
		t.Error("empty options.converter accepted")
	}
}

// TestValidateSpecCredentialsSecretRefMustBeDeclared covers the cross-field
// half that stays Go-side (docs/planning/08 E5): credentialsSecretRef's own
// required-ness is now schemas/v1alpha1/fragments/provider/s3source.json's
// job (a static JSON Schema fragment CAN express plain required-ness — see
// cmd/platformctl's negative-test corpus), but a value that IS set must
// still be resolvable against spec.secretRefs, which needs the sibling
// field's contents and so cannot move into the fragment.
func TestValidateSpecCredentialsSecretRefMustBeDeclared(t *testing.T) {
	t.Parallel()
	p := New()
	cfg := provider.Provider{
		Configuration: map[string]any{
			"image":                "datascape-s3source-connect:local",
			"bootstrapServers":     "broker:29092",
			"credentialsSecretRef": "creds",
		},
	}
	if err := p.ValidateSpec(cfg); err == nil {
		t.Error("credentialsSecretRef not listed in spec.secretRefs accepted")
	}
}

// TestValidateSpecWorkers: workers' own positive-integer shape is now
// schemas/v1alpha1/fragments/provider/s3source.json's job (docs/planning/08
// E5) — see cmd/platformctl's negative-test corpus; this only covers that
// every legal value still passes.
func TestValidateSpecWorkers(t *testing.T) {
	t.Parallel()
	p := New()
	base := map[string]any{
		"image":                "datascape-s3source-connect:local",
		"bootstrapServers":     "broker:29092",
		"credentialsSecretRef": "creds",
	}
	for _, v := range []any{1, 2, float64(3)} {
		cfg := provider.Provider{Configuration: merge(base, "workers", v), SecretRefs: []string{"creds"}}
		if err := p.ValidateSpec(cfg); err != nil {
			t.Errorf("workers %v rejected: %v", v, err)
		}
	}
}

func TestValidateSpecWorkersRefusesConnectPortPin(t *testing.T) {
	t.Parallel()
	p := New()
	cfg := provider.Provider{
		Configuration: map[string]any{
			"image":                "datascape-s3source-connect:local",
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

func TestReconcileWorkerWorkersReplicaSet(t *testing.T) {
	t.Parallel()
	rt := fakeruntime.New()
	env := workerEnvelope("s3src", map[string]any{
		"image":                "datascape-s3source-connect:local",
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
		ord := runtime.OrdinalName("s3src", i)
		if _, found, err := rt.Inspect(context.Background(), ord); err != nil || !found {
			t.Errorf("ordinal %s not found after reconcile: found=%v err=%v", ord, found, err)
		}
	}
}

func merge(base map[string]any, key string, value any) map[string]any {
	out := make(map[string]any, len(base)+1)
	for k, v := range base {
		out[k] = v
	}
	out[key] = value
	return out
}
