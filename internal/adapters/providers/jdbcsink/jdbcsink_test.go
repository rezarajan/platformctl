package jdbcsink

import (
	"context"
	"testing"

	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// Interface assertions: jdbcsink must implement the capability + validation
// interfaces docs/planning/08 D3 requires (ADR 009).
var (
	_ reconciler.DatabaseSinkCapableProvider = (*Provider)(nil)
	_ reconciler.SpecValidator               = (*Provider)(nil)
	_ reconciler.BindingOptionsValidator     = (*Provider)(nil)
)

func workerEnvelope(name string, configuration map[string]any) resource.Envelope {
	e := resource.Envelope{}
	e.APIVersion = "datascape.io/v1alpha1"
	e.Kind = "Provider"
	e.Metadata.Name = name
	e.Spec = map[string]any{
		"type":          "jdbcsink",
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

func TestSupportedSinkEngines(t *testing.T) {
	p := New()
	got := p.SupportedSinkEngines()
	want := map[string]bool{"postgres": true, "mysql": true}
	if len(got) != len(want) {
		t.Fatalf("SupportedSinkEngines = %v, want exactly %v", got, want)
	}
	for _, e := range got {
		if !want[e] {
			t.Errorf("unexpected engine %q in SupportedSinkEngines", e)
		}
	}
}

func TestReconcileWorkerBootstrapServersInferred(t *testing.T) {
	rt := fakeruntime.New()
	env := workerEnvelope("jsink", map[string]any{
		"image": "datascape-jdbcsink-connect:local",
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
	ctrState, found, err := rt.Inspect(context.Background(), "jsink")
	if err != nil || !found {
		t.Fatalf("Inspect: found=%v err=%v", found, err)
	}
	if got := ctrState.Env["BOOTSTRAP_SERVERS"]; got != "broker:29092" {
		t.Errorf("container BOOTSTRAP_SERVERS = %q, want %q", got, "broker:29092")
	}
}

func TestReconcileWorkerBootstrapServersRequiredWithoutInference(t *testing.T) {
	rt := fakeruntime.New()
	env := workerEnvelope("jsink", map[string]any{"image": "datascape-jdbcsink-connect:local"})
	p := New()
	req := reconciler.Request{Resource: env, Provider: env, Runtime: rt}
	if _, err := p.Reconcile(context.Background(), req); err == nil {
		t.Fatal("want an error when bootstrapServers is neither declared nor inferable")
	}
}

// sinkRequest builds the Binding-kind Request buildDesiredConnector
// consumes: a managed target Source reached via its own Provider container
// name (the common case), mirroring sinkRequest's construction in
// s3sink_test.go.
func sinkRequest(registryURL string, options map[string]any) reconciler.Request {
	worker := workerEnvelope("jsink-worker", map[string]any{
		"image":                "datascape-jdbcsink-connect:local",
		"bootstrapServers":     "broker:29092",
		"credentialsSecretRef": "creds",
	})
	es := simpleEnvelope("EventStream", "events", map[string]any{
		"providerRef": map[string]any{"name": "broker"},
	})
	tgt := simpleEnvelope("Source", "warehouse", map[string]any{
		"engine":      "postgres",
		"providerRef": map[string]any{"name": "warehouse-pg"},
		"postgres":    map[string]any{"database": "analytics", "schema": "public"},
	})
	pgProvider := simpleEnvelope("Provider", "warehouse-pg", map[string]any{
		"type":    "postgres",
		"runtime": map[string]any{"type": "fake"},
	})
	bindingSpec := map[string]any{
		"mode":        "sink",
		"sourceRef":   map[string]any{"name": "events"},
		"targetRef":   map[string]any{"name": "warehouse"},
		"providerRef": map[string]any{"name": "jsink-worker"},
	}
	if options != nil {
		bindingSpec["options"] = options
	}
	b := simpleEnvelope("Binding", "events-to-warehouse", bindingSpec)
	return reconciler.Request{
		Resource: b,
		Provider: worker,
		Resources: map[resource.Key]resource.Envelope{
			es.Key():         es,
			tgt.Key():        tgt,
			pgProvider.Key(): pgProvider,
			worker.Key():     worker,
		},
		Secrets:           map[string]map[string]string{"creds": {"username": "u", "password": "p"}},
		SchemaRegistryURL: registryURL,
	}
}

func TestBuildDesiredConnectorInsertModeDefault(t *testing.T) {
	d, err := buildDesiredConnector(sinkRequest("http://broker:8081", map[string]any{"format": "avro"}))
	if err != nil {
		t.Fatalf("buildDesiredConnector: %v", err)
	}
	if got, want := d.config["insert.mode"], "insert"; got != want {
		t.Errorf("insert.mode = %q, want %q", got, want)
	}
	if got, want := d.config["connection.url"], "jdbc:postgresql://warehouse-pg:5432/analytics"; got != want {
		t.Errorf("connection.url = %q, want %q", got, want)
	}
	if got, want := d.config["topics.regex"], "^events(\\..*)?$"; got != want {
		t.Errorf("topics.regex = %q, want %q (must match the bare EventStream topic AND any CDC per-table-prefixed one)", got, want)
	}
	// Table name derivation: no options.table override -> the connector's
	// own default (table.name.format: "${topic}") applies, i.e. this key is
	// left unset.
	if _, ok := d.config["table.name.format"]; ok {
		t.Errorf("table.name.format must be unset without an options.table override, got %q", d.config["table.name.format"])
	}
	if _, ok := d.config["pk.mode"]; ok {
		t.Errorf("pk.mode must be unset for insert mode, got %q", d.config["pk.mode"])
	}
}

func TestBuildDesiredConnectorUpsertModeDefaultsToRecordKey(t *testing.T) {
	d, err := buildDesiredConnector(sinkRequest("http://broker:8081", map[string]any{
		"format": "avro",
		"mode":   "upsert",
	}))
	if err != nil {
		t.Fatalf("buildDesiredConnector: %v", err)
	}
	if got, want := d.config["insert.mode"], "upsert"; got != want {
		t.Errorf("insert.mode = %q, want %q", got, want)
	}
	if got, want := d.config["pk.mode"], "record_key"; got != want {
		t.Errorf("pk.mode = %q, want %q (no pkFields override)", got, want)
	}
	if _, ok := d.config["pk.fields"]; ok {
		t.Error("pk.fields must be unset when relying on the full record key")
	}
}

func TestBuildDesiredConnectorUpsertWithExplicitPKFields(t *testing.T) {
	d, err := buildDesiredConnector(sinkRequest("http://broker:8081", map[string]any{
		"format":   "avro",
		"mode":     "upsert",
		"pkFields": []any{"id", "tenant"},
	}))
	if err != nil {
		t.Fatalf("buildDesiredConnector: %v", err)
	}
	if got, want := d.config["pk.mode"], "record_value"; got != want {
		t.Errorf("pk.mode = %q, want %q", got, want)
	}
	if got, want := d.config["pk.fields"], "id,tenant"; got != want {
		t.Errorf("pk.fields = %q, want %q", got, want)
	}
}

func TestBuildDesiredConnectorTableOverride(t *testing.T) {
	d, err := buildDesiredConnector(sinkRequest("http://broker:8081", map[string]any{
		"format": "avro",
		"table":  "attendance",
	}))
	if err != nil {
		t.Fatalf("buildDesiredConnector: %v", err)
	}
	if got, want := d.config["table.name.format"], "attendance"; got != want {
		t.Errorf("table.name.format = %q, want %q", got, want)
	}
}

func TestBuildDesiredConnectorUnwrapOption(t *testing.T) {
	d, err := buildDesiredConnector(sinkRequest("http://broker:8081", map[string]any{
		"format": "avro",
		"unwrap": true,
	}))
	if err != nil {
		t.Fatalf("buildDesiredConnector: %v", err)
	}
	if got, want := d.config["transforms"], "unwrap"; got != want {
		t.Errorf("transforms = %q, want %q", got, want)
	}
	if got, want := d.config["transforms.unwrap.type"], "io.debezium.transforms.ExtractNewRecordState"; got != want {
		t.Errorf("transforms.unwrap.type = %q, want %q", got, want)
	}
}

func TestBuildDesiredConnectorUnwrapAbsentByDefault(t *testing.T) {
	d, err := buildDesiredConnector(sinkRequest("http://broker:8081", map[string]any{"format": "avro"}))
	if err != nil {
		t.Fatalf("buildDesiredConnector: %v", err)
	}
	if _, ok := d.config["transforms"]; ok {
		t.Error("transforms must be unset when options.unwrap is not declared")
	}
}

func TestBuildDesiredConnectorAvroConverters(t *testing.T) {
	d, err := buildDesiredConnector(sinkRequest("http://broker:8081", map[string]any{"format": "avro"}))
	if err != nil {
		t.Fatalf("buildDesiredConnector: %v", err)
	}
	for _, k := range []string{"key.converter", "value.converter"} {
		if got, want := d.config[k], "io.confluent.connect.avro.AvroConverter"; got != want {
			t.Errorf("%s = %q, want %q", k, got, want)
		}
	}
	for _, k := range []string{"key.converter.schema.registry.url", "value.converter.schema.registry.url"} {
		if got, want := d.config[k], "http://broker:8081"; got != want {
			t.Errorf("%s = %q, want %q", k, got, want)
		}
	}
}

func TestBuildDesiredConnectorAvroWithoutRegistryFails(t *testing.T) {
	if _, err := buildDesiredConnector(sinkRequest("", map[string]any{"format": "avro"})); err == nil {
		t.Fatal("want an error for avro without a resolved registry URL")
	}
}

func TestBuildDesiredConnectorMissingCredentialsFails(t *testing.T) {
	req := sinkRequest("http://broker:8081", map[string]any{"format": "avro"})
	req.Secrets = nil
	if _, err := buildDesiredConnector(req); err == nil {
		t.Fatal("want an error when no credentials resolve")
	}
}

// TestDeadLetterConfigTranslation covers docs/planning/08 D6's requirement
// that DLQ options work on this provider's sink Binding too.
func TestDeadLetterConfigTranslation(t *testing.T) {
	req := sinkRequest("http://broker:8081", map[string]any{
		"format":     "avro",
		"deadLetter": map[string]any{"stream": "dlq-events"},
	})
	d, err := buildDesiredConnector(req)
	if err != nil {
		t.Fatalf("buildDesiredConnector: %v", err)
	}
	want := map[string]string{
		"errors.tolerance":                                "all",
		"errors.deadletterqueue.topic.name":               "dlq-events",
		"errors.deadletterqueue.topic.replication.factor": "1",
		"errors.deadletterqueue.context.headers.enable":   "true",
	}
	for k, v := range want {
		if got := d.config[k]; got != v {
			t.Errorf("%s = %q, want %q", k, got, v)
		}
	}
}

func TestDeadLetterConfigReplicationFromEventStream(t *testing.T) {
	req := sinkRequest("http://broker:8081", map[string]any{
		"format":     "avro",
		"deadLetter": map[string]any{"stream": "dlq-events", "tolerance": "none"},
	})
	dlq := simpleEnvelope("EventStream", "dlq-events", map[string]any{
		"providerRef": map[string]any{"name": "broker"},
		"replication": 3,
	})
	req.Resources[dlq.Key()] = dlq
	d, err := buildDesiredConnector(req)
	if err != nil {
		t.Fatalf("buildDesiredConnector: %v", err)
	}
	if got := d.config["errors.tolerance"]; got != "none" {
		t.Errorf("errors.tolerance = %q, want none", got)
	}
	if got := d.config["errors.deadletterqueue.topic.replication.factor"]; got != "3" {
		t.Errorf("errors.deadletterqueue.topic.replication.factor = %q, want 3", got)
	}
}

// TestBuildDesiredConnectorExternalSourceViaConnection mirrors debezium's
// coverage of the SOURCE-side Connection resolution, applied to the TARGET
// side: an external Source's managed Connection resolves the JDBC host via
// runtime.EnsureReachable and supplies its own secretRef credentials in
// preference to the provider-level credentialsSecretRef.
func TestBuildDesiredConnectorExternalSourceViaConnection(t *testing.T) {
	worker := workerEnvelope("jsink-worker", map[string]any{
		"image":                "datascape-jdbcsink-connect:local",
		"bootstrapServers":     "broker:29092",
		"credentialsSecretRef": "fallback-creds",
	})
	es := simpleEnvelope("EventStream", "events", map[string]any{
		"providerRef": map[string]any{"name": "broker"},
	})
	tgt := simpleEnvelope("Source", "ext-warehouse", map[string]any{
		"engine":        "postgres",
		"external":      true,
		"connectionRef": map[string]any{"name": "warehouse-conn"},
		"postgres":      map[string]any{"database": "analytics"},
	})
	connSecret := "conn-creds"
	conn := simpleEnvelope("Connection", "warehouse-conn", map[string]any{
		"providerRef": map[string]any{"name": "proxy"},
		"target":      "real-warehouse.internal:5432",
		"port":        6543,
		"secretRef":   map[string]any{"name": connSecret},
	})
	bindingSpec := map[string]any{
		"mode":        "sink",
		"sourceRef":   map[string]any{"name": "events"},
		"targetRef":   map[string]any{"name": "ext-warehouse"},
		"providerRef": map[string]any{"name": "jsink-worker"},
		"options":     map[string]any{"format": "avro"},
	}
	b := simpleEnvelope("Binding", "events-to-ext-warehouse", bindingSpec)
	req := reconciler.Request{
		Resource: b,
		Provider: worker,
		Resources: map[resource.Key]resource.Envelope{
			es.Key():     es,
			tgt.Key():    tgt,
			conn.Key():   conn,
			worker.Key(): worker,
		},
		Secrets: map[string]map[string]string{
			connSecret:       {"username": "conn-user", "password": "conn-pass"},
			"fallback-creds": {"username": "fallback-user", "password": "fallback-pass"},
		},
		SchemaRegistryURL: "http://broker:8081",
	}
	d, err := buildDesiredConnector(req)
	if err != nil {
		t.Fatalf("buildDesiredConnector: %v", err)
	}
	if got, want := d.config["connection.url"], "jdbc:postgresql://warehouse-conn:6543/analytics"; got != want {
		t.Errorf("connection.url = %q, want %q", got, want)
	}
	if got, want := d.config["connection.user"], "conn-user"; got != want {
		t.Errorf("connection.user = %q, want %q (Connection secretRef must win over provider-level fallback)", got, want)
	}
	if d.preflightConnectionName != "warehouse-conn" {
		t.Errorf("preflightConnectionName = %q, want %q", d.preflightConnectionName, "warehouse-conn")
	}
}

func TestValidateBindingOptionsRequiresSchemaCarryingFormat(t *testing.T) {
	p := New()
	if err := p.ValidateBindingOptions("sink", map[string]any{}); err == nil {
		t.Error("unset options.format accepted, want an error (jdbcsink requires avro/protobuf)")
	}
	if err := p.ValidateBindingOptions("sink", map[string]any{"format": "json"}); err == nil {
		t.Error("options.format: json accepted, want an error")
	}
	if err := p.ValidateBindingOptions("sink", map[string]any{"format": "avro"}); err != nil {
		t.Errorf("valid options.format rejected: %v", err)
	}
}

func TestValidateBindingOptionsMode(t *testing.T) {
	p := New()
	base := map[string]any{"format": "avro"}
	if err := p.ValidateBindingOptions("sink", merge(base, "mode", "upsert")); err != nil {
		t.Errorf("valid options.mode rejected: %v", err)
	}
	if err := p.ValidateBindingOptions("sink", merge(base, "mode", "delete")); err == nil {
		t.Error("bogus options.mode accepted")
	}
}

func TestValidateBindingOptionsPKFieldsShape(t *testing.T) {
	p := New()
	base := map[string]any{"format": "avro"}
	if err := p.ValidateBindingOptions("sink", merge(base, "pkFields", []any{"id"})); err != nil {
		t.Errorf("valid options.pkFields rejected: %v", err)
	}
	if err := p.ValidateBindingOptions("sink", merge(base, "pkFields", []any{})); err == nil {
		t.Error("empty options.pkFields accepted")
	}
	if err := p.ValidateBindingOptions("sink", merge(base, "pkFields", []any{""})); err == nil {
		t.Error("empty-string options.pkFields entry accepted")
	}
}

func TestValidateSpecCredentialsSecretRefOptional(t *testing.T) {
	p := New()
	cfg := provider.Provider{
		Configuration: map[string]any{
			"image":            "datascape-jdbcsink-connect:local",
			"bootstrapServers": "broker:29092",
		},
	}
	if err := p.ValidateSpec(cfg); err != nil {
		t.Errorf("ValidateSpec without credentialsSecretRef rejected: %v (must be optional — a Connection may supply its own)", err)
	}
}

func TestValidateSpecCredentialsSecretRefMustBeDeclared(t *testing.T) {
	p := New()
	cfg := provider.Provider{
		Configuration: map[string]any{
			"image":                "datascape-jdbcsink-connect:local",
			"bootstrapServers":     "broker:29092",
			"credentialsSecretRef": "creds",
		},
	}
	if err := p.ValidateSpec(cfg); err == nil {
		t.Error("credentialsSecretRef not listed in spec.secretRefs accepted")
	}
}

func TestValidateSpecWorkers(t *testing.T) {
	p := New()
	base := map[string]any{
		"image":            "datascape-jdbcsink-connect:local",
		"bootstrapServers": "broker:29092",
	}
	for _, v := range []any{1, 2, float64(3)} {
		cfg := provider.Provider{Configuration: merge(base, "workers", v)}
		if err := p.ValidateSpec(cfg); err != nil {
			t.Errorf("workers %v rejected: %v", v, err)
		}
	}
	for _, v := range []any{0, -1, "two", 1.5} {
		cfg := provider.Provider{Configuration: merge(base, "workers", v)}
		if err := p.ValidateSpec(cfg); err == nil {
			t.Errorf("workers %v (%T) accepted, want an error", v, v)
		}
	}
}

func TestValidateSpecWorkersRefusesConnectPortPin(t *testing.T) {
	p := New()
	cfg := provider.Provider{
		Configuration: map[string]any{
			"image":            "datascape-jdbcsink-connect:local",
			"bootstrapServers": "broker:29092",
			"workers":          2,
			"connectPort":      18186,
		},
	}
	if err := p.ValidateSpec(cfg); err == nil {
		t.Fatal("want an error when connectPort is pinned alongside workers")
	}
}

// TestReconcileWorkerWorkersReplicaSet mirrors s3sink's/debezium's identical
// coverage of docs/planning/08 C3.
func TestReconcileWorkerWorkersReplicaSet(t *testing.T) {
	rt := fakeruntime.New()
	env := workerEnvelope("jsink", map[string]any{
		"image":            "datascape-jdbcsink-connect:local",
		"bootstrapServers": "broker:29092",
		"workers":          2,
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
		ord := runtime.OrdinalName("jsink", i)
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
