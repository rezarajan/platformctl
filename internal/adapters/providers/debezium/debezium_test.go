package debezium

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
		"type":          "debezium",
		"runtime":       map[string]any{"type": "fake"},
		"configuration": configuration,
	}
	return e
}

// TestReconcileWorkerBootstrapServersInferred covers docs/planning/08 E2:
// when spec.configuration.bootstrapServers is unset, the worker container
// starts with req.KafkaBootstrapServers (the engine's graph-inferred
// value) instead of failing — and publishes the effective value into
// providerState so it stays visible (`state inspect`), not silently baked
// in.
func TestReconcileWorkerBootstrapServersInferred(t *testing.T) {
	t.Parallel()
	rt := fakeruntime.New()
	env := workerEnvelope("cdc", map[string]any{"replicationSecretRef": "creds"})
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
	ctrState, found, err := rt.Inspect(context.Background(), "cdc")
	if err != nil || !found {
		t.Fatalf("Inspect: found=%v err=%v", found, err)
	}
	if got := ctrState.Env["BOOTSTRAP_SERVERS"]; got != "broker:29092" {
		t.Errorf("container BOOTSTRAP_SERVERS = %q, want %q", got, "broker:29092")
	}
}

// TestReconcileWorkerBootstrapServersRequiredWithoutInference: when neither
// spec.configuration.bootstrapServers nor req.KafkaBootstrapServers is set
// (nothing to infer from), the worker still fails clearly instead of
// starting with an empty Kafka address.
func TestReconcileWorkerBootstrapServersRequiredWithoutInference(t *testing.T) {
	t.Parallel()
	rt := fakeruntime.New()
	env := workerEnvelope("cdc", map[string]any{"replicationSecretRef": "creds"})
	p := New()
	req := reconciler.Request{Resource: env, Provider: env, Runtime: rt}
	if _, err := p.Reconcile(context.Background(), req); err == nil {
		t.Fatal("want an error when bootstrapServers is neither declared nor inferable")
	}
}

// TestReconcileWorkerBootstrapServersExplicitWins: an explicit
// spec.configuration.bootstrapServers always wins over
// req.KafkaBootstrapServers — the engine only populates the latter when the
// former is absent (engine.resolveKafkaBootstrapServers), but this guards
// the provider's own fallback order too.
func TestReconcileWorkerBootstrapServersExplicitWins(t *testing.T) {
	t.Parallel()
	rt := fakeruntime.New()
	env := workerEnvelope("cdc", map[string]any{
		"replicationSecretRef": "creds",
		"bootstrapServers":     "explicit-broker:29092",
	})
	p := New()
	req := reconciler.Request{
		Resource:              env,
		Provider:              env,
		Runtime:               rt,
		KafkaBootstrapServers: "inferred-broker:29092",
	}
	st, err := p.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := st.ProviderState["bootstrapServers"]; got != "explicit-broker:29092" {
		t.Errorf("providerState[bootstrapServers] = %v, want explicit value to win", got)
	}
}

// TestApplyConverterConfigJSON covers the default/unset format: unchanged
// pre-D1 behavior (JsonConverter, schemas disabled), no registry needed.
func TestApplyConverterConfigJSON(t *testing.T) {
	t.Parallel()
	for _, format := range []string{"", "json"} {
		config := map[string]string{}
		if err := applyConverterConfig(config, format, "", ""); err != nil {
			t.Fatalf("format %q: %v", format, err)
		}
		if config["key.converter"] != "org.apache.kafka.connect.json.JsonConverter" {
			t.Errorf("format %q: key.converter = %q", format, config["key.converter"])
		}
		if config["key.converter.schemas.enable"] != "false" {
			t.Errorf("format %q: schemas.enable = %q, want false", format, config["key.converter.schemas.enable"])
		}
		if _, ok := config["key.converter.schema.registry.url"]; ok {
			t.Errorf("format %q: unexpected schema.registry.url key set for json", format)
		}
	}
}

// TestApplyConverterConfigAvro covers docs/planning/08 D1's Avro path: the
// Confluent Avro converter class, wired to the registry URL the engine
// resolved (never a guessed address) — and a clear error, not a silently
// wrong config, when the registry URL is empty (dependency-graph ordering
// should prevent this in practice; this is the defensive fallback).
func TestApplyConverterConfigAvro(t *testing.T) {
	t.Parallel()
	config := map[string]string{}
	if err := applyConverterConfig(config, "avro", "", "http://kafka-cluster:8081"); err != nil {
		t.Fatalf("applyConverterConfig: %v", err)
	}
	if config["key.converter"] != "io.confluent.connect.avro.AvroConverter" {
		t.Errorf("key.converter = %q", config["key.converter"])
	}
	if config["value.converter"] != "io.confluent.connect.avro.AvroConverter" {
		t.Errorf("value.converter = %q", config["value.converter"])
	}
	if config["key.converter.schema.registry.url"] != "http://kafka-cluster:8081" {
		t.Errorf("key.converter.schema.registry.url = %q", config["key.converter.schema.registry.url"])
	}
	if config["value.converter.schema.registry.url"] != "http://kafka-cluster:8081" {
		t.Errorf("value.converter.schema.registry.url = %q", config["value.converter.schema.registry.url"])
	}
	if _, ok := config["key.converter.schemas.enable"]; ok {
		t.Error("schemas.enable should not be set for avro (implicit in the converter)")
	}
	// DNS-label resource names legally contain hyphens, which are illegal in
	// Avro namespaces — without the adjustment modes the registry 422s every
	// hyphenated topic prefix and the task FAILs after registration.
	if config["schema.name.adjustment.mode"] != "avro" {
		t.Errorf("schema.name.adjustment.mode = %q, want \"avro\"", config["schema.name.adjustment.mode"])
	}
	if config["field.name.adjustment.mode"] != "avro" {
		t.Errorf("field.name.adjustment.mode = %q, want \"avro\"", config["field.name.adjustment.mode"])
	}

	if err := applyConverterConfig(map[string]string{}, "avro", "", ""); err == nil {
		t.Error("want an error when format is avro but no registry URL resolved")
	}
}

// TestApplyConverterConfigProtobuf mirrors the avro case for protobuf.
func TestApplyConverterConfigProtobuf(t *testing.T) {
	t.Parallel()
	config := map[string]string{}
	if err := applyConverterConfig(config, "protobuf", "", "http://kafka-cluster:8081"); err != nil {
		t.Fatalf("applyConverterConfig: %v", err)
	}
	if config["key.converter"] != "io.confluent.connect.protobuf.ProtobufConverter" {
		t.Errorf("key.converter = %q", config["key.converter"])
	}
	if err := applyConverterConfig(map[string]string{}, "protobuf", "", ""); err == nil {
		t.Error("want an error when format is protobuf but no registry URL resolved")
	}
}

// TestApplyConverterConfigConverterOverride: an explicit options.converter
// wins over the format-derived default class, for both json and
// schema-carrying formats.
func TestApplyConverterConfigConverterOverride(t *testing.T) {
	t.Parallel()
	config := map[string]string{}
	if err := applyConverterConfig(config, "avro", "com.example.CustomAvroConverter", "http://kafka-cluster:8081"); err != nil {
		t.Fatalf("applyConverterConfig: %v", err)
	}
	if config["key.converter"] != "com.example.CustomAvroConverter" || config["value.converter"] != "com.example.CustomAvroConverter" {
		t.Errorf("converter override not applied: %+v", config)
	}
}

// TestApplyConverterConfigUnknownFormat: a format outside json/avro/protobuf
// is rejected — compatibility.Check should already have caught this at
// validate time, but the provider defends its own invariant too.
func TestApplyConverterConfigUnknownFormat(t *testing.T) {
	t.Parallel()
	if err := applyConverterConfig(map[string]string{}, "xml", "", ""); err == nil {
		t.Error("want an error for an unrecognized format")
	}
}

// TestValidateBindingOptionsFormat covers the shape half of docs/planning/08
// D1 (registry availability is compatibility.Check's job, not this one's).
func TestValidateBindingOptionsFormat(t *testing.T) {
	t.Parallel()
	p := New()
	for _, format := range []string{"json", "avro", "protobuf"} {
		if err := p.ValidateBindingOptions("cdc", map[string]any{"format": format}); err != nil {
			t.Errorf("format %q rejected: %v", format, err)
		}
	}
	if err := p.ValidateBindingOptions("cdc", map[string]any{"format": "xml"}); err == nil {
		t.Error("want an error for an unrecognized options.format")
	}
	if err := p.ValidateBindingOptions("cdc", map[string]any{"converter": ""}); err == nil {
		t.Error("want an error for an empty options.converter")
	}
	if err := p.ValidateBindingOptions("cdc", map[string]any{"converter": "com.example.Custom"}); err != nil {
		t.Errorf("valid options.converter rejected: %v", err)
	}
}

// TestReconcileWorkerWorkersReplicaSet covers docs/planning/08 C3:
// declaring configuration.workers: N fans the Connect worker out to N
// ordinals (ContainerSpec.Replicas, StableIdentity: false — no per-ordinal
// storage/hostname identity needed, unlike redpanda's brokers), and the
// declared count is echoed into providerState for operators.
func TestReconcileWorkerWorkersReplicaSet(t *testing.T) {
	t.Parallel()
	rt := fakeruntime.New()
	env := workerEnvelope("cdc", map[string]any{
		"replicationSecretRef": "creds",
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
		ord := runtime.OrdinalName("cdc", i)
		if _, found, err := rt.Inspect(context.Background(), ord); err != nil || !found {
			t.Errorf("ordinal %s not found after reconcile: found=%v err=%v", ord, found, err)
		}
	}
}

// TestReconcileWorkerWorkersUndeclaredIsSingleContainer guards the zero-
// behavior-change bar: configuration.workers absent must reconcile the
// exact pre-C3 single-container shape (no "-0" ordinal suffix), the same
// assertion ADR 017's brokers opt-in makes for redpanda.
func TestReconcileWorkerWorkersUndeclaredIsSingleContainer(t *testing.T) {
	t.Parallel()
	rt := fakeruntime.New()
	env := workerEnvelope("cdc", map[string]any{
		"replicationSecretRef": "creds",
		"bootstrapServers":     "broker:29092",
	})
	p := New()
	req := reconciler.Request{Resource: env, Provider: env, Runtime: rt}
	if _, err := p.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if _, found, _ := rt.Inspect(context.Background(), "cdc"); !found {
		t.Fatal("literal container \"cdc\" not found")
	}
	if _, found, _ := rt.Inspect(context.Background(), "cdc-0"); found {
		t.Error("ordinal \"cdc-0\" must not exist when workers is undeclared")
	}
}

// TestValidateSpecWorkers covers the shape half of docs/planning/08 C3
// (gate enforcement is cmd/platformctl's checkHighAvailabilityGate, not
// this method — docs/adr/017 §a.8's established split). workers' own
// positive-integer shape is now schemas/v1alpha1/fragments/provider/
// debezium.json's job (docs/planning/08 E5) — see cmd/platformctl's
// negative-test corpus; this only covers that every legal value still
// passes.
func TestValidateSpecWorkers(t *testing.T) {
	t.Parallel()
	p := New()
	base := map[string]any{"bootstrapServers": "broker:29092"}
	for _, v := range []any{1, 2, float64(3)} {
		cfg := provider.Provider{Configuration: mergeConfig(base, "workers", v)}
		if err := p.ValidateSpec(cfg); err != nil {
			t.Errorf("workers %v rejected: %v", v, err)
		}
	}
}

// TestValidateSpecWorkersRefusesConnectPortPin guards the real Docker bug
// a pinned connectPort combined with workers > 1 would hit: every ordinal
// would inherit the identical HostPort (ordinalContainerSpec copies Ports
// verbatim), so the second ordinal's container create fails with a
// port-already-allocated error — refused at validate instead, mirroring
// docs/adr/017 §a.4's identical refusal for redpanda's brokers.
func TestValidateSpecWorkersRefusesConnectPortPin(t *testing.T) {
	t.Parallel()
	p := New()
	cfg := provider.Provider{Configuration: map[string]any{
		"bootstrapServers": "broker:29092",
		"workers":          2,
		"connectPort":      18085,
	}}
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

// TestServerIDUniquePerConnector guards docs/planning/07 §2.2: two MySQL
// connectors on the same server must not share a replication server id (the
// previous formula was constant per engine, so they kicked each other's
// binlog session off).
func TestServerIDUniquePerConnector(t *testing.T) {
	t.Parallel()
	a := serverID("orders-cdc")
	b := serverID("customers-cdc")
	if a == b {
		t.Fatalf("serverID collided for distinct connectors: %d", a)
	}
	if a < 100000 || b < 100000 {
		t.Errorf("serverID below floor: %d, %d", a, b)
	}
	if a != serverID("orders-cdc") {
		t.Error("serverID not deterministic for the same name")
	}
}
