package redpanda

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// endpointsFrom parses providerState["endpoints"] the same way a real state
// load does: ToState()'s []map[string]any survives a JSON round-trip as
// []any of map[string]any, not the Go slice type directly — reading it
// straight out of a freshly-returned status.Status (no persistence in
// between, as in these tests) would skip that round-trip and always find
// nothing (docs/planning/08 D1, internal/domain/endpoint's own tests).
func endpointsFrom(providerState map[string]any) endpoint.List {
	raw, err := json.Marshal(providerState[endpoint.Key])
	if err != nil {
		return nil
	}
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil
	}
	return endpoint.FromState(decoded)
}

func providerEnvelope(name string, configuration map[string]any) resource.Envelope {
	e := resource.Envelope{}
	e.APIVersion = "datascape.io/v1alpha1"
	e.Kind = "Provider"
	e.Metadata.Name = name
	e.Spec = map[string]any{
		"type":          "redpanda",
		"runtime":       map[string]any{"type": "fake"},
		"configuration": configuration,
	}
	return e
}

// TestSchemaRegistryEnabled covers the enabled|disabled enum
// (docs/planning/08 D1) — unset and anything but the literal "enabled"
// string leaves the registry off.
func TestSchemaRegistryEnabled(t *testing.T) {
	cases := []struct {
		name string
		v    any
		want bool
	}{
		{"unset", nil, false},
		{"enabled", "enabled", true},
		{"disabled", "disabled", false},
		{"wrong type", true, false},
		{"typo", "Enabled", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := provider.Provider{Configuration: map[string]any{}}
			if c.v != nil {
				cfg.Configuration["schemaRegistry"] = c.v
			}
			if got := schemaRegistryEnabled(cfg); got != c.want {
				t.Errorf("schemaRegistryEnabled(%v) = %v, want %v", c.v, got, c.want)
			}
		})
	}
}

// TestValidateSpecSchemaRegistry: schemaRegistry's enum shape itself
// (docs/planning/08 E5) is now schemas/v1alpha1/fragments/provider/
// redpanda.json's job (see cmd/platformctl's negative-test corpus) — this
// only covers that ValidateSpec still accepts every legal value (nothing
// here rejects a legal manifest) and stays a no-op for the field otherwise.
func TestValidateSpecSchemaRegistry(t *testing.T) {
	p := New()
	for _, v := range []string{"enabled", "disabled"} {
		cfg := provider.Provider{Configuration: map[string]any{"schemaRegistry": v}}
		if err := p.ValidateSpec(cfg); err != nil {
			t.Errorf("ValidateSpec rejected valid value %q: %v", v, err)
		}
	}
	// Unset is valid (registry stays off).
	if err := p.ValidateSpec(provider.Provider{Configuration: map[string]any{}}); err != nil {
		t.Errorf("ValidateSpec rejected an unset schemaRegistry: %v", err)
	}
}

// TestSupportedSchemaFormats implements reconciler.SchemaRegistryCapableProvider:
// docs/planning/08 D1's compatibility check reads this to decide whether a
// Binding's avro/protobuf format is viable against this broker's config.
func TestSupportedSchemaFormats(t *testing.T) {
	p := New()
	disabled := provider.Provider{Configuration: map[string]any{}}
	if got := p.SupportedSchemaFormats(disabled); len(got) != 1 || got[0] != "json" {
		t.Errorf("disabled registry supported formats = %v, want [json]", got)
	}
	enabled := provider.Provider{Configuration: map[string]any{"schemaRegistry": "enabled"}}
	got := p.SupportedSchemaFormats(enabled)
	want := map[string]bool{"avro": true, "json": true, "protobuf": true}
	if len(got) != len(want) {
		t.Fatalf("enabled registry supported formats = %v, want %v", got, want)
	}
	for _, f := range got {
		if !want[f] {
			t.Errorf("unexpected format %q in %v", f, got)
		}
	}
}

// TestSchemaRegistryHostPortDistinctFromKafkaPort guards against a
// regression where both auto-allocated ports hash on the same seed (the
// broker's own name) and collide — hostport.Resolve is deterministic per
// *name*, so the schema registry's seed must differ from the Kafka port's.
func TestSchemaRegistryHostPortDistinctFromKafkaPort(t *testing.T) {
	cfg := provider.Provider{Configuration: map[string]any{}}
	const name = "lake-redpanda"
	kafkaPort := hostPort(cfg, name)
	registryPort := schemaRegistryHostPort(cfg, name)
	if kafkaPort == registryPort {
		t.Fatalf("kafka host port and schema-registry host port collided: both %d", kafkaPort)
	}
	// Deterministic across calls.
	if schemaRegistryHostPort(cfg, name) != registryPort {
		t.Error("schemaRegistryHostPort not deterministic")
	}
	// An explicit pin wins over auto-allocation.
	cfg.Configuration["schemaRegistryPort"] = 18081
	if got := schemaRegistryHostPort(cfg, name); got != 18081 {
		t.Errorf("explicit schemaRegistryPort not honored: got %d", got)
	}
}

func TestSchemaRegistryInternalAddr(t *testing.T) {
	if got, want := schemaRegistryInternalAddr("kafka-cluster"), "http://kafka-cluster:8081"; got != want {
		t.Errorf("schemaRegistryInternalAddr = %q, want %q", got, want)
	}
}

// TestKafkaBootstrapAddress covers reconciler.KafkaBootstrapAddressProvider
// (docs/planning/08 E2): identical to internalAddr, and stable across an
// empty vs. populated cfg since the internal Kafka port isn't configurable.
func TestKafkaBootstrapAddress(t *testing.T) {
	p := New()
	if got, want := p.KafkaBootstrapAddress("lake-redpanda", provider.Provider{}), "lake-redpanda:29092"; got != want {
		t.Errorf("KafkaBootstrapAddress = %q, want %q", got, want)
	}
	cfg := provider.Provider{Configuration: map[string]any{"kafkaPort": 19093}}
	if got, want := p.KafkaBootstrapAddress("lake-redpanda", cfg), "lake-redpanda:29092"; got != want {
		t.Errorf("KafkaBootstrapAddress with host kafkaPort set = %q, want %q (internal port is fixed, not the host port)", got, want)
	}
}

// TestReconcileBrokerRegistryDisabled is a regression guard: the pre-D1
// behavior (no schema-registry port) is unchanged when the field is unset —
// "kafka" and the always-on admin-API "metrics" endpoint (docs/planning/08
// C9) are the only two endpoints published.
func TestReconcileBrokerRegistryDisabled(t *testing.T) {
	rt := fakeruntime.New()
	env := providerEnvelope("local-redpanda", map[string]any{})
	p := New()
	st, err := p.Reconcile(context.Background(), reconciler.Request{Resource: env, Provider: env, Runtime: rt})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	eps := endpointsFrom(st.ProviderState)
	if len(eps) != 2 {
		t.Fatalf("endpoints = %+v, want exactly two (\"kafka\", \"metrics\")", eps)
	}
	names := map[string]bool{}
	for _, ep := range eps {
		names[ep.Name] = true
	}
	if !names["kafka"] || !names["metrics"] {
		t.Fatalf("endpoints = %+v, want \"kafka\" and \"metrics\"", eps)
	}
	ctrState, found, err := rt.Inspect(context.Background(), "local-redpanda")
	if err != nil || !found {
		t.Fatalf("Inspect: found=%v err=%v", found, err)
	}
	for _, port := range ctrState.Ports {
		if port.ContainerPort == schemaRegistryPort {
			t.Fatalf("schema-registry port %d published while disabled", schemaRegistryPort)
		}
	}
}

// TestReconcileBrokerRegistryEnabledPublishesPort: with the registry
// enabled, the broker container is configured with the schema-registry
// port (Audience: host) even though the fake runtime cannot serve real HTTP
// for waitSchemaRegistryReady to observe — EnsureContainer's recorded spec
// is enough to prove the wiring reaches the runtime port, independent of
// readiness (which the Docker integration test covers for real).
func TestReconcileBrokerRegistryEnabledPublishesPort(t *testing.T) {
	rt := fakeruntime.New()
	env := providerEnvelope("local-redpanda", map[string]any{"schemaRegistry": "enabled"})
	p := New()
	// waitSchemaRegistryReady's retry loop otherwise runs for its full
	// hardcoded 120s timeout, since the fake runtime never actually serves
	// HTTP; a short context deadline makes WithReachable's ctx.Done() branch
	// return almost immediately instead.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := p.Reconcile(ctx, reconciler.Request{Resource: env, Provider: env, Runtime: rt})
	if err == nil {
		t.Fatal("want an error: the fake runtime cannot serve the readiness check's real HTTP request")
	}
	ctrState, found, ierr := rt.Inspect(context.Background(), "local-redpanda")
	if ierr != nil || !found {
		t.Fatalf("Inspect: found=%v err=%v", found, ierr)
	}
	var gotRegistryPort bool
	for _, port := range ctrState.Ports {
		if port.ContainerPort == schemaRegistryPort {
			gotRegistryPort = true
			if port.Audience != runtime.AudienceHost {
				t.Errorf("schema-registry port audience = %q, want %q", port.Audience, runtime.AudienceHost)
			}
		}
	}
	if !gotRegistryPort {
		t.Fatalf("ports = %+v, want a %d entry for the schema registry", ctrState.Ports, schemaRegistryPort)
	}
}

// TestValidateSpecBrokers covers docs/adr/017 §a.4/§a.8's SpecValidator
// half: host-port pins and the schema registry are refused alongside a
// declared brokers count. (brokers' own positive-integer shape is now
// schemas/v1alpha1/fragments/provider/redpanda.json's job, docs/planning/08
// E5 — see cmd/platformctl's negative-test corpus. The HighAvailability
// gate itself is enforced by cmd/platformctl's checkHighAvailabilityGate,
// not here.)
func TestValidateSpecBrokers(t *testing.T) {
	p := New()
	for _, v := range []any{1, 3, float64(3)} {
		cfg := provider.Provider{Configuration: map[string]any{"brokers": v}}
		if err := p.ValidateSpec(cfg); err != nil {
			t.Errorf("ValidateSpec rejected valid brokers %v: %v", v, err)
		}
	}
	for _, key := range []string{"kafkaPort", "adminPort", "schemaRegistryPort"} {
		cfg := provider.Provider{Configuration: map[string]any{"brokers": 3, key: 19192}}
		if err := p.ValidateSpec(cfg); err == nil {
			t.Errorf("ValidateSpec accepted a %s pin combined with brokers", key)
		}
	}
	cfg := provider.Provider{Configuration: map[string]any{"brokers": 3, "schemaRegistry": "enabled"}}
	if err := p.ValidateSpec(cfg); err == nil {
		t.Error("ValidateSpec accepted schemaRegistry: enabled combined with brokers")
	}
	// A pin without brokers stays valid (the legacy single-broker shape).
	if err := p.ValidateSpec(provider.Provider{Configuration: map[string]any{"kafkaPort": 19192}}); err != nil {
		t.Errorf("ValidateSpec rejected a kafkaPort pin without brokers: %v", err)
	}
}

// TestValidateStreamReplication covers reconciler.StreamReplicationValidator
// (docs/adr/017 §a.7): replication must not exceed the configured broker
// count; an undeclared brokers key bounds it at 1.
func TestValidateStreamReplication(t *testing.T) {
	p := New()
	if err := p.ValidateStreamReplication(provider.Provider{Configuration: map[string]any{"brokers": 3}}, 3); err != nil {
		t.Errorf("replication 3 <= brokers 3 rejected: %v", err)
	}
	err := p.ValidateStreamReplication(provider.Provider{Configuration: map[string]any{"brokers": 2}}, 3)
	if err == nil {
		t.Fatal("replication 3 > brokers 2 accepted")
	}
	if !strings.Contains(err.Error(), "3") || !strings.Contains(err.Error(), "2") {
		t.Errorf("error must name both numbers, got: %v", err)
	}
	if err := p.ValidateStreamReplication(provider.Provider{Configuration: map[string]any{}}, 2); err == nil {
		t.Error("replication 2 with brokers undeclared (capacity 1) accepted")
	}
	if err := p.ValidateStreamReplication(provider.Provider{Configuration: map[string]any{}}, 1); err != nil {
		t.Errorf("replication 1 with brokers undeclared rejected: %v", err)
	}
	// Redpanda refuses even factors > 1 outright ("replication factor must
	// be odd"); validate must catch it before apply does (ADR 011).
	if err := p.ValidateStreamReplication(provider.Provider{Configuration: map[string]any{"brokers": 4}}, 2); err == nil {
		t.Error("even replication factor 2 accepted; redpanda requires odd factors")
	}
	if err := p.ValidateStreamReplication(provider.Provider{Configuration: map[string]any{"brokers": 4}}, 3); err != nil {
		t.Errorf("odd replication 3 <= brokers 4 rejected: %v", err)
	}
}

// TestKafkaBootstrapAddressMultiBroker: with brokers declared the graph-
// inferred bootstrap address is the comma-joined ordinal list (docs/adr/017
// §a.4), still computed from manifest facts alone.
func TestKafkaBootstrapAddressMultiBroker(t *testing.T) {
	p := New()
	cfg := provider.Provider{Configuration: map[string]any{"brokers": 3}}
	want := "lake-redpanda-0:29092,lake-redpanda-1:29092,lake-redpanda-2:29092"
	if got := p.KafkaBootstrapAddress("lake-redpanda", cfg); got != want {
		t.Errorf("KafkaBootstrapAddress(brokers: 3) = %q, want %q", got, want)
	}
}

// reconcileBrokerSetOnFake runs a brokers-declared reconcile against the
// fake runtime with a short deadline: the container wiring all happens, then
// the cluster-formation wait necessarily fails (the fake cannot serve real
// Kafka — the same pattern TestReconcileBrokerRegistryEnabledPublishesPort
// uses for the registry's HTTP readiness). Returns the reconcile error.
func reconcileBrokerSetOnFake(t *testing.T, rt *fakeruntime.Runtime, env resource.Envelope) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	p := New()
	_, err := p.Reconcile(ctx, reconciler.Request{Resource: env, Provider: env, Runtime: rt})
	return err
}

// TestReconcileBrokerSet proves the multi-broker reconcile shape against the
// fake runtime (docs/adr/017): three ordinal-named units with the declared
// listener set, per-ordinal + aggregate endpoint facts via
// brokerSetProviderState (question b's decision), and idempotency of the
// container wiring. Cluster-formation readiness itself (which the fake
// cannot serve) is covered live by TestRedpandaHAEndToEnd.
func TestReconcileBrokerSet(t *testing.T) {
	rt := fakeruntime.New()
	env := providerEnvelope("rp-ha", map[string]any{"brokers": 3})
	if err := reconcileBrokerSetOnFake(t, rt, env); err == nil {
		t.Fatal("want a cluster-formation error: the fake runtime cannot serve real Kafka")
	}

	for i := 0; i < 3; i++ {
		ord := runtime.OrdinalName("rp-ha", i)
		ordState, found, err := rt.Inspect(context.Background(), ord)
		if err != nil || !found {
			t.Fatalf("ordinal %s: found=%v err=%v", ord, found, err)
		}
		declared := map[int]string{}
		for _, port := range ordState.Ports {
			declared[port.ContainerPort] = port.Audience
		}
		if declared[externalKafkaPort] != runtime.AudienceHost {
			t.Errorf("%s: external kafka port audience = %q, want host", ord, declared[externalKafkaPort])
		}
		if declared[internalKafkaPort] != runtime.AudienceInternal || declared[rpcPort] != runtime.AudienceInternal {
			t.Errorf("%s: internal/rpc listener not declared internal: %v", ord, declared)
		}
	}

	providerState, err := brokerSetProviderState(context.Background(), rt, "rp-ha", 3, "fake-id")
	if err != nil {
		t.Fatalf("brokerSetProviderState: %v", err)
	}
	eps := endpointsFrom(providerState)
	names := map[string]string{}
	for _, ep := range eps {
		names[ep.Name] = ep.Internal
	}
	wantList := "rp-ha-0:29092,rp-ha-1:29092,rp-ha-2:29092"
	if names["kafka"] != wantList {
		t.Errorf("aggregate kafka endpoint Internal = %q, want %q", names["kafka"], wantList)
	}
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("kafka-%d", i)
		if want := fmt.Sprintf("rp-ha-%d:29092", i); names[key] != want {
			t.Errorf("endpoint %s Internal = %q, want %q", key, names[key], want)
		}
	}
	if _, ok := names["metrics"]; !ok {
		t.Error("metrics endpoint not published for the set shape")
	}
	if got := providerState["brokers"]; got != 3 {
		t.Errorf("providerState brokers = %v, want 3", got)
	}

	// Idempotency of the container wiring: an unchanged second reconcile
	// makes zero mutations (both attempts then fail the formation wait
	// identically).
	before := rt.MutationCount
	if err := reconcileBrokerSetOnFake(t, rt, env); err == nil {
		t.Fatal("want a cluster-formation error on the second reconcile too")
	}
	if rt.MutationCount != before {
		t.Errorf("second identical Reconcile mutated runtime state (%d -> %d)", before, rt.MutationCount)
	}
}

// TestReconcileBrokerSetScaleDownRefused pins docs/adr/017 §a.5: shrinking
// brokers is refused at reconcile with an error naming both counts and the
// destroy-and-recreate remedy.
func TestReconcileBrokerSetScaleDownRefused(t *testing.T) {
	rt := fakeruntime.New()
	p := New()
	env3 := providerEnvelope("rp-ha", map[string]any{"brokers": 3})
	if err := reconcileBrokerSetOnFake(t, rt, env3); err == nil {
		t.Fatal("want a cluster-formation error: the fake runtime cannot serve real Kafka")
	}
	env1 := providerEnvelope("rp-ha", map[string]any{"brokers": 1})
	_, err := p.Reconcile(context.Background(), reconciler.Request{Resource: env1, Provider: env1, Runtime: rt})
	if err == nil {
		t.Fatal("scale-down brokers 3 -> 1 was not refused")
	}
	for _, want := range []string{"3", "1", "destroy and recreate"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("scale-down refusal missing %q: %v", want, err)
		}
	}
	// The refusal changed nothing: all three ordinals still present.
	for i := 0; i < 3; i++ {
		if _, found, _ := rt.Inspect(context.Background(), runtime.OrdinalName("rp-ha", i)); !found {
			t.Errorf("ordinal %d missing after refused scale-down", i)
		}
	}
}

// TestProbeBrokerSetMissingOrdinal covers docs/adr/017 §a.6's runtime half:
// a missing ordinal is drift with a reason naming exactly which broker.
func TestProbeBrokerSetMissingOrdinal(t *testing.T) {
	rt := fakeruntime.New()
	p := New()
	env := providerEnvelope("rp-ha", map[string]any{"brokers": 3})
	if err := reconcileBrokerSetOnFake(t, rt, env); err == nil {
		t.Fatal("want a cluster-formation error: the fake runtime cannot serve real Kafka")
	}
	if err := rt.Remove(context.Background(), runtime.OrdinalName("rp-ha", 1)); err != nil {
		t.Fatalf("out-of-band ordinal removal: %v", err)
	}
	st, err := p.Probe(context.Background(), reconciler.Request{Resource: env, Provider: env, Runtime: rt})
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	c, ok := st.Condition(status.DriftDetected)
	if !ok || c.Status != status.True {
		t.Fatalf("DriftDetected = %+v, want True", c)
	}
	if !strings.Contains(c.Reason, status.ReasonBrokerMissing) || !strings.Contains(c.Reason, "rp-ha-1") {
		t.Errorf("drift reason = %q, want %s naming rp-ha-1", c.Reason, status.ReasonBrokerMissing)
	}
}
