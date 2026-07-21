package redpanda

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
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

// TestValidateSpecSchemaRegistry: a typo'd schemaRegistry value fails at
// validate, never as a half-applied platform.
func TestValidateSpecSchemaRegistry(t *testing.T) {
	p := New()
	for _, v := range []string{"enabled", "disabled"} {
		cfg := provider.Provider{Configuration: map[string]any{"schemaRegistry": v}}
		if err := p.ValidateSpec(cfg); err != nil {
			t.Errorf("ValidateSpec rejected valid value %q: %v", v, err)
		}
	}
	cfg := provider.Provider{Configuration: map[string]any{"schemaRegistry": "yes"}}
	if err := p.ValidateSpec(cfg); err == nil {
		t.Error("ValidateSpec accepted an invalid schemaRegistry value")
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

// TestReconcileBrokerRegistryDisabled is a regression guard: the pre-D1
// behavior (no schema-registry port, one "kafka" endpoint only) is
// unchanged when the field is unset.
func TestReconcileBrokerRegistryDisabled(t *testing.T) {
	rt := fakeruntime.New()
	env := providerEnvelope("local-redpanda", map[string]any{})
	p := New()
	st, err := p.Reconcile(context.Background(), reconciler.Request{Resource: env, Provider: env, Runtime: rt})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	eps := endpointsFrom(st.ProviderState)
	if len(eps) != 1 || eps[0].Name != "kafka" {
		t.Fatalf("endpoints = %+v, want exactly one \"kafka\" endpoint", eps)
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
