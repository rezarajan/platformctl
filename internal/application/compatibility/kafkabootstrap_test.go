package compatibility

import (
	"errors"
	"strings"
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
)

// kafkaBootstrapStub is a local double for reconciler.KafkaBootstrapAddressProvider
// (docs/planning/08 E2) — mirrors redpanda's real KafkaBootstrapAddress
// (name + fixed port), without importing the concrete adapter (CLAUDE.md's
// application-test layering exception).
type kafkaBootstrapStub struct{ stubProvider }

func (kafkaBootstrapStub) KafkaBootstrapAddress(name string, _ provider.Provider) string {
	return name + ":29092"
}

// typeResolver dispatches by spec.type, unlike the single-impl resolver()
// helper — needed whenever a test wires two distinct provider types (a
// Connect worker and its broker) into one manifest set.
func typeResolver(byType map[string]reconciler.Provider) ProviderResolver {
	return func(t string) (reconciler.Provider, error) {
		if impl, ok := byType[t]; ok {
			return impl, nil
		}
		return nil, errors.New("unknown provider type " + t)
	}
}

// cdcSinkManifests builds a worker Provider (type "debezium") wired to a
// Binding(mode: cdc) that targets an EventStream realized by a broker
// Provider (type "redpanda") — the minimal graph
// ResolveKafkaBootstrapAddress walks.
func cdcSinkManifests(workerName, brokerName, eventStreamName, bindingName string) []resource.Envelope {
	return []resource.Envelope{
		envelope("Provider", workerName, map[string]any{
			"type":    "debezium",
			"runtime": map[string]any{"type": "fake"},
		}),
		envelope("Provider", brokerName, map[string]any{
			"type":    "redpanda",
			"runtime": map[string]any{"type": "fake"},
		}),
		envelope("Source", "student-database", map[string]any{
			"engine":      "postgres",
			"providerRef": map[string]any{"name": "local-postgres"},
		}),
		envelope("EventStream", eventStreamName, map[string]any{
			"providerRef": map[string]any{"name": brokerName},
		}),
		envelope("Binding", bindingName, map[string]any{
			"mode":        "cdc",
			"sourceRef":   map[string]any{"name": "student-database"},
			"targetRef":   map[string]any{"name": eventStreamName},
			"providerRef": map[string]any{"name": workerName},
		}),
	}
}

func TestResolveKafkaBootstrapAddress_InfersFromGraph(t *testing.T) {
	t.Parallel()
	manifests := cdcSinkManifests("worker", "broker", "events", "cdc-binding")
	resolve := typeResolver(map[string]reconciler.Provider{
		"debezium": stubProvider{"debezium"},
		"redpanda": kafkaBootstrapStub{stubProvider{"redpanda"}},
	})
	workerEnv := manifests[0]

	got := ResolveKafkaBootstrapAddress(workerEnv, manifests, resolve)
	if want := "broker:29092"; got != want {
		t.Errorf("ResolveKafkaBootstrapAddress = %q, want %q", got, want)
	}
}

func TestResolveKafkaBootstrapAddress_NoMatchingBindingReturnsEmpty(t *testing.T) {
	t.Parallel()
	manifests := cdcSinkManifests("worker", "broker", "events", "cdc-binding")
	resolve := typeResolver(map[string]reconciler.Provider{
		"debezium": stubProvider{"debezium"},
		"redpanda": kafkaBootstrapStub{stubProvider{"redpanda"}},
	})
	// A Provider nothing in the manifest set references as providerRef.
	orphan := envelope("Provider", "unused-worker", map[string]any{
		"type":    "debezium",
		"runtime": map[string]any{"type": "fake"},
	})

	if got := ResolveKafkaBootstrapAddress(orphan, manifests, resolve); got != "" {
		t.Errorf("ResolveKafkaBootstrapAddress = %q, want \"\" (no Binding references this Provider)", got)
	}
}

func TestResolveKafkaBootstrapAddress_AmbiguousReturnsEmpty(t *testing.T) {
	t.Parallel()
	// Two Bindings on the same worker, wired to two different brokers via
	// two different EventStreams — an unambiguous single answer doesn't
	// exist, so the caller must fall back to requiring an explicit value.
	manifests := []resource.Envelope{
		envelope("Provider", "worker", map[string]any{
			"type":    "debezium",
			"runtime": map[string]any{"type": "fake"},
		}),
		envelope("Provider", "broker-a", map[string]any{
			"type":    "redpanda",
			"runtime": map[string]any{"type": "fake"},
		}),
		envelope("Provider", "broker-b", map[string]any{
			"type":    "redpanda",
			"runtime": map[string]any{"type": "fake"},
		}),
		envelope("Source", "db-a", map[string]any{
			"engine":      "postgres",
			"providerRef": map[string]any{"name": "local-postgres"},
		}),
		envelope("Source", "db-b", map[string]any{
			"engine":      "postgres",
			"providerRef": map[string]any{"name": "local-postgres"},
		}),
		envelope("EventStream", "events-a", map[string]any{
			"providerRef": map[string]any{"name": "broker-a"},
		}),
		envelope("EventStream", "events-b", map[string]any{
			"providerRef": map[string]any{"name": "broker-b"},
		}),
		envelope("Binding", "cdc-a", map[string]any{
			"mode":        "cdc",
			"sourceRef":   map[string]any{"name": "db-a"},
			"targetRef":   map[string]any{"name": "events-a"},
			"providerRef": map[string]any{"name": "worker"},
		}),
		envelope("Binding", "cdc-b", map[string]any{
			"mode":        "cdc",
			"sourceRef":   map[string]any{"name": "db-b"},
			"targetRef":   map[string]any{"name": "events-b"},
			"providerRef": map[string]any{"name": "worker"},
		}),
	}
	resolve := typeResolver(map[string]reconciler.Provider{
		"debezium": stubProvider{"debezium"},
		"redpanda": kafkaBootstrapStub{stubProvider{"redpanda"}},
	})
	workerEnv := manifests[0]

	if got := ResolveKafkaBootstrapAddress(workerEnv, manifests, resolve); got != "" {
		t.Errorf("ResolveKafkaBootstrapAddress = %q, want \"\" (ambiguous: two distinct brokers)", got)
	}
}

func TestResolveKafkaBootstrapAddress_NonCapableProviderReturnsEmpty(t *testing.T) {
	t.Parallel()
	manifests := cdcSinkManifests("worker", "broker", "events", "cdc-binding")
	// The EventStream's own realizing Provider doesn't implement
	// KafkaBootstrapAddressProvider (e.g. a future non-Kafka EventStream
	// backend) — no address to infer.
	resolve := typeResolver(map[string]reconciler.Provider{
		"debezium": stubProvider{"debezium"},
		"redpanda": stubProvider{"redpanda"},
	})
	workerEnv := manifests[0]

	if got := ResolveKafkaBootstrapAddress(workerEnv, manifests, resolve); got != "" {
		t.Errorf("ResolveKafkaBootstrapAddress = %q, want \"\" (broker provider lacks the capability)", got)
	}
}

// specValidatingBootstrapStub mirrors debezium/s3sink's real ValidateSpec:
// spec.configuration.bootstrapServers is required.
type specValidatingBootstrapStub struct{ stubProvider }

func (specValidatingBootstrapStub) ValidateSpec(cfg provider.Provider) error {
	if v, _ := cfg.Configuration["bootstrapServers"].(string); v == "" {
		return errors.New("spec.configuration.bootstrapServers is required")
	}
	return nil
}

// TestCheck_BootstrapServersInferredAtValidate is the ADR 011 end-to-end
// case (docs/planning/08 E2): a manifest set that omits
// bootstrapServers but has an unambiguous graph-inferable value must
// validate cleanly — the same completeness guarantee as an explicit value,
// checked through the full Check() path (not just ResolveKafkaBootstrapAddress
// in isolation).
func TestCheck_BootstrapServersInferredAtValidate(t *testing.T) {
	t.Parallel()
	manifests := cdcSinkManifests("worker", "broker", "events", "cdc-binding")
	// The worker must also be CDC-capable for Check()'s mode-pairing
	// capability check to pass.
	resolve := typeResolver(map[string]reconciler.Provider{
		"debezium": cdcSpecValidatingStub{specValidatingBootstrapStub{stubProvider{"debezium"}}},
		"redpanda": kafkaBootstrapStub{stubProvider{"redpanda"}},
		"postgres": stubProvider{"postgres"},
	})
	if err := Check(manifests, resolve); err != nil {
		t.Fatalf("Check() rejected an inferable bootstrapServers: %v", err)
	}
}

// TestCheck_BootstrapServersAmbiguousStillRequiresExplicit proves the
// inference never masks a genuine misconfiguration: when the graph can't
// unambiguously supply bootstrapServers, ValidateSpec's own requirement
// still fires.
func TestCheck_BootstrapServersAmbiguousStillRequiresExplicit(t *testing.T) {
	t.Parallel()
	manifests := []resource.Envelope{
		envelope("Provider", "worker", map[string]any{
			"type":    "debezium",
			"runtime": map[string]any{"type": "fake"},
		}),
	}
	resolve := typeResolver(map[string]reconciler.Provider{
		"debezium": specValidatingBootstrapStub{stubProvider{"debezium"}},
	})
	err := Check(manifests, resolve)
	if err == nil {
		t.Fatal("Check() accepted a Provider with no bootstrapServers and no inferable graph")
	}
	if !strings.Contains(err.Error(), "bootstrapServers") {
		t.Errorf("unexpected error: %v", err)
	}
}

// cdcSpecValidatingStub layers CDCCapableProvider on top of
// specValidatingBootstrapStub so Check()'s mode-pairing capability check
// (reconciler.CDCCapableProvider) also passes for the worker.
type cdcSpecValidatingStub struct {
	specValidatingBootstrapStub
}

func (cdcSpecValidatingStub) SupportedSourceEngines() []string {
	return []string{"postgres", "mysql", "mongodb"}
}
