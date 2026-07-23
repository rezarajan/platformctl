package redpanda

import (
	"testing"

	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/reconciler/conformance"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// TestConformance is the container-lifecycle exemplar (docs/planning/08 E6,
// ADR 028): drives the Provider (broker) kind of Reconcile/Probe/Destroy —
// pure container-lifecycle against runtime.ContainerRuntime, no real Kafka
// wire protocol required. The EventStream kind (reconcileTopic) dials a real
// Kafka admin client (kadm/kgo) with no port/interface seam to fake — every
// existing unit test in this package already scopes itself to the broker
// path for the same reason (see e.g. TestReconcileBrokerRegistryDisabled);
// EventStream reconciliation is proven by the Docker integration suite, not
// the fast tier. This is a deliberate, documented scoping decision (see
// docs/contributing/provider-authoring.md), not an oversight — the
// lifecycle contract itself does not care which Kind a Provider is
// reconciling.
//
// The schema registry is left disabled in this fixture: enabling it makes
// reconcileBroker's waitSchemaRegistryReady dial real HTTP
// (TestReconcileBrokerRegistryEnabledPublishesPort below documents the same
// constraint for the pre-existing unit tests), which the fake runtime cannot
// serve — exactly the class of provider behavior this exemplar's own doc
// comment says stays out of the fast tier.
func TestConformance(t *testing.T) {
	conformance.Run(t, conformance.Harness{
		NewRuntime: func() runtime.ContainerRuntime { return fakeruntime.New() },
		Provider:   func() reconciler.Provider { return New() },
		Resource: func(rt runtime.ContainerRuntime, namePrefix string, i int) reconciler.Request {
			name := namePrefix + "-a"
			if i == 1 {
				name = namePrefix + "-b"
			}
			env := providerEnvelope(name, map[string]any{})
			return reconciler.Request{
				Resource: env,
				Provider: env,
				Runtime:  rt,
				Facts:    reconciler.StaticFacts{},
			}
		},
		// CapabilityChecks exercises the two error-returning capability
		// interfaces redpanda declares (docs/planning/02 §4.2):
		// SpecValidator (cross-field validate-time rules a JSON Schema
		// fragment can't express) and StreamReplicationValidator (docs/adr/017
		// §a.7 — bounding an EventStream's replication against this
		// Provider's own configured broker count). proxy/noop declare
		// neither, which is why their own exemplars pass CapabilityChecks:
		// nil — the suite must not fabricate a check a provider doesn't own.
		CapabilityChecks: func(p reconciler.Provider) []conformance.CapabilityCheck {
			sv := p.(reconciler.SpecValidator)
			srv := p.(reconciler.StreamReplicationValidator)
			return []conformance.CapabilityCheck{
				{
					Name: "ValidateSpec/brokers-with-kafkaPort-pin-refused",
					Invoke: func() error {
						return sv.ValidateSpec(provider.Provider{
							Type:          "redpanda",
							Configuration: map[string]any{"brokers": 3, "kafkaPort": 9092},
						})
					},
					WantSubstrings: []string{"spec.configuration.kafkaPort cannot be combined with spec.configuration.brokers"},
				},
				{
					Name: "ValidateStreamReplication/exceeds-broker-count",
					Invoke: func() error {
						return srv.ValidateStreamReplication(provider.Provider{
							Type:          "redpanda",
							Configuration: map[string]any{"brokers": 1},
						}, 3)
					},
					WantSubstrings: []string{"spec.replication 3 exceeds the configured broker count 1"},
				},
				{
					Name: "ValidateStreamReplication/even-factor-refused",
					Invoke: func() error {
						return srv.ValidateStreamReplication(provider.Provider{
							Type:          "redpanda",
							Configuration: map[string]any{"brokers": 3},
						}, 2)
					},
					WantSubstrings: []string{"spec.replication 2 is even; redpanda requires an odd replication factor"},
				},
			}
		},
	})
}
