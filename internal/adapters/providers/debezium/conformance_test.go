package debezium

import (
	"testing"

	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/reconciler/conformance"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// TestConformance is the E6 conformance-suite retrofit's exemplar for
// debezium (docs/planning/08 E6 done-note's recorded follow-up; ADR 028):
// drives the Provider (Kafka Connect worker) kind's Reconcile/Probe/
// Destroy — reconcileWorker is pure container-lifecycle
// (providerkit.EnsureInstance, settling on the worker container's own
// declared HealthCheck, which the fake always reports healthy for; no real
// Kafka/Connect dial anywhere in the worker's own startup path), the same
// Connect-worker family shape s3sink/jdbcsink/s3source's own exemplars
// share (docs/planning/08 C3).
//
// The Binding (connector) kind is OUT of this fast-tier suite, mirroring
// redpanda's EventStream scoping decision
// (docs/contributing/provider-authoring.md §6): reconcileConnector
// registers/diffs a live CDC connector via kafkaconnect's real Kafka
// Connect REST client (connectorConfigDrift's GetConnectorConfig, a live
// preflight database dial via providerkit.VerifyDatabaseConnection) — no
// dialer seam to intercept short of faking both Connect's REST API and a
// real database wire protocol, which ADR 028 §2's fake-honesty rule would
// require pinning against real systems' observed behavior before trusting
// a green result. Covered instead by the Docker integration suite
// (cmd/platformctl's CDC scenarios).
//
// CapabilityChecks exercises the two error-returning capability interfaces
// this provider declares: SpecValidator (the connectPort/workers mutual
// exclusion) and BindingOptionsValidator (options.tables must be a
// non-empty list of table names).
func TestConformance(t *testing.T) {
	conformance.Run(t, conformance.Harness{
		NewRuntime: func() runtime.ContainerRuntime { return fakeruntime.New() },
		Provider:   func() reconciler.Provider { return New() },
		Resource: func(rt runtime.ContainerRuntime, namePrefix string, i int) reconciler.Request {
			name := namePrefix + "-a"
			if i == 1 {
				name = namePrefix + "-b"
			}
			env := workerEnvelope(name, map[string]any{
				"bootstrapServers": "broker:29092",
			})
			return reconciler.Request{
				Resource: env,
				Provider: env,
				Runtime:  rt,
				Facts:    reconciler.StaticFacts{},
			}
		},
		CapabilityChecks: func(p reconciler.Provider) []conformance.CapabilityCheck {
			sv := p.(reconciler.SpecValidator)
			bov := p.(reconciler.BindingOptionsValidator)
			return []conformance.CapabilityCheck{
				{
					Name: "ValidateSpec/connectPort-with-workers-refused",
					Invoke: func() error {
						return sv.ValidateSpec(provider.Provider{
							Type:          "debezium",
							Configuration: map[string]any{"bootstrapServers": "broker:29092", "workers": float64(3), "connectPort": float64(8083)},
						})
					},
					WantSubstrings: []string{"spec.configuration.connectPort cannot be combined with spec.configuration.workers"},
				},
				{
					Name: "ValidateBindingOptions/tables-empty-refused",
					Invoke: func() error {
						return bov.ValidateBindingOptions("cdc", map[string]any{"tables": []any{}})
					},
					WantSubstrings: []string{"options.tables must be a non-empty list of table names"},
				},
			}
		},
	})
}
