package noop

import (
	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/reconciler/conformance"
	"github.com/rezarajan/platformctl/internal/ports/runtime"

	"testing"
)

// TestConformance is the trivial exemplar (docs/planning/08 E6, ADR 028):
// noop makes zero runtime calls and publishes no providerState at all —
// proving the suite is meaningful (doesn't crash, doesn't vacuously demand
// facts a provider legitimately has none of) at the empty end of the
// provider-complexity spectrum. See internal/adapters/providers/redpanda and
// internal/adapters/providers/proxy for the container-lifecycle and
// settledness/dial-through exemplars.
func TestConformance(t *testing.T) {
	conformance.Run(t, conformance.Harness{
		NewRuntime: func() runtime.ContainerRuntime { return fakeruntime.New() },
		Provider:   func() reconciler.Provider { return New() },
		Resource: func(rt runtime.ContainerRuntime, namePrefix string, i int) reconciler.Request {
			name := namePrefix + "-a"
			if i == 1 {
				name = namePrefix + "-b"
			}
			env := resource.Envelope{
				GroupVersionKind: resource.GroupVersionKind{APIVersion: "datascape.io/v1alpha1", Kind: "Provider"},
				Metadata:         resource.Metadata{Name: name},
				Spec: map[string]any{
					"type":    "noop",
					"runtime": map[string]any{"type": "fake"},
				},
			}
			return reconciler.Request{
				Resource: env,
				Provider: env,
				Runtime:  rt,
				Facts:    reconciler.StaticFacts{},
			}
		},
		// CapabilityChecks: nil — noop implements no capability interface
		// beyond the base reconciler.Provider.
	})
}
