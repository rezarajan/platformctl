package placeholder

import (
	"testing"

	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/reconciler/conformance"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// TestConformance is the E6 conformance-suite retrofit's exemplar for
// placeholder (docs/planning/08 E6 done-note's recorded follow-up; ADR 028):
// the Phase 1 "prove the runtime path" provider is pure container-lifecycle
// (EnsureNetwork/EnsureVolume/EnsureContainer/WaitHealthy — see
// placeholder.go's Reconcile), has exactly one Kind ("Provider") and
// declares no capability interface beyond the base reconciler.Provider —
// making it fast-tier-provable end to end with zero scoping decisions
// needed, the same "trivial end of the spectrum" role
// internal/adapters/providers/noop plays among the original three
// exemplars (docs/contributing/provider-authoring.md §10).
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
					"type":    "container",
					"runtime": map[string]any{"type": "fake"},
					"configuration": map[string]any{
						"image": "datascape-placeholder:local",
					},
				},
			}
			return reconciler.Request{
				Resource: env,
				Provider: env,
				Runtime:  rt,
				Facts:    reconciler.StaticFacts{},
			}
		},
		// CapabilityChecks: nil — placeholder implements no capability
		// interface beyond the base reconciler.Provider.
	})
}
