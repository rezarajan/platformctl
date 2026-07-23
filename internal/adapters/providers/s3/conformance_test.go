package s3

import (
	"testing"

	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/reconciler/conformance"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// TestConformance is the E6 conformance-suite retrofit's exemplar for s3
// (docs/planning/08 E6 done-note's recorded follow-up; ADR 028): drives the
// Provider (instance) kind's single-container shape —
// spec.configuration.nodes left undeclared — through Reconcile/Probe/
// Destroy. reconcileInstance's non-node path is pure container-lifecycle
// (providerkit.EnsureInstance: EnsureNetwork/EnsureVolume/EnsureContainer/
// WaitHealthy, settling on the container's own declared HealthCheck, which
// the fake always reports healthy for — no waitAPIReady-style real dial),
// the same container-lifecycle shape internal/adapters/providers/redpanda's
// own broker exemplar established.
//
// Two shapes are deliberately OUT of this fast-tier suite, mirroring
// redpanda's EventStream scoping decision
// (docs/contributing/provider-authoring.md §6 — "if your provider's
// serving check speaks a real application-layer wire protocol... there is
// currently no general-purpose fake for it"):
//
//   - The Dataset kind (reconcileDataset/probeDataset) dials a real S3-API
//     client (the MinIO Go SDK: ensureBucket, ensureLifecycle, bucketExists,
//     prefixListable) with no dialer seam to intercept short of faking the
//     full S3 API surface, which ADR 028 §2's fake-honesty rule would
//     require pinning against a real MinIO's observed behavior before
//     trusting a green result — covered instead by the Docker integration
//     suite (cmd/platformctl/*_integration_test.go's lakehouse/backup
//     scenarios).
//   - spec.configuration.nodes (the StableIdentity distributed-MinIO shape,
//     docs/planning/08 C4) settles via waitNodeSetServing, which opens a
//     real S3-API ListBuckets call through the fake's reported address —
//     the identical real-protocol constraint, so this suite exercises only
//     the pre-C4 single-container path (the shape every manifest that
//     doesn't opt into distributed mode uses).
//
// CapabilityChecks exercises ValidateSpec's two independent cross-field
// rules: configuration.imagePullSecretRef must be wired into
// spec.secretRefs (docs/planning/08 A1), and configuration.nodes rejects
// MinIO's unsupported 2-3 node topology (docs/planning/08 C4) — both
// already unit-tested in s3_test.go; reproduced here as the suite's own
// documented capability-error-format evidence.
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
					"type":       "s3",
					"runtime":    map[string]any{"type": "fake"},
					"secretRefs": []any{"root"},
					"configuration": map[string]any{
						"rootSecretRef": "root",
					},
				},
			}
			return reconciler.Request{
				Resource: env,
				Provider: env,
				Runtime:  rt,
				Secrets: map[string]map[string]string{
					"root": {"username": "minioadmin", "password": "minioadmin-pw"},
				},
				Facts: reconciler.StaticFacts{},
			}
		},
		CapabilityChecks: func(p reconciler.Provider) []conformance.CapabilityCheck {
			sv := p.(reconciler.SpecValidator)
			return []conformance.CapabilityCheck{
				{
					Name: "ValidateSpec/imagePullSecretRef-not-wired",
					Invoke: func() error {
						return sv.ValidateSpec(provider.Provider{
							Type:          "s3",
							Configuration: map[string]any{"rootSecretRef": "root", "imagePullSecretRef": "registry-creds"},
							SecretRefs:    []string{"root"}, // registry-creds deliberately not listed
						})
					},
					WantSubstrings: []string{"imagePullSecretRef", "must also be listed in spec.secretRefs"},
				},
				{
					Name: "ValidateSpec/nodes-unsupported-topology",
					Invoke: func() error {
						return sv.ValidateSpec(provider.Provider{
							Type:          "s3",
							Configuration: map[string]any{"rootSecretRef": "root", "nodes": float64(2)},
							SecretRefs:    []string{"root"},
						})
					},
					WantSubstrings: []string{"is not a supported MinIO topology"},
				},
			}
		},
	})
}
