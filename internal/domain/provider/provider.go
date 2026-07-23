// Package provider defines the Provider kind: a technology (type) and where
// it runs (runtime). See docs/planning/03-resource-model-reference.md §4.
package provider

import (
	"fmt"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

type Provider struct {
	Type          string // "redpanda" | "postgres" | "debezium" | "s3" | "minio" | "s3sink" | "openlineage" | "noop"
	RuntimeType   string // RuntimeTypeDocker | RuntimeTypeKubernetes | RuntimeTypeFake
	RuntimeConfig map[string]any
	Configuration map[string]any // provider-specific, schema keyed by type
	SecretRefs    []string
	// External and ConnectionRef mirror source.Source's identical fields
	// (docs/planning/03 §3.3): a Provider declaring spec.external: true
	// realizes nothing — Datascape never creates/deletes it — and the
	// engine's generic no-provider external path
	// (isExternalNoProvider/reconcileExternal) verifies spec.connectionRef
	// reachability instead of ever calling this Provider's own
	// Reconcile/Probe/Destroy for kind "Provider". A Dataset/Source/Catalog
	// naming this Provider in its own (non-external) providerRef still
	// takes the ordinary reconcile path — resolving this Provider's
	// ConnectionRef itself is that provider implementation's job (e.g. s3's
	// datasetEndpoint, docs/planning/08 C4), mirroring exactly how debezium
	// already resolves an external Source's ConnectionRef.
	External      bool
	ConnectionRef *string
}

func FromEnvelope(e resource.Envelope) (Provider, error) {
	p := Provider{}
	p.Type, _ = e.Spec["type"].(string)
	if rt, ok := e.Spec["runtime"].(map[string]any); ok {
		p.RuntimeType, _ = rt["type"].(string)
		p.RuntimeConfig = rt
	}
	if cfg, ok := e.Spec["configuration"].(map[string]any); ok {
		p.Configuration = cfg
	}
	if refs, ok := e.Spec["secretRefs"].([]any); ok {
		for _, r := range refs {
			if s, ok := r.(string); ok {
				p.SecretRefs = append(p.SecretRefs, s)
			}
		}
	}
	if ext, ok := e.Spec["external"].(bool); ok {
		p.External = ext
	}
	if ref := refName(e.Spec, "connectionRef"); ref != "" {
		p.ConnectionRef = &ref
	}
	return p, p.validate(e.Metadata.Name)
}

func (p Provider) validate(name string) error {
	if p.Type == "" {
		return fmt.Errorf("Provider %q: spec.type is required", name)
	}
	if p.RuntimeType == "" {
		return fmt.Errorf("Provider %q: spec.runtime.type is required", name)
	}
	if p.External && p.ConnectionRef == nil {
		return fmt.Errorf("Provider %q: spec.connectionRef is required when spec.external is true", name)
	}
	return nil
}

func refName(spec map[string]any, field string) string {
	ref, ok := spec[field].(map[string]any)
	if !ok {
		return ""
	}
	name, _ := ref["name"].(string)
	return name
}

// HasSecretRef reports whether name appears in spec.secretRefs — the
// precondition for the engine resolving it and handing its values to the
// provider.
func (p Provider) HasSecretRef(name string) bool {
	for _, r := range p.SecretRefs {
		if r == name {
			return true
		}
	}
	return false
}

// Runtime-type vocabulary (docs/adr/030 decision 3): RuntimeType is the
// dispatch fact (docs/adr/007 amendment — path choice reads this domain
// field, never a type assertion on the runtime), and a dispatch fact
// deserves a greppable, typo-proof spelling. These are the only values a
// registered runtime factory answers to (application/registry).
const (
	RuntimeTypeDocker     = "docker"
	RuntimeTypeKubernetes = "kubernetes"
	RuntimeTypeFake       = "fake"
)
