// Package provider defines the Provider kind: a technology (type) and where
// it runs (runtime). See docs/planning/03-resource-model-reference.md §4.
package provider

import (
	"fmt"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

type Provider struct {
	Type          string // "redpanda" | "postgres" | "debezium" | "s3" | "minio" | "s3sink" | "openlineage" | "noop"
	RuntimeType   string // "docker" | "kubernetes" (future) | "external" (future)
	RuntimeConfig map[string]any
	Configuration map[string]any // provider-specific, schema keyed by type
	SecretRefs    []string
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
	return p, p.validate(e.Metadata.Name)
}

func (p Provider) validate(name string) error {
	if p.Type == "" {
		return fmt.Errorf("Provider %q: spec.type is required", name)
	}
	if p.RuntimeType == "" {
		return fmt.Errorf("Provider %q: spec.runtime.type is required", name)
	}
	return nil
}
