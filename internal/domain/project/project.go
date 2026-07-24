// Package project defines the Project kind: the project root config
// (datascape.yaml), loaded before the manifest set (docs/adr/035 decision
// 1, docs/planning/08-production-readiness-plan.md §7.12 M1). A Project
// declares the ONE runtime every Provider in the project targets — the
// Go-module shape — so a Provider can drop its own spec.runtime entirely
// and inherit this one. See internal/application/manifest.LoadProject/
// ResolveProjectRuntime for how a Project is loaded and applied, and
// docs/planning/03-resource-model-reference.md §1.1 for the documented
// shape.
package project

import (
	"fmt"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

// Runtime is the project's single declared runtime. Type is the dispatch
// fact (docs/adr/007 amendment, docs/adr/030 decision 3 — the same
// vocabulary provider.RuntimeType uses: provider.RuntimeTypeDocker/
// RuntimeTypeKubernetes/RuntimeTypeFake). Config is the full spec.runtime
// map as declared — type plus any runtime-specific fields (network,
// access, resources, ...), the identical shape a Provider's own
// spec.runtime carries — copied verbatim onto every Provider that omits
// its own.
type Runtime struct {
	Type   string
	Config map[string]any
}

// Project is the parsed form of datascape.yaml.
type Project struct {
	Name    string
	Runtime Runtime
	// ZeroTrust defaults to true (docs/adr/035 decision 3). M1 only
	// parses and stores this field; no engine behavior reads it yet — M4
	// wires ZeroTrust default-on behavior from it.
	ZeroTrust bool
}

// FromEnvelope parses a Project's spec, mirroring
// internal/domain/provider.FromEnvelope's shape/conventions.
func FromEnvelope(e resource.Envelope) (Project, error) {
	p := Project{Name: e.Metadata.Name, ZeroTrust: true}
	if rt, ok := e.Spec["runtime"].(map[string]any); ok {
		p.Runtime.Type, _ = rt["type"].(string)
		p.Runtime.Config = rt
	}
	if v, ok := e.Spec["zeroTrust"].(bool); ok {
		p.ZeroTrust = v
	}
	return p, p.validate(e.Metadata.Name)
}

func (p Project) validate(name string) error {
	if p.Runtime.Type == "" {
		return fmt.Errorf("Project %q: spec.runtime.type is required", name)
	}
	return nil
}
