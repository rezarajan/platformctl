// project.go loads the optional project root config, ProjectFileName
// (datascape.yaml), and resolves it onto the Provider envelopes in a
// manifest set (docs/adr/035 decision 1, docs/planning/08 §7.12 M1). See
// internal/domain/project's package comment for the Project shape.
package manifest

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/rezarajan/platformctl/internal/domain/project"
	"github.com/rezarajan/platformctl/internal/domain/resource"
)

// ProjectFileName is the project config's reserved, canonical file name.
// collectFiles excludes it from the ordinary manifest document scan (it
// is not a governed-set document — see schemas.KindFiles's comment on
// "Project"); LoadProject reads it directly instead.
const ProjectFileName = "datascape.yaml"

// LoadProject reads ProjectFileName at path's root — path itself when it
// is a directory, its parent directory when path names a single manifest
// file — and returns the parsed Project. Returns (nil, nil) when no such
// file exists: the backward-compat path (docs/planning/08 M1 accept
// criteria) — an existing manifest set with no datascape.yaml is
// untouched by anything in this file.
func LoadProject(path string) (*project.Project, error) {
	root := path
	if info, err := os.Stat(path); err == nil && !info.IsDir() {
		root = filepath.Dir(path)
	}
	proj, err := readProjectFile(root)
	if err != nil {
		return nil, err
	}
	if proj == nil {
		return nil, nil
	}
	// The ROOT datascape.yaml declares the inventory's single runtime.
	if err := proj.RequireRuntime(); err != nil {
		return nil, err
	}
	return proj, nil
}

// loadIncludeProject reads the datascape.yaml of an INCLUDED member directory
// (docs/adr/035 / M7 — the Helm/Kustomize include pattern): a directory named
// in a parent's spec.resources composes via its OWN datascape.yaml listing
// spec.resources. Returns (nil, nil) when the directory has no datascape.yaml
// (its own error, raised by the caller with member context). Unlike the root
// LoadProject it does NOT require a runtime — the member inherits the root's —
// and REFUSES a runtime override, so the single-runtime invariant holds
// across the whole composed tree, not just among Providers.
func loadIncludeProject(dir string) (*project.Project, error) {
	proj, err := readProjectFile(dir)
	if err != nil {
		return nil, err
	}
	if proj == nil {
		return nil, nil
	}
	if proj.Runtime.Type != "" {
		return nil, fmt.Errorf("%s: an included member must not declare spec.runtime — a project targets one runtime, declared once at the root datascape.yaml", filepath.Join(dir, ProjectFileName))
	}
	return proj, nil
}

// readProjectFile reads, schema-validates, and parses the datascape.yaml in
// dir, returning (nil, nil) when absent. It applies every check common to a
// root project and an included member (exactly-one-document, kind Project,
// apiVersion), leaving the root/include-specific runtime rules to its two
// callers above.
func readProjectFile(dir string) (*project.Project, error) {
	projectPath := filepath.Join(dir, ProjectFileName)

	data, err := os.ReadFile(projectPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read %s: %w", projectPath, err)
	}

	dec := yaml.NewDecoder(strings.NewReader(string(data)))
	var raw map[string]any
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("%s: %w", projectPath, err)
	}
	var extra map[string]any
	if derr := dec.Decode(&extra); derr == nil {
		return nil, fmt.Errorf("%s: exactly one document expected (found more than one)", projectPath)
	}

	if err := validateAgainstSchema(raw); err != nil {
		return nil, fmt.Errorf("%s: %w", projectPath, err)
	}
	env, err := envelopeFrom(raw)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", projectPath, err)
	}
	if env.Kind != "Project" {
		return nil, fmt.Errorf("%s: expected kind Project, got %q", projectPath, env.Kind)
	}
	if !strings.HasPrefix(env.APIVersion, "datascape.io/") {
		return nil, fmt.Errorf("%s: unsupported apiVersion %q (expected datascape.io/v1alpha1)", projectPath, env.APIVersion)
	}

	proj, err := project.FromEnvelope(env)
	if err != nil {
		return nil, err
	}
	return &proj, nil
}

// ResolveProjectRuntime populates every Provider envelope's spec.runtime
// from proj (docs/adr/035 decision 1): a Provider with no spec.runtime
// inherits a CLONE of the project's runtime map (cloned so no two
// Providers, nor the project itself, ever alias the same map — a later
// per-Provider mutation, e.g. a future resources-default pass, must never
// leak across Providers that merely happened to inherit together). A
// Provider that DOES declare spec.runtime is an explicit override,
// refused unless its type matches the project runtime's type (family).
//
// This single per-Provider check is ALSO the single-runtime-per-inventory
// enforcement the Go-module shape calls for (docs/planning/08 M1 item 3):
// once every Provider is proven, one at a time, to resolve to the SAME
// project.Runtime.Type — whether by inheriting it or by an override that
// was refused unless it matched — no two Providers in the inventory can
// ever diverge. A separate whole-inventory scan afterward would be
// checking a fact this loop has already made true; there is deliberately
// no second pass.
//
// proj == nil is the total backward-compat no-op (no datascape.yaml):
// envelopes are never touched, and a Provider with no spec.runtime still
// fails exactly as it always has — provider.Provider.validate's own
// "spec.runtime.type is required", reached a few frames downstream in
// Validate. This is deliberate: examples/cdc-attendance mixes a
// runtime:docker Provider set with one runtime:fake stand-in Provider,
// with no datascape.yaml, and is exercised live by
// cmd/platformctl/acceptance_integration_test.go — proof that
// per-Provider runtime divergence with NO project file must keep
// working exactly as today.
func ResolveProjectRuntime(envelopes []resource.Envelope, proj *project.Project) error {
	if proj == nil {
		return nil
	}
	for _, e := range envelopes {
		if e.Kind != "Provider" {
			continue
		}
		rt, declared := e.Spec["runtime"].(map[string]any)
		if !declared {
			e.Spec["runtime"] = cloneRuntimeMap(proj.Runtime.Config)
			continue
		}
		rtType, _ := rt["type"].(string)
		if rtType != proj.Runtime.Type {
			return fmt.Errorf(
				"Provider %q declares runtime %s but the project runtime is %s; a project targets one runtime — put %q in its own project folder",
				e.Metadata.Name, rtType, proj.Runtime.Type, e.Metadata.Name,
			)
		}
	}
	return nil
}

func cloneRuntimeMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
