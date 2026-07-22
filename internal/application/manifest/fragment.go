// Provider-owned schema fragments (docs/planning/08 E5): Provider
// spec.configuration, Source/Catalog spec.<engine>, and Binding spec.options
// are open-ended blocks in the core Kind schemas (schemas/v1alpha1/*.json) —
// deliberately so, since a new provider/engine must not require a core
// schema change (docs/planning/02 §3.3). Each provider instead ships a
// narrow JSON-Schema fragment for its own slice of that open block
// (schemas/v1alpha1/fragments/), registered by discriminator in
// schemas.ProviderConfigFragments/SourceEngineFragments/
// CatalogEngineFragments/BindingOptionsFragments. This file composes those
// fragments into Validate so a schema-legal-but-provider-invalid
// configuration fails at validate with a named, actionable error (ADR 011)
// instead of surfacing only at apply/reconcile time.
//
// Fragments are shape-only: type/enum/range/required-field checks a static
// JSON Schema can express. Cross-field rules (a *SecretRef value must also
// appear in spec.secretRefs; bootstrapServers is required only once
// graph-inference has had a chance to supply it; mutual exclusion between a
// replica-count field and a host-port pin) stay in the provider's
// SpecValidator/BindingOptionsValidator Go code — a JSON Schema fragment
// cannot express "this string must equal a value that lives in a sibling
// array/another resource" without hard-coding the value, so those checks are
// not migratable in principle, not merely left for later.
package manifest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/schemas"
)

var (
	fragmentsOnce   sync.Once
	fragmentSchemas map[string]*jsonschema.Schema
	fragmentErr     error
)

// compiledFragments lazily compiles every registered fragment exactly once,
// keyed "<kind>/<discriminator>" (kind one of "provider", "source",
// "catalog", "binding"; discriminator the map key from the corresponding
// schemas.*Fragments map — a provider type, an engine name, or
// "<mode>-<providerType>").
func compiledFragments() (map[string]*jsonschema.Schema, error) {
	fragmentsOnce.Do(func() {
		c := jsonschema.NewCompiler()
		// meta.json is registered so a fragment's nameRef-shaped field (e.g.
		// trino's catalogRef, grafana's prometheusRef) can $ref it by its
		// existing absolute $id, exactly like the core Kind schemas do.
		if err := addResource(c, "v1alpha1/meta.json"); err != nil {
			fragmentErr = err
			return
		}
		result := make(map[string]*jsonschema.Schema)
		groups := []struct {
			kind  string
			files map[string]string
		}{
			{"provider", schemas.ProviderConfigFragments},
			{"source", schemas.SourceEngineFragments},
			{"catalog", schemas.CatalogEngineFragments},
			{"binding", schemas.BindingOptionsFragments},
		}
		// A path can be shared by more than one discriminator (mysql/mariadb,
		// s3/minio both point at one file) — compile each distinct path once,
		// then fan the compiled *Schema out to every discriminator key that
		// names it.
		compiledByPath := make(map[string]*jsonschema.Schema)
		for _, g := range groups {
			for _, path := range g.files {
				if _, done := compiledByPath[path]; done {
					continue
				}
				data, err := schemas.FragmentFS.ReadFile(path)
				if err != nil {
					fragmentErr = fmt.Errorf("read embedded fragment %s: %w", path, err)
					return
				}
				doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
				if err != nil {
					fragmentErr = fmt.Errorf("parse embedded fragment %s: %w", path, err)
					return
				}
				var id struct {
					ID string `json:"$id"`
				}
				if err := json.Unmarshal(data, &id); err != nil || id.ID == "" {
					fragmentErr = fmt.Errorf("embedded fragment %s has no $id", path)
					return
				}
				if err := c.AddResource(id.ID, doc); err != nil {
					fragmentErr = fmt.Errorf("register embedded fragment %s: %w", path, err)
					return
				}
				sch, err := c.Compile(id.ID)
				if err != nil {
					fragmentErr = fmt.Errorf("compile embedded fragment %s: %w", path, err)
					return
				}
				compiledByPath[path] = sch
			}
			for disc, path := range g.files {
				result[g.kind+"/"+disc] = compiledByPath[path]
			}
		}
		fragmentSchemas = result
	})
	return fragmentSchemas, fragmentErr
}

func fragmentMustLoad() map[string]*jsonschema.Schema {
	sch, err := compiledFragments()
	if err != nil {
		// Same posture as compiledMustLoad: a fragment failing to compile is
		// a build defect (a provider author's mistake caught at build time
		// for every consumer, not per-manifest), not a user input problem.
		panic(err)
	}
	return sch
}

// validateBlockAgainstSchema round-trips block through JSON (so numeric/
// nested types match what the validator expects, independent of the YAML
// decoder's Go types — the same reasoning validateAgainstSchema in schema.go
// uses for whole envelopes) and validates it against sch.
func validateBlockAgainstSchema(sch *jsonschema.Schema, block map[string]any) error {
	if block == nil {
		block = map[string]any{}
	}
	buf, err := json.Marshal(block)
	if err != nil {
		return err
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(buf))
	if err != nil {
		return err
	}
	return sch.Validate(doc)
}

// validateProviderConfigurationFragment checks e's spec.configuration
// against the fragment registered for providerType, if any.
func validateProviderConfigurationFragment(e resource.Envelope, providerType string) error {
	if _, registered := schemas.ProviderConfigFragments[providerType]; !registered {
		return nil
	}
	sch := fragmentMustLoad()["provider/"+providerType]
	if sch == nil {
		return nil
	}
	cfg, _ := e.Spec["configuration"].(map[string]any)
	if err := validateBlockAgainstSchema(sch, cfg); err != nil {
		return fmt.Errorf("%s %q: spec.configuration (type %s): %w", e.Kind, e.Metadata.Name, providerType, err)
	}
	return nil
}

// validateEngineFragment checks an engine-discriminated block (Source
// spec.<engine>, Catalog spec.<engine>) against the fragment registered for
// engine within fragments, if any.
func validateEngineFragment(e resource.Envelope, engine string, block map[string]any, fragments map[string]string, kindPrefix string) error {
	if _, registered := fragments[engine]; !registered {
		return nil
	}
	sch := fragmentMustLoad()[kindPrefix+"/"+engine]
	if sch == nil {
		return nil
	}
	if err := validateBlockAgainstSchema(sch, block); err != nil {
		return fmt.Errorf("%s %q: spec.%s (engine %s): %w", e.Kind, e.Metadata.Name, engine, engine, err)
	}
	return nil
}

// validateBindingOptionsFragment checks a Binding's spec.options against the
// fragment registered for "<mode>-<providerType>", if any. providerType is
// resolved from providerTypeByKey — the Provider envelope named by this
// Binding's providerRef, looked up ahead of time across the whole envelope
// set (a Binding may appear before its Provider in file order). An
// unresolvable providerRef is silently skipped here: application/
// compatibility already produces the authoritative, graph-aware
// "does not resolve to a Provider" error for that case, and duplicating it
// here with less context would only confuse the message a user sees first.
func validateBindingOptionsFragment(e resource.Envelope, mode, sourceRefField string, options map[string]any, providerTypeByKey map[resource.Key]string) error {
	ref := resource.RefFromSpec(e.Spec, sourceRefField)
	if ref.Name == "" {
		return nil
	}
	key := ref.Key(e.Metadata.Namespace, "Provider")
	providerType, ok := providerTypeByKey[key]
	if !ok {
		return nil
	}
	disc := mode + "-" + providerType
	if _, registered := schemas.BindingOptionsFragments[disc]; !registered {
		return nil
	}
	sch := fragmentMustLoad()["binding/"+disc]
	if sch == nil {
		return nil
	}
	if err := validateBlockAgainstSchema(sch, options); err != nil {
		return fmt.Errorf("Binding %q: spec.options (mode %s, provider type %s): %w", e.Metadata.Name, mode, providerType, err)
	}
	return nil
}
