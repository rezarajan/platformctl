// Package manifest loads a directory or file list, parses YAML/JSON into
// Envelopes, and runs kind-specific validation.
// See docs/planning/02-architecture.md §5.1.
package manifest

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/rezarajan/platformctl/internal/domain/binding"
	"github.com/rezarajan/platformctl/internal/domain/catalog"
	"github.com/rezarajan/platformctl/internal/domain/connection"
	"github.com/rezarajan/platformctl/internal/domain/dataset"
	"github.com/rezarajan/platformctl/internal/domain/eventstream"
	"github.com/rezarajan/platformctl/internal/domain/project"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/secret"
	"github.com/rezarajan/platformctl/internal/domain/source"
	"github.com/rezarajan/platformctl/schemas"
)

// KnownKinds is the closed set of v1 kinds.
var KnownKinds = map[string]bool{
	"Provider":        true,
	"Source":          true,
	"EventStream":     true,
	"Binding":         true,
	"Dataset":         true,
	"SecretReference": true,
	"Catalog":         true,
	"Connection":      true,
}

// Load reads one path (file or directory) and returns validated envelopes.
// Multi-document YAML files are supported.
//
// docs/adr/035 decision 1 (docs/planning/08 M1): before the manifest
// documents are collected, LoadProject reads the optional project root
// config (datascape.yaml) at path's root — nil when absent, the total
// backward-compat no-op. Once every envelope is decoded below,
// ResolveProjectRuntime populates any Provider's missing spec.runtime
// from it (or refuses a mismatched override) BEFORE Validate runs, so
// every caller of Load (this CLI's loadAndValidate, `policy test`,
// compose) gets a fully runtime-resolved envelope set for free, with no
// signature change.
func Load(path string) ([]resource.Envelope, error) {
	proj, err := LoadProject(path)
	if err != nil {
		return nil, err
	}

	files, err := collectFiles(path, proj)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		if proj != nil && len(proj.Resources) > 0 {
			return nil, fmt.Errorf("the %s at %s declares spec.resources but they resolved to no manifest files (*.yaml, *.yml, *.json)", ProjectFileName, path)
		}
		return nil, fmt.Errorf("no manifest files (*.yaml, *.yml, *.json) found under %s", path)
	}

	var envelopes []resource.Envelope
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", f, err)
		}
		dec := yaml.NewDecoder(strings.NewReader(string(data)))
		for docIndex := 0; ; docIndex++ {
			var raw map[string]any
			if err := dec.Decode(&raw); err != nil {
				if err.Error() == "EOF" {
					break
				}
				return nil, fmt.Errorf("%s (document %d): %w", f, docIndex+1, err)
			}
			if len(raw) == 0 {
				continue
			}
			if err := validateAgainstSchema(raw); err != nil {
				return nil, fmt.Errorf("%s (document %d): %w", f, docIndex+1, err)
			}
			env, err := envelopeFrom(raw)
			if err != nil {
				return nil, fmt.Errorf("%s (document %d): %w", f, docIndex+1, err)
			}
			envelopes = append(envelopes, env)
		}
	}

	if err := ResolveProjectRuntime(envelopes, proj); err != nil {
		return nil, err
	}

	if err := Validate(envelopes); err != nil {
		return nil, err
	}
	return envelopes, nil
}

// Validate runs envelope-level and kind-specific validation over the set.
func Validate(envelopes []resource.Envelope) error {
	seen := make(map[resource.Key]bool)
	// providerTypeByKey resolves a Binding's providerRef to its Provider's
	// spec.type ahead of the main pass below (docs/planning/08 E5): a
	// Binding may appear before its Provider in file order, and a Binding's
	// spec.options fragment is keyed by "<mode>-<providerType>".
	providerTypeByKey := make(map[resource.Key]string, len(envelopes))
	for _, e := range envelopes {
		if e.Kind != "Provider" {
			continue
		}
		if t, _ := e.Spec["type"].(string); t != "" {
			providerTypeByKey[e.Key()] = t
		}
	}
	for _, e := range envelopes {
		if err := e.Validate(); err != nil {
			return err
		}
		if !KnownKinds[e.Kind] {
			return fmt.Errorf("%s %q: unknown kind (known: Provider, Source, EventStream, Binding, Dataset, Catalog, Connection, SecretReference)", e.Kind, e.Metadata.Name)
		}
		if !strings.HasPrefix(e.APIVersion, "datascape.io/") {
			return fmt.Errorf("%s %q: unsupported apiVersion %q (expected datascape.io/v1alpha1)", e.Kind, e.Metadata.Name, e.APIVersion)
		}
		k := e.Key()
		if seen[k] {
			return fmt.Errorf("duplicate resource %s", k)
		}
		seen[k] = true

		var err error
		switch e.Kind {
		case "Provider":
			var p provider.Provider
			p, err = provider.FromEnvelope(e)
			if err == nil {
				err = validateProviderConfigurationFragment(e, p.Type)
			}
		case "Source":
			var s source.Source
			s, err = source.FromEnvelope(e)
			if err == nil {
				err = validateEngineFragment(e, s.Engine, s.EngineConfig, schemas.SourceEngineFragments, "source")
			}
		case "EventStream":
			_, err = eventstream.FromEnvelope(e)
		case "Binding":
			var b binding.Binding
			b, err = binding.FromEnvelope(e)
			if err == nil {
				err = validateBindingOptionsFragment(e, string(b.Mode), "providerRef", b.Options, providerTypeByKey)
			}
		case "Dataset":
			_, err = dataset.FromEnvelope(e)
		case "Catalog":
			var c catalog.Catalog
			c, err = catalog.FromEnvelope(e)
			if err == nil {
				err = validateEngineFragment(e, c.Engine, c.EngineConfig, schemas.CatalogEngineFragments, "catalog")
			}
		case "Connection":
			_, err = connection.FromEnvelope(e)
		case "SecretReference":
			err = secretFromEnvelope(e)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func secretFromEnvelope(e resource.Envelope) error {
	ref := secret.SecretReference{Name: e.Metadata.Name}
	backend, _ := e.Spec["backend"].(string)
	ref.Backend = secret.Backend(backend)
	if keys, ok := e.Spec["keys"].([]any); ok {
		for _, k := range keys {
			if s, ok := k.(string); ok {
				ref.Keys = append(ref.Keys, s)
			}
		}
	}
	return ref.Validate()
}

func envelopeFrom(raw map[string]any) (resource.Envelope, error) {
	e := resource.Envelope{}
	e.APIVersion, _ = raw["apiVersion"].(string)
	e.Kind, _ = raw["kind"].(string)

	meta, _ := raw["metadata"].(map[string]any)
	e.Metadata.Name, _ = meta["name"].(string)
	e.Metadata.Namespace, _ = meta["namespace"].(string)
	e.Metadata.Namespace = resource.NormalizeNamespace(e.Metadata.Namespace)
	domain, _ := meta["domain"].(string)
	e.Metadata.Domain = resource.NormalizeDomain(domain)
	e.Metadata.Labels = stringMap(meta["labels"])
	e.Metadata.Annotations = stringMap(meta["annotations"])
	// Decoded explicitly like every sibling field: this copy-field-by-field
	// decoder silently DROPPED metadata.protect from its introduction until
	// the 2026-07 production review (doc 11) — engine-level protect tests
	// stayed green because they construct Envelopes directly, bypassing
	// this loader, so the NFR-3 safety refusal never fired for a real
	// manifest. Found while H5 hit the identical gap for metadata.domain.
	e.Metadata.Protect, _ = meta["protect"].(bool)
	if observers, ok := meta["observers"].([]any); ok {
		for _, o := range observers {
			if om, ok := o.(map[string]any); ok {
				name, _ := om["name"].(string)
				namespace, _ := om["namespace"].(string)
				e.Metadata.Observers = append(e.Metadata.Observers, resource.ObserverRef{Name: name, Namespace: namespace})
			}
		}
	}

	if spec, ok := raw["spec"].(map[string]any); ok {
		e.Spec = spec
	} else {
		e.Spec = map[string]any{}
	}

	if _, hasStatus := raw["status"]; hasStatus {
		return e, fmt.Errorf("%s %q: status is populated by Datascape and must not be hand-authored", e.Kind, e.Metadata.Name)
	}
	return e, nil
}

func stringMap(v any) map[string]string {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, val := range m {
		if s, ok := val.(string); ok {
			out[k] = s
		}
	}
	return out
}

// collectFiles resolves the governed manifest files for a path.
//
// When the project declares spec.resources (docs/adr/035 / M7 — the
// Helm/Kustomize include-members pattern), the set is EXACTLY those declared
// members, resolved recursively and in declared order: a member is a FILE
// (loaded) or a DIRECTORY (composed via its OWN datascape.yaml's
// spec.resources). Nothing is auto-discovered, so a data-platform's planes
// (platform/, sources/, cdc/, sinks/, ...) are named members and anything not
// named — a policies/ channel, a build context, scratch files — is never a
// governed document.
//
// With no spec.resources (proj == nil, the legacy no-datascape.yaml layout,
// or a project that only sets runtime/zeroTrust) the set is the flat
// *.yaml/*.yml/*.json directly in path, in lexical order — exactly as before
// datascape.yaml existed.
func collectFiles(path string, proj *project.Project) ([]string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat %s: %w", path, err)
	}
	if !info.IsDir() {
		return []string{path}, nil
	}
	if proj != nil && len(proj.Resources) > 0 {
		// Declared order is preserved (the author chose it); no sort.
		return collectResources(path, proj.Resources)
	}
	files, err := manifestFilesIn(path)
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	return files, nil
}

// collectResources resolves an explicit spec.resources member list (relative
// to baseDir) into manifest files, recursing into directory members through
// their own datascape.yaml. Declared order is preserved throughout.
func collectResources(baseDir string, resources []string) ([]string, error) {
	var files []string
	for _, r := range resources {
		full := filepath.Join(baseDir, filepath.FromSlash(r))
		info, err := os.Stat(full)
		if err != nil {
			return nil, fmt.Errorf("%s resource %q: %w", ProjectFileName, r, err)
		}
		if !info.IsDir() {
			switch filepath.Ext(full) {
			case ".yaml", ".yml", ".json":
				files = append(files, full)
			default:
				return nil, fmt.Errorf("%s resource %q: not a manifest file (*.yaml, *.yml, *.json) or directory", ProjectFileName, r)
			}
			continue
		}
		sub, err := loadIncludeProject(full)
		if err != nil {
			return nil, err
		}
		if sub == nil {
			return nil, fmt.Errorf("%s resource %q is a directory but has no %s to declare its members — a directory member composes via its own %s listing spec.resources (the Helm/Kustomize include pattern)", ProjectFileName, r, ProjectFileName, ProjectFileName)
		}
		if len(sub.Resources) == 0 {
			return nil, fmt.Errorf("%s resource %q: its %s declares no spec.resources (an included directory must list its own members)", ProjectFileName, r, ProjectFileName)
		}
		subFiles, err := collectResources(full, sub.Resources)
		if err != nil {
			return nil, err
		}
		files = append(files, subFiles...)
	}
	return files, nil
}

// manifestFilesIn returns the manifest files directly in dir (non-recursive),
// skipping the reserved datascape.yaml project file. The legacy flat layout.
func manifestFilesIn(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read dir %s: %w", dir, err)
	}
	var files []string
	for _, entry := range entries {
		if entry.IsDir() || entry.Name() == ProjectFileName {
			continue
		}
		switch filepath.Ext(entry.Name()) {
		case ".yaml", ".yml", ".json":
			files = append(files, filepath.Join(dir, entry.Name()))
		}
	}
	return files, nil
}
