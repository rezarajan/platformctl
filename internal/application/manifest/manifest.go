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
	"github.com/rezarajan/platformctl/internal/domain/dataset"
	"github.com/rezarajan/platformctl/internal/domain/eventstream"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/secret"
	"github.com/rezarajan/platformctl/internal/domain/source"
)

// KnownKinds is the closed set of v1 kinds.
var KnownKinds = map[string]bool{
	"Provider":        true,
	"Source":          true,
	"EventStream":     true,
	"Binding":         true,
	"Dataset":         true,
	"SecretReference": true,
}

// Load reads one path (file or directory) and returns validated envelopes.
// Multi-document YAML files are supported.
func Load(path string) ([]resource.Envelope, error) {
	files, err := collectFiles(path)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
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

	if err := Validate(envelopes); err != nil {
		return nil, err
	}
	return envelopes, nil
}

// Validate runs envelope-level and kind-specific validation over the set.
func Validate(envelopes []resource.Envelope) error {
	seen := make(map[resource.Key]bool)
	for _, e := range envelopes {
		if err := e.Validate(); err != nil {
			return err
		}
		if !KnownKinds[e.Kind] {
			return fmt.Errorf("%s %q: unknown kind (known: Provider, Source, EventStream, Binding, Dataset, SecretReference)", e.Kind, e.Metadata.Name)
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
			_, err = provider.FromEnvelope(e)
		case "Source":
			_, err = source.FromEnvelope(e)
		case "EventStream":
			_, err = eventstream.FromEnvelope(e)
		case "Binding":
			_, err = binding.FromEnvelope(e)
		case "Dataset":
			_, err = dataset.FromEnvelope(e)
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
	e.Metadata.Labels = stringMap(meta["labels"])
	e.Metadata.Annotations = stringMap(meta["annotations"])
	if observers, ok := meta["observers"].([]any); ok {
		for _, o := range observers {
			if om, ok := o.(map[string]any); ok {
				name, _ := om["name"].(string)
				e.Metadata.Observers = append(e.Metadata.Observers, resource.ObserverRef{Name: name})
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

func collectFiles(path string) ([]string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat %s: %w", path, err)
	}
	if !info.IsDir() {
		return []string{path}, nil
	}
	var files []string
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("read dir %s: %w", path, err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		switch filepath.Ext(entry.Name()) {
		case ".yaml", ".yml", ".json":
			files = append(files, filepath.Join(path, entry.Name()))
		}
	}
	sort.Strings(files)
	return files, nil
}
