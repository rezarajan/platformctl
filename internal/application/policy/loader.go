// Package policy is the deterministic policy evaluator (docs/adr/021-policy-
// engine-zero-trust.md): a pure function over (policies, envelopes, graph,
// plan, findings) -> decisions, plus the loader that reads policy documents
// from their own channel (never the governed manifest set) and the
// zero-trust starter pack `platformctl policy init zero-trust` writes.
package policy

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/rezarajan/platformctl/internal/domain/policy"
)

// LoadDir reads every *.yaml/*.yml/*.json file directly under dir (no
// recursion — mirrors internal/application/manifest.collectFiles exactly,
// including multi-document YAML support), schema-validates each document,
// decodes it, and returns the full policy set after
// internal/domain/policy.Validate's cross-cutting checks (unique rule ids,
// closed selector shape, effect enum, matches-regex compile).
//
// dir may be a single file or a directory; a directory that doesn't exist
// is treated as "no policies" (nil, nil) rather than an error, since the
// conventional .datascape/policies/ directory is optional by design — a
// repository with no governance need never create it.
func LoadDir(dir string) ([]policy.Policy, error) {
	info, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("stat %s: %w", dir, err)
	}

	var files []string
	if !info.IsDir() {
		files = []string{dir}
	} else {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return nil, fmt.Errorf("read dir %s: %w", dir, err)
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			switch filepath.Ext(entry.Name()) {
			case ".yaml", ".yml", ".json":
				files = append(files, filepath.Join(dir, entry.Name()))
			}
		}
		sort.Strings(files)
	}
	if len(files) == 0 {
		return nil, nil
	}

	var policies []policy.Policy
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
			p, err := policy.Decode(raw)
			if err != nil {
				return nil, fmt.Errorf("%s (document %d): %w", f, docIndex+1, err)
			}
			policies = append(policies, p)
		}
	}

	if err := policy.Validate(policies); err != nil {
		return nil, err
	}
	return policies, nil
}
