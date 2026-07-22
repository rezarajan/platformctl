package policy

import (
	"embed"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/rezarajan/platformctl/internal/domain/policy"
)

// packsFS embeds the built-in policy packs `platformctl policy init` writes
// — the blueprint pattern (internal/application/blueprint) applied to
// governance (ADR 021 §4: "the blueprint pattern applied to governance").
//
//go:embed templates
var packsFS embed.FS

const packsRoot = "templates"

// packNames lists the shipped policy packs; zero-trust is the only one ADR
// 021 §4 defines today.
var packNames = []string{"zero-trust"}

// PackNames returns the shipped pack names, for usage/error text.
func PackNames() []string { return append([]string(nil), packNames...) }

// PackExists reports whether name is a known pack.
func PackExists(name string) bool {
	for _, n := range packNames {
		if n == name {
			return true
		}
	}
	return false
}

// WritePack renders pack name's embedded templates into dir (created if
// needed), returning the destination paths written, relative to dir and
// sorted — mirrors internal/application/blueprint.Write's contract exactly:
// refuses to overwrite any pre-existing file unless force is true, leaving
// no partial write behind.
func WritePack(name, dir string, force bool) ([]string, error) {
	if !PackExists(name) {
		return nil, fmt.Errorf("unknown policy pack %q (known: %s)", name, strings.Join(PackNames(), ", "))
	}
	root := packsRoot + "/" + name

	var rel []string
	err := fs.WalkDir(packsFS, root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		r, err := filepath.Rel(root, filepath.FromSlash(path))
		if err != nil {
			return err
		}
		rel = append(rel, filepath.ToSlash(r))
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("policy pack %q: %w", name, err)
	}
	sort.Strings(rel)

	if !force {
		for _, r := range rel {
			dest := filepath.Join(dir, r)
			if _, statErr := os.Stat(dest); statErr == nil {
				return nil, fmt.Errorf("%s already exists (use --force to overwrite)", dest)
			}
		}
	}

	written := make([]string, 0, len(rel))
	for _, r := range rel {
		data, readErr := packsFS.ReadFile(root + "/" + r)
		if readErr != nil {
			return nil, readErr
		}
		dest := filepath.Join(dir, r)
		if mkErr := os.MkdirAll(filepath.Dir(dest), 0o755); mkErr != nil {
			return nil, mkErr
		}
		if writeErr := os.WriteFile(dest, data, 0o644); writeErr != nil {
			return nil, writeErr
		}
		destRel, relErr := filepath.Rel(dir, dest)
		if relErr != nil {
			destRel = dest
		}
		written = append(written, filepath.ToSlash(destRel))
	}
	sort.Strings(written)
	return written, nil
}

// BuiltinRuleIDs returns every rule id in the zero-trust pack, sorted — the
// explain-catalog completeness guard's source of truth (mirrors
// internal/application/lint.BuiltinCodes' role for lint codes), computed
// once from the embedded template itself rather than hand-duplicated
// alongside it, so the two can never silently drift.
func BuiltinRuleIDs() ([]string, error) {
	data, err := packsFS.ReadFile(packsRoot + "/zero-trust/policy.yaml")
	if err != nil {
		return nil, err
	}
	var raw map[string]any
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	p, err := policy.Decode(raw)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(p.Rules()))
	for _, r := range p.Rules() {
		ids = append(ids, r.ID)
	}
	sort.Strings(ids)
	return ids, nil
}
