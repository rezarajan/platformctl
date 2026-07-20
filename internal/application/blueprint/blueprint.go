// Package blueprint embeds the manifest-set templates `platformctl init`
// scaffolds from (docs/planning/08 §E1). Each blueprint is a directory
// under templates/<name>/ containing ready-to-apply manifests, an
// env.template naming every secret key the blueprint's SecretReferences
// need (written out as .env), and a README. Templates rely on the
// providers' default images and auto-assigned host ports (already
// supported — see internal/domain/hostport and each provider's
// defaultImage) so no manifest edits are required for `platformctl
// validate` to pass immediately after `init`.
//
// This package holds no adapter imports (embedded static assets only), so
// it is safe for internal/application per the layering invariant in
// CLAUDE.md.
package blueprint

import (
	"embed"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

//go:embed templates
var templatesFS embed.FS

const templatesRoot = "templates"

// envTemplateName is the source filename standing in for the destination
// dotfile: go:embed's default "templates" pattern excludes files/dirs
// whose name begins with "." (only an "all:" prefix would include them),
// so the template tree cannot itself contain ".env" — Write renames this
// file to ".env" on the way out.
const envTemplateName = "env.template"

// Info is the machine-readable descriptor of a shipped blueprint, returned
// by List and rendered by `platformctl init --list`.
type Info struct {
	Name      string   `json:"name" yaml:"name"`
	Summary   string   `json:"summary" yaml:"summary"`
	Providers []string `json:"providers" yaml:"providers"`
}

// catalog is the fixed, hand-maintained metadata for each shipped
// blueprint; the manifest content itself lives under templates/<name>/
// and is embedded verbatim by Write. Keep this list and the templates/
// directory in sync — TestCatalogMatchesEmbeddedTemplates enforces it.
var catalog = []Info{
	{
		Name:      "cdc-to-lake",
		Summary:   "Postgres change data capture through Debezium and Redpanda, landing as objects in MinIO.",
		Providers: []string{"postgres", "debezium", "redpanda", "s3sink", "minio"},
	},
	{
		Name:      "lakehouse",
		Summary:   "cdc-to-lake plus a Nessie catalog, an OpenLineage (Marquez) lineage backend, and a Connection-fronted external CDC source.",
		Providers: []string{"postgres", "debezium", "redpanda", "s3sink", "minio", "nessie", "openlineage", "proxy"},
	},
	{
		Name:      "stream-basics",
		Summary:   "A Redpanda broker with a couple of EventStream topics; no databases or sinks.",
		Providers: []string{"redpanda"},
	},
	{
		Name:      "external-cdc",
		Summary:   "An external database reached through a managed Connection, captured by a managed Debezium worker into Redpanda.",
		Providers: []string{"redpanda", "proxy", "debezium"},
	},
}

// List returns the shipped blueprints, in a stable, declared order.
func List() []Info {
	out := make([]Info, len(catalog))
	copy(out, catalog)
	return out
}

// Names returns the shipped blueprint names, for usage/error text.
func Names() []string {
	names := make([]string, len(catalog))
	for i, b := range catalog {
		names[i] = b.Name
	}
	return names
}

// Exists reports whether name is a known blueprint.
func Exists(name string) bool {
	for _, b := range catalog {
		if b.Name == name {
			return true
		}
	}
	return false
}

// Write renders blueprint name's embedded templates into dir (created if
// needed), returning the destination paths written, relative to dir and
// sorted. It refuses to overwrite any pre-existing file unless force is
// true, and refuses (leaving no partial write behind) if any destination
// file already exists and force is false.
func Write(name, dir string, force bool) ([]string, error) {
	if !Exists(name) {
		return nil, fmt.Errorf("unknown blueprint %q (known: %s)", name, strings.Join(Names(), ", "))
	}
	root := templatesRoot + "/" + name

	var rel []string
	err := fs.WalkDir(templatesFS, root, func(path string, d fs.DirEntry, err error) error {
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
		return nil, fmt.Errorf("blueprint %q: %w", name, err)
	}
	sort.Strings(rel)

	if !force {
		for _, r := range rel {
			dest := destPath(dir, r)
			if _, statErr := os.Stat(dest); statErr == nil {
				return nil, fmt.Errorf("%s already exists (use --force to overwrite)", dest)
			}
		}
	}

	written := make([]string, 0, len(rel))
	for _, r := range rel {
		data, readErr := templatesFS.ReadFile(root + "/" + r)
		if readErr != nil {
			return nil, readErr
		}
		dest := destPath(dir, r)
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

// destPath maps a template-relative path onto its destination path under
// dir, renaming envTemplateName to ".env".
func destPath(dir, rel string) string {
	if filepath.Base(rel) == envTemplateName {
		rel = filepath.Join(filepath.Dir(rel), ".env")
	}
	return filepath.Join(dir, rel)
}
