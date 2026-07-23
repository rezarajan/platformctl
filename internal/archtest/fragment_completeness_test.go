package archtest

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/rezarajan/platformctl/internal/application/manifest"
)

// fragmentCorpusRoots are every place docs/planning/08 I10 names as a
// shipped manifest source: cmd/platformctl's integration/output-contract
// testdata, the two worked examples, and every `platformctl init` blueprint
// template. A key a fragment (schemas/v1alpha1/fragments/**,
// additionalProperties: false) rejects, used anywhere here, would fail live
// at apply/reconcile without validate ever catching it first — exactly how
// the ingress fragment's httpsPort field escaped E5's original review (doc
// 11: found only by a systematic sweep run once by hand at the day's
// closing gate). This test is that sweep, made permanent.
var fragmentCorpusRoots = []string{
	"../../cmd/platformctl/testdata",
	"../../examples",
	"../../internal/application/blueprint/templates",
}

// negativeCorpusDirName is excluded from the sweep: every manifest under it
// is deliberately schema/fragment-invalid — rejecting them IS the point
// (proven by its own consumer, not this completeness guard).
const negativeCorpusDirName = "negative-corpus"

// fragmentDoc is one decoded YAML/JSON document plus the file it came from
// and the directory that groups it into a manifest set (the same grouping
// a real `platformctl validate <dir>` call would load together — a
// Binding's providerRef only resolves against Providers in the same
// directory, mirroring internal/application/manifest.Validate's
// providerTypeByKey, built per Load() call over one directory/file).
type fragmentDoc struct {
	file string
	dir  string
	raw  map[string]any
}

// collectFragmentCorpusDocs walks every root in fragmentCorpusRoots,
// skipping negativeCorpusDirName entirely, and decodes every *.yaml/*.yml/
// *.json file's documents (multi-document YAML supported, same as
// manifest.Load).
func collectFragmentCorpusDocs(t *testing.T) []fragmentDoc {
	t.Helper()
	var files []string
	for _, root := range fragmentCorpusRoots {
		absRoot, err := filepath.Abs(root)
		if err != nil {
			t.Fatalf("resolve %s: %v", root, err)
		}
		if _, err := os.Stat(absRoot); err != nil {
			t.Fatalf("fragment-completeness corpus root %s does not exist: %v", absRoot, err)
		}
		err = filepath.WalkDir(absRoot, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if d.Name() == negativeCorpusDirName {
					return filepath.SkipDir
				}
				return nil
			}
			switch strings.ToLower(filepath.Ext(path)) {
			case ".yaml", ".yml", ".json":
				files = append(files, path)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", absRoot, err)
		}
	}
	sort.Strings(files)
	if len(files) == 0 {
		t.Fatal("no manifest files found under any fragment-completeness corpus root — did the paths move?")
	}

	var docs []fragmentDoc
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		dir := filepath.Dir(f)
		dec := yaml.NewDecoder(strings.NewReader(string(data)))
		for docIndex := 0; ; docIndex++ {
			var raw map[string]any
			if err := dec.Decode(&raw); err != nil {
				if err.Error() == "EOF" {
					break
				}
				t.Fatalf("%s (document %d): decode: %v", f, docIndex+1, err)
			}
			if len(raw) == 0 {
				continue
			}
			docs = append(docs, fragmentDoc{file: f, dir: dir, raw: raw})
		}
	}
	return docs
}

func kindOf(raw map[string]any) string {
	k, _ := raw["kind"].(string)
	return k
}

func specOf(raw map[string]any) map[string]any {
	spec, _ := raw["spec"].(map[string]any)
	return spec
}

func refName(spec map[string]any, field string) string {
	ref, ok := spec[field].(map[string]any)
	if !ok {
		return ""
	}
	name, _ := ref["name"].(string)
	return name
}

// TestFragmentCompletenessSweep is docs/planning/08 I10's guard: every
// Provider spec.configuration, Source/Catalog spec.<engine> block, and
// Binding spec.options block used anywhere in the shipped-manifest corpus
// (testdata minus negative-corpus, examples, blueprint templates) must
// satisfy the fragment registered for its discriminator — the same
// discriminators and the same compiled schemas
// internal/application/manifest's fragment composition (fragment.go) uses
// at real validate time, reached here through the exported
// manifest.FragmentCheck so this test can never silently drift from
// production behavior.
func TestFragmentCompletenessSweep(t *testing.T) {
	t.Parallel()
	docs := collectFragmentCorpusDocs(t)

	// providerTypeByKey resolves a Binding's providerRef to its Provider's
	// spec.type, scoped per directory (manifest set) exactly like
	// manifest.Validate's own providerTypeByKey is scoped per Load() call —
	// a Binding may appear before its Provider in file order within one
	// directory, but two different scenario directories reusing the same
	// Provider name must not resolve into each other.
	providerTypeByDir := map[string]map[string]string{} // dir -> name -> type
	for _, d := range docs {
		if kindOf(d.raw) != "Provider" {
			continue
		}
		spec := specOf(d.raw)
		providerType, _ := spec["type"].(string)
		meta, _ := d.raw["metadata"].(map[string]any)
		name, _ := meta["name"].(string)
		if providerType == "" || name == "" {
			continue
		}
		if providerTypeByDir[d.dir] == nil {
			providerTypeByDir[d.dir] = map[string]string{}
		}
		providerTypeByDir[d.dir][name] = providerType
	}

	var failures []string
	for _, d := range docs {
		spec := specOf(d.raw)
		if spec == nil {
			continue
		}
		meta, _ := d.raw["metadata"].(map[string]any)
		name, _ := meta["name"].(string)

		var fragKind, discriminator string
		var block map[string]any

		switch kindOf(d.raw) {
		case "Provider":
			providerType, _ := spec["type"].(string)
			if providerType == "" {
				continue
			}
			fragKind, discriminator = "provider", providerType
			block, _ = spec["configuration"].(map[string]any)
		case "Source", "Catalog":
			engine, _ := spec["engine"].(string)
			if engine == "" {
				continue
			}
			fragKind, discriminator = strings.ToLower(kindOf(d.raw)), engine
			block, _ = spec[engine].(map[string]any)
		case "Binding":
			mode, _ := spec["mode"].(string)
			providerName := refName(spec, "providerRef")
			if mode == "" || providerName == "" {
				continue
			}
			providerType, ok := providerTypeByDir[d.dir][providerName]
			if !ok {
				// Unresolvable within this directory — same posture as
				// fragment.go's validateBindingOptionsFragment: leave it to
				// application/compatibility's own clearer error, not this
				// sweep's concern.
				continue
			}
			fragKind, discriminator = "binding", mode+"-"+providerType
			block, _ = spec["options"].(map[string]any)
		default:
			continue
		}

		if err := manifest.FragmentCheck(fragKind, discriminator, block); err != nil {
			rel, relErr := filepath.Rel(mustAbs(t, "../.."), d.file)
			if relErr != nil {
				rel = d.file
			}
			failures = append(failures, "  "+rel+": "+kindOf(d.raw)+" "+name+" ("+fragKind+"/"+discriminator+"): "+err.Error())
		}
	}

	if len(failures) > 0 {
		sort.Strings(failures)
		t.Fatalf("manifest(s) in the fragment-completeness corpus (cmd/platformctl/testdata excl. negative-corpus, examples, internal/application/blueprint/templates) use a key their own fragment rejects:\n%s", strings.Join(failures, "\n"))
	}
}

func mustAbs(t *testing.T, path string) string {
	t.Helper()
	abs, err := filepath.Abs(path)
	if err != nil {
		t.Fatalf("resolve %s: %v", path, err)
	}
	return abs
}
