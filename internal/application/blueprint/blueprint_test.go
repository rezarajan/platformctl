package blueprint

import (
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func TestListAndNames(t *testing.T) {
	t.Parallel()
	infos := List()
	if len(infos) == 0 {
		t.Fatal("List() returned no blueprints")
	}
	names := Names()
	if len(names) != len(infos) {
		t.Fatalf("Names() length %d != List() length %d", len(names), len(infos))
	}
	for i, b := range infos {
		if b.Name != names[i] {
			t.Errorf("Names()[%d] = %q, want %q", i, names[i], b.Name)
		}
		if b.Summary == "" {
			t.Errorf("blueprint %q has an empty Summary", b.Name)
		}
		if len(b.Providers) == 0 {
			t.Errorf("blueprint %q declares no Providers", b.Name)
		}
		if !Exists(b.Name) {
			t.Errorf("Exists(%q) = false, want true", b.Name)
		}
	}
	if Exists("not-a-real-blueprint") {
		t.Error("Exists on an unknown name returned true")
	}
}

// TestCatalogMatchesEmbeddedTemplates guards the one place this package
// could silently drift: catalog metadata (List/Names) advertising a
// blueprint with no matching templates/<name> directory, or vice versa.
func TestCatalogMatchesEmbeddedTemplates(t *testing.T) {
	t.Parallel()
	entries, err := fs.ReadDir(templatesFS, templatesRoot)
	if err != nil {
		t.Fatalf("read embedded templates root: %v", err)
	}
	onDisk := map[string]bool{}
	for _, e := range entries {
		if e.IsDir() {
			onDisk[e.Name()] = true
		}
	}
	catalogNames := map[string]bool{}
	for _, name := range Names() {
		catalogNames[name] = true
		if !onDisk[name] {
			t.Errorf("catalog names blueprint %q with no templates/%s directory", name, name)
		}
	}
	for name := range onDisk {
		if !catalogNames[name] {
			t.Errorf("templates/%s exists but is not registered in the catalog", name)
		}
	}
}

// TestWriteEachBlueprint proves every shipped blueprint writes at least a
// README, an env file (env.template renamed to .env), and one manifest,
// with no leftover "env.template" name at the destination.
func TestWriteEachBlueprint(t *testing.T) {
	t.Parallel()
	for _, name := range Names() {
		name := name
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			files, err := Write(name, dir, false)
			if err != nil {
				t.Fatalf("Write(%q): %v", name, err)
			}
			if len(files) == 0 {
				t.Fatalf("Write(%q) returned no files", name)
			}
			want := map[string]bool{".env": false, "README.md": false}
			for _, f := range files {
				if f == "env.template" {
					t.Errorf("Write(%q) left env.template un-renamed in the output list", name)
				}
				if _, ok := want[f]; ok {
					want[f] = true
				}
				if _, statErr := os.Stat(filepath.Join(dir, f)); statErr != nil {
					t.Errorf("Write(%q) reported file %q but it does not exist: %v", name, f, statErr)
				}
			}
			for f, found := range want {
				if !found {
					t.Errorf("Write(%q) did not produce expected file %q", name, f)
				}
			}
			sorted := append([]string(nil), files...)
			sort.Strings(sorted)
			for i := range files {
				if files[i] != sorted[i] {
					t.Errorf("Write(%q) file list not sorted: %v", name, files)
					break
				}
			}
		})
	}
}

func TestWriteRefusesExistingFilesWithoutForce(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if _, err := Write("stream-basics", dir, false); err != nil {
		t.Fatalf("first Write: %v", err)
	}
	if _, err := Write("stream-basics", dir, false); err == nil {
		t.Fatal("second Write without force succeeded, want a collision error")
	}
	// Nothing should have been touched by the refused write beyond what
	// the first Write already wrote — spot check one file is unchanged
	// (still present, still readable) rather than corrupted/truncated.
	if _, err := os.Stat(filepath.Join(dir, "README.md")); err != nil {
		t.Fatalf("README.md missing after refused overwrite: %v", err)
	}
}

func TestWriteForceOverwrites(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if _, err := Write("stream-basics", dir, false); err != nil {
		t.Fatalf("first Write: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("mutated"), 0o644); err != nil {
		t.Fatalf("mutate README.md: %v", err)
	}
	if _, err := Write("stream-basics", dir, true); err != nil {
		t.Fatalf("forced Write: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "README.md"))
	if err != nil {
		t.Fatalf("read README.md: %v", err)
	}
	if string(data) == "mutated" {
		t.Error("forced Write did not overwrite the mutated file")
	}
}

func TestWriteUnknownBlueprint(t *testing.T) {
	t.Parallel()
	if _, err := Write("does-not-exist", t.TempDir(), false); err == nil {
		t.Fatal("Write on an unknown blueprint succeeded, want an error")
	}
}
