package docsgen

import (
	"os"
	"path/filepath"
	"testing"
)

// referenceDir is the committed output of `platformctl docs build`,
// relative to this package.
const referenceDir = "../../../docs/reference"

// TestGeneratedReferenceInSync guards docs/remediation/F-002: the
// committed docs/reference/*.md must always match what Build() produces
// from the current schemas, or the reference silently drifts from the
// schemas it claims to document (as happened when deletionPolicy shipped
// without a regeneration pass). Failing this test names the fix: run
// `go run ./cmd/platformctl docs build --out docs/reference` and commit.
func TestGeneratedReferenceInSync(t *testing.T) {
	t.Parallel()
	pages, err := Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	committed, err := os.ReadDir(referenceDir)
	if err != nil {
		t.Fatalf("read %s: %v", referenceDir, err)
	}
	committedNames := make(map[string]bool, len(committed))
	for _, entry := range committed {
		if !entry.IsDir() {
			committedNames[entry.Name()] = true
		}
	}

	for name, generated := range pages {
		want := generated
		got, err := os.ReadFile(filepath.Join(referenceDir, name))
		if err != nil {
			t.Errorf("docs/reference/%s is missing (generator produces it): run `go run ./cmd/platformctl docs build --out docs/reference` and commit", name)
			continue
		}
		if string(got) != want {
			t.Errorf("docs/reference/%s is out of sync with the current schemas: run `go run ./cmd/platformctl docs build --out docs/reference` and commit", name)
		}
		delete(committedNames, name)
	}

	for stale := range committedNames {
		t.Errorf("docs/reference/%s is committed but no longer generated: run `go run ./cmd/platformctl docs build --out docs/reference` and commit (this will remove it)", stale)
	}
}
