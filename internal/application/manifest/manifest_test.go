package manifest

import (
	"os"
	"path/filepath"
	"testing"
)

// TestLoadDecodesMetadataProtect pins the doc 11 loader-gap fix: the
// field-by-field metadata decoder silently dropped protect from its
// introduction — engine tests stayed green by constructing Envelopes
// directly, so the NFR-3 refusal never fired for a real manifest. This
// test goes through the REAL loader, the path that was broken.
func TestLoadDecodesMetadataProtect(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "m.yaml"), []byte(`apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: keeper
  protect: true
spec:
  type: noop
  runtime: {type: fake}
`), 0o600); err != nil {
		t.Fatal(err)
	}
	envs, err := Load(dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(envs) != 1 || !envs[0].Metadata.Protect {
		t.Fatalf("metadata.protect not decoded through the real loader: %+v", envs)
	}
}
