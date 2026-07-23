package localfile

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/state"
	"github.com/rezarajan/platformctl/internal/ports/state/conformance"
)

func TestConformance(t *testing.T) {
	t.Parallel()
	conformance.Run(t, func(t *testing.T) state.StateStore {
		return New(filepath.Join(t.TempDir(), "state.json"))
	})
}

func TestLoadMigratesV1KeysToDefaultNamespace(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(path, []byte(`{
  "version": 1,
  "resources": {
    "Provider/legacy": {
      "specHash": "abc123",
      "lifecycle": "Managed"
    }
  }
}`), 0o644); err != nil {
		t.Fatal(err)
	}

	store := New(path)
	st, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	key := resource.Key{Namespace: resource.DefaultNamespace, Kind: "Provider", Name: "legacy"}
	rs, ok := st.Resources[key]
	if !ok {
		t.Fatalf("migrated resource %s missing", key)
	}
	if rs.LastApplied != nil {
		t.Fatalf("v1 migration should not fabricate lastApplied: %+v", rs.LastApplied)
	}
	if st.Version != state.CurrentVersion {
		t.Fatalf("version = %d, want %d", st.Version, state.CurrentVersion)
	}

	if err := store.Save(context.Background(), st); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"default/Provider/legacy"`) {
		t.Fatalf("saved state did not use v2 namespace-aware key:\n%s", data)
	}
}
