package postgres

import (
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/provider"
)

// TestStorageResolution covers docs/planning/08 B3: configuration.storage
// is optional and, when present, its size string parses through
// storagesize.ParseBytes into VolumeSpec's runtime-agnostic bytes/class.
func TestStorageResolution(t *testing.T) {
	t.Parallel()
	cfg := provider.Provider{Configuration: map[string]any{}}
	const name = "db"

	size, class, err := storage(cfg, name)
	if err != nil || size != 0 || class != "" {
		t.Fatalf("storage() with no stanza = %d, %q, %v; want 0, \"\", nil", size, class, err)
	}

	cfg.Configuration["storage"] = map[string]any{"size": "50Gi", "class": "fast-ssd"}
	size, class, err = storage(cfg, name)
	if err != nil {
		t.Fatalf("storage(): %v", err)
	}
	if want := int64(50) * 1 << 30; size != want {
		t.Errorf("size = %d, want %d", size, want)
	}
	if class != "fast-ssd" {
		t.Errorf("class = %q, want fast-ssd", class)
	}

	cfg.Configuration["storage"] = map[string]any{"size": "not-a-size"}
	if _, _, err := storage(cfg, name); err == nil {
		t.Fatal("storage() accepted an unparseable size")
	}
}
