package main

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rezarajan/platformctl/internal/adapters/state/localfile"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/state"
)

// TestInventorySurfacesEndpoints: inventory reads recorded endpoints and
// pairs each with the SecretReference holding its credentials.
func TestInventorySurfacesEndpoints(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "state.json")
	store := localfile.New(stateFile)

	st := state.State{Version: state.CurrentVersion, Resources: map[resource.Key]state.ResourceState{}}
	st.Resources[resource.Key{Namespace: resource.DefaultNamespace, Kind: "Provider", Name: "datascape-rp-test"}] = state.ResourceState{
		Lifecycle: "Managed",
		Provider: map[string]any{
			endpoint.Key: endpoint.List{
				{Name: "kafka", Scheme: "kafka", Host: "127.0.0.1:19192", Internal: "datascape-rp-test:29092"},
			}.ToState(),
		},
	}
	if err := store.Save(context.Background(), st); err != nil {
		t.Fatal(err)
	}

	out, err, code := run(t, "inventory", "testdata/redpanda-scenario", "--state-file", stateFile)
	if err != nil || code != 0 {
		t.Fatalf("inventory failed (code %d): %v\n%s", code, err, out)
	}
	for _, want := range []string{"kafka", "127.0.0.1:19192", "datascape-rp-test:29092", "default/Provider/datascape-rp-test"} {
		if !strings.Contains(out, want) {
			t.Errorf("inventory output missing %q:\n%s", want, out)
		}
	}

	// json output is machine-readable.
	out, _, _ = run(t, "inventory", "testdata/redpanda-scenario", "--state-file", stateFile, "-o", "json")
	if !strings.Contains(out, `"endpoint": "kafka"`) {
		t.Errorf("json inventory missing endpoint field:\n%s", out)
	}
}

// TestInventoryEmptyState: with nothing applied, inventory says so rather
// than printing an empty table.
func TestInventoryEmptyState(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "state.json")
	out, err, code := run(t, "inventory", "testdata/redpanda-scenario", "--state-file", stateFile)
	if err != nil || code != 0 {
		t.Fatalf("inventory on empty state failed: %v\n%s", err, out)
	}
	if !strings.Contains(out, "no service endpoints") {
		t.Errorf("expected empty-state message, got:\n%s", out)
	}
}
