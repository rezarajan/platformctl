package state

import (
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

// TestMigrationChainHasNoGaps is the template a new migration must satisfy:
// migrations must be contiguous starting at version 1 with no gaps, and
// must cover every version up to CurrentVersion-1 — a new format change
// (CurrentVersion++) means appending exactly one entry whose FromVersion is
// the version being retired, never rewriting Normalize's decode loop.
func TestMigrationChainHasNoGaps(t *testing.T) {
	t.Parallel()
	want := 1
	for i, m := range migrations {
		if m.FromVersion != want {
			t.Fatalf("migrations[%d].FromVersion = %d, want %d (chain must be contiguous starting at 1)", i, m.FromVersion, want)
		}
		if m.Name == "" {
			t.Fatalf("migrations[%d] has no Name", i)
		}
		want++
	}
	if want != CurrentVersion {
		t.Fatalf("migrations chain covers up to version %d, but CurrentVersion is %d — add a migration or lower CurrentVersion", want, CurrentVersion)
	}
}

// TestNormalizeAppliesMigrationsInOrder guards the chain mechanism itself,
// independent of any single migration's content: a v1 state runs through
// every migrator up to CurrentVersion in one Normalize call.
func TestNormalizeAppliesMigrationsInOrder(t *testing.T) {
	t.Parallel()
	s := State{Version: 1, RawResources: map[string]ResourceState{
		"Provider/legacy": {SpecHash: "abc123", Lifecycle: "Managed"},
	}}
	s.Normalize()
	if s.Version != CurrentVersion {
		t.Fatalf("version = %d, want %d", s.Version, CurrentVersion)
	}
	key := resource.Key{Namespace: resource.DefaultNamespace, Kind: "Provider", Name: "legacy"}
	rs, ok := s.Resources[key]
	if !ok {
		t.Fatalf("migrated resource %s missing from %+v", key, s.Resources)
	}
	if rs.SpecHash != "abc123" {
		t.Errorf("migrated resource lost data: SpecHash = %q, want %q", rs.SpecHash, "abc123")
	}
}

// TestNormalizeNoopAtCurrentVersion guards against a migration accidentally
// firing for state that's already current — Normalize must not mutate
// RawResources when nothing needs upgrading.
func TestNormalizeNoopAtCurrentVersion(t *testing.T) {
	t.Parallel()
	s := State{Version: CurrentVersion, RawResources: map[string]ResourceState{
		"default/Provider/current": {SpecHash: "xyz"},
	}}
	s.Normalize()
	key := resource.Key{Namespace: resource.DefaultNamespace, Kind: "Provider", Name: "current"}
	if _, ok := s.Resources[key]; !ok {
		t.Fatalf("resource %s missing after no-op normalize: %+v", key, s.Resources)
	}
}
