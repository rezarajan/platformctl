// Package conformance is the shared contract test suite every StateStore
// adapter must pass. See docs/planning/02-architecture.md §9.
package conformance

import (
	"context"
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/state"
)

// Factory returns a fresh, empty store per invocation.
type Factory func(t *testing.T) state.StateStore

func Run(t *testing.T, factory Factory) {
	t.Helper()
	ctx := context.Background()

	t.Run("Load_empty", func(t *testing.T) {
		s := factory(t)
		st, err := s.Load(ctx)
		if err != nil {
			t.Fatalf("Load on empty store: %v", err)
		}
		if len(st.Resources) != 0 {
			t.Fatalf("empty store returned %d resources", len(st.Resources))
		}
	})

	t.Run("Save_then_Load_roundtrip", func(t *testing.T) {
		s := factory(t)
		st, _ := s.Load(ctx)
		key := resource.Key{Kind: "Provider", Name: "test-noop"}
		st.Resources[key] = state.ResourceState{SpecHash: "abc123", Lifecycle: "Managed"}
		if err := s.Save(ctx, st); err != nil {
			t.Fatalf("Save: %v", err)
		}
		loaded, err := s.Load(ctx)
		if err != nil {
			t.Fatalf("Load after Save: %v", err)
		}
		got, ok := loaded.Resources[key]
		if !ok {
			t.Fatalf("saved resource %s missing after reload", key)
		}
		if got.SpecHash != "abc123" {
			t.Errorf("SpecHash = %q, want %q", got.SpecHash, "abc123")
		}
		if loaded.Version != state.CurrentVersion {
			t.Errorf("Version = %d, want %d", loaded.Version, state.CurrentVersion)
		}
	})

	t.Run("Lock_excludes_second_locker", func(t *testing.T) {
		s := factory(t)
		unlock, err := s.Lock(ctx)
		if err != nil {
			t.Fatalf("first Lock: %v", err)
		}
		if _, err := s.Lock(ctx); err == nil {
			t.Fatalf("second Lock succeeded while first is held")
		}
		if err := unlock(); err != nil {
			t.Fatalf("unlock: %v", err)
		}
		unlock2, err := s.Lock(ctx)
		if err != nil {
			t.Fatalf("Lock after unlock: %v", err)
		}
		_ = unlock2()
	})
}
