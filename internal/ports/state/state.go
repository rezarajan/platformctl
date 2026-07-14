// Package state defines the StateStore port.
// See docs/planning/02-architecture.md §4.3 and §7.
package state

import (
	"context"

	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
)

// CurrentVersion is the state format version written by this build. A schema
// migration path is required before any breaking change to the state format
// ships — write a migrator, don't just bump the number.
const CurrentVersion = 1

type ResourceState struct {
	SpecHash  string         `json:"specHash"` // last-applied spec hash
	Status    status.Status  `json:"status"`
	Lifecycle string         `json:"lifecycle"`
	Imported  bool           `json:"imported,omitempty"`
	Provider  map[string]any `json:"providerState,omitempty"`
}

type State struct {
	Version   int                            `json:"version"`
	Resources map[resource.Key]ResourceState `json:"-"`
	// RawResources is the serialized form; resource.Key is not a valid JSON
	// map key, so persistence flattens to "Kind/Name" strings.
	RawResources map[string]ResourceState `json:"resources"`
}

// Normalize syncs the typed map from the raw serialized form after Load.
func (s *State) Normalize() {
	s.Resources = make(map[resource.Key]ResourceState, len(s.RawResources))
	for k, v := range s.RawResources {
		s.Resources[parseKey(k)] = v
	}
}

// Flatten syncs the raw serialized form from the typed map before Save.
func (s *State) Flatten() {
	s.RawResources = make(map[string]ResourceState, len(s.Resources))
	for k, v := range s.Resources {
		s.RawResources[k.String()] = v
	}
}

func parseKey(s string) resource.Key {
	for i := 0; i < len(s); i++ {
		if s[i] == '/' {
			return resource.Key{Kind: s[:i], Name: s[i+1:]}
		}
	}
	return resource.Key{Name: s}
}

type StateStore interface {
	Load(ctx context.Context) (State, error)
	Save(ctx context.Context, s State) error
	Lock(ctx context.Context) (unlock func() error, err error)
}
