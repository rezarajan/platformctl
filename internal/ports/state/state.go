// Package state defines the StateStore port.
// See docs/planning/02-architecture.md §4.3 and §7.
package state

import (
	"context"
	"net/url"
	"strings"

	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
)

// CurrentVersion is the state format version written by this build. A schema
// migration path is required before any breaking change to the state format
// ships — write a migrator, don't just bump the number.
const CurrentVersion = 2

type ResourceState struct {
	SpecHash     string             `json:"specHash"` // last-applied spec hash
	SecretHash   string             `json:"secretHash,omitempty"`
	Status       status.Status      `json:"status"`
	Lifecycle    string             `json:"lifecycle"`
	Imported     bool               `json:"imported,omitempty"`
	Provider     map[string]any     `json:"providerState,omitempty"`
	LastApplied  *resource.Envelope `json:"lastApplied,omitempty"`
	Dependencies []resource.Key     `json:"dependencies,omitempty"`
	// DependencyHashes records secret fingerprints this resource was last
	// reconciled against, keyed by serialized dependency key.
	DependencyHashes map[string]string `json:"dependencyHashes,omitempty"`
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
	version := s.Version
	if version == 0 {
		version = 1
	}
	for k, v := range s.RawResources {
		var key resource.Key
		if version < 2 {
			key = parseV1Key(k)
		} else {
			key = ParseKey(k)
		}
		if v.LastApplied != nil {
			v.LastApplied.Metadata.Namespace = resource.NormalizeNamespace(v.LastApplied.Metadata.Namespace)
		}
		s.Resources[key] = v
	}
	s.Version = CurrentVersion
}

// Flatten syncs the raw serialized form from the typed map before Save.
func (s *State) Flatten() {
	s.RawResources = make(map[string]ResourceState, len(s.Resources))
	for k, v := range s.Resources {
		k.Namespace = resource.NormalizeNamespace(k.Namespace)
		if v.LastApplied != nil {
			v.LastApplied.Metadata.Namespace = resource.NormalizeNamespace(v.LastApplied.Metadata.Namespace)
		}
		s.RawResources[KeyString(k)] = v
	}
}

func KeyString(k resource.Key) string {
	parts := []string{
		url.PathEscape(resource.NormalizeNamespace(k.Namespace)),
		url.PathEscape(k.Kind),
		url.PathEscape(k.Name),
	}
	return strings.Join(parts, "/")
}

func ParseKey(s string) resource.Key {
	parts := strings.Split(s, "/")
	if len(parts) == 3 {
		namespace, _ := url.PathUnescape(parts[0])
		kind, _ := url.PathUnescape(parts[1])
		name, _ := url.PathUnescape(parts[2])
		return resource.Key{Namespace: resource.NormalizeNamespace(namespace), Kind: kind, Name: name}
	}
	return parseV1Key(s)
}

func parseV1Key(s string) resource.Key {
	kind, name, ok := strings.Cut(s, "/")
	if !ok {
		return resource.Key{Namespace: resource.DefaultNamespace, Name: s}
	}
	return resource.Key{Namespace: resource.DefaultNamespace, Kind: kind, Name: name}
}

type StateStore interface {
	Load(ctx context.Context) (State, error)
	Save(ctx context.Context, s State) error
	Lock(ctx context.Context) (unlock func() error, err error)
}
