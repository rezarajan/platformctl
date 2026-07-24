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
	// MediationFabric records docs/planning/08 L2's engine-owned platform
	// mediation fabric, when one has been ensured — nil otherwise (the
	// gate-off/no-mediated-edge default, and every state file predating
	// L2). Deliberately NOT a Resources/RawResources entry: plan.Compute's
	// own orphan sweep (computeApplyDeletes) treats every Resources entry
	// as "should be present in the current desired manifest set, or be
	// deleted" — correct authoritative-apply semantics for every
	// user-declared Kind, but wrong here, since no manifest Kind ever
	// represents the platform fabric at all (it is provisioned like a
	// network, not declared like a Provider). Putting it in Resources was
	// tried and found live to self-destruct on the very next plan/apply/
	// destroy call of an unrelated manifest (docs/planning/08 L2's Done-
	// note records the finding) — this field exists specifically to avoid
	// that sweep while still surviving Save/Load like any other state.
	MediationFabric *MediationFabricState `json:"mediationFabric,omitempty"`
}

// MediationFabricState is the persisted record for the one engine-owned
// platform mediation fabric a deployment may have (docs/planning/08 L2).
// Provider carries only non-secret host facts (docs/adr/013's
// fingerprints-only discipline) — the same shape ResourceState.Provider
// holds for every other managed object, just addressed directly instead of
// through resource.Key since there is exactly one of these per deployment.
type MediationFabricState struct {
	Status   status.Status  `json:"status"`
	Provider map[string]any `json:"providerState,omitempty"`
}

// migration upgrades a State in place from FromVersion to FromVersion+1,
// mutating RawResources only — Normalize's decode loop runs once, after
// every pending migration, so a migrator's only job is producing raw keys
// the *next* version's decode understands.
type migration struct {
	FromVersion int
	Name        string
	Apply       func(*State)
}

// migrations is the ordered chain, one entry per version this package has
// ever shipped, keyed by the version it upgrades away from. A new format
// change (CurrentVersion++) means appending exactly one entry here — never
// rewriting Normalize's decode loop — see state/migration_test.go for the
// template a new one should follow and the contiguity invariant it checks.
var migrations = []migration{
	{
		FromVersion: 1,
		Name:        "v1-namespace-aware-keys",
		// v1 raw keys are bare "Kind/Name" (no namespace, no escaping); v2
		// keys are url-escaped "namespace/kind/name" (KeyString). Re-key
		// every entry through the v1 parser so the v2 decode below sees
		// consistent input regardless of which version it started at.
		Apply: func(s *State) {
			upgraded := make(map[string]ResourceState, len(s.RawResources))
			for k, v := range s.RawResources {
				upgraded[KeyString(parseV1Key(k))] = v
			}
			s.RawResources = upgraded
		},
	},
}

// Normalize syncs the typed map from the raw serialized form after Load,
// running any pending migrations first.
func (s *State) Normalize() {
	if s.Version == 0 {
		s.Version = 1
	}
	for _, m := range migrations {
		if s.Version == m.FromVersion {
			m.Apply(s)
			s.Version = m.FromVersion + 1
		}
	}
	s.Resources = make(map[resource.Key]ResourceState, len(s.RawResources))
	for k, v := range s.RawResources {
		key := ParseKey(k)
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
