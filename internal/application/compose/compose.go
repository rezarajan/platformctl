// Package compose is the headless engine behind `platformctl
// add`/`wire`/`expose` (docs/planning/08 E9, docs/adr/024-interactive-
// composition.md). The one architectural rule (ADR 024): composition
// compiles to manifest patches — new YAML files and/or additive `.env` key
// appends — and never applies anything or bypasses the files it writes.
// `validate -> lint -> plan -> apply` is unchanged; this package only ever
// proposes and writes manifest text.
//
// This package holds no interactive/TUI imports (huh, bubbletea) — charm
// imports are confined to cmd/platformctl and internal/cliutil, enforced by
// internal/archtest's confinement test — so the engine (candidate
// computation, patch generation) stays headless and unit-testable, exactly
// the engine/TUI seam ADR 024's "Interaction layer" section describes.
package compose

import (
	"fmt"

	"github.com/rezarajan/platformctl/internal/application/compatibility"
	"github.com/rezarajan/platformctl/internal/application/manifest"
	"github.com/rezarajan/platformctl/internal/domain/graph"
	"github.com/rezarajan/platformctl/internal/domain/resource"
)

// Resolver mirrors compatibility.ProviderResolver — the seam that lets this
// package ask "does provider type X support capability Y" without
// importing any concrete adapter (CLAUDE.md's layering invariant).
type Resolver = compatibility.ProviderResolver

// Snapshot is a tolerant-mode load of an existing manifest set — the
// "loadAndValidate front-end, tolerant mode" ADR 024's "Graph-aware reuse"
// section calls for. Unlike cmd/platformctl's loadAndValidate, a graph or
// compatibility failure here degrades to Warning instead of a hard error:
// composition must still be able to compute best-effort reuse candidates
// against a set someone is mid-edit on. Only a manifest.Load failure
// (nothing parseable at all) is fatal — there is nothing left to scan for
// candidates.
type Snapshot struct {
	Dir       string
	Envelopes []resource.Envelope
	Graph     *graph.Graph // nil when graph.Build failed (degraded)
	Warning   string       // non-empty when degraded to best-effort
}

// LoadTolerant loads dir's manifest set the same way `validate` does, but
// degrades a graph-build or compatibility failure to Snapshot.Warning
// instead of returning an error.
func LoadTolerant(dir string, resolve Resolver) (Snapshot, error) {
	envelopes, err := manifest.Load(dir)
	if err != nil {
		return Snapshot{}, fmt.Errorf("compose: loading %s: %w", dir, err)
	}
	snap := Snapshot{Dir: dir, Envelopes: envelopes}

	g, err := graph.Build(envelopes)
	if err != nil {
		snap.Warning = fmt.Sprintf("existing manifest set has a graph error (%v); candidate computation is best-effort", err)
		return snap, nil
	}
	snap.Graph = g

	if resolve != nil {
		if err := compatibility.Check(envelopes, resolve); err != nil {
			snap.Warning = fmt.Sprintf("existing manifest set is not fully compatible (%v); candidate computation is best-effort", err)
		}
	}
	return snap, nil
}

// byKindType returns every envelope of the given Kind, additionally
// filtered by spec.type when specType is non-empty (Provider's
// discriminator).
func (s Snapshot) byKindType(kind, specType string) []resource.Envelope {
	var out []resource.Envelope
	for _, e := range s.Envelopes {
		if e.Kind != kind {
			continue
		}
		if specType != "" {
			t, _ := e.Spec["type"].(string)
			if t != specType {
				continue
			}
		}
		out = append(out, e)
	}
	return out
}

// byName finds a resource by Kind+name (default namespace only — the
// scope every shipped blueprint and composite operates in).
func (s Snapshot) byName(kind, name string) (resource.Envelope, bool) {
	for _, e := range s.Envelopes {
		if e.Kind == kind && e.Metadata.Name == name {
			return e, true
		}
	}
	return resource.Envelope{}, false
}

// NameExists reports whether any resource in the set already has this
// Kind+name — the collision check every Plan function makes before
// proposing a brand new resource name (ADR 024: "Name collisions prompt
// for a new name; nothing is ever overwritten silently").
func (s Snapshot) NameExists(kind, name string) bool {
	_, ok := s.byName(kind, name)
	return ok
}

// RefChoice is the machine both a --flag ("existing:<name>" | "new") and
// an interactive select render to: reuse an existing candidate, or create
// a new one (ADR 024: "Flag mode is the same machine" as the interactive
// prompts).
type RefChoice struct {
	New  bool
	Name string // the existing candidate's name, meaningful only when !New
}

// ParseRefChoice parses a --broker/--sink/--lake/--provider-style flag
// value: "new" or "existing:<name>". flagName is used only for the error
// message.
func ParseRefChoice(flagName, value string) (RefChoice, error) {
	if value == "new" {
		return RefChoice{New: true}, nil
	}
	const prefix = "existing:"
	if len(value) > len(prefix) && value[:len(prefix)] == prefix {
		name := value[len(prefix):]
		if name == "" {
			return RefChoice{}, fmt.Errorf("--%s %q: name after %q must not be empty", flagName, value, prefix)
		}
		return RefChoice{Name: name}, nil
	}
	return RefChoice{}, fmt.Errorf("--%s %q is invalid: must be \"new\" or \"existing:<name>\"", flagName, value)
}
