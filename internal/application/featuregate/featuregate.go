// Package featuregate holds the feature gate registry and stage metadata.
// The registry consults it before allowing a provider/runtime/behavior to be
// used; disabled gates fail fast with a clear message.
// See docs/planning/02-architecture.md §5.6 and the roadmap doc §12.
package featuregate

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Stage string

const (
	Alpha Stage = "Alpha"
	Beta  Stage = "Beta"
	GA    Stage = "GA"
)

type GateState struct {
	Stage   Stage
	Default bool
	Enabled bool
}

// zeroTrustFamily is the set of gates the single ZeroTrust gate subsumes
// (docs/adr/035 decision 3): the developer thinks only "zero-trust on
// (default for a project) or off", never these four individually. When
// ZeroTrust is enabled, each of these reports enabled — so a Connection is
// mediated, access is graph-scoped, and policy (incl. label selectors) is
// enforced, all at once, with zero call-site changes. Each stays
// independently settable for backward compatibility (a manifest set with no
// datascape.yaml that enabled one of these directly still works, ZeroTrust
// off).
var zeroTrustFamily = map[string]bool{
	"MediatedConnections": true,
	"GraphScopedAccess":   true,
	"LabelScopedAccess":   true,
	"PolicyEngine":        true,
}

type Registry struct {
	mu    sync.RWMutex
	gates map[string]GateState
}

// enabledLocked resolves a gate's effective state under the ZeroTrust
// subsumption. Caller holds at least RLock.
func (r *Registry) enabledLocked(name string) bool {
	if r.gates[name].Enabled {
		return true
	}
	if zeroTrustFamily[name] && r.gates["ZeroTrust"].Enabled {
		return true
	}
	return false
}

func NewRegistry() *Registry {
	return &Registry{gates: make(map[string]GateState)}
}

// Register adds a gate; Enabled starts at Default.
func (r *Registry) Register(name string, stage Stage, def bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gates[name] = GateState{Stage: stage, Default: def, Enabled: def}
}

// Enabled reports whether a gate is on. Unknown gates are disabled.
func (r *Registry) Enabled(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.enabledLocked(name)
}

// Require returns a clear error when a gate is disabled or unknown.
func (r *Registry) Require(name string) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	g, ok := r.gates[name]
	if !ok {
		return fmt.Errorf("feature gate %q is not registered", name)
	}
	if !r.enabledLocked(name) {
		return fmt.Errorf("feature gate %q (stage: %s) is disabled; enable with --feature-gates=%s=true", name, g.Stage, name)
	}
	return nil
}

// Apply parses a --feature-gates=Name=true,Other=false override string.
func (r *Registry) Apply(overrides string) error {
	if overrides == "" {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, pair := range strings.Split(overrides, ",") {
		name, val, found := strings.Cut(strings.TrimSpace(pair), "=")
		if !found {
			return fmt.Errorf("invalid feature gate override %q (expected Name=true|false)", pair)
		}
		g, ok := r.gates[name]
		if !ok {
			return fmt.Errorf("unknown feature gate %q", name)
		}
		b, err := strconv.ParseBool(val)
		if err != nil {
			return fmt.Errorf("feature gate %q: invalid value %q", name, val)
		}
		g.Enabled = b
		r.gates[name] = g
	}
	return nil
}

// List returns all gates sorted by name, for `--help`/docs output.
func (r *Registry) List() []struct {
	Name string
	GateState
} {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]struct {
		Name string
		GateState
	}, 0, len(r.gates))
	for name, g := range r.gates {
		out = append(out, struct {
			Name string
			GateState
		}{name, g})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
