// Package plan implements the diff engine: desired (manifests) vs. state.
// Deterministic by design (NFR-1): actions are driven by the spec/state hash
// comparison, never live state. See docs/planning/02-architecture.md §5.4.
package plan

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/rezarajan/platformctl/internal/domain/graph"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/state"
)

type Action string

const (
	ActionCreate    Action = "create"
	ActionUpdate    Action = "update"
	ActionConfigure Action = "configure" // External resources only — never create/delete
	ActionNoop      Action = "no-op"
	ActionDelete    Action = "delete" // produced by destroy plans only
)

type Entry struct {
	Key      resource.Key `json:"key"`
	Action   Action       `json:"action"`
	Reason   string       `json:"reason"`
	SpecHash string       `json:"specHash,omitempty"`
}

type Plan struct {
	Entries []Entry `json:"entries"`
	// Levels preserves topological ordering for the executor.
	Levels [][]resource.Key `json:"-"`
}

// HasChanges reports whether any entry is not a no-op.
func (p Plan) HasChanges() bool {
	for _, e := range p.Entries {
		if e.Action != ActionNoop {
			return true
		}
	}
	return false
}

// Compute builds a plan for the given envelopes against prior state, in
// dependency order.
func Compute(envelopes []resource.Envelope, st state.State, g *graph.Graph) (Plan, error) {
	byKey := make(map[resource.Key]resource.Envelope, len(envelopes))
	for _, e := range envelopes {
		byKey[e.Key()] = e
	}

	levels := g.TopologicalLevels()
	p := Plan{Levels: levels}

	for _, level := range levels {
		for _, key := range level {
			e := byKey[key]
			hash, err := SpecHash(e)
			if err != nil {
				return p, fmt.Errorf("%s: hash spec: %w", key, err)
			}
			prior, exists := st.Resources[key]
			lifecycle := resource.LifecycleOf(e, exists && prior.Imported)

			entry := Entry{Key: key, SpecHash: hash}
			switch {
			case lifecycle == resource.External:
				if !exists || prior.SpecHash != hash {
					entry.Action = ActionConfigure
					entry.Reason = "external resource; configuration differs from last applied"
				} else {
					entry.Action = ActionNoop
					entry.Reason = "external resource; configuration unchanged"
				}
			case !exists:
				entry.Action = ActionCreate
				entry.Reason = "not present in state"
			case prior.SpecHash != hash:
				entry.Action = ActionUpdate
				entry.Reason = "spec changed since last apply"
			default:
				entry.Action = ActionNoop
				entry.Reason = "spec unchanged"
			}
			p.Entries = append(p.Entries, entry)
		}
	}
	return p, nil
}

// ComputeDestroy builds a teardown plan: reverse dependency order, managed
// resources only unless the include flags are set.
func ComputeDestroy(envelopes []resource.Envelope, st state.State, g *graph.Graph, includeExternal, includeImported bool) (Plan, error) {
	byKey := make(map[resource.Key]resource.Envelope, len(envelopes))
	for _, e := range envelopes {
		byKey[e.Key()] = e
	}

	levels := g.TopologicalLevels()
	p := Plan{}
	// Reverse level order for teardown: dependents before dependencies.
	for i := len(levels) - 1; i >= 0; i-- {
		p.Levels = append(p.Levels, levels[i])
		for _, key := range levels[i] {
			e := byKey[key]
			prior, exists := st.Resources[key]
			lifecycle := resource.LifecycleOf(e, exists && prior.Imported)

			entry := Entry{Key: key}
			switch lifecycle {
			case resource.External:
				if includeExternal {
					entry.Action = ActionDelete
					entry.Reason = "external resource included via --include-external"
				} else {
					entry.Action = ActionNoop
					entry.Reason = "external resource; skipped (use --include-external to include)"
				}
			case resource.Imported:
				if includeImported {
					entry.Action = ActionDelete
					entry.Reason = "imported resource included via --include-imported"
				} else {
					entry.Action = ActionNoop
					entry.Reason = "imported resource; skipped (use --include-imported to include)"
				}
			default:
				if exists {
					entry.Action = ActionDelete
					entry.Reason = "managed resource present in state"
				} else {
					entry.Action = ActionNoop
					entry.Reason = "not present in state; nothing to destroy"
				}
			}
			p.Entries = append(p.Entries, entry)
		}
	}
	return p, nil
}

// SpecHash computes a content hash of the resource's normalized spec.
// JSON round-trip with sorted keys makes it order-insensitive and stable.
func SpecHash(e resource.Envelope) (string, error) {
	normalized, err := normalize(map[string]any{
		"apiVersion": e.APIVersion,
		"kind":       e.Kind,
		"name":       e.Metadata.Name,
		"observers":  observerNames(e),
		"spec":       e.Spec,
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(normalized)
	return hex.EncodeToString(sum[:]), nil
}

func observerNames(e resource.Envelope) []string {
	names := make([]string, 0, len(e.Metadata.Observers))
	for _, o := range e.Metadata.Observers {
		names = append(names, o.Name)
	}
	sort.Strings(names)
	return names
}

// normalize produces canonical JSON: encoding/json sorts map keys, which is
// the property the hash depends on.
func normalize(v any) ([]byte, error) {
	return json.Marshal(v)
}
