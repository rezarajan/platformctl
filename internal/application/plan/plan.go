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
	ActionCreate        Action = "create"
	ActionUpdate        Action = "update"
	ActionConfigure     Action = "configure" // External resources only — never create/delete
	ActionNoop          Action = "no-op"
	ActionDelete        Action = "delete"
	ActionOrphanUnknown Action = "orphan-unknown"
	ActionRefused       Action = "refused" // metadata.protect: true blocks a would-be delete
)

type Entry struct {
	Key        resource.Key `json:"key"`
	Action     Action       `json:"action"`
	Reason     string       `json:"reason"`
	SpecHash   string       `json:"specHash,omitempty"`
	SecretHash string       `json:"secretHash,omitempty"`
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
	return ComputeWithSecretHashes(envelopes, st, g, nil)
}

// ComputeWithSecretHashes builds a plan like Compute, additionally comparing
// resolved SecretReference fingerprints. The fingerprints are one-way hashes
// supplied by the apply path after preflight resolution; plain plan remains
// deterministic from manifests + state only.
func ComputeWithSecretHashes(envelopes []resource.Envelope, st state.State, g *graph.Graph, secretHashes map[resource.Key]string) (Plan, error) {
	byKey := make(map[resource.Key]resource.Envelope, len(envelopes))
	for _, e := range envelopes {
		byKey[e.Key()] = e
	}

	levels := g.TopologicalLevels()
	p := Plan{Levels: levels}

	// changed tracks non-noop keys so changes cascade to dependents:
	// a resource realized *from* another (e.g. a Binding whose Dataset's
	// format changed) must re-reconcile even though its own spec is
	// unchanged. Levels are topological, so one pass sees every dependency
	// before its dependents.
	changed := make(map[resource.Key]bool)

	for _, level := range levels {
		for _, key := range level {
			e := byKey[key]
			hash, err := SpecHash(e)
			if err != nil {
				return p, fmt.Errorf("%s: hash spec: %w", key, err)
			}
			prior, exists := st.Resources[key]
			lifecycle := resource.LifecycleOf(e, exists && prior.Imported)
			secretHash := ""
			if e.Kind == "SecretReference" && secretHashes != nil {
				secretHash = secretHashes[key]
			}

			entry := Entry{Key: key, SpecHash: hash, SecretHash: secretHash}
			switch {
			case lifecycle == resource.External:
				if !exists || prior.SpecHash != hash || secretMaterialChanged(e, exists, prior, secretHash) {
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
			case secretMaterialChanged(e, exists, prior, secretHash):
				entry.Action = ActionUpdate
				if prior.SecretHash == "" {
					entry.Reason = "resolved secret fingerprint not recorded in state"
				} else {
					entry.Reason = "resolved secret material changed since last apply"
				}
			default:
				entry.Action = ActionNoop
				entry.Reason = "spec unchanged"
			}
			if entry.Action == ActionNoop && lifecycle == resource.Managed {
				for _, dep := range g.Edges[key] {
					if changed[dep] {
						entry.Action = ActionUpdate
						entry.Reason = fmt.Sprintf("dependency %s changed", dep)
						break
					}
					if dependencySecretChanged(dep, byKey, st, prior, secretHashes) {
						entry.Action = ActionUpdate
						entry.Reason = fmt.Sprintf("dependency %s secret material changed since last apply", dep)
						break
					}
				}
			}
			if entry.Action != ActionNoop {
				changed[key] = true
			}
			p.Entries = append(p.Entries, entry)
		}
	}
	deleteLevels, deleteEntries := computeApplyDeletes(byKey, st)
	p.Levels = append(p.Levels, deleteLevels...)
	p.Entries = append(p.Entries, deleteEntries...)
	return p, nil
}

func secretMaterialChanged(env resource.Envelope, exists bool, prior state.ResourceState, secretHash string) bool {
	if env.Kind != "SecretReference" || !exists || secretHash == "" {
		return false
	}
	return prior.SecretHash == "" || prior.SecretHash != secretHash
}

func dependencySecretChanged(dep resource.Key, desired map[resource.Key]resource.Envelope, st state.State, prior state.ResourceState, secretHashes map[resource.Key]string) bool {
	depEnv, ok := desired[dep]
	if !ok || depEnv.Kind != "SecretReference" {
		return false
	}
	currentHash := ""
	if secretHashes != nil {
		currentHash = secretHashes[dep]
	}
	if currentHash == "" {
		currentHash = st.Resources[dep].SecretHash
	}
	if currentHash == "" {
		return false
	}
	return prior.DependencyHashes[state.KeyString(dep)] != currentHash
}

func computeApplyDeletes(desired map[resource.Key]resource.Envelope, st state.State) ([][]resource.Key, []Entry) {
	absent := make(map[resource.Key]state.ResourceState)
	for key, rs := range st.Resources {
		if _, ok := desired[key]; !ok {
			absent[key] = rs
		}
	}
	if len(absent) == 0 {
		return nil, nil
	}

	depth := make(map[resource.Key]int, len(absent))
	visiting := make(map[resource.Key]bool, len(absent))
	var visit func(resource.Key) int
	visit = func(k resource.Key) int {
		if d, ok := depth[k]; ok {
			return d
		}
		if visiting[k] {
			return 0
		}
		visiting[k] = true
		max := 0
		for _, dep := range absent[k].Dependencies {
			if _, ok := absent[dep]; !ok {
				continue
			}
			if d := visit(dep) + 1; d > max {
				max = d
			}
		}
		visiting[k] = false
		depth[k] = max
		return max
	}
	maxDepth := 0
	for key := range absent {
		if d := visit(key); d > maxDepth {
			maxDepth = d
		}
	}
	normal := make([][]resource.Key, maxDepth+1)
	for key, d := range depth {
		normal[d] = append(normal[d], key)
	}
	for _, level := range normal {
		sort.Slice(level, func(i, j int) bool { return level[i].String() < level[j].String() })
	}

	var levels [][]resource.Key
	var entries []Entry
	for i := len(normal) - 1; i >= 0; i-- {
		level := normal[i]
		if len(level) == 0 {
			continue
		}
		levels = append(levels, level)
		for _, key := range level {
			rs := absent[key]
			entry := Entry{Key: key}
			switch {
			case rs.LastApplied == nil:
				entry.Action = ActionOrphanUnknown
				entry.Reason = "state entry predates last-applied state; re-apply a manifest for this resource or remove it with destroy before authoritative apply can delete it"
			case rs.LastApplied.Metadata.Protect:
				entry.Action = ActionRefused
				entry.Reason = fmt.Sprintf("%s is protected (metadata.protect: true in its last-applied manifest); restore its manifest, set metadata.protect: false and re-apply to lift the block, then remove it", key)
			default:
				entry.Action = ActionDelete
				entry.Reason = "present in state but absent from desired manifests"
			}
			entries = append(entries, entry)
		}
	}
	return levels, entries
}

// isProtected reports metadata.protect for a would-be delete: the manifest's
// own value if one was loaded (e.Kind != ""), otherwise the last-applied
// value recorded in state (the manifest may have already been removed).
func isProtected(e resource.Envelope, prior state.ResourceState) bool {
	if e.Kind != "" {
		return e.Metadata.Protect
	}
	if prior.LastApplied != nil {
		return prior.LastApplied.Metadata.Protect
	}
	return false
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
			if entry.Action == ActionDelete && isProtected(e, prior) {
				entry.Action = ActionRefused
				entry.Reason = fmt.Sprintf("%s is protected (metadata.protect: true); remove metadata.protect and re-apply to allow deletion", key)
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
		"namespace":  resource.NormalizeNamespace(e.Metadata.Namespace),
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
		names = append(names, resource.NameRef(o).NamespaceOr(e.Metadata.Namespace)+"/"+o.Name)
	}
	sort.Strings(names)
	return names
}

// normalize produces canonical JSON: encoding/json sorts map keys, which is
// the property the hash depends on.
func normalize(v any) ([]byte, error) {
	return json.Marshal(v)
}
