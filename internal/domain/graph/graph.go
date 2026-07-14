// Package graph builds a DAG from providerRef/sourceRef/targetRef/connectionRef
// fields, detects cycles, and produces topological levels.
// See docs/planning/02-architecture.md §5.3.
package graph

import (
	"fmt"
	"sort"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

// refFields are the spec fields that create dependency edges. The referencing
// resource depends on the referenced one.
var refFields = []string{"providerRef", "sourceRef", "targetRef", "connectionRef"}

// refKinds maps a ref field to the Kind(s) the name may resolve to. sourceRef
// and targetRef are mode-dependent for Binding, so those resolve by name
// across all kinds.
type Graph struct {
	// Nodes keyed by resource key.
	Nodes map[resource.Key]resource.Envelope
	// Edges: key -> set of keys it depends on.
	Edges map[resource.Key][]resource.Key
}

// Build constructs the dependency graph for a set of envelopes. A reference to
// a name that doesn't resolve to any resource in the set is an error, unless
// the referencing resource is External (its connectionRef may point outside
// the manifest set — resolved via SecretReference instead).
func Build(envelopes []resource.Envelope) (*Graph, error) {
	g := &Graph{
		Nodes: make(map[resource.Key]resource.Envelope, len(envelopes)),
		Edges: make(map[resource.Key][]resource.Key),
	}

	// Index by name for cross-kind ref resolution.
	byName := make(map[string][]resource.Key)
	for _, e := range envelopes {
		k := e.Key()
		if _, dup := g.Nodes[k]; dup {
			return nil, fmt.Errorf("duplicate resource %s", k)
		}
		g.Nodes[k] = e
		byName[e.Metadata.Name] = append(byName[e.Metadata.Name], k)
	}

	for _, e := range envelopes {
		from := e.Key()
		for _, field := range refFields {
			name := refName(e.Spec, field)
			if name == "" {
				continue
			}
			targets := byName[name]
			if len(targets) == 0 {
				if field == "connectionRef" {
					continue // external connection, resolved via SecretReference at runtime
				}
				return nil, fmt.Errorf("%s: spec.%s %q does not resolve to any resource in the manifest set", from, field, name)
			}
			// Prefer the kind-appropriate target when a name is ambiguous.
			to := targets[0]
			if len(targets) > 1 {
				return nil, fmt.Errorf("%s: spec.%s %q is ambiguous (matches %d resources)", from, field, name, len(targets))
			}
			g.Edges[from] = append(g.Edges[from], to)
		}
		// secretRefs (Provider kind) create edges to SecretReferences.
		if refs, ok := e.Spec["secretRefs"].([]any); ok {
			for _, r := range refs {
				name, ok := r.(string)
				if !ok || name == "" {
					continue
				}
				target := resource.Key{Kind: "SecretReference", Name: name}
				if _, exists := g.Nodes[target]; !exists {
					return nil, fmt.Errorf("%s: spec.secretRefs entry %q does not resolve to a SecretReference", from, name)
				}
				g.Edges[from] = append(g.Edges[from], target)
			}
		}
		// observers create edges too: the resource depends on the observed provider
		// being reconciled first so its endpoint is resolvable.
		for _, obs := range e.Metadata.Observers {
			targets := byName[obs.Name]
			if len(targets) == 0 {
				return nil, fmt.Errorf("%s: metadata.observers entry %q does not resolve to any resource", from, obs.Name)
			}
			g.Edges[from] = append(g.Edges[from], targets[0])
		}
	}

	if cycle := g.findCycle(); cycle != nil {
		return nil, fmt.Errorf("dependency cycle detected: %s", formatCycle(cycle))
	}
	return g, nil
}

// TopologicalLevels returns resources grouped into dependency levels:
// resources in the same level have no dependency relationship and are
// eligible for concurrent reconciliation.
func (g *Graph) TopologicalLevels() [][]resource.Key {
	depth := make(map[resource.Key]int, len(g.Nodes))
	var visit func(k resource.Key) int
	visit = func(k resource.Key) int {
		if d, ok := depth[k]; ok {
			return d
		}
		depth[k] = 0 // provisional; safe because Build rejects cycles
		max := 0
		for _, dep := range g.Edges[k] {
			if d := visit(dep) + 1; d > max {
				max = d
			}
		}
		depth[k] = max
		return max
	}

	maxDepth := 0
	for k := range g.Nodes {
		if d := visit(k); d > maxDepth {
			maxDepth = d
		}
	}

	levels := make([][]resource.Key, maxDepth+1)
	for k, d := range depth {
		levels[d] = append(levels[d], k)
	}
	for _, level := range levels {
		sort.Slice(level, func(i, j int) bool { return level[i].String() < level[j].String() })
	}
	return levels
}

// Dependents returns the transitive set of resources that depend on k.
func (g *Graph) Dependents(k resource.Key) map[resource.Key]bool {
	reverse := make(map[resource.Key][]resource.Key)
	for from, deps := range g.Edges {
		for _, to := range deps {
			reverse[to] = append(reverse[to], from)
		}
	}
	out := make(map[resource.Key]bool)
	var walk func(resource.Key)
	walk = func(cur resource.Key) {
		for _, d := range reverse[cur] {
			if !out[d] {
				out[d] = true
				walk(d)
			}
		}
	}
	walk(k)
	return out
}

func (g *Graph) findCycle() []resource.Key {
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[resource.Key]int, len(g.Nodes))
	var stack []resource.Key

	var dfs func(k resource.Key) []resource.Key
	dfs = func(k resource.Key) []resource.Key {
		color[k] = gray
		stack = append(stack, k)
		for _, dep := range g.Edges[k] {
			switch color[dep] {
			case gray:
				// Found a cycle: slice the stack from the first occurrence of dep.
				for i, s := range stack {
					if s == dep {
						return append(stack[i:], dep)
					}
				}
			case white:
				if c := dfs(dep); c != nil {
					return c
				}
			}
		}
		stack = stack[:len(stack)-1]
		color[k] = black
		return nil
	}

	keys := make([]resource.Key, 0, len(g.Nodes))
	for k := range g.Nodes {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
	for _, k := range keys {
		if color[k] == white {
			stack = stack[:0]
			if c := dfs(k); c != nil {
				return c
			}
		}
	}
	return nil
}

func formatCycle(cycle []resource.Key) string {
	s := ""
	for i, k := range cycle {
		if i > 0 {
			s += " -> "
		}
		s += k.String()
	}
	return s
}

func refName(spec map[string]any, field string) string {
	ref, ok := spec[field].(map[string]any)
	if !ok {
		return ""
	}
	name, _ := ref["name"].(string)
	return name
}
