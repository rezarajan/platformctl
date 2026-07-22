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
// resource depends on the referenced one. secretRef is the single-reference
// form used by Connection; Provider's plural secretRefs is handled below.
var refFields = []string{"providerRef", "sourceRef", "targetRef", "connectionRef", "secretRef"}

// configRefField pairs a ref field nested one level under spec.configuration
// with the Kind(s) it must resolve to.
type configRefField struct {
	field   string
	allowed map[string]bool
}

// configRefFields are ref fields nested one level under spec.configuration
// rather than at the spec top level like refFields above — introduced by
// docs/planning/08 D10 for Provider(type: trino).spec.configuration.
// catalogRef (Catalog, must reconcile before the Trino Provider that reads
// its published REST endpoint) and .warehouseProviderRef (Provider, an
// explicit disambiguator for which S3/MinIO Provider backs the catalog's
// warehouse when more than one exists in the manifest — see D10's
// TASK_PROGRESS.md design note on why this exists instead of a
// Catalog.spec.warehouseRef: D8, which would add that field, is not
// implemented on main). spec.configuration is otherwise an open,
// provider-specific bag (never introspected generically) — this list is
// deliberately narrow, naming only the exact fields a specific provider is
// known to place refs in, not a general "any nested Ref-shaped field"
// mechanism. A slice, not a map, so processing order — and therefore which
// error surfaces first when more than one field is invalid — stays
// deterministic (docs/planning/08 §2's "plan output stays deterministic"
// bar), the same reason refFields above is a slice.
var configRefFields = []configRefField{
	{field: "catalogRef", allowed: map[string]bool{"Catalog": true}},
	{field: "warehouseProviderRef", allowed: map[string]bool{"Provider": true}},
}

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

	// Index by namespace/name for cross-kind ref resolution.
	byName := make(map[string][]resource.Key)
	for _, e := range envelopes {
		k := e.Key()
		if _, dup := g.Nodes[k]; dup {
			return nil, fmt.Errorf("duplicate resource %s", k)
		}
		g.Nodes[k] = e
		byName[nameIndexKey(k.Namespace, k.Name)] = append(byName[nameIndexKey(k.Namespace, k.Name)], k)
	}

	for _, e := range envelopes {
		from := e.Key()
		for _, field := range refFields {
			ref := resource.RefFromSpec(e.Spec, field)
			if ref.Name == "" {
				continue
			}
			if err := validateRef(from, field, ref); err != nil {
				return nil, err
			}
			targets := filterKinds(byName[nameIndexKey(ref.NamespaceOr(e.Metadata.Namespace), ref.Name)], allowedKinds(field))
			if len(targets) == 0 {
				// Every reference — connectionRef included — must resolve
				// in-set: the engine will demand it at apply time, and a
				// dangling reference caught only then is a broken developer
				// experience.
				return nil, fmt.Errorf("%s: spec.%s %q does not resolve to any resource in namespace %q", from, field, ref.Name, ref.NamespaceOr(e.Metadata.Namespace))
			}
			// Prefer the kind-appropriate target when a name is ambiguous.
			to := targets[0]
			if len(targets) > 1 {
				return nil, fmt.Errorf("%s: spec.%s %q is ambiguous in namespace %q (matches %d resources)", from, field, ref.Name, ref.NamespaceOr(e.Metadata.Namespace), len(targets))
			}
			g.Edges[from] = append(g.Edges[from], to)
		}
		// Nested configuration-level refs (D10): same resolution/validation
		// as the top-level pass above, scoped to spec.configuration.
		if configBlock, ok := e.Spec["configuration"].(map[string]any); ok {
			for _, crf := range configRefFields {
				ref := resource.RefFromSpec(configBlock, crf.field)
				if ref.Name == "" {
					continue
				}
				if err := validateRef(from, "configuration."+crf.field, ref); err != nil {
					return nil, err
				}
				targets := filterKinds(byName[nameIndexKey(ref.NamespaceOr(e.Metadata.Namespace), ref.Name)], crf.allowed)
				if len(targets) == 0 {
					return nil, fmt.Errorf("%s: spec.configuration.%s %q does not resolve to any resource in namespace %q", from, crf.field, ref.Name, ref.NamespaceOr(e.Metadata.Namespace))
				}
				to := targets[0]
				if len(targets) > 1 {
					return nil, fmt.Errorf("%s: spec.configuration.%s %q is ambiguous in namespace %q (matches %d resources)", from, crf.field, ref.Name, ref.NamespaceOr(e.Metadata.Namespace), len(targets))
				}
				g.Edges[from] = append(g.Edges[from], to)
			}
		}
		// secretRefs (Provider kind) create edges to SecretReferences.
		if refs, ok := e.Spec["secretRefs"].([]any); ok {
			for _, r := range refs {
				name, ok := r.(string)
				if !ok || name == "" {
					continue
				}
				if err := resource.ValidateDNSLabel("spec.secretRefs.name", name); err != nil {
					return nil, fmt.Errorf("%s: %w", from, err)
				}
				target := resource.Key{Namespace: from.Namespace, Kind: "SecretReference", Name: name}
				if _, exists := g.Nodes[target]; !exists {
					return nil, fmt.Errorf("%s: spec.secretRefs entry %q does not resolve to a SecretReference in namespace %q", from, name, from.Namespace)
				}
				g.Edges[from] = append(g.Edges[from], target)
			}
		}
		// observers create edges too: the resource depends on the observed provider
		// being reconciled first so its endpoint is resolvable.
		for _, obs := range e.Metadata.Observers {
			ref := resource.NameRef{Name: obs.Name, Namespace: obs.Namespace}
			if err := validateRef(from, "metadata.observers", ref); err != nil {
				return nil, err
			}
			target := ref.Key(e.Metadata.Namespace, "Provider")
			if _, ok := g.Nodes[target]; !ok {
				return nil, fmt.Errorf("%s: metadata.observers entry %q does not resolve to a Provider in namespace %q", from, obs.Name, target.Namespace)
			}
			g.Edges[from] = append(g.Edges[from], target)
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

// Dependencies returns the transitive set of resources k depends on.
func (g *Graph) Dependencies(k resource.Key) map[resource.Key]bool {
	out := make(map[resource.Key]bool)
	var walk func(resource.Key)
	walk = func(cur resource.Key) {
		for _, d := range g.Edges[cur] {
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

func nameIndexKey(namespace, name string) string {
	return resource.NormalizeNamespace(namespace) + "\x00" + name
}

func validateRef(from resource.Key, field string, ref resource.NameRef) error {
	if err := resource.ValidateDNSLabel(field+".name", ref.Name); err != nil {
		return fmt.Errorf("%s: %w", from, err)
	}
	if ref.Namespace != "" {
		if err := resource.ValidateDNSLabel(field+".namespace", ref.Namespace); err != nil {
			return fmt.Errorf("%s: %w", from, err)
		}
	}
	return nil
}

func allowedKinds(field string) map[string]bool {
	switch field {
	case "providerRef":
		return map[string]bool{"Provider": true}
	case "connectionRef":
		return map[string]bool{"Connection": true, "SecretReference": true}
	case "secretRef":
		return map[string]bool{"SecretReference": true}
	default:
		return nil
	}
}

func filterKinds(keys []resource.Key, allowed map[string]bool) []resource.Key {
	if allowed == nil {
		return keys
	}
	var out []resource.Key
	for _, k := range keys {
		if allowed[k.Kind] {
			out = append(out, k)
		}
	}
	return out
}
