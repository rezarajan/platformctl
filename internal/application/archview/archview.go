// Package archview derives the *architecture* of a manifest set — how data
// flows and which technology realizes each asset — from the resource
// envelopes. This is distinct from the reconcile dependency DAG
// (internal/domain/graph): that answers "what order do I apply in", this
// answers "what does my platform look like". Bindings collapse into labelled
// data-flow edges between their endpoints, Providers connect to the assets
// they realize, and observers connect assets to their lineage backend.
package archview

import (
	"sort"

	"github.com/rezarajan/platformctl/internal/domain/binding"
	"github.com/rezarajan/platformctl/internal/domain/resource"
)

type EdgeKind string

const (
	// Pipeline is a data-movement edge realized by a Binding: source asset
	// to target asset, labelled with the mode and realizing provider.
	Pipeline EdgeKind = "pipeline"
	// Realizes connects a Provider to an asset it stands up.
	Realizes EdgeKind = "realizes"
	// Observes connects an asset to a lineage Provider named in observers.
	Observes EdgeKind = "observes"
	// Backs connects a Connection to the external target it forwards to
	// (a synthetic node) or an asset to the Connection it reaches through.
	Reaches EdgeKind = "reaches"
)

type Node struct {
	Key       resource.Key
	Kind      string // resource Kind, or "External" for synthetic targets
	Lifecycle string // Managed | External | Imported (assets only)
	Detail    string // provider type, engine, address — a one-line summary
}

type Edge struct {
	From resource.Key
	To   resource.Key
	Kind EdgeKind
	// Label annotates the edge (mode·provider for a pipeline, "forwards to"
	// for a Connection target).
	Label string
	// Observers lists lineage providers, on Pipeline edges only.
	Observers []string
}

type View struct {
	Nodes []Node
	Edges []Edge
}

// Build constructs the architecture view from a validated envelope set.
func Build(envelopes []resource.Envelope) *View {
	byKey := make(map[resource.Key]resource.Envelope, len(envelopes))
	for _, e := range envelopes {
		byKey[e.Key()] = e
	}

	v := &View{}
	seen := map[resource.Key]bool{}
	addNode := func(n Node) {
		if seen[n.Key] {
			return
		}
		seen[n.Key] = true
		v.Nodes = append(v.Nodes, n)
	}

	for _, e := range envelopes {
		if e.Kind == "Binding" {
			continue // bindings render as edges, not nodes
		}
		addNode(Node{
			Key:       e.Key(),
			Kind:      e.Kind,
			Lifecycle: resource.LifecycleOf(e, false).String(),
			Detail:    detailOf(e),
		})
	}

	for _, e := range envelopes {
		switch e.Kind {
		case "Binding":
			b, err := binding.FromEnvelope(e)
			if err != nil {
				continue
			}
			label := string(b.Mode)
			if b.ProviderRef != "" {
				label += " · " + b.ProviderRef
			}
			observers := make([]string, 0, len(e.Metadata.Observers))
			for _, o := range e.Metadata.Observers {
				observers = append(observers, o.Name)
			}
			target := resolveByName(byKey, b.TargetRef)
			v.Edges = append(v.Edges, Edge{
				From:      resolveByName(byKey, b.SourceRef),
				To:        target,
				Kind:      Pipeline,
				Label:     label,
				Observers: observers,
			})
			// Lineage attaches at the target asset in the graph views.
			for _, o := range observers {
				v.Edges = append(v.Edges, Edge{From: target, To: resource.Key{Kind: "Provider", Name: o}, Kind: Observes})
			}
		default:
			// Realization: the providerRef stands up this asset.
			if p := refName(e.Spec, "providerRef"); p != "" {
				v.Edges = append(v.Edges, Edge{
					From: resource.Key{Kind: "Provider", Name: p},
					To:   e.Key(),
					Kind: Realizes,
				})
			}
			// Reachability: an asset consuming a Connection.
			if c := refName(e.Spec, "connectionRef"); c != "" {
				if _, ok := byKey[resource.Key{Kind: "Connection", Name: c}]; ok {
					v.Edges = append(v.Edges, Edge{
						From: e.Key(),
						To:   resource.Key{Kind: "Connection", Name: c},
						Kind: Reaches,
					})
				}
			}
			// Observers on a non-Binding asset (uncommon) attach directly.
			for _, obs := range e.Metadata.Observers {
				v.Edges = append(v.Edges, Edge{From: e.Key(), To: resource.Key{Kind: "Provider", Name: obs.Name}, Kind: Observes})
			}
		}
	}

	// A Connection's target is a synthetic node so the external system it
	// fronts is visible in the picture.
	for _, e := range envelopes {
		if e.Kind != "Connection" {
			continue
		}
		if target, _ := e.Spec["target"].(string); target != "" {
			tk := resource.Key{Kind: "External", Name: target}
			addNode(Node{Key: tk, Kind: "External", Detail: "external system"})
			v.Edges = append(v.Edges, Edge{From: e.Key(), To: tk, Kind: Reaches, Label: "forwards to"})
		}
	}

	sort.Slice(v.Nodes, func(i, j int) bool { return v.Nodes[i].Key.String() < v.Nodes[j].Key.String() })
	sort.Slice(v.Edges, func(i, j int) bool {
		if v.Edges[i].From != v.Edges[j].From {
			return v.Edges[i].From.String() < v.Edges[j].From.String()
		}
		return v.Edges[i].To.String() < v.Edges[j].To.String()
	})
	return v
}

// resolveByName finds a resource by bare name across kinds (Binding refs are
// name-only). Falls back to a synthetic key when unresolved.
func resolveByName(byKey map[resource.Key]resource.Envelope, name string) resource.Key {
	for k := range byKey {
		if k.Name == name {
			return k
		}
	}
	return resource.Key{Kind: "?", Name: name}
}

func detailOf(e resource.Envelope) string {
	switch e.Kind {
	case "Provider":
		if t, _ := e.Spec["type"].(string); t != "" {
			return "type: " + t
		}
	case "Source", "Catalog":
		if eng, _ := e.Spec["engine"].(string); eng != "" {
			return "engine: " + eng
		}
	case "Dataset":
		b, _ := e.Spec["bucket"].(string)
		f, _ := e.Spec["format"].(string)
		if b != "" {
			return b + " (" + f + ")"
		}
	case "Connection":
		if t, _ := e.Spec["target"].(string); t != "" {
			return "→ " + t
		}
	}
	return ""
}

func refName(spec map[string]any, field string) string {
	ref, ok := spec[field].(map[string]any)
	if !ok {
		return ""
	}
	name, _ := ref["name"].(string)
	return name
}
