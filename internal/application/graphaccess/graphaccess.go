// Package graphaccess derives the docs/adr/026 access-request set — "the
// platform already holds the complete access-request graph... every
// manifest reference is an explicit, reviewed, versioned access
// declaration" — from an already-built internal/domain/graph.Graph.
//
// DeriveEdges is intentionally generic and reusable: docs/planning/08 H7
// (graph-scoped network access, ADR 026's own primary deliverable) is the
// eventual consumer for the FULL edge set, compiling it into per-edge
// NetworkPolicies/Docker networks. This task (H6, amended by docs/adr/027)
// needs only the narrower MEDIATED subset — edges terminating in a
// Connection realized by a mediation-capable Provider — so
// CompileMediatedConnections below layers on top of DeriveEdges rather than
// duplicating graph traversal. Splitting the two now (instead of building
// only what H6 needs and leaving generalization for H7) is a deliberate
// down payment docs/adr/027 asks for explicitly: "build it as a reusable
// application-layer function, H7 will consume it later."
//
// This package is application-layer, not domain/ports: docs/planning/02
// §1's layering invariant still binds it (CLAUDE.md's "internal/adapters
// implement ports... only cmd/platformctl and internal/application/registry
// import concrete adapters"), so it never imports registry or any adapter.
// "Is this Provider mediation-capable" is answered by a caller-supplied
// predicate (MediationCapable) — the engine, which alone constructs
// reconciler.Provider instances through the registry, supplies it by
// type-asserting to reconciler.MediationCapableProvider. This keeps
// graphaccess pure graph/resource-set logic, testable with plain stub
// predicates and no registry/adapter wiring at all.
package graphaccess

import (
	"sort"

	"github.com/rezarajan/platformctl/internal/domain/graph"
	"github.com/rezarajan/platformctl/internal/domain/resource"
)

// Edge is one access-request edge derived from the declared reference
// graph: From's manifest declares a reference (providerRef, sourceRef,
// targetRef, connectionRef, secretRef, warehouseRef, via, observers, a
// managed Connection's runtime-name-resolved target, ...) that reaches To.
// "No reference edge -> no path" (docs/adr/026 decision 1) makes this slice
// exactly the graph-scoped request set.
type Edge struct {
	From resource.Key
	To   resource.Key
}

// DeriveEdges flattens a built Graph's dependency edges into the docs/adr/026
// access-request set, deduplicated and returned in a deterministic order
// (matches graph.Graph's own "plan output stays deterministic" discipline,
// docs/planning/08 §2) so callers (H7's future NetworkPolicy/per-edge-network
// compiler, this task's MediatedSubset/CompileMediatedConnections) get
// reproducible output across runs of the same manifest.
func DeriveEdges(g *graph.Graph) []Edge {
	seen := make(map[Edge]bool)
	var edges []Edge
	for from, tos := range g.Edges {
		for _, to := range tos {
			e := Edge{From: from, To: to}
			if seen[e] {
				continue
			}
			seen[e] = true
			edges = append(edges, e)
		}
	}
	sort.Slice(edges, func(i, j int) bool {
		if edges[i].From != edges[j].From {
			return edges[i].From.String() < edges[j].From.String()
		}
		return edges[i].To.String() < edges[j].To.String()
	})
	return edges
}

// MediationCapable reports whether the Provider realizing a Connection is
// mediation-capable (implements reconciler.MediationCapableProvider) — see
// this package's doc comment for why the predicate, rather than a direct
// reconciler/registry import, crosses the layering boundary.
type MediationCapable func(providerEnv resource.Envelope) bool

// MediatedSubset narrows edges to exactly the ones docs/adr/027 promotes to
// Layer 1: edges whose To is a non-external Connection resource realized by
// a mediation-capable Provider. A MediatedConnection is identified
// structurally, by its realizing provider's capability — docs/adr/022: "the
// existing Connection abstraction... realized by a mediator provider
// instead of a plain forwarder" — not by a distinct schema flag, exactly
// like ingress/proxy/wireguard already select their own realization purely
// via providerRef + scheme.
func MediatedSubset(edges []Edge, resources map[resource.Key]resource.Envelope, capable MediationCapable) []Edge {
	var out []Edge
	for _, e := range edges {
		if isMediatedConnection(e.To, resources, capable) {
			out = append(out, e)
		}
	}
	return out
}

func isMediatedConnection(k resource.Key, resources map[resource.Key]resource.Envelope, capable MediationCapable) bool {
	env, ok := resources[k]
	if !ok || env.Kind != "Connection" {
		return false
	}
	if external, _ := env.Spec["external"].(bool); external {
		return false
	}
	provEnv, ok := resolveProviderRef(env, resources)
	if !ok {
		return false
	}
	return capable(provEnv)
}

func resolveProviderRef(env resource.Envelope, resources map[resource.Key]resource.Envelope) (resource.Envelope, bool) {
	ref := resource.RefFromSpec(env.Spec, "providerRef")
	if ref.Name == "" {
		return resource.Envelope{}, false
	}
	provKey := ref.Key(env.Metadata.Namespace, "Provider")
	provEnv, ok := resources[provKey]
	return provEnv, ok
}

// MediatedConnection is one Connection resource realized by a
// mediation-capable Provider, together with its dial side (every resource
// whose declared reference reaches it — the consumers RealizeEdge's Dial
// authorization applies to) and its bind side (the resource(s) the
// Connection's own spec.target names — the near side of the real system
// that must run as the mediation plane's dark listener, RealizeEdge's Bind
// authorization). Both sides are resource.Key, not yet minted identities —
// the caller (the openziti adapter's Reconcile) mints/looks up each key's
// mediation.WorkloadIdentity via internal/domain/naming.WorkloadIdentityURI
// and this package's own resource set, keeping identity minting itself out
// of pure graph logic.
type MediatedConnection struct {
	Connection resource.Key
	Consumers  []resource.Key
	Targets    []resource.Key
}

// CompileMediatedConnections derives every MediatedConnection in the
// resource set — the H6-specific glue layered on DeriveEdges/MediatedSubset
// per this package's doc comment. Consumers are every resource with a
// declared reference reaching the Connection (dial side); Targets are the
// resources the Connection's own edges reach, excluding its realizing
// Provider and any SecretReference (bind side — the managed-Connection
// target-host resolution graph.Build already performs, docs/domain/graph.go
// "A MANAGED Connection's spec.target..."). A Connection whose target does
// not resolve in-set (a genuinely external upstream behind an otherwise
// mediated entrypoint) yields zero Targets — RealizeEdge callers skip the
// bind side and only compile dial authorization in that case; recorded as
// the expected shape, not an error, mirroring graph.Build's own leniency
// there.
func CompileMediatedConnections(g *graph.Graph, resources map[resource.Key]resource.Envelope, capable MediationCapable) []MediatedConnection {
	reverse := make(map[resource.Key][]resource.Key)
	for from, tos := range g.Edges {
		for _, to := range tos {
			reverse[to] = append(reverse[to], from)
		}
	}

	var keys []resource.Key
	for k := range resources {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })

	var out []MediatedConnection
	for _, k := range keys {
		if !isMediatedConnection(k, resources, capable) {
			continue
		}
		mc := MediatedConnection{Connection: k}
		for _, to := range g.Edges[k] {
			if to.Kind == "Provider" || to.Kind == "SecretReference" {
				continue
			}
			mc.Targets = append(mc.Targets, to)
		}
		mc.Consumers = append(mc.Consumers, reverse[k]...)
		sort.Slice(mc.Targets, func(i, j int) bool { return mc.Targets[i].String() < mc.Targets[j].String() })
		sort.Slice(mc.Consumers, func(i, j int) bool { return mc.Consumers[i].String() < mc.Consumers[j].String() })
		out = append(out, mc)
	}
	return out
}
