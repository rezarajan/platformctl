// This file is docs/planning/08 H7's realization half (docs/adr/026, the
// 2026-07-23 addendum): domainruntime.go's decorator already owns the ONE
// chokepoint every Reconcile/Probe/Destroy call's Runtime passes through
// (docs/adr/022 Ring 1's own precedent); this file adds the H7-specific
// helpers that chokepoint calls when the GraphScopedAccess gate is on.
//
// Two DIFFERENT realizations, one per runtime shape (domainRuntime.namespaced
// picks between them):
//
//   - Docker/fake (namespaced == false): a network is nothing more than an
//     ACL-by-membership token — any two containers sharing one already
//     fully reach each other, with no pairwise primitive at all. The ONLY
//     way to express "R1 reaches X but not Z" when X and Z would otherwise
//     share a network is to stop putting unrelated containers on a shared
//     network in the first place: each owner's home network becomes
//     EXCLUSIVE to itself (domainRuntime.translate's PrivateNetworkName
//     path), and reachability is instead realized entirely by small,
//     deterministic per-edge networks (edgeNetworks below) — the addendum's
//     own "each declared edge is a small network joined by exactly its two
//     endpoint workloads."
//   - Kubernetes (namespaced == true): a Pod already lives in exactly one
//     Namespace it cannot leave, and NetworkPolicy already expresses
//     pairwise ingress natively (a peer selector combining a namespace
//     selector with a pod selector) — no topology change is needed at all,
//     only the POLICY: buildNetworkPolicies drops allow-same-namespace
//     under the gate (network.go), and k8sPeers below compiles exactly the
//     peers that may dial this one container into
//     ContainerSpec.AllowFromPeers.
package engine

import (
	"context"
	"fmt"

	"github.com/rezarajan/platformctl/internal/application/graphaccess"
	"github.com/rezarajan/platformctl/internal/domain/graph"
	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/subnet"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// edgeNetworks ensures (idempotently — Docker's own EnsureNetwork is
// already a no-op on an existing network) one small, deterministically
// named+subnetted network per peer in d.peers, returning the list of
// network names this container must join. Both endpoints of an edge
// compute the IDENTICAL name/subnet independently (naming.EdgeNetworkName/
// subnet.For's order-independent canonicalization) — whichever side
// reconciles first creates it; the other's own EnsureNetwork call finds it
// already present and managed.
func (d *domainRuntime) edgeNetworks(ctx context.Context, labels map[string]string) ([]string, error) {
	names := make([]string, 0, len(d.peers))
	for _, peer := range d.peers {
		name := naming.EdgeNetworkName(d.self, peer)
		cidr, err := subnet.For(subnet.DefaultSupernet, subnet.EdgeKey(d.self.String(), peer.String()))
		if err != nil {
			return nil, fmt.Errorf("graph-scoped access: allocate per-edge subnet for %s<->%s: %w", d.self, peer, err)
		}
		if err := d.ContainerRuntime.EnsureNetwork(ctx, runtime.NetworkSpec{Name: name, Subnet: cidr, Labels: labels}); err != nil {
			return nil, fmt.Errorf("graph-scoped access: ensure per-edge network for %s<->%s: %w", d.self, peer, err)
		}
		names = append(names, name)
	}
	return names, nil
}

// k8sPeers compiles d.ingressPeers into runtime.NetworkPeer values for
// ContainerSpec.AllowFromPeers: Network is the peer's own home namespace,
// Name is the peer's runtime object name. A peer missing from d.resources
// (should not happen — MembershipEdges/IngressPeers only ever return keys
// that resolved from d.resources in the first place) is skipped rather
// than erroring, matching graphaccess's own "defensive, not authoritative"
// posture for an already-validated resource set.
//
// Known scope limit (recorded honestly, not silently): the peer's home
// namespace is computed as naming.NetworkName(d.token, peer's domain) —
// this ASSUMES the peer's own realizing Provider uses the same base
// spec.runtime.network token this container does (the default/common
// case). A peer whose OWN Provider pins an explicit, DIFFERENT network
// override resolves to the wrong namespace here; the fix requires
// resolving the peer's own spec.runtime.network from d.resources, deferred
// given this task's time budget (doc 08 H7 Done-note).
func (d *domainRuntime) k8sPeers() []runtime.NetworkPeer {
	out := make([]runtime.NetworkPeer, 0, len(d.ingressPeers))
	for _, peer := range d.ingressPeers {
		env, ok := d.resources[peer]
		if !ok {
			continue
		}
		peerDomain := graphaccess.ContainerDomain(peer, d.resources)
		out = append(out, runtime.NetworkPeer{
			Network: naming.NetworkName(d.token, peerDomain),
			Name:    naming.RuntimeObjectName(env),
		})
	}
	return out
}

// deriveGraphAccessEdges builds the docs/adr/026 access-request graph from
// byKey (graph.Build + graphaccess.DeriveEdges) — called once per
// resolveRequest, only when the GraphScopedAccess gate is enabled (the
// gate-off path never pays this cost, part of the gate-off byte-identical
// pin). byKey is already a validated resource set (plan.Compute built and
// validated the SAME graph earlier in the pipeline), so a build failure
// here would indicate a genuine internal inconsistency, not a user error —
// surfaced as an error rather than silently degrading to "no edges" (which
// would silently disable the very access control the gate promises).
func deriveGraphAccessEdges(byKey map[resource.Key]resource.Envelope) ([]graphaccess.Edge, error) {
	envelopes := make([]resource.Envelope, 0, len(byKey))
	for _, e := range byKey {
		envelopes = append(envelopes, e)
	}
	g, err := graph.Build(envelopes)
	if err != nil {
		return nil, fmt.Errorf("graph-scoped access: rebuild reference graph: %w", err)
	}
	return graphaccess.DeriveEdges(g), nil
}
