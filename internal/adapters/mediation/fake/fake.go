// Package fake is an in-memory, honest implementation of the mediation
// ports (mediation.MediationProvider, mediation.AddressResolver,
// mediation.FabricProvisioner) — the fast-tier test double the mediation
// conformance suite proves against, and the double L3/L4 engine tests
// drive instead of standing up a live OpenZiti controller.
//
// "Honest" in the ADR 028 sense: it upholds every contract the port
// documents — Ensure*-idempotency (a second identical call makes zero
// additional state mutations, reported via Mutations()), "already gone is
// success" on revoke/destroy, deterministic SPIFFE-aligned URIs from the
// naming authority, and the fingerprints-only discipline (never any key
// material). It is NOT a mock: it stores real state and answers Observed*
// from it, so a test that realizes an edge and observes it back exercises
// the same round-trip the OpenZiti adapter does. A deliberately-broken
// fake (see the conformance suite's own negative self-proof) is the only
// thing that fails the suite.
package fake

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"

	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/mediation"
)

// Fake is the in-memory mediation control plane. The zero value is not
// usable; construct with New.
type Fake struct {
	mu         sync.Mutex
	identities map[string]mediation.WorkloadIdentity // keyed by URI
	edges      map[string]mediation.Edge             // keyed by edgeKey(from,to)
	fabricUp   bool
	mutations  int
}

// New returns an empty fake mediation control plane.
func New() *Fake {
	return &Fake{
		identities: map[string]mediation.WorkloadIdentity{},
		edges:      map[string]mediation.Edge{},
	}
}

// Mutations reports how many real state changes have occurred — the same
// idempotency-evidence seam the fake ContainerRuntime exposes. The
// conformance suite asserts a second identical call adds zero.
func (f *Fake) Mutations() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mutations
}

func edgeKey(from, to string) string { return from + "\x00" + to }

// fingerprint is a deterministic public-key-fingerprint stand-in derived
// from the URI — stable across calls (idempotency) and carrying no key
// material (there is no key), exactly the property the real fingerprint
// must have from a caller's perspective.
func fingerprint(uri string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(uri))
	return fmt.Sprintf("SHA256:fake:%016x", h.Sum64())
}

// MintIdentity implements mediation.MediationProvider.
func (f *Fake) MintIdentity(_ context.Context, node resource.Envelope) (mediation.WorkloadIdentity, error) {
	uri := naming.WorkloadIdentityURI(node)
	id := mediation.WorkloadIdentity{URI: uri, Fingerprint: fingerprint(uri), Labels: node.Metadata.Labels}
	f.mu.Lock()
	defer f.mu.Unlock()
	if existing, ok := f.identities[uri]; ok && existing.Fingerprint == id.Fingerprint && labelsEqual(existing.Labels, id.Labels) {
		return existing, nil // already converged — zero mutations
	}
	f.identities[uri] = id
	f.mutations++
	return id, nil
}

// RealizeEdge implements mediation.MediationProvider.
func (f *Fake) RealizeEdge(_ context.Context, edge mediation.Edge) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.identities[edge.From.URI]; !ok {
		return fmt.Errorf("fake mediation: RealizeEdge: From identity %q not minted", edge.From.URI)
	}
	if _, ok := f.identities[edge.To.URI]; !ok {
		return fmt.Errorf("fake mediation: RealizeEdge: To identity %q not minted", edge.To.URI)
	}
	k := edgeKey(edge.From.URI, edge.To.URI)
	if existing, ok := f.edges[k]; ok && existing.Authorized == edge.Authorized {
		return nil // unchanged — zero mutations
	}
	f.edges[k] = edge
	f.mutations++
	return nil
}

// RevokeEdge implements mediation.MediationProvider — "already gone is
// success."
func (f *Fake) RevokeEdge(_ context.Context, edge mediation.Edge) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := edgeKey(edge.From.URI, edge.To.URI)
	if _, ok := f.edges[k]; !ok {
		return nil
	}
	delete(f.edges, k)
	f.mutations++
	return nil
}

// RevokeIdentity implements mediation.MediationProvider: removes the
// identity and every edge referencing it (no dangling policy).
func (f *Fake) RevokeIdentity(_ context.Context, identity mediation.WorkloadIdentity) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.identities[identity.URI]; !ok {
		return nil // already gone
	}
	delete(f.identities, identity.URI)
	for k, e := range f.edges {
		if e.From.URI == identity.URI || e.To.URI == identity.URI {
			delete(f.edges, k)
		}
	}
	f.mutations++
	return nil
}

// ObservedEdges implements mediation.MediationProvider — deterministic order.
func (f *Fake) ObservedEdges(_ context.Context) ([]mediation.Edge, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]mediation.Edge, 0, len(f.edges))
	for _, e := range f.edges {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool {
		return edgeKey(out[i].From.URI, out[i].To.URI) < edgeKey(out[j].From.URI, out[j].To.URI)
	})
	return out, nil
}

// ObservedIdentities implements mediation.MediationProvider — deterministic order.
func (f *Fake) ObservedIdentities(_ context.Context) ([]mediation.WorkloadIdentity, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]mediation.WorkloadIdentity, 0, len(f.identities))
	for _, id := range f.identities {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].URI < out[j].URI })
	return out, nil
}

// DialAddress implements mediation.AddressResolver: a deterministic
// mediated address for the edge, standing up whatever the edge needs
// idempotently. The fake needs nothing beyond recording the edge exists.
func (f *Fake) DialAddress(_ context.Context, edge mediation.AddressEdge) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.fabricUp {
		return "", fmt.Errorf("fake mediation: DialAddress before EnsureFabric")
	}
	// Deterministic, edge-keyed; same edge → same address, always.
	return fmt.Sprintf("mediated://%s.%s", edge.To.Name, edge.To.Namespace), nil
}

// EnsureFabric implements mediation.FabricProvisioner.
func (f *Fake) EnsureFabric(_ context.Context, _ mediation.FabricRequest) (mediation.FabricState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.fabricUp {
		f.fabricUp = true
		f.mutations++
	}
	return mediation.FabricState{ControlPlaneAddress: "fake-control-plane:1280"}, nil
}

// DestroyFabric implements mediation.FabricProvisioner — "already gone is
// success."
func (f *Fake) DestroyFabric(_ context.Context, _ mediation.FabricRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.fabricUp {
		return nil
	}
	f.fabricUp = false
	f.mutations++
	return nil
}

func labelsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
