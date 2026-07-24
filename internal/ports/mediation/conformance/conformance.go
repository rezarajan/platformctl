// Package conformance is the shared contract test suite every
// mediation-port implementation must pass — the fake (fast tier) and the
// OpenZiti adapter (deep tier), the same discipline
// internal/ports/runtime/conformance holds for ContainerRuntime and
// docs/planning/02 §9 mandates for every port. It is what makes
// mediation.MediationProvider a CONTRACT rather than one implementation's
// shape: a second adapter (SPIRE, Consul) is verified against exactly this,
// not against OpenZiti's behavior.
//
// Every assertion here derives from a contract the port's own doc comments
// state: MintIdentity/RealizeEdge idempotency, "already gone is success" on
// revoke/destroy, no-dangling-policy on RevokeIdentity, Observed* round-trip,
// and — for optional capabilities — FabricProvisioner idempotency and
// AddressResolver determinism. docs/planning/08 L2a.
package conformance

import (
	"context"
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/mediation"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// MutationCounter is optionally implemented by a subject that can report how
// many real control-plane mutations occurred — the fake does. When present,
// the suite asserts a second identical call adds zero (the strongest
// idempotency evidence). When absent (a live adapter that cannot cheaply
// count), the suite falls back to Observed*-set stability, which every
// MediationProvider supports.
type MutationCounter interface {
	Mutations() int
}

// Subject is the implementation under test plus the optional capabilities it
// also satisfies. Provider is required; Fabric/Resolver are exercised only
// when non-nil (an adapter that implements them passes them, one that does
// not is simply not tested for what it does not claim — the same
// type-assert-the-capability discipline the ports themselves use).
type Subject struct {
	Provider mediation.MediationProvider
	Fabric   mediation.FabricProvisioner // optional
	Resolver mediation.AddressResolver   // optional
	// Runtime is handed to FabricProvisioner calls; may be nil when Fabric
	// is nil. A fake ContainerRuntime is fine — the suite never asserts on
	// what the fabric does to it, only on EnsureFabric/DestroyFabric
	// idempotency and error contracts.
	Runtime runtime.ContainerRuntime
}

func node(name, namespace string, labels map[string]string) resource.Envelope {
	return resource.Envelope{
		GroupVersionKind: resource.GroupVersionKind{APIVersion: "datascape.io/v1alpha1", Kind: "Provider"},
		Metadata:         resource.Metadata{Name: name, Namespace: namespace, Labels: labels},
		Spec:             map[string]any{"type": "noop"},
	}
}

// Run drives Subject through the full mediation contract. Fatal on any
// contract violation, so a broken adapter (or a broken fake) fails loudly.
func Run(t *testing.T, s Subject) {
	t.Helper()
	ctx := context.Background()
	p := s.Provider

	mc, hasCounter := p.(MutationCounter)
	mutations := func() int {
		if hasCounter {
			return mc.Mutations()
		}
		return -1
	}

	// --- MintIdentity: deterministic + idempotent -----------------------
	a := node("conf-a", "default", nil)
	b := node("conf-b", "default", nil)

	// Observations are asserted by SET-DELTA against a baseline measured
	// BEFORE any of this suite's own mints, never by cross-referencing the
	// caller's original URIs: ObservedEdges/ObservedIdentities are drift
	// primitives an adapter may answer in its OWN deterministic encoding
	// (OpenZiti's role-attribute names are a documented lossy encoding of
	// the SPIFFE URI — the engine diffs desired-vs-observed by that
	// encoding, ADR H6, not by the original URI). Requiring exact-URI
	// round-trip would fail a legitimate adapter; requiring the set to grow
	// by one per mint/realize and shrink by one per revoke tests the actual
	// contract, tolerant of pre-existing state on a shared live controller.
	edgesBaseline := observedEdgeCount(t, ctx, p)
	idBaseline := observedIdentityCount(t, ctx, p)

	idA, err := p.MintIdentity(ctx, a)
	if err != nil {
		t.Fatalf("MintIdentity(a): %v", err)
	}
	// URI is the deterministic, always-present half (the SPIFFE-aligned
	// subject). Fingerprint is a CERTIFICATE fingerprint — the port's own
	// doc states a zero value means "not yet minted", and an identity that
	// is minted but not yet ENROLLED (no tunneler has consumed its JWT)
	// legitimately has none yet: the OpenZiti adapter returns "" here,
	// which is correct, not a defect. So the contract this suite enforces
	// is URI-deterministic + Fingerprint-STABLE across calls, never
	// Fingerprint-non-empty (that would fit only an eager fake).
	if idA.URI == "" {
		t.Fatalf("MintIdentity(a) returned no URI: %+v", idA)
	}
	idA2, err := p.MintIdentity(ctx, a)
	if err != nil {
		t.Fatalf("MintIdentity(a) second call: %v", err)
	}
	if idA2.URI != idA.URI || idA2.Fingerprint != idA.Fingerprint {
		t.Errorf("MintIdentity not deterministic: %+v vs %+v", idA, idA2)
	}
	if hasCounter {
		before := mutations()
		if _, err := p.MintIdentity(ctx, a); err != nil {
			t.Fatalf("MintIdentity(a) third call: %v", err)
		}
		if after := mutations(); after != before {
			t.Errorf("MintIdentity not idempotent: mutation count %d -> %d on unchanged re-mint", before, after)
		}
	}
	idB, err := p.MintIdentity(ctx, b)
	if err != nil {
		t.Fatalf("MintIdentity(b): %v", err)
	}
	if got := observedIdentityCount(t, ctx, p); got != idBaseline+2 {
		t.Errorf("after minting a+b, ObservedIdentities delta = %d, want +2 over the pre-mint baseline", got-idBaseline)
	}

	// --- RealizeEdge: idempotent, grows the observed edge set by one ----
	edge := mediation.Edge{From: idA, To: idB, Authorized: mediation.DialBind{Dial: true}}
	if err := p.RealizeEdge(ctx, edge); err != nil {
		t.Fatalf("RealizeEdge(a->b): %v", err)
	}
	if hasCounter {
		before := mutations()
		if err := p.RealizeEdge(ctx, edge); err != nil {
			t.Fatalf("RealizeEdge second call: %v", err)
		}
		if after := mutations(); after != before {
			t.Errorf("RealizeEdge not idempotent: mutation count %d -> %d on unchanged re-realize", before, after)
		}
	}
	if got := observedEdgeCount(t, ctx, p); got != edgesBaseline+1 {
		t.Errorf("after RealizeEdge(a->b), ObservedEdges delta = %d, want +1 — the drift-detection round-trip is broken", got-edgesBaseline)
	}

	// --- RevokeEdge: removes exactly it, "already gone is success" ------
	if err := p.RevokeEdge(ctx, edge); err != nil {
		t.Fatalf("RevokeEdge(a->b): %v", err)
	}
	if got := observedEdgeCount(t, ctx, p); got != edgesBaseline {
		t.Errorf("after RevokeEdge(a->b), ObservedEdges delta = %d, want 0 (back to baseline)", got-edgesBaseline)
	}
	if err := p.RevokeEdge(ctx, edge); err != nil {
		t.Errorf("RevokeEdge of an already-absent edge must be a no-op, got: %v", err)
	}

	// --- RevokeIdentity: cascades its edges, "already gone is success" --
	// Re-realize so RevokeIdentity has an edge to cascade-remove.
	if err := p.RealizeEdge(ctx, edge); err != nil {
		t.Fatalf("RealizeEdge(a->b) before RevokeIdentity: %v", err)
	}
	if err := p.RevokeIdentity(ctx, idA); err != nil {
		t.Fatalf("RevokeIdentity(a): %v", err)
	}
	if got := observedEdgeCount(t, ctx, p); got != edgesBaseline {
		t.Errorf("RevokeIdentity(a) left a dangling edge (ObservedEdges delta %d, want 0) — the posture-decay the port forbids", got-edgesBaseline)
	}
	if got := observedIdentityCount(t, ctx, p); got != idBaseline+1 {
		t.Errorf("after RevokeIdentity(a), ObservedIdentities delta = %d, want +1 (only b remains of the two minted)", got-idBaseline)
	}
	if err := p.RevokeIdentity(ctx, idA); err != nil {
		t.Errorf("RevokeIdentity of an already-absent identity must be a no-op, got: %v", err)
	}
	// Clean up b.
	if err := p.RevokeIdentity(ctx, idB); err != nil {
		t.Fatalf("RevokeIdentity(b) cleanup: %v", err)
	}
	if got := observedIdentityCount(t, ctx, p); got != idBaseline {
		t.Errorf("after cleanup, ObservedIdentities delta = %d, want 0 (back to baseline)", got-idBaseline)
	}

	// --- FabricProvisioner (optional): idempotent stand-up + teardown ---
	if s.Fabric != nil {
		req := mediation.FabricRequest{Runtime: s.Runtime, Labels: map[string]string{runtime.LabelManagedBy: runtime.ManagedByValue}}
		fs, err := s.Fabric.EnsureFabric(ctx, req)
		if err != nil {
			t.Fatalf("EnsureFabric: %v", err)
		}
		if fs.ControlPlaneAddress == "" {
			t.Errorf("EnsureFabric returned no ControlPlaneAddress")
		}
		if hasCounter {
			before := mutations()
			if _, err := s.Fabric.EnsureFabric(ctx, req); err != nil {
				t.Fatalf("EnsureFabric second call: %v", err)
			}
			if after := mutations(); after != before {
				t.Errorf("EnsureFabric not idempotent: mutation count %d -> %d", before, after)
			}
		}
		if err := s.Fabric.DestroyFabric(ctx, req); err != nil {
			t.Fatalf("DestroyFabric: %v", err)
		}
		if err := s.Fabric.DestroyFabric(ctx, req); err != nil {
			t.Errorf("DestroyFabric of an already-absent fabric must be a no-op, got: %v", err)
		}
	}

	// --- AddressResolver (optional): deterministic dial address ---------
	if s.Resolver != nil {
		if s.Fabric != nil {
			if _, err := s.Fabric.EnsureFabric(ctx, mediation.FabricRequest{Runtime: s.Runtime, Labels: map[string]string{runtime.LabelManagedBy: runtime.ManagedByValue}}); err != nil {
				t.Fatalf("EnsureFabric before DialAddress: %v", err)
			}
		}
		ae := mediation.AddressEdge{
			From: resource.Key{Namespace: "default", Kind: "Provider", Name: "conf-a"},
			To:   resource.Key{Namespace: "default", Kind: "Provider", Name: "conf-b"},
		}
		addr1, err := s.Resolver.DialAddress(ctx, ae)
		if err != nil {
			t.Fatalf("DialAddress: %v", err)
		}
		if addr1 == "" {
			t.Errorf("DialAddress returned empty address")
		}
		addr2, err := s.Resolver.DialAddress(ctx, ae)
		if err != nil {
			t.Fatalf("DialAddress second call: %v", err)
		}
		if addr2 != addr1 {
			t.Errorf("DialAddress not deterministic for the same edge: %q vs %q", addr1, addr2)
		}
	}
}

func observedEdgeCount(t *testing.T, ctx context.Context, p mediation.MediationProvider) int {
	t.Helper()
	edges, err := p.ObservedEdges(ctx)
	if err != nil {
		t.Fatalf("ObservedEdges: %v", err)
	}
	return len(edges)
}

func observedIdentityCount(t *testing.T, ctx context.Context, p mediation.MediationProvider) int {
	t.Helper()
	ids, err := p.ObservedIdentities(ctx)
	if err != nil {
		t.Fatalf("ObservedIdentities: %v", err)
	}
	return len(ids)
}
