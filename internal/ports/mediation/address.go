package mediation

import (
	"context"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

// AddressEdge names one ADR-026 declared graph edge purely by resource.Key
// — docs/planning/08 L1's minimal port extension. From is the consumer
// resource whose manifest reference forms the edge; To is the resource.Key
// under which the target's address is published/resolved today (e.g. the
// realizing Provider a Facts.Endpoint lookup is keyed on, or the broker
// Provider a graph-resolved wrapper like KafkaBootstrapServers targets).
// Deliberately NOT mediation.Edge (which names two already-minted
// WorkloadIdentity values): the engine's resolution chokepoint (ADR 034
// "why the engine can do this with zero provider changes") needs to ask
// "what address do I dial for this edge" at Request-build time, before —
// and independent of — the heavier MintIdentity/RealizeEdge flow L2/L3
// build on top of this port. A real adapter implementing AddressResolver
// is free to mint identities and realize the edge internally as part of
// answering DialAddress; this type only carries what the question needs.
type AddressEdge struct {
	From resource.Key
	To   resource.Key
}

// AddressResolver is an OPTIONAL capability a MediationProvider may
// implement, additive to the MintIdentity/RealizeEdge/RevokeEdge/
// RevokeIdentity/ObservedEdges/ObservedIdentities contract every adapter
// (openziti) already satisfies — a separate interface, not a new method on
// MediationProvider itself, for exactly the reason
// reconciler.MediationCapableProvider sits beside reconciler.
// ConnectionCapableProvider rather than folding into it (docs/planning/02
// §4.2's capability-interface pattern): this answers a different question
// ("what address do I dial for this edge") than the identity/authorization
// lifecycle MediationProvider's other methods answer, and a caller
// type-asserts for it rather than requiring every MediationProvider to grow
// it. docs/planning/08 L1 proves the engine-owned substitution seam against
// a fake implementing only this interface; the openziti adapter does not
// implement it in this task — wiring the real fabric (controller/router,
// per-workload identity, per-edge dial policy) into a DialAddress
// implementation is L2/L3, which is exactly the point of proving the seam
// provider-independent first (ADR 034's "why the engine can do this with
// zero provider changes").
type AddressResolver interface {
	// DialAddress returns the address a consumer should dial to reach
	// edge.To through the mediation fabric, standing up (or reusing) any
	// identity/authorization state the concrete implementation needs to
	// answer it — idempotent by the same Ensure*-idempotent discipline
	// every MediationProvider method already holds (docs/planning/08 L1's
	// idempotency proof: identical edge, identical answer, no additional
	// control-plane writes once already realized).
	DialAddress(ctx context.Context, edge AddressEdge) (string, error)
}
