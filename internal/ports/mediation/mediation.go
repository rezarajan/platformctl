// Package mediation defines the MediationProvider port: docs/adr/027's
// Layer 1, "the authoritative zero-trust enforcement plane" — mints
// per-workload cryptographic identity from the naming authority, realizes
// per-edge dial/bind authorization compiled from the declared resource
// graph (docs/adr/026), and revokes cleanly on teardown.
//
// This port is deliberately technology-silent: nothing in this file, or in
// any package that imports it outside internal/adapters/providers/openziti,
// may name a specific mediation technology. OpenZiti is the first (and, at
// authorship time, only) adapter — docs/adr/022's research note — but the
// whole point of a port (mirroring runtime.ContainerRuntime's
// Docker/Kubernetes abstraction, docs/planning/02 §4.1) is that a future
// SPIRE-native or Consul-intentions adapter implements the identical
// interface without this package, or any caller of it, changing. An
// archtest (internal/archtest/mediation_layering_test.go) pins the "no
// ziti/openziti import outside the adapter directory" invariant mechanically
// so this stays true by construction, not by convention.
//
// # Identity discipline (docs/adr/013)
//
// Every type and method in this file carries identity by SPIFFE-aligned URI
// (internal/domain/naming.WorkloadIdentityURI) and public-key fingerprint
// only. Private key material never crosses this boundary — it is minted,
// held, and rotated entirely inside the realizing adapter, never returned
// to a caller, never placed in reconciler.Request, status.Status,
// providerState, or a log line. A future adapter that cannot uphold this
// (e.g. one whose control plane hands back a private key on identity
// creation) must discard it immediately after deriving the fingerprint,
// the same discipline docs/adr/023's wireguard adapter holds for its own
// file-mounted key.
package mediation

import (
	"context"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

// WorkloadIdentity is a minted cryptographic identity for one resource-graph
// node. URI is the SPIFFE-aligned subject the identity attests
// (internal/domain/naming.WorkloadIdentityURI's output, unchanged) — the
// stable, deterministic, re-derivable half. Fingerprint is a public-key/
// certificate fingerprint (adapter-defined encoding, e.g. hex SHA-256),
// safe to persist in state/status/logs and to display to an operator for
// audit — the ONLY key-derived value this type, or any type in this
// package, ever carries. A zero-value Fingerprint means "not yet minted."
type WorkloadIdentity struct {
	URI         string
	Fingerprint string
	// Labels carries the node's (MintIdentity) or edge endpoint's
	// (RealizeEdge/RevokeEdge's From/To) metadata.labels — docs/planning/08
	// K4, docs/adr/033 decision 4: "the mediation port carries endpoint
	// labels" so an adapter can additionally compile attribute-based
	// authorization (identity role attributes, attribute-scoped
	// service-policies) alongside the identity-exact authorization this
	// port has always required, keeping the admission plane (ADR 033's
	// matchEdge.selector policy) and the enforcement plane (this port)
	// checking the SAME facts (ADR 027 Layer 1's "the receiving side
	// refuses any peer not presenting the identity the graph authorizes" —
	// now backed by the same label facts the graph's own selector rules
	// evaluate). nil/empty (the common case: an endpoint with no labels,
	// or every caller before K4) is not an error — every adapter method
	// already handles a zero-value WorkloadIdentity, so this field is
	// purely additive. Never used as identity material itself (it is not
	// part of the SPIFFE-aligned subject or the fingerprint), only as an
	// input an adapter may derive attributes from.
	Labels map[string]string
}

// DialBind is which direction(s) of an edge an identity is authorized for
// — the distinction docs/adr/022's "Research findings" draws from the AWS
// VPC Lattice lesson: raw-TCP mediation authorizes connection
// *establishment* (who may dial, who may bind/listen), never per-request
// content. At least one of Dial/Bind is true for any Edge a caller should
// realize; both false is a no-op RealizeEdge callers should not construct
// (revoke the edge instead).
type DialBind struct {
	// Dial: From may initiate a connection to the service the edge
	// authorizes (the CDC Binding's dial side, docs/planning/08 H6's
	// accept scenario).
	Dial bool
	// Bind: From may listen as the service the edge authorizes (the
	// source database's bind side — the "dark service" posture,
	// docs/adr/022: no listening port on any shared network, reachable
	// only through the mediation plane).
	Bind bool
}

// Edge is one compiled, per-edge authorization between two workload
// identities — the mediated subset of the docs/adr/026 reference graph
// (internal/application/graphaccess.MediatedSubset), never hand-authored.
// From and To must already be minted (via MintIdentity) before RealizeEdge
// is called with an Edge naming them.
type Edge struct {
	From       WorkloadIdentity
	To         WorkloadIdentity
	Authorized DialBind
}

// MediationProvider is the capability seam docs/adr/027 promotes from "a
// mesh feature" (H6's original framing) to THE zero-trust guarantee: every
// method is Ensure*-idempotent by contract, the same discipline
// runtime.ContainerRuntime's Ensure* methods hold (docs/planning/02 §4.1)
// — calling MintIdentity or RealizeEdge twice with identical inputs must
// make zero additional control-plane writes once observed state already
// matches desired state. A conformance suite proves this the same way
// runtime adapters are proven (docs/planning/02 §9); see
// internal/adapters/providers/openziti's own tests for the first
// implementation's proof.
type MediationProvider interface {
	// MintIdentity ensures a cryptographic identity exists for node,
	// deriving its SPIFFE-aligned subject via
	// internal/domain/naming.WorkloadIdentityURI. Idempotent: a second
	// call for the same node returns the same URI/Fingerprint and makes
	// no additional control-plane writes once the identity already
	// exists with the expected subject and (docs/planning/08 K4) any
	// label-derived attributes an adapter compiles from node.Metadata.
	// Labels already converge. The returned WorkloadIdentity's Labels
	// field carries node.Metadata.Labels straight through (see that
	// field's own doc comment).
	MintIdentity(ctx context.Context, node resource.Envelope) (WorkloadIdentity, error)

	// RealizeEdge compiles and applies dial/bind authorization for one
	// edge — both identities named by edge must already be minted.
	// Idempotent: re-applying an unchanged Edge makes no additional
	// control-plane writes; applying a changed Authorized value for an
	// existing (From, To) pair updates it in place (no orphaned
	// stale-permission policy left behind).
	RealizeEdge(ctx context.Context, edge Edge) error

	// RevokeEdge removes exactly the authorization named by edge,
	// leaving both identities and any other edge referencing them
	// intact. Idempotent: revoking an edge that is already absent is a
	// no-op, never an error (mirrors runtime.ContainerRuntime.Remove's
	// "already gone is success" contract).
	RevokeEdge(ctx context.Context, edge Edge) error

	// RevokeIdentity removes identity and every edge referencing it —
	// teardown must never leave a dangling policy naming a deleted
	// identity (the posture-decay docs/planning/09 §4 warns against).
	// Idempotent: revoking an identity that is already absent is a
	// no-op, never an error.
	RevokeIdentity(ctx context.Context, identity WorkloadIdentity) error

	// ObservedEdges reports the mediation plane's actual, currently
	// enforced edge set — the drift-detection primitive. The engine
	// diffs this against the desired Edge set from the same graph
	// derivation (internal/application/graphaccess) and heals any
	// divergence: docs/planning/08 H6's accept criterion ("drift on
	// out-of-band Ziti policy edits detected and healed") is this method
	// plus the engine's existing drift-diff-and-heal loop, not new drift
	// machinery.
	ObservedEdges(ctx context.Context) ([]Edge, error)

	// ObservedIdentities is the identity-side half of the same drift
	// check: an identity minted out-of-band, or one this provider should
	// own but no longer finds, is drift the same way an unlabeled or
	// missing container is (docs/planning/08 A8).
	ObservedIdentities(ctx context.Context) ([]WorkloadIdentity, error)
}
