package runtime

import "context"

// Isolation enforcement states (docs/adr/027-enforcement-layering.md's
// claims table, docs/planning/08 H8): the tri-state IsolationObserver
// answers with. Enforced/NotEnforced are both *observed* facts — a live
// probe concluded one way or the other; Unknown means no conclusive
// observation was possible this run (Reason always explains why) and must
// never be treated as either of the other two — ADR 027: "an unverifiable
// claim is treated as false, not as hoped-true."
const (
	// IsolationEnforced: Layer 2 (network segmentation — Kubernetes
	// NetworkPolicy, Docker network membership) was observed to actually
	// deny what it declares, or is enforced by construction (Docker: a
	// container's network membership IS the mechanism, nothing to probe).
	IsolationEnforced = "enforced"
	// IsolationNotEnforced: a live probe caught the fabric NOT enforcing
	// what was compiled (e.g. a Kubernetes CNI that silently ignores
	// NetworkPolicy) — never a hard failure (docs/adr/027: Layer 1,
	// identity-attested mediated connections, is the authoritative
	// guarantee; Layer 2 is best-effort defense-in-depth, honestly
	// reported).
	IsolationNotEnforced = "not-enforced"
	// IsolationUnknown: no conclusive observation was possible (too little
	// to probe against, a bounded wait expired, the runtime doesn't
	// implement this capability at all). Reason always names the specific
	// cause.
	IsolationUnknown = "unknown"
)

// IsolationStatus is ObserveIsolationEnforcement's result.
type IsolationStatus struct {
	// State is one of the Isolation* constants above.
	State string
	// Reason is a human-readable detail — always set for NotEnforced/
	// Unknown; optional (but usually populated) for Enforced.
	Reason string
}

// IsolationObserver is an optional ContainerRuntime capability
// (docs/adr/027-enforcement-layering.md, docs/planning/08 H8) answering
// "does this runtime's Layer 2 (network segmentation) actually enforce
// what was compiled, right now?" — never assumed from the mere existence
// of the compiled objects (a NetworkPolicy can sit inert on a CNI that
// doesn't implement it; docs/planning/08 B7's own caveat, closed live by
// TestNetworkPolicyEnforcementIsLive). This capability productizes that
// probe into something any command can call.
//
// A runtime that doesn't implement this interface at all (obtained
// directly, bypassing application/registry) has nothing to say about
// isolation; callers going through the registry always get an answer —
// see application/registry's haGuardRuntime, which promotes this
// capability the same way it promotes IngressCapableRuntime/
// MemberSetRuntime (docs/adr/018's addendum, the "registry-promotion
// gotcha": embedding the runtime.ContainerRuntime *interface* only
// promotes that interface's own declared method set, so without an
// explicit delegating method the capability silently vanishes for every
// runtime obtained through the registry, including one that genuinely
// implements it) and reports IsolationUnknown, never an error, when the
// underlying adapter doesn't implement it.
//
// Contract: never returns a non-nil error for an ordinary observation
// failure — every failure mode (too few resources to cross-check, a
// bounded wait that expired, an inconclusive dial) degrades to
// IsolationStatus{State: IsolationUnknown, Reason: ...} instead of
// aborting the caller. ADR 027's "an unverifiable claim is treated as
// false, not as hoped-true" cuts both ways: the honesty probe itself must
// never be the reason an apply/status/drift command fails outright. A
// non-nil error is reserved for something that isn't really about
// isolation at all (e.g. the caller's own ctx was already done).
//
// Docker: enforced by construction (network membership IS the isolation
// mechanism — a container only reaches what it's attached to; nothing to
// probe). Kubernetes: a live, bounded canary probe against the cluster's
// actual CNI (internal/adapters/runtime/kubernetes/isolation.go) —
// productizing TestNetworkPolicyEnforcementIsLive's mechanism.
type IsolationObserver interface {
	ObserveIsolationEnforcement(ctx context.Context) (IsolationStatus, error)
}
