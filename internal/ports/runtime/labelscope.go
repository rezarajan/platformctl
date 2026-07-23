package runtime

// LabelScopedAccessQuery is an optional ContainerRuntime capability
// (docs/planning/08 K4, docs/adr/033 decision 4) answering "is the
// LabelScopedAccess gate currently enabled for this request" — the
// mediation-layer counterpart to graphaccess's own labelScopedAccessEnabled
// bool (internal/application/engine/domainruntime.go), reached the exact
// same way runtime.AddressQualifier is (see that interface's own doc
// comment, docs/planning/08 H9): a mediation adapter's Reconcile only ever
// receives a reconciler.Request, whose Runtime field is the one channel an
// engine-resolved, per-request fact (like a feature gate's current state)
// can reach an adapter through without a new bespoke Request field (frozen
// by internal/archtest's request_facts_frozen_test.go) or the adapter
// importing internal/application/registry, which CLAUDE.md's layering
// invariant forbids.
//
// Only internal/application/engine's domainRuntime decorator implements
// this — no real ContainerRuntime adapter (Docker/Kubernetes/fake) needs
// to, and none does: the gate's state is engine bookkeeping (which feature
// gates the Registry reports enabled for this call), not a runtime-
// technology concern. A caller that type-asserts req.Runtime against this
// interface and finds it missing (a unit test wiring a bare fake, for
// instance) should treat the gate as disabled — the same "capability
// absent means the pre-feature behavior" default every other optional
// capability in this package already holds.
//
// Only internal/adapters/providers/openziti calls this today: the
// mediation port's label-derived role-attribute/attribute-scoped
// service-policy compilation (docs/planning/08 K4) must ride the SAME
// LabelScopedAccess gate docs/adr/033's K2/K3 waves already registered —
// "gate off = today's name-only policies, byte-identical, pinned" is this
// task's own accept bar, and this is the mechanism that lets the adapter
// see the gate's state at all without naming the registry or the gate's
// own string key anywhere outside internal/application/engine.
type LabelScopedAccessQuery interface {
	// LabelScopedAccessEnabled reports the LabelScopedAccess feature gate's
	// current state for this request. A pure, side-effect-free query —
	// mirrors AddressQualifier.QualifyTargetAddress's own "never refuses"
	// posture, just narrower (no context, no error: reading an
	// already-resolved bool cannot fail).
	LabelScopedAccessEnabled() bool
}
