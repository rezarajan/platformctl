package engine

import (
	"context"

	"github.com/rezarajan/platformctl/internal/application/plan"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/state"
)

// reconcileHook implements a kindHandler's "make it so" behavior, replacing
// reconcileOne's default resolveRequest+Provider.Reconcile flow.
type reconcileHook func(e *Engine, ctx context.Context, entry plan.Entry, env resource.Envelope, byKey map[resource.Key]resource.Envelope, deps DependencyGraph, st *state.State) error

// probeHook implements a kindHandler's live-status check, replacing
// probeOneAgainstState's default resolveRequest+Provider.Probe flow.
type probeHook func(e *Engine, ctx context.Context, env resource.Envelope, byKey map[resource.Key]resource.Envelope, rs state.ResourceState, fullState *state.State) status.Status

// deleteHook implements a kindHandler's teardown, replacing the default
// resolveRequest+Provider.Destroy flow shared by applyDeleteOne and Destroy.
type deleteHook func(e *Engine, ctx context.Context, env resource.Envelope, key resource.Key, st *state.State) error

// kindHandler is one row of the engine's kind/lifecycle dispatch table: a
// match predicate plus the per-action hooks that stand in for the default
// provider-driven flow in reconcileOne, probeOneAgainstState, applyDeleteOne,
// and Destroy. A nil hook means "no override for this action" — the caller
// keeps running its own default logic. Handlers are consulted only through
// lookupKindHandler; no method re-derives a kind/lifecycle condition inline
// (G2: docs/planning/08-production-readiness-plan.md §7.6).
type kindHandler struct {
	// name identifies the case for diagnostics/tests; not consulted by
	// lookupKindHandler itself.
	name string
	// match reports whether env is this special case.
	match func(env resource.Envelope) bool

	reconcile reconcileHook
	probe     probeHook
	del       deleteHook
}

// kindHandlers is the engine's single kind/lifecycle dispatch table.
//
// Today's three built-in cases are mutually exclusive by construction:
// SecretReference's schema forbids a "spec.external" field (additionalProperties:
// false in schemas/v1alpha1/secretreference.json), so Kind == "SecretReference"
// and isExternal(env) can never both hold for a validated envelope. That
// means the table's own iteration order never changes which handler a given
// resource matches — it is written below in reconcileOne's historical
// if-chain order (SecretReference, then external-no-provider, then
// external-with-provider) for readability, not because order is
// load-bearing. See this package's introducing commit for the per-method
// order this table replaces, including the one place two methods disagreed
// on it (applyDeleteOne/Destroy check the External-lifecycle case before
// SecretReference; reconcileOne/probeOneAgainstState check SecretReference
// first) — harmless given mutual exclusivity, preserved here as a single
// table rather than resolved, since resolving it would be a behavior change
// outside this refactor's scope.
var kindHandlers = []*kindHandler{
	{
		name:  "SecretReference",
		match: func(env resource.Envelope) bool { return env.Kind == "SecretReference" },
		reconcile: func(e *Engine, ctx context.Context, entry plan.Entry, env resource.Envelope, _ map[resource.Key]resource.Envelope, deps DependencyGraph, st *state.State) error {
			return e.reconcileSecretReference(ctx, entry, env, deps, st)
		},
		probe: func(e *Engine, ctx context.Context, env resource.Envelope, _ map[resource.Key]resource.Envelope, rs state.ResourceState, _ *state.State) status.Status {
			return e.secretReferenceStatus(ctx, env, rs.SecretHash)
		},
		del: deleteStateOnly,
	},
	{
		name:  "ExternalNoProvider",
		match: isExternalNoProvider,
		reconcile: func(e *Engine, ctx context.Context, entry plan.Entry, env resource.Envelope, byKey map[resource.Key]resource.Envelope, deps DependencyGraph, st *state.State) error {
			return e.reconcileExternal(ctx, entry, env, byKey, deps, st)
		},
		probe: func(e *Engine, ctx context.Context, env resource.Envelope, byKey map[resource.Key]resource.Envelope, _ state.ResourceState, _ *state.State) status.Status {
			// externalDatabaseTLSGate (docs/planning/08 I2): mirrors
			// probeOneAgainstState's own ReasonProbeFailed conversion of a
			// resolveRequest gate error — this handler's probe never goes
			// through resolveRequest, so it degrades the same way here.
			if msg, ok := e.externalDatabaseTLSGate(env); !ok {
				now := e.Clock.Now()
				gs := status.Status{}
				gs.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonProbeFailed, Message: msg}, now)
				gs.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonProbeFailed, Message: msg}, now)
				return gs
			}
			return e.externalConnectionStatus(ctx, env, byKey)
		},
		del: deleteStateOnly,
	},
	{
		name:  "ExternalWithProvider",
		match: func(env resource.Envelope) bool { return isExternal(env) && !isExternalNoProvider(env) },
		reconcile: func(e *Engine, ctx context.Context, entry plan.Entry, env resource.Envelope, byKey map[resource.Key]resource.Envelope, deps DependencyGraph, st *state.State) error {
			return e.reconcileExternalWithProvider(ctx, entry, env, byKey, deps, st)
		},
		// probe and del are nil: an External resource with a providerRef
		// still has real infrastructure behind it (configured via
		// ConfigureExternal), so it is probed/destroyed through the same
		// provider-driven default flow as a Managed resource.
	},
}

// lookupKindHandler returns the kindHandler whose match predicate applies to
// env, or nil if none does — the common case, since most resources go
// through the default provider-driven flow. At most one handler matches a
// given env today (see kindHandlers' doc comment); were that ever to change,
// the first match in table order wins.
func lookupKindHandler(env resource.Envelope) *kindHandler {
	for _, h := range kindHandlers {
		if h.match(env) {
			return h
		}
	}
	return nil
}

// deleteStateOnly is the deleteHook shared by SecretReference and
// no-provider-External: nothing in the platform realizes either, so
// "deleting" one is exactly forgetting its state entry. It does not touch
// e.stateMu: callers hold whatever locking discipline they already use
// around their own default-path delete (applyDeleteOne locks; Destroy runs
// strictly sequentially and does not).
func deleteStateOnly(e *Engine, ctx context.Context, _ resource.Envelope, key resource.Key, st *state.State) error {
	delete(st.Resources, key)
	return e.StateStore.Save(ctx, *st)
}
