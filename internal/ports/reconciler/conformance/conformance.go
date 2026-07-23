// Package conformance is the fast-tier provider lifecycle contract suite
// every reconciler.Provider should be run against (docs/planning/08 E6,
// docs/adr/028-test-tiering.md). ADR 028 re-scopes E6 as "the pyramid's
// missing middle": a contract suite driving any reconciler.Provider through
// Reconcile/Probe/Destroy/idempotency/settledness semantics against a fake
// runtime and fake technology, in milliseconds, t.Parallel() throughout — a
// provider's fast-tier evidence, not e2e. See
// docs/contributing/provider-authoring.md for the full author-facing
// walkthrough; this file is the executable half of that contract.
//
// Mirrors internal/ports/runtime/conformance's shape deliberately: Run
// takes an already-constructed dependency from its caller and imports
// nothing under internal/adapters — the one invariant (CLAUDE.md: ports
// import domain and other ports packages only) applies here exactly as it
// does there. The concrete fake runtime (and any "fake technology" a
// provider's own settledness check dials through — see
// internal/adapters/providers/proxy's exemplar test for the real
// net.Listener trick) is constructed by the calling _test.go file, which
// lives under internal/adapters/providers/... and may import adapters
// freely.
package conformance

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// settleBudget/probeBudget bound how long a single Reconcile/Probe call may
// take against a fake runtime before this suite treats it as evidence of an
// accidental real wait/retry loop leaking into the fast tier (ADR 028:
// "fakes are synchronous" — ports/runtime's own ScaledWait deadlines are
// tens of seconds, so a budget of low seconds cleanly separates "the happy
// path completed immediately" from "something is polling/sleeping").
// Generous enough to tolerate a loaded CI runner or -race overhead; tight
// enough that a genuine regression (a settle-poll that doesn't short-circuit
// on the fake's immediate success) still fails loudly instead of silently
// costing the fast tier its whole reason to exist.
const (
	settleBudget = 2 * time.Second
	probeBudget  = 1 * time.Second
)

// MutationCounter is optionally implemented by a runtime.ContainerRuntime
// that can report how many real state mutations occurred — the fake does.
// Deliberately duplicated from internal/ports/runtime/conformance's
// identical interface rather than imported: the two conformance suites
// exercise different ports and are meant to stay independently readable;
// both are satisfied by the same fake.Runtime.Mutations() method, so no
// adapter needs two implementations.
type MutationCounter interface {
	Mutations() int
}

// CapabilityCheck is one capability-interface invocation a Harness wants
// verified against that interface's own documented error-format contract
// (docs/planning/02-architecture.md §4.2 — e.g. SpecValidator.ValidateSpec,
// StreamReplicationValidator.ValidateStreamReplication). Invoke calls the
// capability method with a deliberately invalid input and returns the
// resulting error; Run fails the check if Invoke returns nil (the whole
// point of a CapabilityCheck is proving a known-invalid input is rejected,
// never silently accepted) or if the error's text is missing any of
// WantSubstrings.
type CapabilityCheck struct {
	// Name is the subtest name (e.g. "ValidateSpec/brokers+kafkaPort").
	Name string
	// Invoke performs the capability call against a deliberately invalid
	// input and returns its error.
	Invoke func() error
	// WantSubstrings are fragments the returned error's text must contain —
	// every declared capability error in this codebase names the concrete
	// fact that failed (a field name, an observed vs. wanted number), never
	// a bare generic message; this is the mechanical check for that
	// discipline.
	WantSubstrings []string
}

// Harness supplies the per-provider pieces this suite needs to drive ANY
// reconciler.Provider through the lifecycle contract — see
// docs/contributing/provider-authoring.md for a full walkthrough and
// internal/adapters/providers/{noop,redpanda,proxy}'s own conformance test
// files for three worked examples spanning a trivial provider, a
// container-lifecycle provider, and a settledness/dial-through provider.
type Harness struct {
	// NewRuntime constructs a fresh, isolated runtime.ContainerRuntime for
	// one subtest — called once per subtest (never shared across subtests,
	// including the two fixtures of the statelessness subtest's own
	// runtime, which per-subtest still gets exactly one NewRuntime() call)
	// so mutation counts and container state never leak between subtests.
	// The returned value must implement MutationCounter, or the idempotency
	// subtest cannot observe mutating calls and Run fails outright.
	NewRuntime func() runtime.ContainerRuntime

	// Provider constructs a fresh instance of the provider under test — the
	// exact `func() reconciler.Provider { return X.New() }` shape
	// application/registry.RegisterProvider itself takes (docs/planning/08
	// F5: a conformant provider holds no cross-call state, so reusing one
	// instance across every subtest would be equally correct — a fresh
	// instance per subtest is what this Harness does anyway, so a provider
	// that accidentally DOES hold cross-call state has nowhere to hide it
	// between subtests).
	Provider func() reconciler.Provider

	// Resource builds fixture i's (0 or 1) reconciler.Request against rt.
	// namePrefix isolates one Run call's resource names (parallel-safe —
	// mirrors internal/ports/runtime/conformance.Run's identical
	// convention); combine it with i to name fixture 0 and fixture 1
	// distinctly (e.g. namePrefix+"-a" / namePrefix+"-b") so the
	// statelessness subtest's two fixtures never collide on the same
	// runtime. The returned Request must be immediately ready to Reconcile
	// to Ready in one call against a fresh rt — any "fake technology" a
	// provider's own settledness check dials through (a real net.Listener
	// standing in for an upstream, e.g.) is set up here, entirely inside
	// the calling _test.go file; the conformance package itself never
	// touches a real socket.
	Resource func(rt runtime.ContainerRuntime, namePrefix string, i int) reconciler.Request

	// CapabilityChecks is optional (nil when the provider under test
	// declares no error-returning capability interface — a
	// ConnectionCapableProvider whose only methods return values, not
	// errors, has nothing to check here). See CapabilityCheck's doc
	// comment.
	CapabilityChecks func(p reconciler.Provider) []CapabilityCheck
}

// Run executes the fast-tier provider lifecycle contract suite against h.
// Every subtest is t.Parallel() and, against a fake runtime plus fake
// technology, completes in milliseconds (ADR 028 §1: "Providers get their
// fast-tier evidence there, not from e2e").
func Run(t *testing.T, h Harness) {
	t.Helper()
	if h.NewRuntime == nil || h.Provider == nil || h.Resource == nil {
		t.Fatal("conformance.Run: Harness.NewRuntime, Provider, and Resource are all required")
	}

	// Settledness (NFR-11, docs/planning/02 §4.1): Ready implies
	// probe-clean immediately — no separate wait needed after Reconcile
	// itself returns.
	t.Run("Settledness_ReconcileToReadyIsImmediatelyProbeClean", func(t *testing.T) {
		t.Parallel()
		runSettledness(t, h)
	})
	// Idempotency (NFR-2): a second Reconcile against an unchanged spec
	// makes zero mutating runtime calls.
	t.Run("Idempotency_SecondReconcileMakesNoMutatingRuntimeCalls", func(t *testing.T) {
		t.Parallel()
		runIdempotency(t, h)
	})
	// Probe honesty: point-in-time, never an internal wait/retry loop.
	t.Run("Probe_IsPointInTimeNotAWaitLoop", func(t *testing.T) {
		t.Parallel()
		runProbeHonesty(t, h)
	})
	// Destroy convergence, including destroy-when-already-gone.
	t.Run("Destroy_ConvergesAndIsIdempotentWhenAlreadyGone", func(t *testing.T) {
		t.Parallel()
		runDestroyConvergence(t, h)
	})
	// Request statelessness: two interleaved resources through one
	// Provider instance never cross-contaminate.
	t.Run("Statelessness_InterleavedResourcesDoNotCrossContaminate", func(t *testing.T) {
		t.Parallel()
		runStatelessness(t, h)
	})
	// providerState/endpoint publication rules (ADR 015: published facts
	// only — never a blank placeholder).
	t.Run("ProviderState_PublishedEndpointsCarryRealFacts", func(t *testing.T) {
		t.Parallel()
		runProviderStatePublication(t, h)
	})
	// Capability-interface error formats (docs/planning/02 §4.2), where the
	// provider declares one.
	t.Run("CapabilityErrorFormats", func(t *testing.T) {
		t.Parallel()
		runCapabilityChecks(t, h)
	})
}

func runSettledness(t *testing.T, h Harness) {
	t.Helper()
	rt := h.NewRuntime()
	p := h.Provider()
	req := h.Resource(rt, "conf-settle", 0)

	start := time.Now()
	st, err := p.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if elapsed := time.Since(start); elapsed > settleBudget {
		t.Errorf("Reconcile took %s, want under %s — the fast tier runs against fakes only, which are synchronous (ADR 028)", elapsed, settleBudget)
	}
	if !st.IsReady() {
		t.Fatalf("Reconcile did not report Ready: %+v", st.Conditions)
	}

	// NFR-11: Ready implies probe-clean AT THAT MOMENT — a single Probe
	// call, no retry, must already agree.
	probeSt, err := p.Probe(context.Background(), req)
	if err != nil {
		t.Fatalf("Probe immediately after a Ready Reconcile: %v", err)
	}
	if !probeSt.IsReady() {
		t.Errorf("Probe reported not-Ready immediately after Reconcile reported Ready (NFR-11 violation — Reconcile must settle to the SAME serving check Probe uses before declaring Ready): %+v", probeSt.Conditions)
	}
	if d, ok := probeSt.Condition(status.DriftDetected); ok && d.Status == status.True {
		t.Errorf("Probe reported DriftDetected immediately after a fresh Reconcile: %+v", d)
	}
}

func runIdempotency(t *testing.T, h Harness) {
	t.Helper()
	rt := h.NewRuntime()
	mc, ok := rt.(MutationCounter)
	if !ok {
		t.Fatal("Harness.NewRuntime() returned a runtime.ContainerRuntime that does not implement MutationCounter — the idempotency subtest cannot observe mutating calls")
	}
	p := h.Provider()
	req := h.Resource(rt, "conf-idem", 0)

	if _, err := p.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	before := mc.Mutations()
	st2, err := p.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	if !st2.IsReady() {
		t.Errorf("second Reconcile did not report Ready: %+v", st2.Conditions)
	}
	after := mc.Mutations()
	if after != before {
		t.Errorf("second Reconcile against an unchanged spec made %d mutating runtime call(s) (Mutations %d -> %d) — Ensure* must no-op when actual state already matches spec (NFR-2)", after-before, before, after)
	}
}

func runProbeHonesty(t *testing.T, h Harness) {
	t.Helper()
	rt := h.NewRuntime()
	p := h.Provider()
	req := h.Resource(rt, "conf-probe", 0)
	if _, err := p.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	for i := 0; i < 2; i++ {
		start := time.Now()
		st, err := p.Probe(context.Background(), req)
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("Probe call %d: %v", i+1, err)
		}
		if elapsed > probeBudget {
			t.Errorf("Probe call %d took %s, want under %s — Probe must be a point-in-time check, never an internal wait/retry loop", i+1, elapsed, probeBudget)
		}
		if !st.IsReady() {
			t.Errorf("Probe call %d reported not-Ready against an unchanged, just-reconciled resource: %+v", i+1, st.Conditions)
		}
	}
}

func runDestroyConvergence(t *testing.T, h Harness) {
	t.Helper()
	rt := h.NewRuntime()
	p := h.Provider()
	req := h.Resource(rt, "conf-destroy", 0)
	if _, err := p.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if err := p.Destroy(context.Background(), req); err != nil {
		t.Fatalf("first Destroy: %v", err)
	}
	// Destroy-when-already-gone must also converge, not error.
	if err := p.Destroy(context.Background(), req); err != nil {
		t.Errorf("second Destroy (resource already gone) returned an error — Destroy must converge idempotently: %v", err)
	}
}

func runStatelessness(t *testing.T, h Harness) {
	t.Helper()
	rt := h.NewRuntime()
	p := h.Provider()
	reqA := h.Resource(rt, "conf-state", 0)
	reqB := h.Resource(rt, "conf-state", 1)

	// Interleaved, not sequential-per-resource: reconcile A, THEN B, before
	// probing either — a provider holding cross-call state keyed by "the
	// last resource seen" would show it here.
	if _, err := p.Reconcile(context.Background(), reqA); err != nil {
		t.Fatalf("Reconcile fixture A: %v", err)
	}
	if _, err := p.Reconcile(context.Background(), reqB); err != nil {
		t.Fatalf("Reconcile fixture B: %v", err)
	}
	stA, err := p.Probe(context.Background(), reqA)
	if err != nil {
		t.Fatalf("Probe fixture A after interleaving with B's Reconcile: %v", err)
	}
	if !stA.IsReady() {
		t.Errorf("fixture A not Ready after interleaving with B (cross-call state leak?): %+v", stA.Conditions)
	}
	stB, err := p.Probe(context.Background(), reqB)
	if err != nil {
		t.Fatalf("Probe fixture B after interleaving with A's Reconcile: %v", err)
	}
	if !stB.IsReady() {
		t.Errorf("fixture B not Ready after interleaving with A (cross-call state leak?): %+v", stB.Conditions)
	}
}

// runProviderStatePublication decodes providerState[endpoint.Key] (when
// present — some providers, e.g. noop, publish no endpoint facts at all,
// which is not itself a violation) and checks every published endpoint
// carries a real address, never a blank placeholder (ADR 015: "publish,
// don't construct" — a consumer treats an absent fact as "not published
// yet"; a provider must never publish a present-but-empty one instead).
func runProviderStatePublication(t *testing.T, h Harness) {
	t.Helper()
	rt := h.NewRuntime()
	p := h.Provider()
	req := h.Resource(rt, "conf-publish", 0)
	st, err := p.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	raw, ok := st.ProviderState[endpoint.Key]
	if !ok {
		return // this provider publishes no endpoint facts — nothing to check
	}
	eps := endpoint.FromState(raw)
	if len(eps) == 0 {
		t.Fatalf("providerState[%q] is present but decoded to zero endpoints — malformed publication", endpoint.Key)
	}
	for _, ep := range eps {
		if ep.Name == "" {
			t.Errorf("published endpoint has an empty Name: %+v", ep)
		}
		if ep.Host == "" && ep.Internal == "" {
			t.Errorf("published endpoint %q has neither Host nor Internal set — ADR 015 forbids publishing an unresolved/blank fact: %+v", ep.Name, ep)
		}
	}
}

func runCapabilityChecks(t *testing.T, h Harness) {
	t.Helper()
	if h.CapabilityChecks == nil {
		return // this provider declares no error-returning capability interface
	}
	p := h.Provider()
	for _, c := range h.CapabilityChecks(p) {
		c := c
		t.Run(c.Name, func(t *testing.T) {
			t.Parallel()
			err := c.Invoke()
			if err == nil {
				t.Fatalf("%s: want an error (this check exists to prove a deliberately invalid input is rejected), got nil", c.Name)
			}
			for _, want := range c.WantSubstrings {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("%s: error = %q, want it to contain %q (docs/planning/02 §4.2's naming discipline: name the concrete fact that failed)", c.Name, err.Error(), want)
				}
			}
		})
	}
}
