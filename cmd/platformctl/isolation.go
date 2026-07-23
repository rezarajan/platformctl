package main

import (
	"context"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	runtimeport "github.com/rezarajan/platformctl/internal/ports/runtime"
)

// observeIsolation is docs/planning/08 H8's (docs/adr/027) trigger point:
// it calls each distinct declared runtime's IsolationObserver capability
// at most once per invocation of this function — deduped by runtime type +
// kubeconfig/context (mirroring kubernetesPreflight's own cfgKey pattern
// above), since ObserveIsolationEnforcement's cluster-wide observation
// does not vary per Provider's own network/namespace. "Cache the
// observation per-command-run" means exactly this: an in-process memo
// scoped to one call, never persisted to state or disk — every trigger
// point below gets a live, current answer (ADR 027: "never assumed"), but
// a command whose manifest set declares many resources on the same
// cluster never spawns more than one canary pair.
//
// Trigger policy (deliberately not every command — doc 02's command
// contracts, decided here since H8's own spec text second-guesses itself
// on this point):
//   - apply (preflight, before the confirmation prompt): the primary,
//     named probing point.
//   - drift: already a live, cluster-touching probe by contract (doc 02:
//     "Probe live infrastructure"); adding this one more bounded call is
//     consistent with what it already does.
//   - status: also already touches a live Kubernetes cluster today, via
//     loadAndValidate's kubernetesPreflight connectivity check — so a
//     bounded isolation probe is consistent with status's existing
//     live-connectivity posture, and it is the accept-bar command (doc 08
//     H8: "status shows it").
//   - inventory: same reasoning as status, for parity ("inventory
//     includes it").
//   - validate is deliberately EXCLUDED: doc 02 pins it "no state, no
//     runtime calls," and ADR 027's own "observed, never assumed"
//     discipline cuts against an offline command fabricating or caching a
//     possibly-stale live network fact. plan/destroy/import/graph are
//     excluded too, to keep the probing surface no wider than the accept
//     bar requires.
func (a *app) observeIsolation(ctx context.Context, envelopes []resource.Envelope) map[string]runtimeport.IsolationStatus {
	type cfgKey struct{ runtimeType, kubeconfig, context string }
	out := map[string]runtimeport.IsolationStatus{}
	seen := map[cfgKey]bool{}
	for _, e := range envelopes {
		if e.Kind != "Provider" {
			continue
		}
		p, err := provider.FromEnvelope(e)
		if err != nil || p.RuntimeType == "" {
			continue
		}
		// Kubernetes only: its CNI's actual NetworkPolicy enforcement is
		// genuinely uncertain (docs/adr/027's whole point) and worth a
		// live line every run. Docker's answer is constant — "enforced by
		// construction," true unconditionally (see
		// internal/adapters/runtime/docker/isolation.go) — printing it on
		// every status/apply/drift/inventory call would be pure noise for
		// the overwhelmingly common Docker-runtime case, so it's skipped
		// here; the capability itself still exists at the port level
		// (unit-tested, reachable via `platformctl explain
		// IsolationEnforced`) for any programmatic consumer. A runtime
		// type this registry doesn't even recognize as network-boundary-
		// bearing (e.g. the "fake" test double) is skipped the same way —
		// only continue would print, which is worse than saying nothing.
		if p.RuntimeType != "kubernetes" {
			continue
		}
		kubeconfig, _ := p.RuntimeConfig["kubeconfig"].(string)
		contextName, _ := p.RuntimeConfig["context"].(string)
		key := cfgKey{p.RuntimeType, kubeconfig, contextName}
		if seen[key] {
			continue
		}
		seen[key] = true

		rt, err := a.reg.Runtime(p.RuntimeType, p.RuntimeConfig)
		if err != nil {
			continue // unconstructable runtime already failed elsewhere (kubernetesPreflight/gates)
		}
		obs, ok := rt.(runtimeport.IsolationObserver)
		if !ok {
			continue // never happens through the registry (haGuardRuntime always implements it); kept defensive
		}
		pctx, cancel := context.WithTimeout(ctx, runtimeport.ScaledWait(2*time.Minute))
		result, _ := obs.ObserveIsolationEnforcement(pctx)
		cancel()

		label := p.RuntimeType
		if kubeconfig != "" || contextName != "" {
			disambiguator := contextName
			if disambiguator == "" {
				disambiguator = kubeconfig
			}
			label = fmt.Sprintf("%s (%s)", p.RuntimeType, disambiguator)
		}
		out[label] = result
	}
	return out
}

// isolationReasonToken maps an IsolationStatus.State to the explain-
// catalog token naming it (internal/domain/status/reasons.go), so a
// printed isolation note is directly pasteable into `platformctl explain`.
func isolationReasonToken(state string) string {
	switch state {
	case runtimeport.IsolationEnforced:
		return status.ReasonIsolationEnforced
	case runtimeport.IsolationNotEnforced:
		return status.ReasonIsolationNotEnforced
	default:
		return status.ReasonIsolationUnknown
	}
}

// printIsolationNotes renders observeIsolation's result as human-readable
// lines (docs/planning/08 H8: "status/preflight report `network isolation:
// enforced | not-enforced | unknown(<reason>)`") — a WARNING, never an
// error, on not-enforced (ADR 027: Layer 1 is the guarantee, this is
// honesty reporting about Layer 2 only). A no-op when obs is empty (no
// Kubernetes/Docker Provider declared, or every construction attempt
// failed upstream).
func printIsolationNotes(w io.Writer, obs map[string]runtimeport.IsolationStatus) {
	labels := make([]string, 0, len(obs))
	for label := range obs {
		labels = append(labels, label)
	}
	sort.Strings(labels)
	for _, label := range labels {
		st := obs[label]
		token := isolationReasonToken(st.State)
		switch st.State {
		case runtimeport.IsolationEnforced:
			fmt.Fprintf(w, "network isolation (%s): enforced [%s]\n", label, token)
		case runtimeport.IsolationNotEnforced:
			fmt.Fprintf(w, "WARNING: network isolation (%s): NOT ENFORCED by this cluster's CNI [%s] — %s — only Layer 1 (identity-attested mediated connections, docs/adr/022) protects you; see docs/adr/027 and `platformctl explain %s`\n", label, token, st.Reason, token)
		default:
			fmt.Fprintf(w, "network isolation (%s): unknown [%s] — %s\n", label, token, st.Reason)
		}
	}
}
