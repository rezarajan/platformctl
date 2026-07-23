// This file is docs/planning/08 L1's engine-owned edge-mediation seam
// (docs/adr/034): resolveRequest's two named call sites
// (resolveSchemaRegistryURL, resolveKafkaBootstrapServers) each already
// resolve a single, well-defined (consumer, target) pair before this task —
// mediatedAddress is the ONE chokepoint both funnel through to ask "should
// this pair's address be substituted for a mediated one", so the substitution
// decision (gate check, nil-Mediation check, direct-transport check) lives
// in exactly one place rather than being repeated at each call site.
package engine

import (
	"context"
	"fmt"

	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/mediation"
)

// transportDirect reports whether env declares spec.transport: direct
// (docs/planning/08 L1, docs/adr/034) — the manifest's explicit opt-out of
// the mediated-by-default posture for the edges env itself declares. Reads
// the raw spec field directly rather than decoding through
// binding.FromEnvelope/connection.FromEnvelope a second time here: the
// field is already validated (unset or exactly "direct") by each Kind's own
// FromEnvelope before validate/plan ever lets a manifest reach apply, so a
// raw map read is safe and avoids a decode+error-handling path that can
// only ever observe an already-valid value at this point.
func transportDirect(env resource.Envelope) bool {
	t, _ := env.Spec["transport"].(string)
	return t == "direct"
}

// mediatedKafkaTransportDirect answers transportDirect's question for
// KafkaBootstrapServers (docs/planning/08 L1): unlike SchemaRegistryURL,
// there is no single declaring resource passed to resolveKafkaBootstrapServers
// — compatibility.ResolveKafkaBootstrapTarget's traversal finds every
// Binding whose providerRef names the Connect worker and whose EventStream
// resolves to the same broker. ADR 034's default is mediated, so the edge
// stays mediated (returns false) unless EVERY contributing Binding
// declares transport: direct — one Binding omitting the declaration is
// enough to keep the shared worker->broker edge mediated, and zero
// contributing Bindings (should not happen if ok was true, but checked
// defensively) is never "direct" either.
//
// Known scope limit, stated honestly rather than hidden: several Bindings
// sharing one worker+broker pair with DIFFERING transport declarations
// collapse to "mediated" here — the conservative reading of ADR 034's
// "mediated unless explicitly declared direct", not an attempt to average
// or pick a winner. Splitting one worker's Kafka connectivity into
// per-Binding transport decisions is out of L1's scope (the worker dials
// the broker once, for all its Bindings).
func mediatedKafkaTransportDirect(bindingKeys []resource.Key, byKey map[resource.Key]resource.Envelope) bool {
	if len(bindingKeys) == 0 {
		return false
	}
	for _, k := range bindingKeys {
		env, ok := byKey[k]
		if !ok || !transportDirect(env) {
			return false
		}
	}
	return true
}

// mediatedAddress is the substitution decision every L1 call site funnels
// through: it returns (address, true, nil) only when the MediatedTransport
// gate is on, e.Mediation is wired, and direct is false — every other case
// returns (_, false, nil), telling the caller to keep resolving its own
// unmediated address exactly as before this task (the gate-off/nil-
// Mediation/transport-direct byte-identical paths this task's tests pin).
// A non-nil error means the gate is on and mediation was genuinely
// requested but failed — ADR 034 promotes mediation to "the authoritative
// zero-trust enforcement plane" once the gate is flipped, so a failed dial
// address request fails the resolve (refuses to hand a provider an
// unmediated address it never asked for) rather than silently falling back
// to plaintext.
func (e *Engine) mediatedAddress(ctx context.Context, edge mediation.AddressEdge, direct bool) (address string, substituted bool, err error) {
	if direct || e.Mediation == nil {
		return "", false, nil
	}
	if !e.Registry.GateEnabled("MediatedTransport") {
		return "", false, nil
	}
	addr, err := e.Mediation.DialAddress(ctx, edge)
	if err != nil {
		return "", false, fmt.Errorf("mediated transport: resolve dial address for edge %s -> %s: %w", edge.From, edge.To, err)
	}
	return addr, true, nil
}
