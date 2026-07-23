package compatibility

import (
	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
)

// ResolveKafkaBootstrapAddress infers the in-network Kafka address a
// Connect-worker Provider (debezium, s3sink) should join, from the manifest
// graph alone — no live state (docs/planning/08 E2, ADR 015: "graph-resolved
// manifest facts"). It finds every Binding whose providerRef names
// workerEnv, resolves each one's EventStream endpoint (sourceRef or
// targetRef), and asks that EventStream's own realizing Provider — via
// reconciler.KafkaBootstrapAddressProvider — for its address, computed from
// that Provider's own name and declared/default port (never guessed by
// string convention here).
//
// Returns "" when workerEnv is referenced by no Binding wired to an
// EventStream, when the EventStream's Provider doesn't implement the
// capability (a provider type without a Kafka-shaped listener), or when
// more than one distinct address would result — ambiguous, so the caller
// must fall back to requiring an explicit spec.configuration.
// bootstrapServers, exactly as before this inference existed.
func ResolveKafkaBootstrapAddress(workerEnv resource.Envelope, envelopes []resource.Envelope, resolve ProviderResolver) string {
	_, addr, _, ok := resolveKafkaBootstrapTarget(workerEnv, envelopes, resolve)
	if !ok {
		return ""
	}
	return addr
}

// ResolveKafkaBootstrapTarget is docs/planning/08 L1's addition: the same
// traversal and ambiguity rule as ResolveKafkaBootstrapAddress, but also
// returning the resolved broker Provider's resource.Key and every
// contributing Binding's resource.Key. KafkaBootstrapServers is a
// graph-resolved manifest fact, not a published one (ADR 015 — see
// reconciler.Request.KafkaBootstrapServers's own doc comment), so it
// cannot be substituted through Facts the way SchemaRegistryURL is; the
// engine's mediation seam needs the TARGET identity (to build a
// mediation.AddressEdge) and the contributing Bindings (to check whether
// EVERY one of them declares spec.transport: direct — ADR 034's default
// is mediated, so any contributing Binding NOT declaring direct keeps the
// edge mediated) without re-running this same Binding/EventStream/Provider
// traversal a second time in the engine.
func ResolveKafkaBootstrapTarget(workerEnv resource.Envelope, envelopes []resource.Envelope, resolve ProviderResolver) (target resource.Key, addr string, bindings []resource.Key, ok bool) {
	return resolveKafkaBootstrapTarget(workerEnv, envelopes, resolve)
}

func resolveKafkaBootstrapTarget(workerEnv resource.Envelope, envelopes []resource.Envelope, resolve ProviderResolver) (target resource.Key, addr string, bindings []resource.Key, ok bool) {
	idx := newIndex(envelopes)
	workerKey := workerEnv.Key()
	type match struct {
		target   resource.Key
		bindings []resource.Key
	}
	addrs := map[string]match{}

	for _, e := range envelopes {
		if e.Kind != "Binding" {
			continue
		}
		provEnv, ok := idx.resolveKind(e, resource.RefFromSpec(e.Spec, "providerRef"), "Provider")
		if !ok || provEnv.Key() != workerKey {
			continue
		}

		var esEnv resource.Envelope
		var found bool
		for _, field := range []string{"sourceRef", "targetRef"} {
			if cand, ok := idx.resolveKind(e, resource.RefFromSpec(e.Spec, field), "EventStream"); ok {
				esEnv, found = cand, true
				break
			}
		}
		if !found {
			continue
		}

		esProvEnv, ok := idx.resolveKind(esEnv, resource.RefFromSpec(esEnv.Spec, "providerRef"), "Provider")
		if !ok {
			continue
		}
		esProv, err := provider.FromEnvelope(esProvEnv)
		if err != nil {
			continue
		}
		impl, err := resolve(esProv.Type)
		if err != nil {
			continue
		}
		kb, ok := impl.(reconciler.KafkaBootstrapAddressProvider)
		if !ok {
			continue
		}
		if a := kb.KafkaBootstrapAddress(naming.RuntimeObjectName(esProvEnv), esProv); a != "" {
			m := addrs[a]
			m.target = esProvEnv.Key()
			m.bindings = append(m.bindings, e.Key())
			addrs[a] = m
		}
	}

	if len(addrs) != 1 {
		return resource.Key{}, "", nil, false
	}
	for a, m := range addrs {
		return m.target, a, m.bindings, true
	}
	return resource.Key{}, "", nil, false // unreachable
}
