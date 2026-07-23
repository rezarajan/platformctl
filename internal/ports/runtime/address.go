package runtime

import (
	"context"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

// AddressQualifier is an optional ContainerRuntime capability
// (docs/planning/08 H9, docs/adr/022 Ring 2) answering "is this raw
// 'host:port' bind-side address, as handed to me by a provider, actually
// dialable from where I am, or does it name a resource living in a
// DIFFERENT metadata.domain than my own and need substrate-specific
// qualification first?" — the domain-of-record FQDN gap the H6 Kubernetes
// addendum recorded as designed-but-unexercised: a mediated Connection's
// bind side (its spec.target) is handed directly to the mediator's own
// control-plane API rather than through EnsureContainer/EnsureNetwork, so
// it never passes through the engine's normal network-name translation
// (internal/application/engine/domainruntime.go's own "translate" chokepoint)
// at all.
//
// Only internal/application/engine's domainRuntime decorator implements
// this — no real ContainerRuntime adapter (Docker/Kubernetes/fake) needs
// to, and none does: it is pure per-request domain bookkeeping the engine
// already holds (which resource is being reconciled, and which domain each
// other in-set resource declared), not a runtime-technology concern. A
// caller that type-asserts req.Runtime against this interface and finds it
// missing should treat hostport as already dialable as-is (the pre-H9
// behavior, and the correct answer for Docker/fake, where network
// membership — not DNS naming — is what makes a bare name resolve).
//
// Docker: always a no-op (returns hostport unchanged) — a provider reaching
// across domains there does so by explicit network membership
// (docs/planning/08 H6's configuration.targetNetworks precedent), not by
// name-qualification; Docker's embedded per-network DNS resolves a bare
// container name the instant the caller is actually attached to that
// network, which qualification cannot substitute for. Kubernetes: when
// target and caller live in different domains, hostport's host is
// rewritten to the target's domain-scoped namespace's cluster-DNS FQDN
// ("<host>.<namespace>.svc.cluster.local:<port>") — bare short names only
// resolve within one namespace, so a caller in a different domain's
// namespace needs the qualified form to dial at all.
//
// Only internal/adapters/providers/openziti calls this today (the mediated
// entrypoint's bind side may legitimately name a resource outside the
// mediating Connection's own domain — that crossing IS what mediation is
// for) — see that package's connection.go doc comments.
type AddressQualifier interface {
	// QualifyTargetAddress resolves hostport for dialing FROM the domain
	// caller declares TOWARD the domain target declares — both already-
	// resolved envelopes the calling provider had in hand (never derived by
	// the provider itself from metadata.domain, which stays exclusively an
	// engine-side concern per docs/planning/08 H5's zero-provider-diff
	// invariant). Never returns a non-nil error for an ordinary "nothing to
	// qualify" case (hostport passes back unchanged); an error is reserved
	// for a hostport that cannot even be parsed as "host:port".
	QualifyTargetAddress(ctx context.Context, target, caller resource.Envelope, hostport string) (string, error)
}
