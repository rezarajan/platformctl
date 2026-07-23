// Package naming is the single authority mapping a resource to the runtime
// object name that realizes it — one container name (Docker), one
// Deployment/Service name (Kubernetes) per resource, computed exactly one
// way (docs/planning/08 F4, docs/planning/09 Class 4 / K7).
//
// Before this package existed, every provider and every consumer (the
// engine's Connection probe chief among them) re-derived "what is this
// resource's runtime object called?" independently, by convention: "the
// same as the resource's own name." That convention held everywhere except
// once — a managed Connection's forwarder is realized by a Provider, and
// the first fix guessed the *Provider's* name instead of the *Connection's*
// own name, failing live against a real cluster ("container \"edge\" not
// found") after the identical mistake had already been made and fixed once
// in debezium's equivalent preflight. Neither mistake was a bad guess so
// much as there being no single place a correct guess *had* to come from.
//
// RuntimeObjectName is deliberately the identity function today — the
// convention itself ("named after the realizing resource") is correct and
// unchanged. What changes is that every call site asks this package instead
// of re-deriving it, so a future convention change (a namespace prefix, a
// hash suffix, anything) touches this one file, not every provider and every
// consumer that currently spells out `.Metadata.Name` for the same purpose.
package naming

import (
	"fmt"
	"hash/fnv"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

// RuntimeObjectName returns the ContainerRuntime object name (container,
// Deployment/Service) for the resource that realizes it — the Provider
// itself for an owned instance, or the Connection for a managed forwarder;
// callers pass whichever Envelope actually owns the runtime object, never
// a related-but-different one.
func RuntimeObjectName(env resource.Envelope) string {
	return env.Metadata.Name
}

// NetworkName derives a domain-scoped runtime network name from a base
// network name (spec.runtime.network's configured-or-default value, e.g.
// "datascape") and a resource's metadata.domain (docs/adr/022 Ring 1,
// docs/planning/08 H5). The default domain is a no-op — NetworkName returns
// base unchanged — so a manifest set that never declares a non-default
// domain produces byte-identical network names to before domains existed;
// every other domain gets its own network, "<base>-<domain>".
//
// On Kubernetes a network name already IS the namespace name (docs/planning/08
// B7, internal/adapters/runtime/kubernetes's EnsureNetwork/targetNamespace),
// so this one function gives Ring 1 both runtimes for free: a per-domain
// Docker network and a per-domain Kubernetes namespace are the same string.
func NetworkName(base, domain string) string {
	d := resource.NormalizeDomain(domain)
	if d == resource.DefaultDomain {
		return base
	}
	name := base + "-" + d
	// Kubernetes namespace names (which this doubles as, see above) are
	// DNS labels: 63 chars max. A long base+domain pair is truncated to
	// 53 chars and suffixed with an 8-hex FNV of the FULL name so two
	// long names never silently collide post-truncation and the result
	// stays deterministic (doc 11 GA caveat sweep, item D — recorded at
	// H5's merge gate).
	if len(name) > 63 {
		h := fnv.New32a()
		_, _ = h.Write([]byte(name))
		name = fmt.Sprintf("%s-%08x", name[:54], h.Sum32())
	}
	return name
}
