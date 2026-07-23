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

// identityScheme is the URI scheme workload identities are minted under —
// SPIFFE-aligned per docs/adr/022 ("cryptographic identity as the extension
// of identity-by-handle") and promoted to the authoritative zero-trust
// identity root by docs/adr/027. "datascape" is the trust domain: every
// platformctl-managed workload, on every substrate, shares one trust
// domain, matching a single SPIFFE deployment's convention (multiple trust
// domains is a future multi-tenant concern, not designed here).
const identityScheme = "spiffe://datascape"

// WorkloadIdentityURI derives the SPIFFE-aligned workload identity URI for
// a resource-graph node — the identity subject IS the declared graph node
// (docs/adr/022, docs/adr/027: "identity derives from what a workload is
// declared to be, never from where it happens to run or what IP it
// holds"). Deterministic and side-effect-free: the same node always
// derives the same URI, on any substrate, on any call — the naming
// authority (docs/planning/08 F4) extended to identity the same way
// RuntimeObjectName extends it to runtime object names.
//
// Shape: spiffe://datascape/<namespace>/<kind>/<name> when metadata.domain
// is undeclared/default; spiffe://datascape/<namespace>/<domain>/<kind>/<name>
// when a non-default domain is declared (docs/planning/08 H5,
// resource.NormalizeDomain) — the SAME "undeclared domain is a byte-
// identical no-op" rule NetworkName above already holds, extended to
// identity: a manifest set authored before domains existed, or one that
// never declares a non-default domain, derives the exact URI this function
// produced before H5 merged (naming_test.go pins this explicitly). Kind is
// lowercased (SPIFFE path segments are conventionally lowercase; the
// resource Kind itself stays whatever case the manifest used — only the
// URI segment is folded).
func WorkloadIdentityURI(env resource.Envelope) string {
	ns := resource.NormalizeNamespace(env.Metadata.Namespace)
	kind := lowerASCII(env.Kind)
	domain := resource.NormalizeDomain(env.Metadata.Domain)
	if domain == resource.DefaultDomain {
		return identityScheme + "/" + ns + "/" + kind + "/" + env.Metadata.Name
	}
	return identityScheme + "/" + ns + "/" + domain + "/" + kind + "/" + env.Metadata.Name
}

// lowerASCII avoids pulling in strings.ToLower's Unicode-aware casing for a
// value that is always ASCII by construction (resource Kind names are Go
// identifiers, docs/planning/03 §2.1) — a tiny, dependency-free helper so
// this package's only import stays internal/domain/resource.
func lowerASCII(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + ('a' - 'A')
		}
	}
	return string(b)
}
