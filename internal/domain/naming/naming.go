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

import "github.com/rezarajan/platformctl/internal/domain/resource"

// RuntimeObjectName returns the ContainerRuntime object name (container,
// Deployment/Service) for the resource that realizes it — the Provider
// itself for an owned instance, or the Connection for a managed forwarder;
// callers pass whichever Envelope actually owns the runtime object, never
// a related-but-different one.
func RuntimeObjectName(env resource.Envelope) string {
	return env.Metadata.Name
}
