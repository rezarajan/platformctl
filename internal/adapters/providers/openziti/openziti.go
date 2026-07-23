// Package openziti implements the FIRST adapter for docs/planning/08 H6
// (as amended by docs/adr/027): a `mesh`-class reconciler.Provider that
// realizes docs/adr/022's Ring 2 — per-workload cryptographic identity and
// per-edge dial authorization — on top of OpenZiti (Apache-2.0), a pinned
// controller + router (docs/planning/08 A10 digest-pinning discipline).
//
// # Layering discipline (docs/adr/027, this task's non-negotiable #1)
//
// "OpenZiti is the FIRST adapter... nothing outside the adapter may import
// or name Ziti." internal/ports/mediation and
// internal/application/graphaccess are technology-silent; only this
// directory imports the ziti wire concepts (the Edge Management REST API
// shape) and only this directory's identifiers contain the string "ziti"
// (case-insensitive) outside comments/string literals documenting the
// upstream project name. internal/archtest/mediation_layering_test.go pins
// this mechanically — a grep-based archtest in the same family as every
// other decoupling-contract test in this repo (docs/planning/08 doc 11).
//
// # Mechanism summary (see this package's own doc comments for the "why")
//
//   - Provider-kind Reconcile (instance.go) bootstraps a controller
//     container and a router container using OpenZiti's own
//     ZITI_BOOTSTRAP=true env-var-driven bootstrap (verified live against
//     the pinned images at authorship time: a single EnsureContainer call
//     per role produces a fully working, self-signed-PKI-backed Ziti
//     network with zero manual PKI/config generation) — then authenticates
//     an Edge Management REST client (client.go) and ensures the router is
//     enrolled (fetching its one-time enrollment JWT via REST, matching
//     the same "Go orchestrates entirely at apply time" pattern
//     docs/adr/023 Decision 4 established).
//   - Connection-kind Reconcile (connection.go) compiles the mediated
//     subset of the declared graph (internal/application/graphaccess) for
//     this Connection, mints/looks up a Ziti identity per consumer
//     (identity.go), creates one Ziti Service + a router-hosted, address-
//     bound Terminator (binding "transport" — the router forwards raw TCP
//     natively, no per-target tunneler process; the router reaches the
//     target only via configuration.targetNetworks, keeping the target
//     dark on every network consumers are on), and a Dial service-policy
//     scoped to exactly the consumer identities the graph authorizes — the
//     ADR 026 per-edge authorization, realized as Ziti-native policy. One
//     small `ziti-edge-tunnel` "proxy"-mode container per Connection
//     (matching docs/adr/023's one-runtime-object-per-Connection
//     precedent) gives consumers the familiar `<connection-name>:<port>`
//     dial address, enrolled under the (first) consumer's own identity —
//     see connection.go's doc comment for the >1-consumer follow-up this
//     mirrors from docs/adr/023's own recorded limitation.
//
// # What this adapter deliberately does NOT do
//
// Per-query/per-request authorization on the mediated raw-TCP path — the
// documented industry boundary docs/adr/022 draws from the AWS VPC Lattice
// research (identity authorizes connection establishment; protocol-native
// authz, e.g. Postgres roles, stays the database's own job).
package openziti

import (
	"context"
	"fmt"

	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/mediation"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
)

// Pinned images (docs/planning/08 A10): resolved live via `docker manifest
// inspect` against the openziti/ziti-controller, openziti/ziti-router, and
// openziti/ziti-tunnel repositories at authorship time (2026-07-22), the
// most recent non-prerelease tag (1.5.14) common to all three. Recorded in
// scripts/pinned-images.txt alongside every other pinned image.
const (
	defaultControllerImage = "openziti/ziti-controller:1.5.14@sha256:028d103e0140853c89916982c0e3d692cd7ce2084eb32733533bbdb0607e28c6"
	defaultRouterImage     = "openziti/ziti-router:1.5.14@sha256:0761651d8995915cf2ec00b5b0eba461bae50fca62f5576c5ef813e3472df77b"
	defaultTunnelImage     = "openziti/ziti-tunnel:1.5.14@sha256:5966139d3db0f54b58f979d1e3374a0fd0f132322ecade29b852d2cabedaf861"
)

// Provider is the openziti reconciler.Provider: a MediationCapableProvider
// (mediation.go... no — see identity.go) and a ConnectionCapableProvider
// (scheme "tcp", the docs/adr/023-precedented Connection-seam realization).
type Provider struct{}

// New constructs the provider — stateless, per docs/planning/08 F5: no
// field is set outside a call's own reconciler.Request.
func New() *Provider { return &Provider{} }

func (p *Provider) Type() string { return "openziti" }

// SupportedConnectionSchemes: only "tcp" — raw-TCP mediation is the whole
// point (docs/adr/022's Lattice-lesson framing); an HTTP-aware scheme would
// duplicate the ingress/C8 seam this ADR explicitly leaves alone.
func (p *Provider) SupportedConnectionSchemes() []string { return []string{"tcp"} }

// Mediation satisfies reconciler.MediationCapableProvider: it authenticates
// against the mediation Provider named by req.Provider and returns a
// session bound to that live connection (identity.go's *session implements
// every mediation.MediationProvider method). Each call re-authenticates —
// cheap (one REST round trip) and avoids any cross-call state on *Provider
// itself (docs/planning/08 F5).
func (p *Provider) Mediation(ctx context.Context, req reconciler.Request) (mediation.MediationProvider, error) {
	return newSession(ctx, req)
}

func (p *Provider) Reconcile(ctx context.Context, req reconciler.Request) (status.Status, error) {
	switch req.Resource.Kind {
	case "Provider":
		return p.reconcileInstance(ctx, req)
	case "Connection":
		return p.reconcileConnection(ctx, req)
	default:
		return status.Status{}, fmt.Errorf("openziti: unsupported Kind %q", req.Resource.Kind)
	}
}

func (p *Provider) Destroy(ctx context.Context, req reconciler.Request) error {
	switch req.Resource.Kind {
	case "Provider":
		return p.destroyInstance(ctx, req)
	case "Connection":
		return p.destroyConnection(ctx, req)
	default:
		return fmt.Errorf("openziti: unsupported Kind %q", req.Resource.Kind)
	}
}

func (p *Provider) Probe(ctx context.Context, req reconciler.Request) (status.Status, error) {
	// Probe reuses Reconcile's own serving check (docs/planning/02 §4.1
	// settledness rule: "reconcile runs the SAME serving check its own
	// Probe uses") — both Kind branches below are read-mostly:
	// EnsureContainer/EnsureNetwork/REST upserts are all idempotent
	// no-ops when actual state already matches desired state, so probing
	// via the reconcile path costs nothing extra and cannot drift from
	// what Reconcile itself considers Ready.
	switch req.Resource.Kind {
	case "Provider":
		return p.probeInstance(ctx, req)
	case "Connection":
		return p.probeConnection(ctx, req)
	default:
		return status.Status{}, fmt.Errorf("openziti: unsupported Kind %q", req.Resource.Kind)
	}
}
