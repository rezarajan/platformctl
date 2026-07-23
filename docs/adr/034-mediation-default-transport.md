# ADR 034 — Mediation is the default transport; direct is the declared exception

**Status:** accepted (2026-07-23). **Prompted by:** the owner, closing
the question ADR 027 left open and this week's Stage H audit made
explicit: "Mediation should be the default transport, and ideally
(unless explicitly declared) the only one. The entire concept of d7s is
to give the developer a production-grade data platform in the simplest
way. Everything that makes it production-grade should be
batteries-included."

## What changes

ADR 027 shipped identity-checked dials as a per-Connection opt-in: an
operator routes an edge through a `MediatedConnection` and that edge —
only that edge — gets per-workload identities and dial-time
authorization. Every other declared edge rides the underlay (shared or
graph-scoped networks), where "identity" is network membership. This
ADR inverts the boundary: **every declared graph edge is mediated by
default**. An unmediated edge exists only where the manifest says so
(`transport: direct`), and that declaration is lint-flagged and
policy-deniable — the zero-trust pack ships a deny for it, so a
production posture can forbid direct transport outright.

The enforcement ladder becomes, for every edge: policy admits it (ADR
021/033) → the graph wires it (ADR 026) → **identity checks every dial
of it** (this ADR) → the underlay walls remain as defense-in-depth,
still observed-never-assumed (ADR 027 Layer 2 is unchanged — we still
never trust the network, including our own overlay's underlay).

## Why the engine can do this with zero provider changes

The facts architecture is what makes "batteries-included" honest rather
than aspirational. Consumers never construct endpoint addresses by
convention — they resolve them from published facts and graph
resolution (F4, I9). The engine therefore owns every address any
consumer will ever dial, at one chokepoint. Mediating an edge is, from
a provider's perspective, nothing: the engine provisions the identity,
service, and policies for the edge, stands up the intercept, and hands
the consumer a mediated address THROUGH THE SAME FACTS it already
reads. `domainRuntime` (H5/J5) proved the chokepoint pattern twice;
this is its third and largest tenant. Providers keep speaking
`ContainerRuntime` and reading `Request.Facts` — the mediation fabric
is a platform facility, like networks, not something a manifest
declares provider-by-provider.

Mechanically (the H6-proven shape, generalized): the mediation fabric
(controller + router) becomes platform-owned infrastructure the engine
ensures like it ensures networks; each workload gets one identity
(SPIFFE-named, ADR 027) and one co-located tunneler intercepting its
declared dials; bind-side terminators host each target service;
service-policies authorize exactly the declared edges, carrying K4's
label-derived attributes.

## Costs, stated before they are discovered

1. **The fabric joins the critical path of every connection.**
   Controller/router availability stops being a mediated-Connection
   concern and becomes THE platform's data-plane concern. Controller HA
   and mesh-outage chaos coverage are therefore GA gates for this ADR,
   not nice-to-haves (L5). Established sessions survive controller
   outages in the chosen mesh; new dials do not — that asymmetry gets
   measured and documented, not assumed.
2. **Container count roughly doubles** (a tunneler per workload).
   J5's resource bounds exist at exactly the right moment; tunneler
   footprints are small but nonzero and get bounds like everything
   else.
3. **Protocol hard cases are real.** Kafka's advertised-listener
   redirection means brokers hand clients addresses the intercept must
   own — mediating broker traffic requires advertised-name alignment
   through the overlay and is its own task (L4), proven live before
   the default flips for EventStream edges. Anything with server-side
   redirects gets the same treatment.
4. **Throughput tax.** CDC/lakehouse data planes pay overlay overhead.
   L5 includes a measured before/after on the standing scenarios; the
   number goes in the claims table, whatever it is.
5. **Migration.** The gate (`MediatedTransport`, Alpha, disabled)
   ships byte-identical-off. Flipping it on an existing deployment is
   a planned, per-edge rollout (plan shows each edge's transport
   change), never an implicit big bang.

## Alternatives considered

- **SPIRE-backed attestation feeding per-workload mTLS without an
  overlay**: stronger attestation story, but it pushes protocol
  awareness into every consumer (certs into postgres clients, Kafka
  clients, JDBC...) — exactly the per-provider sprawl the port
  architecture forbids. The mesh keeps providers untouched; SPIRE-class
  attestation can later harden identity ISSUANCE behind the
  MediationProvider port without moving this boundary (recorded as the
  Phase 8+ follow-up it was in ADR 033).
- **Keep opt-in, make the zero-trust pack demand mediation**: honest,
  but it makes production-grade a checklist instead of a default —
  the inverse of the product's premise. Rejected on the owner's
  directive, which this ADR exists to record.

## Relationship to prior ADRs

Amends ADR 027's opt-in boundary (its layering, claims discipline, and
Layer-2 posture are unchanged). Consumes ADR 026 (edges), ADR 033
(labels/attributes), H10 (hardened fabric client). The
`MediationProvider` port remains the seam; OpenZiti remains the first
adapter, not the architecture.

## References

Doc 08 §7.11 Stage L (L1–L5 sequencing + exit criteria), ADR 013 (the
implicit-infrastructure safety bar L2 must meet), doc 11 2026-07-23.
