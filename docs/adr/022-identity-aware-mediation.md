# ADR 022 — Identity-aware mediation: domains, workload identity, and mTLS via an off-the-shelf mediator

**Status:** accepted as design (2026-07-22); implementation is doc 08
Stage H (H5/H6). **Prompted by:** project-owner direction — mTLS is a
common expectation and must not be hand-rolled; state-of-the-art
zero-trust platforms mediate even database connections by policy (the
AWS VPC Lattice reference); Datascape must be able to express "the
source's domain denies access to the sink's domain" and have it both
caught at authoring time and enforced at runtime by whatever provider
can actually mediate.

## Research findings that shape this design (2026-07)

1. **How AWS actually mediates databases.** VPC Lattice reaches TCP
   resources (RDS, arbitrary IP/DNS targets) through *Resource
   Configurations* served by a *Resource Gateway* — a mediation point of
   ingress in the owning VPC. Critically, Lattice's IAM **auth policies
   do not apply to resource configurations**: per-request identity policy
   exists for HTTP/gRPC services; raw TCP gets *placement + connection-
   time* mediation. Lesson: for database/broker protocols, the
   state-of-the-art enforces **who may establish a connection** (network
   admission + mutual identity) and delegates per-operation authz to the
   protocol's native layer (DB users/grants, Kafka ACLs). A design that
   promises per-query IAM on raw Postgres wire would be beyond what even
   AWS ships; Datascape will not promise it.
2. **Off-the-shelf mediators.** OpenZiti (Apache-2.0): an identity-first
   overlay — every participant holds a cryptographic identity; *service
   policies* declare which identities may **dial** or **bind** a service;
   works identically across Docker, Kubernetes, and hosts via routers and
   tunnelers; services can be **dark** (no listening port exposed on any
   network); SPIRE can act as the identity authority. HashiCorp Consul:
   mature multi-platform mesh with automatic mTLS and *intentions*
   (service-to-service allow/deny) — heavier operationally (agents +
   Envoy sidecars per workload). Linkerd/Istio: excellent but effectively
   Kubernetes-only, disqualifying them for a Docker-first product.
   SPIFFE/SPIRE: the 2026 workload-identity standard (short-lived,
   attestation-bound X.509/JWT SVIDs) — the identity *format* to align
   with even before running SPIRE itself.
3. **Data-mesh governance practice.** Domain ownership with policy-as-code
   enforced by the platform layer is the mature 2026 pattern; domains are
   first-class, and cross-domain access is governed by versioned,
   enforced-in-code policy — not documents.

## The question deliberated

*Does every connection go through the mediator and get routed
transparently?* Three candidate architectures:

- **(a) Universal sidecar mesh** — every workload gets a proxy; all
  traffic mediated. Maximum coverage, but: heavy per-container overhead,
  deep runtime intrusion (contradicts the Docker-first simplicity and the
  port's shape), and it mediates same-domain traffic nobody asked to
  mediate. Rejected as the default; recorded as an opt-in future mode
  (below) since OpenZiti tunneler sidecars support it without app
  changes.
- **(b) Mediate nothing; policy on paper** — validate-time denial only.
  Insufficient: the owner's requirement is real enforcement, and doc 09
  §4 already teaches that unenforced posture decays.
- **(c) Mediate at the Connection seam — chosen.** Datascape's unique
  property is that it OWNS the wiring: every cross-resource data path is
  declared, resolved, and reconciled by it. Therefore it does not need to
  intercept traffic universally; it needs to (1) **refuse to wire**
  cross-domain paths that policy denies, and (2) **only wire allowed
  cross-domain paths through an identity-carrying mediated entrypoint** —
  a `MediatedConnection`: the existing Connection abstraction (ADR 002/
  018 — the platform-owned entrypoint providers already consume
  transparently) realized by a mediator provider instead of a plain
  forwarder. Consumers keep addressing the Connection exactly as today;
  "transparent after wiring" is already the Connection contract.

## Decision — three enforcement rings, one policy source

All rings are driven by the same ADR 021 policy objects; each ring
enforces as much as its layer can, and every ring is compiled — never
hand-configured.

**Ring 0 — authoring-time (unique to Datascape).** `metadata.domain`
becomes a first-class optional field (DNS-label; default `default`) on
every kind. The policy vocabulary gains cross-domain selectors evaluated
over graph *edges* (a Binding is an edge source-domain → target-domain;
a Connection consumption is an edge). The owner's exact scenario — a
`cdc` Binding whose `sourceRef` lives in domain `payments` and whose
`targetRef`/sink chain lives in domain `analytics`, with policy
`deny {from: payments, to: analytics}` — fails **at `validate`**,
deterministically, before any infrastructure exists. No mesh can do
this; it is the payoff of the typed graph.

**Ring 1 — network floor.** Domains compile to network segmentation:
per-domain networks/namespaces (Docker: one network per domain;
Kubernetes: the existing B7 default-deny walls per namespace-domain),
so an *undeclared* cross-domain path physically fails rather than
succeeding silently. Allowed cross-domain paths compile to exactly the
holes the mediated entrypoint needs — nothing else. (This generalizes
what B7 + the C7-era external-ingress exception already do.)

**Ring 2 — identity-aware mediation (the mTLS layer, off-the-shelf).**
A `mesh`-class provider — **OpenZiti first** — realizes
`MediatedConnection`s: a pinned controller + router, one cryptographic
identity minted per participating workload, **derived from the F4 naming
authority** (identity format aligned to SPIFFE:
`spiffe://datascape/<namespace>/<kind>/<name>` — cryptographic identity
as the extension of identity-by-handle), and ADR 021 policies compiled
into the mediator's native dial/bind policies. Raw-TCP protocols
(Postgres, Kafka) are in scope because Ziti mediates at dial time —
matching the Lattice lesson: connection-establishment identity +
protocol-native credentials, not per-query IAM. mTLS between mediator
legs is the mediator's own, automatically managed — Datascape never
hand-rolls certificates (C8's entrypoint TLS remains the north-south
story; this ADR is east-west).

Why OpenZiti over Consul as the first mediator: identical operation on
both shipped runtimes, dial-time policy for arbitrary TCP without
protocol awareness, dark services (a mediated database exposes no
listener on any shared network — the strongest posture), lighter
footprint than agent+sidecar-per-workload, and a SPIRE-compatible
identity path. Consul intentions is the documented alternative if
operating experience demands it; the provider seam
(`ConnectionCapableProvider` + the policy-compilation interface) is
mediator-agnostic by construction, so the choice is swappable — the
same open/closed property every other seam in this repo has.

## Explicit boundaries (unchanged from ADR 021, sharpened by research)

- Datascape **defines and compiles** policy; the mediator **enforces**
  it. Datascape is never the data-plane.
- Per-request/per-query authorization on raw TCP protocols is out of
  scope — the industry's own boundary (Lattice). Protocol-native authz
  (DB grants, Kafka ACLs) remains the providers' domain; a future lint
  can flag when a mediated path lacks a dedicated protocol credential.
- Universal transparent interception (mode (a)) is a recorded opt-in
  future (`meshMode: sidecar` per domain), not the default.
- L7 request-identity policy (JWT/OIDC on HTTP routes) belongs to the
  ingress seam (ADR 018/C8 follow-ups), not this ADR.

## Consequences

- `Connection` completes its arc: plain forwarder (proxy) → HTTP router
  (ingress) → TLS entrypoint (C8) → identity-checked mediated entrypoint
  (mesh). One noun, four escalating realizations — no new user-facing
  concepts beyond `metadata.domain` and policy rules.
- The F4 naming authority becomes the identity root; workload identity
  is derivable, stable, and collision-free by construction.
- Migration is opt-in per domain: undeclared domains behave exactly as
  today (single implicit domain, no mediation) — zero behavior change
  until domains and policies are declared.

## References

ADR 002/018 (Connection seam), 013 (secret/label safety), 015
(connectivity plane — this ADR is its zero-trust completion), 020/021
(lints/policy), doc 09 §4 (plane analysis). Research sources: AWS VPC
Lattice docs/FAQ + resource-gateway guides (auth-policy scope; resource
configurations), OpenZiti documentation and SPIRE-integration posts,
Consul intentions/permissive-mTLS docs, 2026 data-mesh governance
surveys (Thoughtworks, Atlan).
