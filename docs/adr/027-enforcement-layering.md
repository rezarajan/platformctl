# ADR 027 — Enforcement layering: identity is authoritative, the network is best-effort

**Status:** accepted (2026-07-22). **Prompted by:** the project owner,
reviewing the GA caveat sweep: reliance on network-layer enforcement is
a design flaw — zero-trust and policy-based-control claims must hold
consistently across Docker, Kubernetes, and future Terraform-provisioned
substrates, and coupling to any one enforcement tool (Calico, a
specific mesh) is unacceptable.

## The flaw, named

Network controls authenticate **location, not workload**, and their
enforcement is delegated to whatever fabric exists: a Kubernetes CNI
that may not enforce NetworkPolicy at all (kindnet, several managed
defaults), Docker's networks (topology-as-ACL, no pairwise semantics
without per-edge networks), cloud security groups (a third model). A
guarantee that varies by substrate is not a guarantee. Zero-trust
doctrine (SPIFFE/SPIRE, NIST 800-207) draws the conclusion this ADR
adopts: **never trust the network; authenticate every connection with
cryptographic workload identity and authorize it against policy at the
endpoint.**

## Decision: two layers with explicit, different contracts

### Layer 1 — identity-attested edges (THE guarantee)

ADR 022 Ring 2 is promoted from "a mesh feature" (H6) to the
authoritative zero-trust enforcement plane:

- **Workload identity** is minted from the naming authority and the
  declared resource graph (SPIFFE-aligned URIs — the graph node IS the
  identity subject). Identity derives from *what a workload is declared
  to be*, never from where it happens to run or what IP it holds.
- **Every allowed edge** (the ADR 026 graph-scoped set) is realized as
  a mutually-authenticated channel: the receiving side refuses any peer
  not presenting the identity the graph authorizes — regardless of
  network reachability. A flat, hostile, or non-enforcing network
  changes nothing.
- **Enforcement travels with the workloads** (tunnel/sidecar/embedded
  listener realized by the mediation provider), which is why the
  guarantee is identical on Docker, Kubernetes, a VM fleet, or
  Terraform-provisioned cloud infrastructure.

**Mediation is a port, not a product.** A `MediationProvider`
capability seam (H6 defines it) abstracts the mediation plane exactly
as `ContainerRuntime` abstracts Docker/Kubernetes: OpenZiti is the
FIRST adapter (ADR 022's analysis stands), never the architecture.
SPIFFE-compatible identity is the interoperability bar so a SPIRE/mesh
adapter can implement the same port. Tight coupling to any single tool
— the owner's Calico concern, one level up — is excluded by
construction and guarded the same way the runtime port is (conformance
suite for mediation adapters).

### Layer 2 — network segmentation (defense-in-depth, honestly reported)

The existing compilation targets stay: Docker per-domain/per-edge
networks, Kubernetes NetworkPolicies, and (future) Terraform security
groups — all compiled from the same declared graph. Their contract is
downgraded explicitly: **best-effort depth, never the guarantee.**
Two consequences:

1. **No enforcement-tool coupling.** platformctl emits standard,
   portable objects (NetworkPolicy is a Kubernetes API, enforced by any
   conforming CNI — Calico in CI is test infrastructure chosen to prove
   the layer CAN work, replaceable by Cilium or any enforcer with zero
   product change; Docker networks are plain Docker; security groups
   will be plain provider resources).
2. **Enforcement is observed, never assumed.** The platform probes
   whether the fabric actually enforces what was compiled (the
   TestNetworkPolicyEnforcementIsLive mechanism productized): `status`/
   preflight report `network isolation: enforced` or
   `network isolation: NOT ENFORCED by this cluster's CNI` — a user on
   a non-enforcing fabric is told, loudly, that only Layer 1 protects
   them. An unverifiable claim is treated as false, not as hoped-true.

### The claims table (what d7s may say, per configuration)

| Configuration | Honest claim |
|---|---|
| Layer 1 active (mediated edges) | Zero-trust: identity-attested, policy-authorized edges — on ANY substrate |
| Layer 2 only, enforcement observed active | Network-segmented least privilege (location-based; defense-in-depth) |
| Layer 2 only, enforcement observed absent | Isolation NOT enforced — reported in status/preflight; validate warns |
| Any configuration, `PolicyEngine` enabled (ADR 021/033) | Governance is auditable, not just enforced: every declared edge's admission is a structured decision event (docs/planning/08 I11 slog seam) and `platformctl policy audit` names, for every edge, WHY it is permitted (no matching deny, an exemption, or a `spec.access` grant) or denied (the specific rule) — including a denied-but-standing edge from a withdrawn allow (ADR 021's severing amendment) |

## Terraform / cloud substrates (future, recorded now)

The same two layers map cleanly: Layer 2 compiles the graph to security
groups/firewall rules (a provider like any other); Layer 1 rides
unchanged, because mediated workloads carry their own enforcement. This
is what makes the zero-trust claim *consistent* across Docker,
Kubernetes, and Terraform: the authoritative layer never depended on
the substrate in the first place.

## Consequences

- H6's spec is amended: deliverables now include the `MediationProvider`
  port + conformance expectations, SPIFFE-aligned identity from the
  naming authority, and per-edge authorization compiled from the ADR
  026 graph. OpenZiti implements; nothing consumes it by name.
- A new task (H8) productizes enforcement observation (the Layer 2
  honesty probe) — small, high-leverage, precedes any GA language about
  isolation.
- ADR 026 is re-scoped by this ADR: graph-scoped *network* access is
  Layer 2 of the same graph; the wording "least privilege" without
  identity attaches to Layer 1.
- Docs/marketing discipline: the claims table above is the only
  permitted phrasing.

## References

ADR 022 (rings; the Lattice raw-TCP lesson), ADR 026 (the graph as the
access-request set), doc 11 (decoupling verification — the chokepoint
Layer 2 compiles through; the GA caveat sweep that exposed
enforcement-by-assumption), SPIFFE/NIST 800-207 (doctrine).
