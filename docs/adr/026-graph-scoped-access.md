# ADR 026 — Graph-scoped access: least privilege compiled from declared references

**Status:** accepted (2026-07-22); scheduled as doc 08 H7. **Prompted
by:** the project owner: resources must not be merely namespace-scoped —
a resource in namespace A that requests access to one resource in B and
one in C gets exactly those two; a sibling in A that requested only the
B resource gets only it; **neither can reach anything else in B or C
unless explicitly requested**.

## Context

The isolation story so far is group-scoped: Kubernetes namespaces get
default-deny + allow-same-namespace walls (B7); domains (H5, ADR 022
Ring 1) add coarse segmentation with mediated crossings. Both answer
"which *groups* may talk"; the owner's requirement is "which *pairs*
may talk" — the principle of least privilege at resource granularity.

The decisive observation: **the platform already holds the complete
access-request graph.** Every manifest reference is an explicit,
reviewed, versioned access declaration — a Binding's
sourceRef/targetRef/providerRef, a Source's connectionRef, a
Connection's `via` and consumers, `warehouseRef`/`catalogRef`,
`spec.secretRefs`, `observers`. References are namespace-qualified
(meta.json), so cross-namespace requests are first-class. Nothing new
needs declaring for the common case: the graph IS the request set.

## Decision

1. **Access compiles from the reference graph.** When the
   `GraphScopedAccess` gate is enabled, a workload may reach exactly
   the endpoints its resource's declared references imply (plus its
   realizing provider's own internal topology — brokers reach brokers),
   and nothing else. No reference edge → no path, same namespace or not.
2. **Wide grants are explicit declarations, not defaults.** "All of
   namespace B" is representable only by an explicit grant (a
   `spec.access: [{namespace: b}]`-class field on the requesting
   resource, shape finalized in H7) — visible in review, visible to the
   H3 policy engine (a `matchGrant` selector lets organizations deny or
   constrain wide grants). Absent a grant, the default is the minimal
   edge set. Policies can also deny *specific* graph edges (H5's
   crossDomain precedent generalizes to edge selectors).
3. **Realization is core-only — zero provider edits.** This rides the
   decoupling contract verified in doc 11 (the H5 decorator): the
   engine derives each resource's *membership set* from the graph and
   injects it at the per-request runtime decorator; providers keep
   passing the logical platform-network token, byte-for-byte unchanged.
   An archtest already freezes this seam.
   - **Kubernetes:** default-deny stays; the allow-same-namespace rule
     is replaced (under the gate) by per-edge NetworkPolicies compiled
     from the graph — pod-selector→pod-selector+port rules, the
     natural K8s expression of pairwise access.
   - **Docker:** networks are the only isolation primitive, so pairwise
     access compiles to **per-edge networks**: each declared edge is a
     small network joined by exactly its two endpoint workloads (the
     transit-network/I1 pattern generalized). Scale note, stated
     honestly: Docker's default address pools bound the network count
     (order tens per daemon without configuration) — fine for dev-sized
     platforms, and the gate's docs say so; production fine-grained
     posture is Kubernetes (or the H6 mesh, below).
4. **Composition with the rest of the stack.** Domains (H5) remain the
   coarse walls between groups; graph-scoped access is the fine grain
   within and across them (both can be on; the intersection applies).
   ADR 022 Ring 2 (H6 mediated connections) is the *identity-verified*
   upgrade of the same edges — same graph, mTLS-attested instead of
   network-reachability-scoped. The progression is deliberate:
   namespace walls → domain walls → graph-scoped reachability →
   identity-attested edges, each subsuming the last, all compiled from
   the same declared graph.
5. **Ready means serving, still.** Compiled access is drift-checked
   like everything else: an out-of-band network attachment beyond the
   membership set is drift (the I1 blast-radius rule generalized);
   probes run from the consumer's vantage (a denied pair must FAIL a
   reachability probe — the negative proof is part of the accept bar).

## Out of scope, with reasons

- L7 authorization (per-topic ACLs, per-database grants): technology-
  specific privilege is provider territory (least-privilege DB users,
  C9 precedent) — this ADR governs network reachability.
- Runtime identity/attestation: ADR 022 Ring 2 (H6).
- Enforcing against non-platformctl actors on the host: doc 09 §4.1's
  one-shot posture; the runtime's own controls govern other actors.

## Gate

`GraphScopedAccess` — Alpha, **disabled** (it flips reachability
semantics for existing sets; opt-in until the negative-proof suite
soaks on both runtimes).

## References

Doc 11 (decoupling verification — the chokepoint this compiles
through), ADR 022 (rings), H5 (domains, the decorator), B7 (K8s
default-deny), ADR 015 (the connectivity plane the probes ride), I1
(transit networks — the per-edge pattern's precedent).
