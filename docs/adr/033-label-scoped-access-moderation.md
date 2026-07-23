# ADR 033 — Label-scoped access moderation: policy grants by selector, not by domain

**Status:** accepted (2026-07-23). **Prompted by:** the owner, after the
Stage H completeness audit (doc 11, 2026-07-23): "simple domain-based
policy access is too broad — specific resources/groups identified by
tags or labels should have their cross-resource access strictly
moderated by policy."

## Assessing the standpoint

The owner is right, with one precision worth recording: the *runtime*
already grants at resource granularity. ADR 026 (H7) derives per-edge
access from the declared reference graph — A/R1 reaches exactly
{B/X, C/Y} and nothing else — so wiring is least-privilege today. What
is still domain-shaped is the **policy vocabulary**: `matchEdge` speaks
only `crossDomain {from, to}`, so the *governance* layer can approve or
refuse edges only at compartment granularity. An operator who wants
"resources labeled `tier: gold` may be consumed only by resources
labeled `clearance: gold`" — or wants a wide grant narrower than a whole
namespace — has no words for it. The refinement, then, is not replacing
domains and not replacing the graph; it is giving policy the same
resolution the runtime already has, and making the two meet.

## The refined model: three planes, each with its own job

1. **Compartments (domains, ADR 022)** bound blast radius. Coarse on
   purpose; they stay.
2. **Wiring (the reference graph, ADR 026)** is the *need-to-connect*
   plane: no declared edge, no path, ever. Selectors never create
   access; nothing in this ADR weakens that.
3. **Moderation (policy, ADR 021 + this ADR)** decides which declared
   edges are *permitted*: `matchEdge` gains `selector {from, to}` —
   Kubernetes-style label selectors (`matchLabels`, `matchExpressions`)
   over each edge endpoint's `metadata.labels`. `crossDomain` becomes
   one special case of a general edge-matching vocabulary.

Access therefore requires BOTH planes: a declared graph edge AND policy
admission — default-deny composition, deny-wins within policy,
admission-time enforcement per ADR 021's amendment (severing =
admission refusal + manifest-driven teardown; withdrawal of an allow
can never silently auto-destroy a live path).

## The self-claim pitfall (the part that makes this zero-trust, not theater)

Label-based access has a classic failure mode: labels are written by
the same author who wants the access, so a consumer can label ITSELF
`clearance: gold`. Guardrails, in order of bite:

- **Label integrity is itself policy-governed**: `matchResource` gains
  the same selector form, so the zero-trust pack can ship rules like
  "deny any resource carrying `clearance: *` outside namespace
  `trusted`" — who may *wear* a label is as governable as who may
  *require* one. Single-operator sets get auditability; multi-team
  sets get real containment because policy files load only from
  outside the governed set (ADR 021 §1 — the governed manifests cannot
  bring their own permissions).
- **Grants are target-side**: a wide grant (`spec.access`) names the
  *audience* by selector; the consumer's own labels give it nothing
  unless a target's grant or a policy rule names them.
- **Labels flow into attested identity** (ADR 027 Layer 1): the
  mediation port carries label-derived attributes so the mediator's
  service-policies enforce by attribute at dial time — the runtime
  check matches the admission check, and the mediator's policy state
  is auditable evidence (the H9 pattern) rather than trust-me
  configuration.

## Decisions

1. `matchEdge.selector {from, to}` (label selectors) joins the policy
   vocabulary; `crossDomain` stays, documented as the compartment
   special case. Evaluation runs over the same graph-derived edges.
2. Label constraints: keys/values validated to the Kubernetes label
   grammar at validate (they already flow to runtime labels; a
   free-form value failing on one runtime only is the ADR 030 class).
3. `spec.access` wide grants gain a selector form scoped WITHIN a
   namespace; the bare namespace-wide form is **deprecated** — kept
   working, lint-flagged (new DL code: "namespace-wide grant; scope it
   with a selector") — because the owner's bar is explicit: nothing
   gets access beyond what it requested.
4. Mediation attributes: `MediationProvider` carries endpoint labels;
   the OpenZiti adapter maps them to identity role attributes and
   attribute-based service-policies. Adapter-agnostic at the port, per
   ADR 027.
5. Every policy edge decision is auditable: structured decision events
   on the I11 slog seam plus `platformctl policy audit` reconstructing
   *why* an edge is permitted (which rule, which selector, which
   grant). A decision that cannot name its justification is a bug.
6. Gate: `LabelScopedAccess` (Alpha, disabled) until the composed
   H9-style scenario passes on both runtimes.

## Out of scope

Auto-severing on policy change (rejected — ADR 021 amendment records
why); external attestation of labels (multi-operator identity
federation is Phase 8+ territory); renaming domains.

## Addendum (2026-07-23, K3): selector-scoped wide grants are INERT, not
downgrading, when LabelScopedAccess is off

Decision 3's selector form (`spec.access[].selector`) rides the SAME
`LabelScopedAccess` gate this ADR registers, independently of
`GraphScopedAccess` (which governs whether ANY grant compiles at all). The
gate-off question this addendum settles: does a selector-bearing grant fall
back to the wider bare-namespace form, or contribute nothing? **INERT**
(admits nobody) is the answer — never wider than the declared intent when
the gate that interprets that intent is off. Falling back to namespace-wide
would silently grant MORE than the manifest author asked for the moment an
operator flips a feature flag off, which is the exact self-widening this
ADR exists to prevent; the bare namespace-wide form remains available and
unaffected (it never depended on the gate to begin with — H7 shipped it
before this ADR existed) for an author who actually wants that breadth.
`internal/application/graphaccess.grantAdmits` implements this; the
`DL022` design lint (docs/adr/020 §4) flags every remaining bare grant so
the deprecated, ungated form is visible and gradually replaced. See
docs/planning/08 §7.10 K3's Done-note for the implementation.

## References

ADR 021 (+ 2026-07-23 amendment), ADR 022, ADR 026, ADR 027 (claims
discipline; attribute enforcement), doc 08 Stage K (K1–K5 sequencing),
doc 11 2026-07-23 Stage H audit.
