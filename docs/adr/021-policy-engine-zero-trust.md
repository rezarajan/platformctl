# ADR 021 — Policy engine: organizational guardrails and zero-trust posture

**Status:** accepted (2026-07-22) — scheduled as doc 08 Stage H (H3/H4);
the runtime-enforcement architecture (domains, workload identity, mTLS
mediation) is ADR 022, scheduled as H5/H6.

## Context

Cloud platforms enforce organizational security posture through
policy-as-code admission controls (AWS SCPs/Config, Azure Policy, GCP Org
Policy, Kubernetes admission + OPA/Gatekeeper/Kyverno). Datascape already
*implements* many zero-trust mechanics as defaults — but defaults are not
governance. Nothing today lets a team declare "in this repository, a
plaintext Connection is forbidden" and have every `validate`/`plan`/`apply`
enforce it deterministically.

### What Datascape already provides (mechanism inventory — the enforcement targets)

| Zero-trust principle | Shipped mechanism |
|---|---|
| Default-deny networking | K8s Namespace default-deny + allow-same-namespace NetworkPolicy (B7); per-container external holes only where an access mode requests them; Docker loopback-default binds (doc 07 §0.7) |
| Least privilege | Minimal RBAC role + preflight sync (B5/B6); dedicated replication/monitoring DB users; no secret values in state/logs/output, fingerprints only (ADR 012/013) |
| Explicit trust boundaries | `Insecure` labeling on every endpoint; TLS termination at the entrypoint (C8); port audiences (F2) |
| Supply chain | Version+digest-pinned images (A10); private-registry auth via SecretReference (A1) |
| Blast-radius control | Ownership labels — unlabeled objects never touched (ADR 013); `protect`/`deletionPolicy`; NFR-3 double flags |

## Can Datascape enforce policy algorithmically and deterministically?

**Yes.** The evaluation inputs are all closed and deterministic:
the resolved manifest graph, the computed plan (a pure diff, NFR-1), the
lint findings (ADR 020, themselves deterministic), and declared/derivable
facts (schemes, audiences, image refs, backends, gates). A policy
evaluation is a pure function `(policies, envelopes, graph, plan,
findings) → decisions`, running inside the existing validate-time
completeness contract (ADR 011): **a policy violation is caught before
anything is touched, with the same DX as every other validate error.**
Plan-scoped policies additionally gate `apply`/`destroy` on the diff
("no deletes of kind Dataset in CI") — still deterministic.

## Decision (design)

### 1. Policy is a distinct input, not a manifest kind

Policies govern what may be applied; putting them inside the governed set
would let the set amend its own guardrails. They load from a separate,
explicit channel: `--policies <dir>` and/or a conventional
`.datascape/policies/` directory, schema-validated like any kind
(`policy.datascape.io/v1alpha1`). Precedence: all loaded policies apply;
`deny` cannot be overridden by a later `allow` (deny-wins, the SCP
convention).

### 2. Policy language: typed rules first, engines later

A deliberate ADR-003-shape choice: **no new dependency class initially.**
Embedding OPA/Rego or CEL buys expressiveness at the cost of a second
language, non-obvious determinism review, and a heavyweight dependency.
The initial format is a typed, JSON-Schema-validated YAML rule with a
closed vocabulary — which covers every zero-trust control in scope:

```yaml
apiVersion: policy.datascape.io/v1alpha1
kind: Policy
metadata: {name: prod-zero-trust}
spec:
  rules:
    - id: no-plaintext-connections
      match: {kind: Connection}                 # + optional label/name selectors
      assert: {field: spec.scheme, in: [https]} # field/equals/in/absent/matches
      effect: deny                              # deny | warn
      message: "prod requires TLS-terminated Connections (ADR 018/C8)"
    - id: images-from-corp-registry
      match: {kind: Provider}
      assert: {field: spec.configuration.image, matches: "^registry\\.corp\\..+@sha256:"}
      effect: deny
    - id: protect-data
      match: {kind: [Dataset, Source]}
      assert: {field: metadata.protect, equals: true}
      effect: deny
    - id: no-isolation-optout
      match: {kind: Provider}
      assert: {field: spec.runtime.networkPolicy, notEquals: "none"}
      effect: deny
    - id: secrets-from-vault-or-k8s
      match: {kind: SecretReference}
      assert: {field: spec.backend, in: [vault, kubernetes]}
      effect: deny
    - id: escalate-duplicate-capture
      matchFinding: {code: DL001}               # promote a lint to enforcement
      effect: deny
    - id: no-dataset-deletes-in-ci
      matchPlan: {action: delete, kind: Dataset}
      effect: deny
```

The rule vocabulary (field selectors on resolved envelopes; finding
selectors over ADR 020's codes; plan selectors over actions×kinds; an
external-egress selector over Connection targets with host/CIDR patterns;
a gate selector) is a **closed, versioned surface** — extending it is a
schema change with the usual doc-03-class discipline. An OPA/CEL backend
can later mount behind the same evaluation seam if real usage exhausts
the vocabulary; that would be its own ADR with the dependency argument
made explicitly.

### 3. Enforcement points and exemptions

- `validate`/`plan`/`apply`/`destroy` all evaluate; `deny` → the standard
  validation-error exit path naming the rule id, message, and resource.
  `warn` → reported, exit 0.
- Exemptions mirror lint waivers and SCP practice:
  `policy.datascape.io/exempt: "<rule-id>: <reason>"` annotations, but —
  unlike lint waivers — **only honored if the policy itself declares
  `exemptible: true`**. A non-exemptible deny has no in-manifest escape;
  that is the point of governance.
- `platformctl policy test <dir>` evaluates policies against a manifest
  set without the rest of validate (CI-friendly authoring loop);
  `platformctl explain <rule-id>` extends the E4 catalog to policy ids.

### 4. The built-in zero-trust pack

Shipped as a documented, versioned starter (`platformctl policy init
zero-trust` writing the pack for local tailoring — the blueprint pattern
applied to governance): the seven rules above plus require-digest-pins,
require-`replication ≥ 3`-when-HA, forbid `Insecure` endpoints on
non-loopback audiences, restrict `external` Connection targets to an
allowlist, forbid the `env` secret backend outside dev. Each rule cites
the mechanism ADR it enforces — policy never invents posture, it makes
the shipped posture mandatory.

### 5. Explicitly out of scope, with reasons

| Not in scope | Why |
|---|---|
| Runtime admission for non-Datascape actors (an operator `kubectl`ing into the namespace, a rogue process on the Docker host) | Datascape is a reconciliation CLI, not a resident admission controller; the runtime's own controls (K8s RBAC/admission, host security) govern other actors. Datascape's contribution is detection (`drift`) and re-convergence (`apply`), plus the NetworkPolicies it provisions. Stated in doc 09 §4.1: one-shot control plane, by design. |
| Per-request identity / mTLS service mesh / L7 authz between workloads | That is a mesh/gateway product. Datascape wires boundaries (TLS termination C8, isolation B7, tunnels D5) and can *require* them via policy — it does not proxy traffic. |
| Being a secrets vault or an IdP | NG-class: Datascape references secrets (ADR 013) and has no user identity model (NG3 — single-operator CLI). Policy governs *what* is applied, not *who* applies. |
| Control-plane AuthZ (who may run `apply`) | Meaningless while NG3 holds. When multi-operator arrives (shared state exists — ADR 003), policy-bundle signing + holder identity is the natural extension; recorded as future, not designed here. |
| Runtime threat detection / audit SIEM | Provision-time tool; the monitoring stack (C9) exposes metrics, and structured logs exist (NFR-4) — consumption belongs to security tooling. |

## What is required (implementation inventory, for when scheduled)

1. `internal/domain/policy` (kind, closed rule vocabulary, JSON Schema) +
   loader on the separate channel; 2. deterministic evaluator wired into
   `loadAndValidate` after compatibility + lint, and into plan/apply on
   the diff; 3. the zero-trust pack + `policy init`/`policy test`;
   4. explain-catalog integration + completeness guards; 5. docs
   (onboarding §governance, doc 03 sibling reference page); 6. lint
   dependency: ADR 020 lands first (policies consume findings).

## References

ADR 011 (the enforcement point's contract), 012/013 (the mechanisms
policy makes mandatory), 018 + C8 (TLS boundary), 020 (findings as policy
facts), doc 09 §4 (plane analysis; one-shot posture), doc 01 NG2/NG3.
