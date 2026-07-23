# K4 progress

Task: docs/planning/08-production-readiness-plan.md §7.10 K4 — Label-derived
attributes through the mediation port (ADR 033 decision 4).

## Setup
- Worktree fast-forward merged to main (0456b72e39231d61237588c16d598b9719048ec0)
  cleanly — zero conflicts, worktree had zero unique commits ahead of that
  point.

## Design decisions
- Port: `mediation.WorkloadIdentity` gains `Labels map[string]string`
  (`internal/ports/mediation/mediation.go`) — carries an endpoint's
  metadata.labels through MintIdentity's return value and RealizeEdge/
  RevokeEdge's Edge.From/Edge.To. Purely additive; nil/empty is the
  pre-K4 common case.
- Gate delivery mechanism: `req.Runtime` is the only channel available to
  an adapter for an engine-resolved per-request fact like a feature gate's
  state (`reconciler.Request`'s field list is frozen by
  internal/archtest/request_facts_frozen_test.go) — mirrors H9's
  AddressQualifier exactly. New optional capability
  `runtime.LabelScopedAccessQuery` (`internal/ports/runtime/labelscope.go`),
  implemented ONLY by `internal/application/engine`'s domainRuntime
  decorator (new `labelScopedGate` field, set UNCONDITIONALLY —
  deliberately distinct from the existing `labelScopedAccessEnabled` field,
  which is zero-valued whenever GraphScopedAccess is off and feeds only the
  K3 grant-realization path; K4's attribute derivation rides the SAME gate
  INDEPENDENTLY of GraphScopedAccess per ADR 033's own addendum). Not added
  to archtest's `optionalCapabilities` list — that list only forces
  forwarding for capabilities a REAL runtime adapter implements; like
  AddressQualifier, this one is engine-only.
- Encoding (openziti adapter, `identity.go`): `label.<key>.<value>`, each
  segment sanitized through the SAME charset filter `identityRoleAttribute`
  already applies to a SPIFFE URI (factored out as
  `sanitizeRoleAttributeSegment`) — disjoint by construction from every
  existing identity/service-name attribute (those never contain ".").
  `labelRoleAttributes` sorts by key for determinism.
- Dial-policy semantics: `dialRoleRefs` returns `#<attr>` role-attribute
  references (Ziti's role-attribute selector) with `AllOf` semantics when
  an endpoint carries labels and the gate is on — mirroring K2's
  matchLabels-is-a-conjunction semantics, so admission and enforcement
  check the SAME fact the SAME way (ADR 027 Layer 1). Unlabeled endpoints
  (or gate off) keep the exact `@<id>` reference with `AnyOf` — byte-
  identical to pre-K4. A single shared `semantic` value on the policy body
  is sufficient because AllOf degrades to an exact match on a singleton
  list, so mixing one attribute-based side with one exact-id side under
  "AllOf" is safe.
- Idempotency / gate-off byte-identical pin:
  - Identity: `session.upsertIdentity` dispatches to
    `client.upsertIdentityConverge(..., converge=true)` only when the gate
    is on; converge=false (gate off) reproduces the EXACT pre-K4 call
    sequence on the already-exists path (findByName, return — zero extra
    GETs, pinned by `TestUpsertIdentityAlreadyExistsMakesNoExtraCallsWhenConvergeFalse`).
  - Service: `upsertService` already performed a GET+maybe-PATCH dance for
    `encryptionRequired` pre-K4 — roleAttributes convergence piggybacks on
    the SAME existing GET/PATCH (zero additional HTTP calls in any case);
    the create body omits the `roleAttributes` key entirely when empty
    (pinned by `TestUpsertServiceOmitsRoleAttributesFromCreateBodyWhenEmpty`).
  - Both convergence paths use order-independent comparison
    (`stringSetEqual`) so re-deriving the same label set never fires a
    spurious PATCH.
- Scope: only Dial policies are compiled (RealizeEdge already never
  realized Bind — connection.go's router-hosted terminator handles bind
  differently); "Dial/Bind" in the task brief is read as referring to the
  DialBind struct generically, not a new Bind-policy mechanism.
  ObservedEdges' decode stays lossy for attribute-based multi-ref policies
  (documented, not fixed — outside K4's own accept bar).

## Status
- [x] Read all required docs/ADRs/precedent files (CLAUDE.md, ADR 033,
      doc 08 §7.10 K1-K4, H9's Done-note, ADR 027, mediation port,
      graphaccess.CompileMediatedConnections, openziti client/connection).
- [x] Port change: `mediation.WorkloadIdentity.Labels`.
- [x] Runtime capability: `runtime.LabelScopedAccessQuery` +
      `domainRuntime.LabelScopedAccessEnabled()`.
- [x] openziti adapter: label→attribute encoding, gate-aware
      identity/service roleAttributes convergence, attribute-based Dial
      policies, connection.go/identity.go wiring.
- [x] Unit tests: identity_test.go (encoding, gate on/off, dialRoleRefs),
      client_test.go (convergence heal + skip-when-unchanged for both
      identities and services, gate-off byte-identical call-count pin,
      attribute-based policy semantic), domainruntime_test.go (raw gate
      forwarding regardless of GraphScopedAccess).
- [x] `gofmt` clean, `go build ./...` clean, `go vet` (both tag sets)
      clean, unfiltered `go test ./...` true-exit=0
      (scratchpad/k4-gotest-full.log).
- [ ] golangci-lint v2.12.2.
- [ ] Extend crossdomain_mediated_integration_test.go's exact-set
      assertion to cover attributes (labels on scenario endpoints +
      identities' roleAttributes + policy semantics via management API).
- [ ] Live Docker: openziti suite + crossdomain-mediated leg (flock-wrapped).
- [ ] Live K8s legs (KUBECONFIG token permitting) or record plainly if not.
- [ ] doc 08 K4 Done-note (additive).
- [ ] Final commit.
