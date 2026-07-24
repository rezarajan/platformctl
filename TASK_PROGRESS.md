# M5 — graph×mediation composition fix (doc 08 §7.12 M5)

## Status: fix implemented + unit-proven; live Docker proof pending.

## The bug (diagnosed doc 11 2026-07-23 capstone)
GraphScopedAccess + MediatedConnection did not compose: a consumer (e.g.
Debezium) reaching a mediated Connection only TRANSITIVELY —
Binding.sourceRef -> Source.connectionRef -> Connection — never got a
graph-scoped edge to the Connection's realizing (mediation) Provider,
because DeriveEdges only flattens DECLARED manifest edges and
graphaccess.ContainerOf never collapses a pass-through Source (no
providerRef of its own) onto anything. The consumer and the dial-side
tunneler (created under the mediation Provider's OWN domainRuntime self,
per engine/domainruntime.go's newDomainRuntime — a Connection's own
reconcile resolves self from ITS OWN providerRef) ended up on disjoint
per-edge networks.

## The fix
New `graphaccess.MediatedConsumerEdges(g, resources, capable)`
(internal/application/graphaccess/graphaccess.go): for each
CompileMediatedConnections entry, walks the FULL transitive dependent set
(graph.Dependents(mc.Connection)) and collapses each dependent to the
first Provider-kind container via ContainerOf; emits one synthetic
Edge{From: container, To: mediationProviderKey} per discovered container.
Both endpoints are already literal, self-resolving container keys, so the
EXISTING (unmodified) EgressPeers/IngressPeers/MembershipEdges pick up
BOTH directions from this one edge — no change to ContainerOf's pinned
"Connection resolves to itself" behavior, no change to the per-edge
network mechanism itself. The dark TARGET (mc.Targets) is never touched.

Wired in internal/application/engine/graphscoped.go's
`deriveGraphAccessEdges` (now takes a `graphaccess.MediationCapable`
predicate and appends `MediatedConsumerEdges`) and a new
`(*Engine).mediationCapable` predicate (type-asserts a registry-
constructed reconciler.Provider to reconciler.MediationCapableProvider,
mirroring graphaccess's own doc-comment-mandated pattern). Call site:
engine.go resolveRequest, `deriveGraphAccessEdges(byKey, e.mediationCapable)`.

## Verification done
- gofmt clean, `go build ./...`, `go vet ./...`, `go vet -tags integration ./...` all clean.
- `go test ./...` — all green (see scratchpad m5-gotest2.log).
- golangci-lint v2.12.2 (pinned in CI) — 0 issues on touched packages.
- New unit tests:
  - graphaccess_test.go: TestMediatedConsumerEdgesFollowsConnectionRefTransitively
    (proves the transitive Binding->Source->Connection edge, proves the
    dark target is never named), TestMediatedConsumerEdgesEmptyWhenNoMediationCapableProvider.
  - engine/graphscoped_test.go: TestGraphScopedAccessMediatedConsumerReachesTunnelerNotDarkTarget
    — end-to-end through the real domainRuntime decorator + fake runtime,
    proves consumer and mediation-Provider container share a network,
    proves NO edge to the dark target. Manually verified this test FAILS
    without the fix (reverted MediatedConsumerEdges append -> reproduces
    the exact diagnosed symptom: "container mesh is not attached to
    network access-...").
- Byte-identical pins (TestGraphScopedAccessGateOffIsByteIdentical, the
  worked-example test, non-mediated graphscoped tests) all still pass
  unchanged.

## Remaining for this task
- [ ] Live Docker proof: graph-scoped + mediated CDC path reaches RUNNING
      (the exact capstone-failing case). Candidate: extend
      cmd/platformctl/testdata/openziti-scenario with GraphScopedAccess=true,
      or build a small dedicated scenario.
- [ ] K8s: check KUBECONFIG=/tmp/claude-1000/platformctl-rbac/platformctl.kubeconfig
      liveness; record gap if dead.
- [ ] Add new integration test as a suite row in scripts/test-impact.sh.
- [ ] Append M5 Done-note to doc 08 (additive).
- [ ] Final commit + report.
