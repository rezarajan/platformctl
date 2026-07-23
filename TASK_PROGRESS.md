# H7 (doc 08 §7.7, ADR 026 as amended) — task progress

Worktree: agent-ac2d83045599a82da. Branch: worktree-agent-ac2d83045599a82da.
Started from `git merge main --no-edit` (pulled in ADR 026 addendum, ADR 027,
ADR 028, H5/H6/H8, openziti). H6's own TASK_PROGRESS.md (its own completed
task, already merged to main) is being REPLACED by this file — out of scope
here except as a design precedent.

## Reading done

- docs/adr/026 (graph-scoped access) INCLUDING the 2026-07-23 addendum
  (deterministic /28 subnets from a dedicated supernet — the "tens of
  networks" bound is dead, thousands of edges is the real envelope).
- docs/adr/027 (enforcement layering — H7 is Layer 2, best-effort,
  observed by H8; claims table).
- docs/adr/022 + addendum (domain-of-record: containers live in the
  REALIZING PROVIDER's domain).
- docs/planning/08 §7.7 H7 + amendment (verbatim spec + accept bar).
- docs/planning/11 (owner's worked example: A/R1->{B/X,C/Y} A/R2->{B/X}
  R2->C/Y FAILS R1->other-B FAILS).
- internal/application/graphaccess/graphaccess.go (H6's DeriveEdges/
  MediatedSubset/CompileMediatedConnections — consumed, not duplicated).
- internal/application/engine/domainruntime.go (the H5 decorator: token
  translation, per-domain holes for Connections, EnsureNetwork/
  EnsureContainer/EnsureVolume/ProbeReachable interception points).
- internal/application/engine/engine.go resolveRequest (the ONE chokepoint
  building the decorator; byKey is already the full validated resource set).
- internal/domain/graph/graph.go (exactly which spec fields create edges;
  Provider/Connection are the only Kinds with runtime containers — every
  other Kind's providerRef names the container that actually realizes it).
- internal/domain/hostport (deterministic hash-into-range precedent for
  the /28 subnet allocator).
- internal/domain/naming (RuntimeObjectName/NetworkName/WorkloadIdentityURI
  — the single-authority pattern this task extends).
- internal/ports/runtime/{runtime.go,isolation.go} (NetworkSpec/
  ContainerSpec shape; IsolationObserver — H8's Layer-2-honesty capability,
  not touched by this task except that Docker's "enforced by construction"
  answer still holds after this task, since Docker per-edge networks ARE
  the mechanism, nothing new to probe).
- internal/adapters/runtime/kubernetes/{network.go,convert.go} (B7's
  default-deny + allow-same-namespace pair; the per-container
  external-ingress-policy pattern this task's per-edge policy mirrors).
- internal/adapters/runtime/docker/docker.go EnsureNetwork (no subnet
  support today — added by this task).
- internal/domain/policy/policy.go + internal/application/policy/
  evaluator.go (matchEdge.crossDomain — the exact pattern matchGrant
  mirrors).
- schemas/v1alpha1/meta.json, schemas/policy/v1alpha1/policy.json,
  schemas/v1alpha1/binding.json (additionalProperties:false per Kind —
  spec.access must be added to each Kind schema individually).

## Design (locked in before coding)

1. **Container-of-record.** Every non-Provider/Connection Kind's runtime
   footprint IS its own providerRef's container (Provider/Connection are
   the only Kinds with runtime objects). `graphaccess.ContainerOf(k,
   resources)` resolves any resource.Key to the Provider/Connection key
   that actually realizes it.
2. **Membership.** `graphaccess.MembershipEdges(edges, self, resources)`
   collapses DeriveEdges' full graph edge set onto container-of-record
   granularity for `self` (a Provider/Connection key), unions in
   `spec.access` wide grants (every other container in a granted
   namespace), dedupes, excludes self (own replica/internal topology is
   unaffected — ADR 026 decision 1's "brokers reach brokers"). Policy
   `matchGrant` denies are validate-time only (mirrors H5's crossDomain
   precedent exactly, per domainruntime.go's own holes comment) — the
   engine-side compiler trusts byKey as already policy-filtered.
3. **Docker realization.** Under the gate, `domainRuntime.translate` maps
   the home token to a PER-CONTAINER-PRIVATE network
   (`naming.NetworkName(base,domain)+"-own-"+hash(ownerKey)`) instead of
   the shared domain-wide network — this is the only way "networks are
   the only isolation primitive" can express pairwise access at all
   (keeping the flat/domain-shared network would make the gate a no-op on
   Docker: everyone already reaches everyone on it). Additionally, for
   every peer in MembershipEdges, both endpoints join a shared,
   deterministically-named+subnetted per-edge network
   (`naming.EdgeNetworkName` + `internal/domain/subnet`, addendum's /28
   scheme from a dedicated supernet, default documented). Same edge (pair)
   -> same network/subnet, order-independent, unit-pinned.
4. **Kubernetes realization.** Namespace/domain assignment is UNCHANGED
   (H5 stays as-is) — only the NetworkPolicy changes: under the gate,
   `buildNetworkPolicies` drops the allow-same-namespace rule (default-deny
   only), and a new per-container policy
   (`internal/ports/runtime.ContainerSpec.AllowFromPeers`, mirroring the
   existing per-container external-ingress-policy pattern) opens ingress
   from exactly the peers whose graph edge reaches this container —
   `NetworkPolicyPeer{NamespaceSelector: peer's namespace, PodSelector:
   peer's io.datascape.name}`. K8s NetworkPolicy governs the destination's
   ingress only (egress is unrestricted by construction in this codebase),
   so only the "who may reach ME" (reverse-edge) direction is compiled per
   container — no analog of Docker's "provider-private home network" is
   needed since the namespace boundary + explicit peers already achieve
   the pairwise bar without touching topology.
5. **Zero provider edits, one chokepoint.** All of the above lives in
   `internal/application/engine/domainruntime.go` (extended) +
   `internal/application/engine/graphscoped.go` (new) + the two runtime
   adapters' Ensure* methods (core-only, no provider package touched).
   `newDomainRuntime` gains two new params: `graphScoped bool` (gate
   state) and the resource set's derived edges — computed once per
   `resolveRequest` call from `byKey` via `graph.Build` +
   `graphaccess.DeriveEdges` (cheap at this codebase's manifest sizes;
   O(n) per call, same order as the pre-existing `consumerDomainHoles`
   O(n) scan). Gate OFF: `newDomainRuntime` takes the exact same code path
   as before this task, byte-for-byte (archtest-pinned).
6. **Wide grants + policy.** `spec.access: [{namespace: <ns>}]` added to
   every Kind schema with a realizing container's worth of network
   identity (provider.json, connection.json, binding.json, source.json,
   eventstream.json, dataset.json) via a shared `meta.json#/$defs/
   accessGrant` fragment. `policy.datascape.io` gets `matchGrant:
   {namespace: <ns>}` (mirrors `matchEdge.crossDomain` exactly) so a
   `deny` rule refuses any resource declaring a grant to that namespace at
   validate time — before compilation, per decision 2.
7. **Gate.** `GraphScopedAccess`, Alpha, disabled (`cmd/platformctl/main.go`).

## Status

- [x] Design locked (this file)
- [x] internal/domain/subnet: deterministic /28 allocator + tests
- [x] internal/domain/naming: EdgeNetworkName/PrivateNetworkName + tests
- [x] internal/ports/runtime: NetworkSpec.Subnet, ContainerSpec.AllowFromPeers/NetworkPeer, IsolationGraphScoped
- [x] internal/adapters/runtime/docker: subnet on EnsureNetwork
- [x] internal/adapters/runtime/kubernetes: per-container graph-scoped ingress policy; allow-same-namespace opt-out (drift-heals existing namespaces too)
- [x] internal/application/graphaccess: ContainerOf/ContainerDomain/AccessGrants/EgressPeers/IngressPeers/MembershipEdges + tests
- [x] internal/application/engine: domainruntime.go extension + graphscoped.go + engine.go wiring (Registry.GateEnabled)
- [x] internal/domain/policy + internal/application/policy: matchGrant + tests
- [x] schemas: spec.access on 6 kind files + meta.json fragment + policy.json; doc 03 same-commit; docs/reference regenerated
- [x] gate registration (main.go) + doc 04 §12
- [x] explain catalog: no new status.Reason tokens needed (H7 has no new Ready/Drift semantics beyond what already exists — pure network-layer compilation); explain_catalog_test.go stays green untouched
- [x] archtest pin: domain_decoupling_test.go extended (naming.PrivateNetworkName/EdgeNetworkName, the H7-specific graphaccess functions) + a positive-case test proving the new fence catches a violation, deliberately narrower than "any graphaccess.* symbol" since H6's openziti already has a legitimate provider-side use of the package
- [x] unit tests across all of the above (subnet, naming, graphaccess, engine, kubernetes adapter, policy) — all green
- [x] accept scenario: fake-runtime engine test (positive+negative, both directions, wide grant, gate-off pin) + LIVE Docker proof (green twice) + LIVE Kubernetes proof (positive proofs pass; negative proof honestly skips — this cluster's CNI does not enforce NetworkPolicy, same as H5/H8's own documented caveat; structured exactly like TestNetworkPolicyEnforcementIsLive, hard-fails under PLATFORMCTL_REQUIRE_NETPOL_ENFORCEMENT)
- [x] gate-off byte-identical pin test (unit + live Docker)
- [x] gofmt/vet/build (both tag sets)/golangci-lint 0
- [x] go test ./... unfiltered, true-exit=0
- [x] impact sweep (--only graphscoped,domains): Docker legs green twice; K8s leg run live twice against the minted RBAC kubeconfig (both honest-skip on the negative assertion, both green on positive assertions + cleanup)
- [x] doc 08 H7 Done-note (additive bullet after the Amended note)
- [ ] final squashed commit (this is the last WIP commit before it)

## Deviations / live findings (recorded as found, not silently worked around)

1. **`runtime.IngressCapableRuntime` cannot signal "is this Kubernetes" through the registry.** First implementation tried `rt.(runtime.IngressCapableRuntime)` to distinguish Docker/fake ("network is ACL-by-membership") from Kubernetes ("network is a namespace boundary"). Found LIVE (Docker apply produced only the bare "datascape" network, none of the H7 machinery): `registry.haGuardRuntime` unconditionally implements `IngressCapableRuntime` (an explicit 6-method delegation trio added for a DIFFERENT reason — docs/adr/018's provider-facing promotion gotcha), so the assertion always succeeded, on every runtime, through the registry — Docker included. Fixed by passing `p.RuntimeType` (the plain "docker"/"kubernetes"/"fake" string already resolved in `resolveRequest`) into `newDomainRuntime` explicitly instead of inferring it via a capability assertion.
2. **Stale `domains-scenario`/`domains-k8s-scenario` fixtures.** A PRE-EXISTING coherence check (`internal/application/compatibility`, commit `d0017d5`, part of the merge — not this task's own code) refuses a Connection whose declared `metadata.domain` doesn't match its realizing Provider's domain. The two H5 fixture manifests were never updated after that check landed (`domains-it-edge` had no domain while `domains-it-bridge` declared `domain: alpha`), so `TestDomainSegmentationEndToEnd`/`...OnKubernetesEndToEnd` were failing on `main` before any of this task's own changes — discovered only because this is the first time this session ran them live. Fixed (one line each: `domain: alpha` added to `domains-it-edge`) to unblock verifying this task didn't regress H5; both domains tests pass live post-fix.
3. **`ProbeReachable`'s Docker implementation cannot isolate a multi-homed vantage container to one interface.** The original "decisive negative proof" (dial B/X from R1's own private home network, expecting failure) FALSE-PASSED live: Docker's real `ProbeReachable` execs a dial from an *existing managed container found on the named network* — R1 is legitimately multi-homed (its private home network AND its own edge networks), so the exec'd dial succeeded via R1's edge-network interface regardless of which network name was passed as the vantage. The underlying network segmentation IS correct (confirmed via `docker inspect`: B/X is never a member of R1's private home network); only the LIVE-TEST METHODOLOGY was flawed for this specific assertion. Fixed by re-anchoring the live assertion on `other-b`, which declares no edge at all and is therefore genuinely single-homed — a confound-free vantage. The fake-runtime unit test (`internal/application/engine/graphscoped_test.go`) did NOT need this fix: the fake adapter's `ProbeReachable` checks the TARGET's own declared `Networks` list directly rather than exec'ing from a vantage container, so it has no multi-homing confound — documented in that test's own comment for anyone comparing the two assertions and wondering why they're anchored differently.
4. **K8s live negative proof: not enforced by this shared cluster's CNI**, exactly the same documented limitation `TestDomainSegmentationOnKubernetesEndToEnd` (H5) and `TestNetworkPolicyEnforcementIsLive` (H8) already carry — the positive proofs (R1→X, R1→Y, R2→X) and the actual compiled `NetworkPolicy` objects (verified directly via `kubectl get networkpolicy -o yaml`: X's policy admits ingress from exactly {r1's namespace+pod, r2's namespace+pod}; `other-b`'s per-container policy does not exist at all since nothing references it; `datascape-allow-same-namespace` is absent from every namespace under the gate, `datascape-default-deny-ingress` present) are all live-verified and correct; only the CNI's actual enforcement of them is unverified locally. `PLATFORMCTL_REQUIRE_NETPOL_ENFORCEMENT` on a Calico-backed CI cluster makes this a hard failure instead of a skip.

