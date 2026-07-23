# H9 progress

Task: docs/planning/08-production-readiness-plan.md §7.7 H9 — Stage H
criterion 3 composed end-to-end (cross-domain deny/exempt/mediate/withdraw).

## Setup
- Worktree was branched from an older `main` (7072b2d) that predates the
  H9 spec itself, ADR 021's severing amendment, testkit.Janitor (ADR 029),
  and the CI shard-partition guard. Fast-forward merged to current `main`
  (234cabe) before starting — worktree had zero unique commits, so this was
  a clean `git merge --ff-only main`. Verified `go build ./...` clean after.

## Design decisions (see final commit message for the full rationale)
- Scenario topology: Source domain "payments" (matches the real postgres
  backend Provider's domain), Connection/mesh/debezium/redpanda/EventStream
  all domain "analytics". This is the ONLY topology that satisfies all of:
  (a) the Binding-level crossDomain edge (Source vs EventStream domain) is
  genuinely cross-domain, (b) Debezium — the single container bridging
  both chains — needs no domain-hole to reach either the mediated
  Connection or redpanda (co-located with both), (c) the router genuinely
  crosses a domain boundary to dial the dark postgres backend, exercising
  the H6 K8s addendum's recorded FQDN gap live.
- This topology produces TWO crossDomain decisions from ONE policy rule
  (Binding sourceRef->targetRef edge, AND the Source's own connectionRef->
  Connection edge — both (payments,analytics)) — unavoidable given Source
  must hold connectionRef (Source resource model) and must differ in
  domain from both EventStream (for edge a) and Connection (forced by the
  network-reachability constraint above). Both get exemption annotations.
- Live K8s bug found and fixed (as anticipated by the task brief):
  `conn.Target` bypassed domain translation entirely (H6 K8s addendum's
  recorded gap). Fixed via a NEW optional runtime.AddressQualifier
  capability (internal/ports/runtime/address.go), implemented only by
  engine's domainRuntime decorator (internal/application/engine/
  domainruntime.go's QualifyTargetAddress) — Docker no-op, Kubernetes
  qualifies conn.Target's host to `<host>.<domain-namespace>.svc.cluster.
  local` when the resolved target's domain differs from the Connection's.
  openziti/connection.go calls it via type-assertion only (no
  .Metadata.Domain/naming.NetworkName/resource.NormalizeDomain reference
  in the openziti package — domain_decoupling_test.go's regex fence stays
  clean, confirmed green). Added `resolveRawMediatedTarget` (unfiltered
  graph.Build edge lookup, since graphaccess.CompileMediatedConnections
  deliberately excludes Provider-kind targets from MediatedConnection.
  Targets for identity-subject purposes — a DIFFERENT concern from "what
  domain does conn.Target's host live in").

## Status
- [x] Read all required docs/ADRs/precedent files.
- [x] Fast-forwarded worktree to current main.
- [x] AddressQualifier port + domainRuntime impl + openziti adapter fix.
  Build clean, archtest clean (domain_decoupling, wrapper_completeness,
  mediation_layering, request_facts_frozen), full `go test ./...` green.
- [x] testdata/crossdomain-mediated-scenario (Docker) + policies/policy.yaml
- [x] testdata/crossdomain-mediated-k8s-scenario (Kubernetes) + policies/
- [x] cmd/platformctl/crossdomain_mediated_integration_test.go (Docker, 5 legs)
- [x] cmd/platformctl/crossdomain_mediated_kubernetes_integration_test.go
      (TestOpenZitiCrossDomainPolicyOnKubernetesEndToEnd — CI shard name
      match confirmed via TestCIScenarioShardsPartitionKubernetesTests)
- [x] scripts/test-impact.sh suite row (`crossdomain-mediated`) +
      TestIntegrationSuiteMapCoversEveryTest green
- [x] gofmt/vet(both tag sets)/build all clean; full `go test ./...` green
- [ ] Live Docker run (flock-wrapped) — next step
- [ ] Live K8s run (flock-wrapped) — or record token-expired if blocked
- [ ] golangci-lint
- [ ] doc 08 H9 Done-note (additive)
- [ ] Final commit

## Names/ports used (avoid colliding with other suites)
- Resources: xd-pg, xd-mesh (ctrl/router), xd-conn, xd-rp, xd-dbz, xd-src,
  xd-events, xd-cdc. Docker host ports: controller 12895, connection port
  25795, redpanda kafka 19295, debezium connect 18295.
- Docker leg postgres volume "xd-pg-data", redpanda volume "xd-rp-data"
  (providerkit.EnsureInstance's "<name>-data" convention) — if the live
  run reports Janitor residue on these, the actual name differs and needs
  correcting from what EnsureInstance/postgres.go/redpanda.go actually do.
