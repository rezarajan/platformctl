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
- [x] Live Docker run (flock-wrapped): PASS 26.73s, all 5 legs, zero
      residue (scratchpad/docker_leg4.log). Two earlier live-found fixes:
      Binding domain coherence; leg-5 NFR-3 double flags for the External
      Source's removal.
- [x] Live K8s run attempted: BLOCKED — minted kubeconfig token expired
      mid-session (kubectl auth can-i: yes minutes earlier, Unauthorized
      at run time). Per brief: recorded, token NOT re-minted, K8s leg
      code-complete/unverified. Compensating unit coverage added:
      TestDomainRuntimeQualifyTargetAddress (also fixed a pinned-network
      inconsistency it exposed: pinned => qualification no-op).
- [x] golangci-lint v2.12.2: 0 issues (merged tree, final).
- [x] doc 08 H9 Done-note appended (additive; criterion-3 box left
      UNCHECKED — Accept demands both runtimes green).
- [x] Final commit

## Coordinator correction (2026-07-23, mid-task)
- Merged main again (now at e993a07): H10 (CA pinning via EST/PKCS7,
  InsecureSkipVerify removed except the documented TOFU bootstrap fetch;
  enrollment JWTs moved Env->FileMount with waitTunnelEnrolled) and K1/K2
  (label grammar + selector policy vocabulary). Merge was CLEAN — no
  conflicts; verified my AddressQualifier fix (connection.go) and my
  listDialPolicies client-side-filter fix (client.go) both survived
  coherently on top of H10's rewrites.
- Re-examined my client fix against H10: main's H10 client.go STILL
  carries the broken `filter=type=%22Dial%22` query (confirmed via
  `git show main:...`), so my fix is a genuinely different defect
  (drift-detection/ObservedEdges broken since H6), NOT a duplicate of
  H10 — kept, applied cleanly by the merge itself.
- GPG signing is unavailable in this session (pinentry timeout/killed,
  reproduced twice). WIP + merge commits made with `-c
  commit.gpgsign=false` (one-off flag, no config change). Final commit
  will follow the brief's GPG protocol (attempt signed; else leave
  staged + COMMIT_MSG.txt).

## Live Docker findings so far (pre-merge, recorded in 4b5eec9)
1. Binding metadata.domain must match realizing Provider's domain
   (ADR 022 addendum coherence check) — fixed in both testdata files.
2. listDialPolicies filter defect (above).
3. Manual live apply of the Docker scenario succeeded end-to-end
   (10/10 Ready, ~24s); Ziti state manually verified EXACT: 1 service
   (spiffe-datascape-default-analytics-connection-xd-conn), 1
   datascape-mediated identity
   (spiffe-datascape-default-payments-source-xd-src), 1 Dial policy
   (dial-<identity>-<service>) with exact @id role refs. Manually
   destroyed cleanly afterward (9 destroyed, external Source no-op'd).

## Names/ports used (avoid colliding with other suites)
- Resources: xd-pg, xd-mesh (ctrl/router), xd-conn, xd-rp, xd-dbz, xd-src,
  xd-events, xd-cdc. Docker host ports: controller 12895, connection port
  25795, redpanda kafka 19295, debezium connect 18295.
- Docker leg postgres volume "xd-pg-data", redpanda volume "xd-rp-data"
  (providerkit.EnsureInstance's "<name>-data" convention) — if the live
  run reports Janitor residue on these, the actual name differs and needs
  correcting from what EnsureInstance/postgres.go/redpanda.go actually do.

---

# E6 conformance retrofit — progress (2026-07-23)

This worktree was reused for a new, unrelated task after H9 (above) closed:
docs/planning/08-production-readiness-plan.md §7 E6 done-note's recorded
follow-up — retrofit `conformance_test.go` (internal/ports/reconciler/
conformance.Run harness) onto the remaining shipped providers, per ADR
028's fast-tier bar (<=60s/test, fakes only), following the
noop/redpanda/proxy exemplar pattern (docs/contributing/
provider-authoring.md).

## Setup
- Worktree fast-forward merged to main (0456b72) before starting — clean,
  no conflicts, `go build ./...` verified after.

## Scope
13 providers, in the brief's order: s3, s3sink, debezium, grafana,
prometheus, nessie, openlineage, trino, wireguard, jdbcsink, s3source,
ingress, placeholder. NOT touched: postgres, mysql, openziti, dbjob (other
agents own those areas).

## Scoping method
For each provider, read every Reconcile/Probe path per Kind and classified
it: **fast-tier-provable** (Ready determination rests solely on
runtime.ContainerRuntime primitives — EnsureContainer/EnsureNetwork/
EnsureVolume/WaitHealthy/Inspect/Remove — settling on the container's own
declared HealthCheck, which the fake always reports healthy for; NO real
protocol dial anywhere in the path) vs. **out of scope** (Reconcile OR the
mandatory post-Reconcile Probe conformance.Run's Settledness subtest
invokes needs a real application-layer protocol dial — HTTP GET/POST,
Kafka Connect REST, S3 API, a runtime-generated status file — with no seam
the fake can serve honestly without impersonating that technology's real
API surface; ADR 028 §2's fake-honesty rule would require pinning any such
fake against a real system's observed behavior, out of this retrofit's
scope). This is the SAME line redpanda's own exemplar drew (broker/
container-lifecycle in; EventStream/real-Kafka-admin out) — applied
uniformly, including to plain HTTP (per docs/contributing/
provider-authoring.md §6's own framing: "a real application-layer wire
protocol" is out of scope regardless of how simple the protocol is; HTTP
GET-returns-200 checks were deliberately NOT special-cased as "trivial
enough for a raw-listener fake" — that would have required a
never-before-built HTTP-response fake-technology harness needing ADR 028 §2
pinning to trust, which this retrofit did not build).

## Result: full harness (conformance.Run, all 7 subtests green + CapabilityChecks)
- placeholder — Provider (only Kind), zero capability interfaces.
- s3 — Provider/instance (single-container path only; Dataset + node-set
  path scoped out, real S3 API). CapabilityChecks: ValidateSpec x2.
- s3sink — Provider/worker (Binding/connector scoped out, real Connect
  REST). CapabilityChecks: ValidateSpec, ValidateBindingOptions.
- debezium — Provider/worker (Binding scoped out). CapabilityChecks:
  ValidateSpec, ValidateBindingOptions.
- jdbcsink — Provider/worker (Binding scoped out). CapabilityChecks:
  ValidateSpec, ValidateBindingOptions.
- s3source — Provider/worker (Binding scoped out). CapabilityChecks:
  ValidateSpec, ValidateBindingOptions.
- wireguard — Provider (network-only, zero containers; Connection scoped
  out — real dial-through AND a runtime-written handshake-status file the
  fake cannot fabricate). CapabilityChecks: ValidateSpec x2.

## Result: scoped out, doc comment + bonus direct ValidateSpec test (no conformance.Run)
- grafana — real HTTP login/health dial unconditional even on first
  Reconcile (CredentialRotation.NoPreviousOrUnchanged still pings).
- prometheus — real HTTP + JSON /api/v1/targets count check.
- trino — real HTTP /v1/info dial, two-container coordination.
- ingress — Reconcile itself is dial-free, but conformance.Run's mandatory
  Probe (Settledness subtest) requires caddyReady's real admin-API dial.

## Result: scoped out, doc comment only (no capability interface to test either)
- nessie — real HTTP + stateful branch-create/exists REST semantics.
- openlineage — real HTTP dial, zero capability interfaces declared.

## Status
- [x] Read CLAUDE.md, doc 08 E6 done-note, ADR 028, provider-authoring.md,
      the three exemplars (noop/redpanda/proxy conformance_test.go).
- [x] Read every one of the 13 providers' Reconcile/Probe/Destroy bodies
      plus capability method set; classified per above.
- [x] Wrote all 13 conformance_test.go files.
- [x] gofmt clean; `go build ./...` clean; `go vet ./...` and
      `go vet -tags integration ./...` both clean.
- [x] All 13 new test files green under `go test -v` AND `go test -race`
      (sub-1.1s per package including race-instrumentation startup —
      comfortably under the 60s fast-tier budget).
- [ ] Full unfiltered `go test ./...` sweep (in progress).
- [ ] golangci-lint v2.12.2.
- [ ] doc 08 E6 additive done-note recording the retrofit completion.
- [ ] Final commit.
