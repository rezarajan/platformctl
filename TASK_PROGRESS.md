# H6 (doc 08 §7.7) AS AMENDED BY ADR 027 — task progress

Worktree: agent-abe0640719d3041f0. Branch: worktree-agent-abe0640719d3041f0.
Started from main @ 7bc49d4 (`git merge main --no-edit` fast-forwarded from
abbbd1b, pulling in ADR 026/027, I10+I11; H5 is unmerged — a sibling
worktree — per the orchestrator's handoff; re-check before verification
phase, build against namespaces if it still hasn't landed).

## Reading done (in the order given)

- [x] docs/adr/027-enforcement-layering.md — Layer 1 (identity, THE
      guarantee) vs Layer 2 (network, best-effort, honestly reported); the
      claims table; H6 deliverable amendment (MediationProvider port,
      SPIFFE identity, ADR 026 per-edge authz, dual-runtime proof).
- [x] docs/adr/022-identity-aware-mediation.md — 3 rings (Ring0 authoring/
      Ring1 network floor/Ring2 identity mediation); OpenZiti chosen over
      Consul/mesh sidecar; identity URI `spiffe://datascape/<namespace>/
      <kind>/<name>`; Lattice raw-TCP lesson (connection-establishment
      identity, not per-query IAM); dark services; MediatedConnection =
      the existing Connection realized by a mediator provider instead of a
      plain forwarder (no new schema flag — providerRef selects the
      mediator, exactly like ingress/proxy/wireguard today).
- [x] docs/adr/026-graph-scoped-access.md — the reference graph (already
      built by internal/domain/graph) IS the access-request set; H7
      compiles per-edge network policy from it later; H6 only needs the
      *mediated subset* (edges into a Connection realized by a
      MediationProvider-capable Provider).
- [x] docs/planning/08 §7.7 H6 + ADR-027 amendment note (verbatim spec).
- [x] docs/adr/023-wireguard-tunnel.md — the provider-on-the-Connection-
      seam precedent: one runtime object per Connection (never shared per
      Provider), ContainerSpec.Sysctls precedent for additive runtime-port
      fields, secret-in-file-mount-never-env/state discipline, pinned
      image-by-digest discipline, TunnelProvider gate posture (Alpha,
      disabled — "new capability surface" bar MediatedConnections inherits).
- [x] internal/ports/runtime/runtime.go, internal/ports/reconciler/
      reconciler.go (capability-interface cluster pattern — marker
      interfaces embedding Provider, e.g. ConnectionCapableProvider/
      BackupCapableProvider), internal/application/registry/registry.go
      (RegisterProvider(type, ctor, gateName); haGuardRuntime's explicit-
      delegation pattern for runtime capabilities obtained through the
      registry — precedent for how MediationProvider capability must be
      reachable through any wrapping).
- [x] docs/planning/02 §4.1 (settledness/ScaledWait rules), §4.2 (Request
      struct, capability marker interface catalog).
- [x] docs/planning/06 §2.1 (task execution protocol), §10 (test-impact
      economy).
- [x] internal/domain/naming/naming.go (F4 authority — RuntimeObjectName;
      this task ADDS a sibling identity-derivation function, doesn't
      change this one).
- [x] internal/domain/graph/graph.go — Edges is exactly the ADR 026 request
      graph (from -> []to, deduped access to build).
- [x] schemas/v1alpha1/connection.json, schemas/v1alpha1/fragments/
      provider/wireguard.json + internal/application/manifest/fragment.go
      (provider-config-fragment registration pattern), schemas/embed.go.

## Design decisions locked in before coding

1. **Port location:** `internal/ports/mediation` (new package, mirrors
   `internal/ports/runtime`'s standalone-port shape rather than being
   folded into `reconciler.go`, because its methods are identity/policy
   CRUD operations invoked by the engine directly — not part of the
   Reconcile/Destroy/Probe request lifecycle). A capability *marker*
   interface `MediationCapableProvider` lives in `reconciler.go` next to
   `ConnectionCapableProvider` (same pattern) so the engine/compatibility
   layer can type-assert a constructed `reconciler.Provider` the same way
   it already does for every other capability.
2. **Identity type:** `mediation.WorkloadIdentity{URI, Fingerprint string}`
   — URI is the SPIFFE-aligned identity, Fingerprint is a public-key
   fingerprint for audit/state display. No private key material ever
   crosses this boundary (ADR 013 discipline) — adapter-internal only.
3. **Edge type:** `mediation.Edge{From, To WorkloadIdentity; DialAllowed,
   BindAllowed bool}` — the compiled per-edge authorization the engine
   hands to `RealizeEdge`.
4. **Naming:** `internal/domain/naming.WorkloadIdentityURI(env) string`
   returns `spiffe://datascape/<namespace>/<kind>/<name>` (ADR 022's exact
   form), deterministic, unit-tested, zero I/O.
5. **Graph derivation:** `internal/application/graphaccess` (new package)
   — `DeriveEdges(g *graph.Graph) []Edge` (reusable, H7's own future
   consumer per the task prompt) + `MediatedSubset(edges, resources,
   isMediationCapable func(providerType string) bool) []Edge` narrowing to
   edges terminating in a Connection whose providerRef resolves to a
   mediation-capable Provider.
6. **Adapter:** `internal/adapters/providers/openziti` — pinned
   controller+router images (A10 digest pinning), a minimal hand-rolled
   REST client against Ziti's Edge Management API (no new SDK dependency,
   matching this repo's "drive the tool directly" ethos), implements
   `reconciler.Provider` + `ConnectionCapableProvider` (scheme `tcp`,
   Connection-seam precedent) + `mediation.MediationProvider`.
7. **Gate:** `MediatedConnections`, Alpha, disabled.

## Status

- [x] Design locked (this file)
- [x] internal/domain/naming: WorkloadIdentityURI + tests (upgraded at the
      H5 merge to include a non-default metadata.domain segment)
- [x] internal/ports/mediation: port + types
- [x] reconciler.go: MediationCapableProvider marker (Mediation(ctx, req),
      request-scoped per F5 — found live that a no-arg Mediation() cannot
      reach a live controller)
- [x] internal/application/graphaccess: DeriveEdges + MediatedSubset +
      CompileMediatedConnections + tests
- [x] internal/adapters/providers/openziti: adapter + unit tests
      (httptest-based REST idempotency proofs + identity/config tests)
- [x] registry.go/main.go wiring + gate registration (MediatedConnections,
      Alpha, disabled)
- [x] schema fragment (provider/openziti.json) + doc 03 §8.2.5 same-commit
- [x] archtest: ziti import fence
      (internal/archtest/mediation_layering_test.go)
- [x] doc 04 §12 row, doc 08 H6 Done-note (claims-table language), doc
      reference regen, explain-catalog (4 new reasons + catalog entries),
      test-impact.sh row (`openziti` suite)
- [x] CDC scenario proof — Docker: live-verified (see Done-note in doc 08
      for the full account: 3 real bugs found+fixed live, positive proof,
      both negative proofs — reachability and wrong-identity — all live).
- [ ] CDC scenario proof — Kubernetes: NOT attempted (time budget) —
      recorded as a deviation below, not silently skipped.
- [x] gofmt/vet/build (both tag sets)/golangci-lint 0 issues
- [x] go test ./... unfiltered, true-exit=0 (post H5/H8 merge too)
- [ ] impact sweep --base main, flock-wrapped, green x2 both runtimes —
      the `openziti` suite itself was run live (manually + via `go test
      -tags integration`) and passed on Docker; the formal
      flock-wrapped script invocation and a second back-to-back run, plus
      any Kubernetes run, were not completed this session (deviation).
- [x] final squashed commit

## Deviations (recorded as found, not silently worked around)

1. **Kubernetes not attempted.** H5 (domains) merged mid-task, consuming a
   large conflict-resolution pass; remaining budget went to fixing and
   live-verifying the Docker path (3 real bugs found only by running live)
   rather than starting a second, unverified substrate. The port/adapter
   design is substrate-agnostic by construction (only calls
   runtime.ContainerRuntime's Ensure* methods) but this is unverified, not
   proven, on Kubernetes.
2. **Known live flake**, reproduced 3/3 runs: `platformctl drift` run
   immediately after `apply` intermittently reports the external Source
   `ExternalEndpointUnreachable` (engine.go's generic `probeTCPReachable`,
   an unretried ~3.75s dial) even though the Binding's own connector is
   genuinely RUNNING and a direct dial succeeds moments later — a fresh
   Ziti circuit's first-connection latency occasionally exceeds that
   budget. This task's own settle-probe (`waitMediatedServing`, ~30s
   bounded retry) keeps the Connection/Binding's own Ready/drift clean;
   the generic external-reachability probe (outside this task's file
   fence) is the residual gap. Not root-caused further given the time
   budget — full account in doc 08's H6 Done-note.
3. **`upsertService` does not update `encryptionRequired` on an existing
   service** (create-only idempotency for that one field) — found live
   while iterating on the encryptionRequired=false fix; harmless for a
   fresh apply (the value is set correctly at creation) but a manifest
   that somehow already had a service created with the old default would
   need a manual delete. Not exercised by the accept scenario; recorded,
   not fixed, given time.
4. **golangci-lint's pre-existing `engine.go:119 logf unused` finding**
   (present in the branch this task started from, before any of this
   task's own edits) resolved itself at the H5/H8 merge — main's own
   commits since then evidently re-used or removed `logf`. Not this
   task's fix; noted for completeness since an earlier progress commit
   flagged it as a pre-existing, out-of-scope issue.
