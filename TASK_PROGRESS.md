# C8: TLS termination and certificate handling — progress

Task: docs/planning/08-production-readiness-plan.md §5 C8. Protocol: doc 08
§2.1. Read first: CLAUDE.md, C8 entry, docs/adr/018, ingress provider (full),
docs/adr/009/013/015.

## Pre-flight findings (recorded before coding)

- **specHash / ContainerSpec.Files leak check (deliverable 1 requirement):**
  `internal/adapters/runtime/docker/image.go:specHash` computes
  `sha256(json.Marshal(spec))` — a **one-way hash** stored as the
  `specGenLabel` container label. The label itself does **not** leak file
  content (confirmed by reading the function). However, `Files` content IS
  copied into the container's real filesystem via `copyFilesIn` (tar
  stream) at creation time, and — critically — a content **change** changes
  the hash, which per `ensureOneContainer` forces a full container
  **replace**. For the shared Caddy container this reproduces exactly the
  restart-blast-radius problem ADR 018 Decision 3 already solved for
  *routes*: any single Connection's cert rotation would restart the shared
  proxy and drop every other Connection's live traffic. Decision: per-
  Connection leaf certs (and provided secretRef cert/key) are loaded
  **exclusively via Caddy's admin API** (`/config/apps/tls/certificates/
  load_pem`, `@id`-tagged exactly like routes), never via
  `ContainerSpec.Files`. Confirmed live (see spike log below): the admin
  API's `GET` on a loaded cert **does** return the private key in
  plaintext to any caller reaching the admin port — same trust boundary
  already documented for the admin surface (shared network, unauthenticated,
  matching Kafka Connect/nessie/prometheus's own posture) — recorded as a
  finding, not a regression, since nothing new is exposed beyond what the
  admin API already trusted.
- **CA persistence (deliverable 2):** the self-signed CA keypair must
  survive across separate `platformctl apply` invocations (separate
  processes, no shared memory) without going through Caddy's admin API
  (which forgets live-loaded config on container restart — no
  `--config` autosave-resume is used) or through platformctl's own state
  (private key values are never persisted there). Decision: Docker —
  persist via `ContainerSpec.Files` at the shared Caddy container's
  Provider-level bootstrap, using the exact read-before-regenerate pattern
  `postgres.liveSuperuser` already established (`rt.ReadFile` the existing
  container's file before deciding to keep vs. rotate) — this is
  Provider-scoped (not per-Connection), so it changes as rarely as the
  bootstrap config itself, consistent with Decision 3's "only changes when
  the bootstrap shape changes" bar. Kubernetes — persist as a Kubernetes
  `kubernetes.io/tls`-shaped Secret the ingress provider owns (not
  referenced by any Ingress), read back via a new `GetTLSSecret` capability
  before regenerating. Both documented in doc 03 additively.
- **Caddy JSON spike (live, real `caddy:2.9.1` container on this host)**:
  confirms the exact admin-API shape needed. Two findings that would have
  been wrong by pure reasoning:
  1. `automatic_https: {disable: true}` on a server does **not** make that
     server speak TLS on its own — it only suppresses ACME/auto-cert +
     auto-redirect. A server needs an explicit
     `tls_connection_policies: [{}]` (empty policy = default cert
     selection by SNI) to actually terminate TLS at all. Without it, a
     listener on `:443` still speaks plain HTTP (`curl` failed with
     "wrong version number" until this was added).
  2. `GET /id/<id>` on an unknown certificate `@id` returns **404** (routes
     return 400 for the same "no such object" case) — both must be treated
     as not-found.
  Spike artifacts: manual curl/openssl session, not committed (throwaway).

## Design decisions

- `Connection.spec.tls`: `{secretRef}` | `{selfSigned: true}` | `{secretName}`
  (K8s cert-manager reference), exactly one set. Refused on `external: true`
  Connections (no entrypoint to terminate at). `scheme: https` and
  `spec.tls` are required together (one implies the other).
- `secretRef`'s cert/key resolve through `Request.Secrets` **only** when the
  ingress `Provider`'s own `spec.secretRefs` lists that name — mirrors
  debezium's `Connection.SecretRef` → `spec.secretRefs` plumbing exactly
  (`internal/adapters/providers/debezium/debezium.go` lines ~299-354).
- Docker: two Caddy HTTP servers (`srv0` plain :80 unchanged, new `srv1`
  TLS :443 with `tls_connection_policies: [{}]`); routes added to whichever
  server matches the Connection's scheme. Certs loaded via
  `/config/apps/tls/certificates/load_pem/`, `@id: cert-<name>`,
  PATCH-or-POST exactly like `ensureRoute`.
- Kubernetes: `networking.k8s.io/v1 Ingress.spec.tls` referencing a
  `kubernetes.io/tls` Secret — either materialized by this provider
  (secretRef/selfSigned) or referenced by name only (cert-manager
  `secretName`, never created/deleted by platformctl).
- New runtime capability (extends `IngressCapableRuntime`, Kubernetes-only):
  `EnsureTLSSecret`/`GetTLSSecret`/`RemoveTLSSecret`. **Must** add explicit
  delegating methods on `registry.haGuardRuntime`
  (`internal/application/registry/registry.go`) — ADR 018's addendum
  already documents this exact embedded-interface gotcha for
  `EnsureIngress` et al.; the same trap applies to any new
  `IngressCapableRuntime` method.
- Gate `TLSTermination` (Alpha, disabled): no existing choke point fits (not
  a distinct provider type like `IngressProvider`/`BackupRestore`, not a
  CLI-flag behavior like `DriftDetection`/`ParallelReconciliation`). New
  choke point: `registry.Registry.RequireGate` (thin public wrapper),
  called from `engine.resolveRequest` when `Resource.Kind == "Connection"`
  and `conn.TLS != nil` — mirrors HighAvailability's own admitted
  imperfection (backstop at the point of use, not full validate-time DX;
  documented as such).
- RBAC: **no new verbs** — `deploy/kubernetes/rbac/role.yaml`'s `secrets`
  entry already grants `get/create/update/delete` cluster-wide, sufficient
  for TLS Secret CRUD. Confirmed before writing any RBAC-related code.
  `role.yaml`'s comment table gets an additive note only.

## Step plan

- [x] 0. TASK_PROGRESS.md + pre-flight findings (this file).
- [x] 1. Read CLAUDE.md, C8 entry, ADR 018, ingress provider (ingress.go,
      docker.go, caddy.go, kubernetes.go), ADR 009/013/015, doc 03 §8.2,
      doc 04 §12, reconciler.go, runtime.go, connection.go, secret.go,
      registry.go, K8s adapter ingress.go/container.go/preflight.go,
      role.yaml/README.md, existing tests.
- [x] 2. Live spike: real Caddy container, admin API TLS load + route +
      `tls_connection_policies` — findings above.
- [x] 3. Domain: `Connection.TLS` field + validate + unit tests. Also:
      `graph.go` gained a `tls.secretRef` nested-ref edge (mirrors D10's
      `configRefFields` pattern, scoped narrowly to this one field) and
      `compatibility.go` validates it resolves to a `SecretReference`,
      exactly like the existing top-level `secretRef` check. Tests:
      `internal/domain/connection/connection_test.go` (new, 9 cases) +
      existing graph/compatibility suites green.
- [x] 4. Schema (`connection.json`) + doc 03 §8.2.2 additive section.
      `docs/reference/connection.md` regenerated
      (`go run ./cmd/platformctl docs build --out docs/reference`);
      `TestGeneratedReferenceInSync` green.
- [x] 5. `status/reasons.go` new TLS reasons (CertHealthy/CertMissing/
      CertInvalid/CertConfigDrift/CAProvisioned).
- [x] 6. `ports/runtime`: `IngressSpec.TLSSecretName`,
      `IngressState.TLSSecretName`, `IngressCapableRuntime` gained
      EnsureTLSSecret/GetTLSSecret/RemoveTLSSecret.
- [x] 7. Kubernetes adapter: `tlssecret.go` (kubernetes.io/tls Secret CRUD,
      ownership-refusal like EnsureIngress) + `ingress.go`'s
      `buildIngress`/`ingressState` wired for `spec.tls`. Unit tests (fake
      clientset): `tlssecret_test.go` (new) + `ingress_test.go` additions.
      All green.
- [x] 8. `registry.go`: `RequireGate` (public wrapper) + `haGuardRuntime`
      TLS-secret delegation (EnsureTLSSecret/GetTLSSecret/RemoveTLSSecret,
      same pattern/reason as the existing Ingress trio) + test extension
      (`ingressCapableFake` gained the three methods,
      `TestRuntime_PromotesIngressCapableRuntime` extended).
- [x] 14 (done early, alongside 8 since it's the same gate-wiring pass).
      Gate wiring: `main.go` registers `TLSTermination` (Alpha, disabled,
      independent of `IngressProvider`'s own gate — a Connection can stay
      plaintext even once ingress routing graduates). `engine.go`'s
      `resolveRequest` checks it via `Registry.RequireGate` when
      `Resource.Kind == "Connection"` and `conn.TLS != nil`. New test file
      `internal/application/engine/tls_gate_test.go`: unregistered gate,
      registered-but-disabled, and enabled cases, plus a plain-http
      Connection proven unaffected (local `fakeconn` stub provider per
      CLAUDE.md's application-test-double rule — no real `ingress` import).
      All green.
- [x] 9. ingress provider: CA/leaf cert generation helpers (`tls.go`,
      ECDSA P-256, structural drift check via `certValidForHost`/
      `certChainsToCA` rather than byte-equality — a freshly generated
      leaf cert's serial/timestamps differ every regeneration, so
      byte-diffing would falsely report drift on an unchanged manifest;
      `certMatchesSecret` byte-compares only the provided-secretRef mode,
      which IS deterministic). Unit tests green (`tls_test.go`, 7 cases).
- [x] 10. ingress provider: Caddy TLS app / `srv1` / cert admin-API calls
      (`caddy.go`). Found live (spike, recorded above): `automatic_https.
      disable: true` alone leaves a listener speaking plain HTTP —
      `tls_connection_policies: [{}]` is what actually turns TLS on; a
      cert's unknown-@id GET is 404 (routes: 400). Unit tests
      (`caddy_test.go` extensions, real `httptest` fake admin server
      distinguishing routes vs certs by @id prefix): srv1 routing,
      ensure/get/delete cert, bootstrap config shape. All green.
- [x] 11. ingress provider: Docker reconcile/probe/destroy TLS paths
      (`docker.go`): `ensureLocalCA` (read-before-regenerate CA
      persistence via `ContainerSpec.Files`+`ReadFile`), `resolveCertDocker`
      (per-mode cert resolution: secretRef validated via
      `tls.X509KeyPair`, selfSigned read-existing-or-generate against the
      persisted CA, secretName refused with a Kubernetes-only message),
      `probeCertDocker` (structural/byte drift checks per mode),
      `tlsServer` (srv0/srv1 + Insecure dispatch). New `docker_test.go`
      (fake runtime + real httptest Caddy-admin fake, no live Docker
      needed for these pure-logic paths) — 7 cases, all green. No
      `docker_test.go` existed pre-C8 (C7 relied on live integration tests
      for Connection-level Docker behavior); this task adds targeted unit
      coverage for the new pure-logic helpers specifically, same
      boundary C7 left, live Docker integration still required for the
      full reconcile/probe/destroy path (step 17).
- [x] 12. ingress provider: Kubernetes reconcile/probe/destroy TLS paths
      (`kubernetes.go`): `resolveCertKubernetes` dispatches per mode —
      secretRef materializes a `tls-<name>` Secret (validated pair),
      selfSigned lazily provisions a Provider-scoped `<provider>-ca`
      Secret (never at Provider-level reconcile, which has no visibility
      into which Connections want TLS) then a `tls-<name>` leaf Secret
      reused across applies when still valid, secretName only ever reads
      (`GetTLSSecret`) — not-yet-issued reports `Ready:false`/
      `CertMissing` without erroring (cert-manager-style eventual
      consistency) but still creates the Ingress referencing it.
      `destroyConnectionKubernetes` removes only the Connection-scoped
      leaf Secret, never the Provider-scoped CA or a cert-manager-owned
      secretName. `probeConnectionKubernetes` extended with cert
      presence/drift checks per mode. New `kubernetes_test.go` (local
      `fakeIngressRuntime` implementing the full extended
      `IngressCapableRuntime` over the fake `ContainerRuntime` — adapter
      package, not `internal/application`, so CLAUDE.md's narrower test-
      double allowlist doesn't apply): 8 cases covering all three modes,
      idempotent reuse, cert-manager eventual issuance, destroy scoping,
      and drift detection. All green; full `go test ./...` green.
- [x] 13. `SupportedConnectionSchemes()` gains `"https"`
      (`ingress.go`, package doc comment updated); existing test renamed/
      updated (`TestSupportedConnectionSchemesIsHTTPAndHTTPS`).
- [x] 14. Gate wiring: done in the step-8 commit (`main.go` registers
      `TLSTermination`; `engine.go` checks it). Marked complete here for
      the record.
- [x] 15. `platformctl inventory`: https URL rendering came free (already
      wired via `Endpoint.Scheme`/`Insecure`, no root.go change needed —
      confirmed by `TestInventorySurfacesSelfSignedCALocation`'s
      structured-output assertions and the ingress provider's own
      `endpoint.List` entries). CA location surfacing added: new
      `inventoryOutput.CertificateAuthorities []caEntry` (the CA's public
      PEM, for `-o json/yaml` tooling consumption) plus a human-readable
      `printSelfSignedCANotes` pointer line (never the raw PEM inline) in
      both the populated-table and empty-state paths. New test
      `TestInventorySurfacesSelfSignedCALocation`
      (`cmd/platformctl/inventory_test.go`) covers both output modes and
      asserts the PEM never leaks into the human-readable path. All
      green.
- [x] 16a. Doc sync (part 1, before live verification): doc 04 §12
      `TLSTermination` row (additive insert, passed the guard hook), ADR
      018 addendum (Caddy admin-API findings, ContainerSpec.Files
      reasoning, K8s Secret capability, gate independence, RBAC
      no-change confirmation), `deploy/kubernetes/rbac/README.md`'s
      `secrets` row note (no new verbs).
- [x] 16b. Doc sync (part 2, after live verification): doc 08's own C8
      "Merged" status note appended (additive insert, passed the guard
      hook) with the actual live results — Docker and Kubernetes suite
      names/timings, the two deliberate deferrals (no live ingress-nginx
      HTTPS round-trip on K8s; no inter-apply expiry warning) named
      explicitly, not silently missing.
- [x] 17a. Docker integration test, run live: `TestIngressTLSEndToEnd`
      (`cmd/platformctl/ingress_tls_integration_test.go` +
      `testdata/ingress-tls-scenario`). Covers, live, all Docker-leg
      accept items: provided-secretRef https endpoint verifies against
      the test's own independently-generated CA (`resp.TLS.
      VerifiedChains`); self-signed path verified against the CA
      published in `inventory -o json` (not a CA the test invented —
      proves the *provider's own* CA is what's actually serving);
      inventory names the CA location in the human-readable path too
      (PEM never inlined there); a third Connection routes to a helper
      upstream created out-of-band with `Audience: internal` (zero host
      port), reachable through the entrypoint but its own `HostAddr(80)`
      confirmed empty both before and after apply; idempotent re-apply
      (`no changes`, shared proxy container ID unchanged); drift on a
      hand-mangled TLS route detected and healed; clean destroy. Result:
      `--- PASS: TestIngressTLSEndToEnd (8.78s)` on first run, and green
      again (8.60s) alongside the full pre-existing `TestIngress*` suite
      (`TestIngressRoutingEndToEnd`, `TestIngressProviderGateGuardsApply`,
      `TestIngressKubernetesEndToEnd`) — no regression. Test names all
      match `scripts/test-impact.sh`'s existing `ingress` suite's
      `-run 'TestIngress'` pattern already, so **no edit to that file was
      needed** (confirmed before touching it, per the file-ownership
      note).
- [x] 17b. Kubernetes TLS integration test, run live under a minted
      minimal-RBAC kubeconfig (doc 06 §8 rule 4 — minted per
      `deploy/kubernetes/rbac/README.md`'s exact steps against the local
      minikube cluster; `kubectl auth can-i create
      secrets/ingresses.networking.k8s.io/pods` all `yes` under it, no
      new verb needed, confirmed before running):
      `TestIngressTLSKubernetesEndToEnd`
      (`cmd/platformctl/ingress_tls_kubernetes_integration_test.go` +
      `testdata/ingress-tls-k8s-scenario`, three Connections: secretRef,
      selfSigned, secretName). Covers: the provided-secretRef Ingress
      references a Secret holding the provided cert/key verbatim; the
      self-signed leaf cert verifies (`crypto/x509` chain verification,
      the object-level bar `TestIngressKubernetesEndToEnd` (C7) already
      established for this runtime — no live ingress-nginx round-trip,
      documented as a deliberate scope match, not a shortcut) against the
      CA the Provider itself provisioned and published; a cert-manager-
      style `secretName` Connection's Ingress is created before the
      Secret exists (referencing-only) and the apply still succeeds
      (Ready:false, not an error) until the Secret is simulated
      out-of-band and a re-apply converges; idempotent re-apply; drift on
      a hand-mangled Ingress heals; clean destroy. Result:
      `--- PASS: TestIngressTLSKubernetesEndToEnd (58.93s)` on first run
      under the minted kubeconfig; `kubectl get ns` confirms no leftover
      namespace after cleanup. Full `TestIngress*` suite re-run alongside
      it (same minted kubeconfig) to confirm no regression — see
      Verification log for the result.
- [x] 18. Verify: gofmt, build, vet, `go test ./...`, accept criteria live,
      `scripts/test-impact.sh --base main` — all green; see Verification
      log.
- [x] 19. Final commit (WIP commits squashed into one, per §2.1 step 0's
      "tidy at the end").

## Verification log

- gofmt: clean (this worktree's files; sibling worktrees excluded).
- `CGO_ENABLED=0 go build -trimpath -buildvcs=false ./cmd/platformctl`: clean.
- `go vet ./...`: clean.
- `go test ./...`: all green (includes the new connection/graph/compatibility/
  engine/registry/kubernetes-adapter/ingress unit suites).
- `docs/reference` regen after final state: no drift.
- Live accept runs (see steps 17a/17b above for full detail):
  - `TestIngressTLSEndToEnd` (Docker): PASS 8.78s first run; PASS 8.60s in-suite.
  - `TestIngressTLSKubernetesEndToEnd` (minted minimal-RBAC kubeconfig):
    PASS 58.93s first run; PASS 60.73s in-suite.
- `scripts/test-impact.sh --base main` (shared ledger + flock, queued behind
  other agents): 13 suites selected (SHARED_CORE touched → wide selection).
  First pass: 12 ran, 11 green, 1 failed — `sink`/`TestParquetSinkEndToEnd`
  failed on `Bind for 127.0.0.1:19102 failed: port is already allocated`, a
  transient host-port collision with another concurrent workload (nothing
  holds 19102 afterward; this diff never touches s3/s3sink — the suite was
  selected only via SHARED_CORE). Green first-pass timings: docker-conformance
  24s, k8s-adapter 381s, redpanda 88s, cdc 171s, connect-ha-dlq 95s,
  acceptance 104s, lakehouse 184s, chaos 92s, prometheus 14s, ingress 117s
  (includes both new TLS suites), blueprints 50s, trino 115s. Re-run:
  `impact: 13 selected, 1 ran, 12 deduped, 0 failed` — the ledger deduped
  every green suite and `sink` passed on re-execution (136.6s), confirming
  the first-pass failure was the transient port collision, not this diff.
  **Net: all 13 affected suites green at this content-state.**
