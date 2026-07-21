# Production Readiness Plan — Stage-Gated Task Backlog

Audit date: 2026-07-17 (Stage F added 2026-07-19 from doc 09's
live-testing audit). Audited against: the full planning package (00–07),
`docs/history/checkpoint.md`, `docs/history/errors.md`, `docs/history/feature-requests.md`, the remediation ledger
(`docs/remediation/`, all 10 findings closed), and direct code inspection of
`internal/` (runtime port, Docker and Kubernetes adapters, secrets router,
state store, registry, engine).

Purpose: take platformctl from its current state (v1.0.0 shipped; Phases 0–6.5
done; Gates 0–2 of doc 07 closed with explicit deferrals; Kubernetes runtime
started, Alpha) to **a fully featured system ready for production data-pipeline
creation** — on Docker and Kubernetes, covering deployment scenarios,
high availability, storage, connections, routing, TLS, monitoring, backup, and
the infrastructure-level provider gaps real pipelines need. The product goal
throughout: a developer designs systems architecture and data flow; platformctl
absorbs the configuration headache.

This document is a **work backlog, not an implementation note**. Every task is
written to be executed in isolation by a single agent session or a human
contributor without needing the rest of this document in context.

---

## 1. Audit summary — current state vs. production service

### What already holds (do not re-do)

- Deterministic plan/apply/destroy with idempotency asserted in every e2e
  suite; authoritative apply-deletes; rename/provider-change plans; drift
  probes verify *desired configuration* (per-provider equivalence table in
  doc 07 §2.1), not just liveness.
- Validate-time completeness contract: schema → kind validation → graph →
  compatibility/capability → provider `SpecValidator` → feature gates
  (docs/history/checkpoint.md "Validate-time completeness").
- Docker runtime: production controls (restart policy, resources, security
  context, log config, aliases, file-mounted secrets, pull policy/digest
  support, observed-port inspection), ownership labels, loopback-default
  binds, refusal of unmanaged same-name objects.
- Nine providers (redpanda, postgres, mysql/mariadb, debezium, s3/minio,
  s3sink, nessie, openlineage, proxy) + Catalog/Connection kinds; secret
  preflight, rotation with fingerprints; endpoint inventory with
  `--for spark|trino|dbt|psql|s3|kafka`; searchable generated docs.
- Kubernetes adapter passes the runtime conformance suite live and ran the
  unmodified redpanda provider end-to-end (doc 07 "Cross-Runtime
  Portability").

### The high-priority gaps this plan closes

| # | Gap | Why it blocks "production service" | Stage |
|---|---|---|---|
| 1 | Kubernetes external reachability: Services are ClusterIP-only; the CLI's own control-plane calls (topic creation, connector REST) fail from outside the cluster | The K8s runtime is unusable for any provider with CLI-side follow-up calls — i.e. almost all of them | B |
| 2 | No shared/remote state, no state ops tooling | Two operators (or CI + a laptop) corrupt each other; a lost laptop loses the platform record | A |
| 3 | K8s storage is a hardcoded 10Gi PVC, no StorageClass choice, no persistence-across-update proof | Data services on K8s need sized, classed, provably persistent volumes | B |
| 4 | No HA anywhere: 1 replica per container, single broker, single Connect worker, single-node MinIO | A node failure takes the pipeline down; unacceptable for production data flow | C |
| 5 | No backup/restore capability for data-bearing resources | Drift healing recreates infra but data recovery is undefined | C |
| 6 | No ingress/routing/TLS: every endpoint is plaintext, host-port or ClusterIP | Production endpoints need stable DNS names, TLS, and controlled exposure | C |
| 7 | No metrics/monitoring stack | "Is the pipeline healthy *right now*" has no answer besides `platformctl status` polling | C |
| 8 | No schema registry → JSON-only sinks; Parquet/Avro blocked | Columnar lake formats are table stakes for a production lakehouse | D |
| 9 | Capability seams with no provider: ingest (Dataset→EventStream), database sink (EventStream→Source), tunnels, TLS termination | Real pipelines need replay-from-lake, serve-to-database, VPC reach | D |
| 10 | Registry auth, GC/doctor tooling, digest pinning workflow, output-contract harness, MariaDB coverage — the explicit Gate 1–3 deferrals | Each is a documented deferral that becomes a sharp edge in production | A/E |
| 11 | No scaffolding/blueprints; provider-authoring contract is convention, not executable | The "headache-free" promise needs a zero-to-pipeline path and a contributor path | E |
| 12 | Live-testing bug classes recur systemically: connectivity/discovery logic lives in dependents; the provider contract accretes setter interfaces (doc 09 audit, 2026-07-19) | Every new provider or runtime re-learns the same bugs; blocks segregating core from provider logic (Phase 8) | F |

---

## 2. How to work this backlog

**Read first, always:** `CLAUDE.md` (layering invariant), then the sections of
docs/planning/02 + 03 the task names. Doc 06 §3's pre-coding checklist applies
to every task here.

**Every task implicitly includes** (do not restate per task):

1. Unit tests in the touched package; integration tests (`//go:build
   integration`) when real infrastructure is exercised.
2. Idempotency: re-running the operation with an unchanged spec makes zero
   mutating calls (conformance-suite bar).
3. Schema changes under `schemas/` update
   `docs/planning/03-resource-model-reference.md` in the same commit, and
   `docs/reference/` is regenerated (`TestGeneratedReferenceInSync` enforces).
4. New providers/behaviors ship behind a feature gate (Alpha, disabled unless
   stated), registered in `internal/application/registry` via
   `cmd/platformctl/main.go`, with the gate added to doc 04 §12's table.
5. Machine output stays valid: any new CLI surface emits exactly one JSON/YAML
   document on stdout under `-o json|yaml`, prose to stderr
   (`cmd/platformctl/output_contract_test.go` pattern).
6. No secret values in state, logs, status, or command output — references and
   fingerprints only.
7. `plan` output stays deterministic (golden tests must not flake).

**Task fields:** *Size* S (≤1 focused session), M (1–2 sessions), L (needs a
design note first, then 2+ sessions). *Depends* lists task IDs that must land
first; tasks without dependency edges are safe to parallelize.

**Stage gating:** a stage's tasks may start early, but a stage is *closed* —
and its graduations happen — only when its exit criteria all hold. Later
stages should not close before earlier ones.

### 2.1 Task execution protocol (follow literally, in order)

Every task below is written to be executed by a single agent session with
no additional context. Execute in exactly this order; do not skip steps:

1. **Read, in order:** `CLAUDE.md`; this task's entry in full (Context, Do,
   Accept); every doc section the task names; `docs/adr/README.md`'s index
   for any ADR the task's area touches (ports/providers → ADR 008/009/015/
   016; destructive surfaces → ADR 013; gates → ADR 014; validation → ADR
   011). Do not start coding before this.
2. **Map the task** to the interfaces it touches: open
   `internal/ports/reconciler/reconciler.go` and/or
   `internal/ports/runtime/runtime.go` and read the doc comments of every
   interface/struct the task names. Never re-derive a signature from
   memory.
3. **If the task is Size L** and no design note exists yet: write
   `docs/adr/<next-free-number>-<kebab-title>.md` first (house style: ADR
   001/003/005/006 — question, options considered, decision, why nothing
   is boxed out, follow-ups), then implement.
4. **Implement** under the standing rules of §2 (tests, idempotency,
   schema→doc-03 sync, feature gate, machine output, no secret values,
   deterministic plans). New dial/wait logic must use
   `runtime.WithReachable` — if you are typing an IP address, a port
   number into a URL, or `time.Sleep` in a retry loop inside a provider,
   stop: that is the ADR-015 violation the arch test will reject.
5. **Verify, in order, all must pass:**
   - `gofmt -l .` (empty output)
   - `go build ./... && go vet ./...`
   - `go test ./...`
   - the task's own Accept list, item by item — run the commands, do not
     reason that they would pass
   - `just test-integration` when the task touches adapters or the engine
     (requires Docker; skip only if the task changed no runtime surface,
     and say so)
6. **Doc sync:** tick nothing you did not verify; append facts additively
   (the guard hook allows checkbox toggles and additive edits; modifying
   existing planning text is blocked — if a task requires it, stop and
   report instead of working around the hook).
7. **Commit** with a conventional-commit subject naming the task ID (e.g.
   `feat(runtime): ... (C2)`), a body stating what was verified and how,
   and any deviation from this protocol called out explicitly.

A deviation you cannot avoid (a doc contradiction, a missing seam, an
Accept item that cannot be satisfied as written) is a **finding, not a
judgment call**: stop at the smallest consistent state, record it in the
commit/report, and leave the decision to the maintainer.

---

## 3. Stage A — Operational hardening (the Docker production service)

Theme: platformctl is already correct on Docker; make it **operable** — by a
team, over months, with private registries, shared state, recoverable
mistakes, and auditable cleanup.

**Stage exit criteria:**
- [x] Two operators on different machines can manage one platform through a
      shared state backend without corruption (locking proven by a
      concurrent-apply test). — A4: `internal/adapters/state/s3` (MinIO
      tested), `TestSharedStateBackendConcurrentApplyOneBlocks`
      (`cmd/platformctl`).
- [x] `platformctl gc plan` / `state doctor` exist and are covered by
      integration tests against deliberately-orphaned objects/state (A2, A3).
- [x] A private-registry image pulls successfully via a `SecretReference`
      (A1 — `TestImagePullAuthPullsFromPrivateRegistry` against a real
      `registry:2` + htpasswd instance).
- [x] `destroy` against a `protect: true` data-bearing resource refuses (A5).
- [x] The out-of-band config-drift and MariaDB integration suites are green
      in CI (A8, A9).

### A1: Registry authentication for private images

- **Size:** M. **Depends:** —.
- **Context:** `ImagePull` in `internal/adapters/runtime/docker/docker.go`
  sends no RegistryAuth header; only daemon-level ambient credentials work
  (doc 07 §1.1 deferral). Kubernetes side has no imagePullSecrets.
- **Do:** Add `ContainerSpec.ImagePullSecretRef` (a secret *reference* by
  name, resolved via the engine's existing secret plumbing — never a literal)
  to `internal/ports/runtime/runtime.go`. Provider schemas gain an optional
  `spec.configuration.imagePullSecretRef`; providers pass it through
  mechanically. Docker adapter builds the base64 auth config for `ImagePull`;
  Kubernetes adapter materializes a `kubernetes.io/dockerconfigjson` Secret in
  the namespace and sets `imagePullSecrets`. Secret keys: `username`,
  `password`, optional `registry`.
- **Accept:** conformance subtest proving a pull-with-auth path is exercised
  (fake runtime records it); Docker integration test against a local
  authenticated registry (`registry:2` with htpasswd) pulls a private image;
  credentials never appear in `docker inspect` env, state, or logs.

### A2: Garbage collection and orphan inspection (`gc`/`doctor`)

- **Size:** M. **Depends:** —.
- **Context:** Doc 07 §1.3, explicitly deferred operator tooling. Ownership
  labels (`io.datascape.*`) are already on every created object; unlabeled
  objects are never touched.
- **Do:** Add `ListManagedNetworks`/`ListManagedVolumes` to the runtime port
  (both adapters; fake included). New commands: `platformctl gc plan`
  (read-only: every runtime object carrying this project's labels that no
  state entry accounts for, grouped by namespace/kind/name) and `platformctl
  gc apply` (removes exactly the plan's list; requires the same
  destructive-action flag pair as NFR-3; honors ownership guards).
- **Accept:** integration test creates a labeled container out-of-band
  (simulating a pre-crash orphan), asserts `gc plan` lists exactly it,
  `gc apply` without flags refuses, with flags removes it and nothing else;
  `-o json` emits one parseable document on both subcommands.

### A3: State inspection and repair (`state inspect|doctor|repair`)

- **Size:** M. **Depends:** —.
- **Context:** Doc 07 §1.4 deferral. `localfile` state has fsync + a v1→v2
  key migration precedent (`state.Normalize`/`parseV1Key`,
  `internal/ports/state/state.go`).
- **Do:** `state inspect` (dump normalized state, `-o json`), `state doctor`
  (report: unparseable entries, entries whose runtime object is gone, legacy
  `ActionOrphanUnknown` entries, version < CurrentVersion), `state repair`
  (apply doctor's safe fixes: re-key legacy entries, drop entries for
  confirmed-gone objects with per-entry confirmation or `--yes`). Formalize
  migration scaffolding: a `migrations []func(*State) error` chain keyed by
  version, with a test template.
- **Accept:** doctor/repair round-trip test on a fixture state file
  containing all defect classes; `repair` is a no-op on healthy state;
  unit tests for the migration chain ordering.

### A4: Shared/remote state backend

- **Size:** L (design note first: `docs/adr/003-shared-state.md`).
  **Depends:** A3 (repair tooling before a second backend multiplies states).
- **Context:** `ports/state.StateStore` is the seam;
  `adapters/state/localfile` is the only implementation. Production teams and
  CI need one source of truth plus locking. Doc 07 §1.4 left the decision open.
- **Do:** Design note choosing the first remote backend — recommendation:
  **S3-compatible object storage** (already a first-class dependency of the
  product; a Postgres backend can follow the same port later) with a lock
  object (conditional-put lease with TTL + holder identity, `platformctl
  state unlock` escape hatch). Then implement `adapters/state/s3` passing the
  existing `ports/state/conformance` suite, wired by a `--state`/config
  stanza (`state: {backend: s3, bucket: ..., prefix: ..., secretRef: ...}`).
  Gate: `SharedStateBackend` (Alpha, disabled).
- **Accept:** state conformance suite green against MinIO in integration;
  a test with two concurrent `apply` processes shows one blocks/fails with a
  named-holder lock error, no interleaved writes; `state doctor` works
  against the remote backend.

### A5: Deletion protection for data-bearing resources

- **Size:** S. **Depends:** —.
- **Context:** Doc 07 §0.4 open item: authoritative apply-delete has no
  opt-out; `spec.deletionPolicy: retain|delete` exists on Dataset/Source but
  guards *sub-resource data*, not the resource's participation in
  apply-deletes/destroy.
- **Do:** Add optional `metadata.protect: true` (meta.json, all kinds).
  `plan` marks a would-be delete of a protected resource as a refusal (its
  own action type, surfaced in output); `apply` and `destroy` fail the run
  with an error naming the resource and the remedy (remove `protect`, then
  re-run). Engine-level enforcement (one guard, like NFR-3), not per-provider.
- **Accept:** plan/engine unit tests: protected resource removed from
  manifest → plan shows refusal, apply errors, nothing deleted; unprotected
  path unchanged; docs/planning/03 documents the field beside deletionPolicy.

### A6: External-lifecycle audit, kind by kind

- **Size:** S. **Depends:** —.
- **Context:** Doc 07 §0.3 open items. The engine enforces
  `ExternalConfigurer` centrally, but nobody has audited each kind ×
  provider for whether `external: true` is supported, refused, or undefined;
  docs/planning/03 doesn't state the contract per kind.
- **Do:** Produce the matrix (kind × shipped provider → supports
  ExternalConfigurer / documented-unsupported), add validate-time refusal for
  unsupported combinations (clear error naming the provider type), and write
  the contract into docs/planning/03 everywhere `external: true` appears.
- **Accept:** matrix committed in docs/planning/03 §3; negative validate test
  per unsupported combination; no combination is silently undefined.

### A7: Generic machine-output contract harness

- **Size:** S. **Depends:** —.
- **Context:** Doc 07 §0.5/§3.2 residual: three commands have dedicated
  output tests (`cmd/platformctl/output_contract_test.go`); no harness sweeps
  every command × exit path.
- **Do:** Table-driven harness enumerating each command (validate, plan,
  apply, destroy, status, graph, drift, inventory, import, docs, gc*, state*)
  × exit paths (success, no-op, changed, drifted, empty, cancelled, error)
  against fake-runtime scenarios; assert stdout parses as JSON and YAML and
  stderr carries the prose. New commands must register in the table (guard:
  harness fails if a cobra command is missing from the enumeration).
- **Accept:** harness in CI; enumeration-completeness guard proven by a test
  that a fake unregistered command fails the harness.

### A8: Out-of-band configuration-drift integration tests

- **Size:** S. **Depends:** —.
- **Context:** Doc 07 §2.1 residual. Mechanisms are unit-covered; no
  integration test mutates real infrastructure config out-of-band.
- **Do:** Integration tests that: ALTER a Redpanda topic's `retention.ms`
  out-of-band → `drift` reports retention mismatch; PATCH a Debezium
  connector's config via its REST API → `drift` reports drifted key names
  (never values); flip a database's CDC-readiness setting where feasible →
  Source drift condition. Then `apply` heals each and `drift` goes clean.
- **Accept:** new cases in the integration suite, green in CI; each asserts
  both detection *and* heal.

### A9: MariaDB integration coverage

- **Size:** S. **Depends:** —.
- **Context:** checkpoint item 3b / doc 07 §3.2: `mariadb` is registered
  (shares the mysql adapter; image + binlog flags differ) but no test applies
  a `type: mariadb` Provider.
- **Do:** Clone the MySQL CDC integration path with `type: mariadb`
  (provider + Source + Debezium Binding through to topic traffic), covering
  the version-profile image selection and binlog settings.
- **Accept:** `mariadb_integration_test.go` green in CI; any divergence found
  between mysql/mariadb behavior is fixed or documented in the adapter.

### A10: Image digest pinning workflow

- **Size:** S. **Depends:** —.
- **Context:** Doc 07 §2.5 deferral: images are version-pinned by tag;
  digests need a refresh workflow so pins don't rot.
- **Do:** Add digests (`repo:tag@sha256:...`) to the default images in
  version profiles, examples, and testdata; add `scripts/refresh-digests.sh`
  (resolves each pinned tag to its current digest, rewrites in place) and a
  CI job (scheduled, non-blocking) that runs it and opens a diff/PR when
  digests moved.
- **Accept:** all release-tested images carry digests; the refresh script is
  idempotent; CI job wired; docs note the support window per image.

---

## 4. Stage B — Kubernetes runtime to Beta

Theme: make `spec.runtime.type: kubernetes` genuinely usable from a
developer's machine against a real cluster, with real storage, secrets, RBAC,
and network semantics. Closes the "Still open" list in doc 07's Cross-Runtime
Portability section. Gate `KubernetesRuntime` graduates **Alpha → Beta,
default enabled** at stage close.

**Stage exit criteria:**
- [x] The full `examples/cdc-attendance/` acceptance scenario applies to
      Ready on a real cluster with `platformctl` running *outside* it, and
      destroys cleanly. — B8: `TestCDCAttendanceExampleOnKubernetes`
      (`cmd/platformctl/kubernetes_examples_integration_test.go`), green in
      CI's kind leg after the external-ingress (`05eeddd`) and
      `RemoveNetwork` (`ca9d719`) fixes.
- [x] The `examples/lakehouse/` scenario does the same. — B8:
      `TestLakehouseExampleOnKubernetes`, including the unmanaged
      `external-orders-db` surviving destroy.
- [x] Volume data provably survives a Deployment update (write → update →
      read conformance test). — B3: `Volume_persists_across_container_update`
      conformance subtest, run on fake, Docker, and live Kubernetes.
- [x] A documented minimal RBAC manifest is sufficient for the full suite
      (verified by running CI's K8s job under it). — B5:
      `deploy/kubernetes/rbac/` + the "Integration tests (Kubernetes,
      minimal RBAC role)" CI job running under a minted, token-scoped
      kubeconfig.
- [x] `inventory` reports honest, observed host-reachable endpoints for every
      provider on Kubernetes. — B2: `Inspect_reports_observed_ports`
      conformance subtest + per-access-mode observed endpoints.

Stage closed 2026-07-19 (`5da8367`, B9): `KubernetesRuntime` graduated
Alpha → Beta, enabled by default.

### B1: External reachability — access modes for the Kubernetes runtime

- **Size:** L (short design note in the PR description is enough; the options
  are already enumerated in doc 07). **Depends:** —.
- **Context:** The single biggest K8s gap (doc 07 Cross-Runtime "Still
  open"). Services are ClusterIP-only; provider control-plane calls made by
  the CLI (redpanda admin API, Connect REST, SQL, S3) fail from outside.
- **Do:** Implement per-runtime access modes on the Provider runtime block:
  `spec.runtime.access: port-forward | node-port | load-balancer |
  in-cluster` (default `port-forward`).
  - `port-forward`: the adapter opens client-go `tools/portforward` tunnels
    on demand for the CLI's own calls (transparent; closed after the
    operation) — this is what makes the laptop workflow just work.
  - `node-port` / `load-balancer`: the Service type changes accordingly;
    observed node IP/port or LB address becomes the published endpoint.
  - `in-cluster`: no exposure; documented for platformctl-in-cluster runs.
  The mode must flow into observed endpoints (B2) so `inventory` tells the
  truth per mode. Schema: `provider.json` runtime block + docs/planning/03.
- **Accept:** integration test (minikube/kind): apply the redpanda Provider +
  EventStream from outside the cluster in each of `port-forward` and
  `node-port` modes; topic creation succeeds; endpoints reported per mode;
  `in-cluster` mode refuses CLI-side calls with a clear error naming the mode.

### B2: Observed endpoint inspection on Kubernetes

- **Size:** S. **Depends:** B1 (modes define what "observed" means).
- **Context:** `ContainerState.Ports`/`HostAddr` are populated from Docker
  inspect but not by the Kubernetes adapter (doc 07 Cross-Runtime sub-item).
- **Do:** Populate `ContainerState.Ports` from the Service/endpoint reality
  per access mode (node-port: node IP + NodePort; load-balancer: LB
  ingress; port-forward: the local tunnel address while held; in-cluster:
  in-network only). Conformance subtest `Inspect_reports_observed_ports`
  must pass unmodified on the K8s adapter.
- **Accept:** conformance green on K8s; `inventory` on a K8s-backed platform
  shows reachable host addresses (or honest "(in-network only)").

### B3: Storage classes, volume sizing, and persistence proof

- **Size:** M. **Depends:** —.
- **Context:** `EnsureVolume` hardcodes a 10Gi PVC request
  (`internal/adapters/runtime/kubernetes/kubernetes.go:218`); no
  StorageClass selection; no test writes data across an update
  (doc 07 Cross-Runtime "Still open").
- **Do:** Add `VolumeSpec.SizeBytes` (0 = adapter default) and
  `VolumeSpec.StorageClass` (empty = cluster default) to the runtime port.
  Docker ignores both (volumes are unsized). Providers expose
  `spec.configuration.storage: {size: "50Gi", class: "..."}` and pass it
  through (mechanical, all volume-creating providers). K8s adapter sets PVC
  requests/StorageClassName; a size *increase* patches the PVC (expansion),
  a decrease is a validate-time refusal. Add the conformance subtest:
  write a file into a mounted volume, force a container update (env change),
  read the file back — run on fake, Docker, and K8s.
- **Accept:** conformance persistence subtest green on all three adapters;
  size/class visible in `kubectl get pvc`; decrease refused at validate with
  a clear message; docs/planning/03 documents the storage stanza.

### B4: Kubernetes SecretStore backend

- **Size:** M. **Depends:** —.
- **Context:** `SecretReference.spec.backend: kubernetes` is schema-legal but
  the router fails fast with "not available"
  (`internal/adapters/secrets/router`). On a K8s runtime, native Secrets are
  the idiomatic backend.
- **Do:** Implement `adapters/secrets/kubernetes`: resolve keys from a
  namespaced K8s Secret (`spec.keys` → Secret data keys; Secret name from the
  SecretReference name or an explicit `spec.kubernetes.name`), using the same
  kubeconfig/context resolution as the runtime adapter. Support `Preflight`.
  Gate: `KubernetesSecretBackend` (Alpha, disabled).
- **Accept:** secretstore contract tests green against envtest or a real
  cluster (integration-tagged); preflight aggregates all missing keys;
  rotation fingerprinting works (change the Secret → `SecretChanged` drift).

### B5: RBAC and ServiceAccount posture

- **Size:** S. **Depends:** B1 (port-forward needs `pods/portforward`).
- **Context:** The adapter uses whatever the ambient kubeconfig grants
  (doc 07 Cross-Runtime "Still open").
- **Do:** Write the minimal Role/ClusterRole + ServiceAccount + binding
  manifests under `deploy/kubernetes/rbac/` covering exactly the verbs the
  adapter uses (namespaces, deployments, services, pvcs, secrets, pods/log,
  pods/exec if used, pods/portforward). Document two postures: cluster-admin
  dev shortcut vs. minimal production role. CI's K8s job runs under the
  minimal role to prove sufficiency.
- **Accept:** full K8s integration suite green under the minimal role;
  README/docs section "Running against Kubernetes" includes the manifests.

### B6: Cluster connection preflight and error clarity

- **Size:** S. **Depends:** —.
- **Context:** kubeconfig/context are already configurable
  (`config["kubeconfig"]`, `config["context"]` in `kubernetes.go`), but an
  unreachable/misconfigured cluster surfaces late, mid-apply, as a raw
  client-go error.
- **Do:** At validate/plan for any manifest set that resolves a kubernetes
  runtime: a fast connectivity + permission preflight (server version call +
  SelfSubjectAccessReview for the core verbs), failing with an error that
  names the kubeconfig path, context, and missing permission.
- **Accept:** unit tests with a stub rest.Config; integration test: wrong
  context name fails validate with the named remedy, not a mid-apply panic.

### B7: NetworkPolicy parity with Docker network isolation

- **Size:** M. **Depends:** —.
- **Context:** On Docker, a named network is an isolation boundary; the K8s
  mapping (network → Namespace) gives DNS parity but **no isolation** — any
  pod in the cluster can reach the services. Silent semantic weakening.
- **Do:** `EnsureNetwork` on K8s additionally creates a default-deny +
  allow-same-namespace NetworkPolicy pair (opt-out via runtime config
  `networkPolicy: none` for clusters without a CNI that enforces them, with
  a stderr warning). Document the semantic mapping and its limits.
- **Accept:** integration test on a policy-enforcing cluster (kind + Calico
  or minikube CNI): in-namespace pod reaches the service, out-of-namespace
  pod cannot; opt-out path warns; conformance unaffected on Docker/fake.

### B8: Kubernetes provider matrix in CI

- **Size:** M. **Depends:** B1, B2, B3 (the suites need reachability and
  storage to pass).
- **Context:** Only redpanda has been run end-to-end on K8s. The whole point
  of the port boundary is that the other eight providers work unmodified —
  prove it, and catch translation bugs like the Cmd/Args one.
- **Do:** A CI job (kind-based) running the existing integration suites —
  CDC, sink, lakehouse — with `spec.runtime.type: kubernetes` via a
  runtime-parameterized manifest fixture. `PLATFORMCTL_REQUIRE_K8S=1`
  enforces (existing mechanism). Triage and fix every translation bug found;
  record genuine per-runtime differences in doc 07's Cross-Runtime section.
- **Accept:** CI matrix has a green K8s leg for cdc + sink + lakehouse;
  doc 07 updated with any new per-runtime differences found.

### B9: Kubernetes docs/schema sync

- **Size:** S. **Depends:** B1–B8 substantially landed.
- **Context:** Doc 07 Cross-Runtime last bullet: `provider.json`'s
  `runtime.type` description and doc 04 Phase 7 prose still describe
  kubernetes as schema-accepted-only in places.
- **Do:** Sweep `schemas/v1alpha1/provider.json`, docs/planning/03/04,
  README for the K8s runtime's actual status, access modes, storage stanza,
  RBAC pointer; graduate `KubernetesRuntime` Alpha → Beta (enabled) in
  `application/featuregate` + doc 04 §12.
- **Accept:** schema-doc-sync agent reports no drift; gate table and code
  agree; README shows the K8s quickstart.

---

## 5. Stage C — Production deployment scenarios: HA, routing, TLS, monitoring, backup

Theme: the pipeline survives failure, is reachable through stable secured
entrypoints, is observable, and its data is recoverable. This is where
"demo-correct" becomes "production-grade". Gate `KubernetesRuntime` targets
**GA** at stage close; new gates: `HighAvailability`, `IngressProvider`,
`TLSTermination`, `MonitoringStackProvider`, `BackupRestore` (all Alpha).

**Stage exit criteria:**
- [ ] A 3-broker Redpanda EventStream with replication factor 3 keeps
      accepting produce/consume while one broker is killed (both runtimes;
      on K8s, brokers spread across nodes when possible).
- [ ] A 2-worker Connect group keeps a CDC Binding RUNNING through the loss
      of one worker.
- [ ] An HTTP endpoint (nessie or minio console) is reachable through a
      routed, TLS-terminated, stable-hostname entrypoint on both runtimes.
- [ ] `platformctl backup && platformctl restore` round-trips a Postgres
      Source and a MinIO Dataset onto fresh infrastructure.
- [ ] `inventory --for prometheus` yields scrape config that collects broker,
      database, connect, and object-store metrics into a managed Prometheus.

### C1: Replicas and stable identity in the runtime port

- **Size:** L (design note `docs/adr/004-replicas-and-identity.md`
  first). **Depends:** Stage B closed (K8s is the runtime where HA is real).
- **Context:** `ContainerSpec` models exactly one container; the K8s adapter
  pins `Replicas: 1` (`convert.go:110`). HA data services need N replicas
  *with stable per-replica identity and storage* (broker IDs, seed lists) —
  a StatefulSet-shaped concept, deliberately excluded from the first
  adapter (doc 07 Cross-Runtime "deliberately not more").
- **Do:** Design note deciding the port shape — recommendation:
  `ContainerSpec.Replicas int` + `ContainerSpec.StableIdentity bool`; when
  StableIdentity, each replica gets ordinal-suffixed name/hostname and its
  own volume set (K8s: StatefulSet + headless Service +
  volumeClaimTemplates; Docker: N containers `name-0..N-1` on the shared
  network, per-ordinal volumes). Per-replica health in `ContainerState`
  (`ReadyReplicas`). PodDisruptionBudget (maxUnavailable 1) and soft
  anti-affinity applied automatically when Replicas > 1 on K8s. Fake runtime
  models it for engine tests. Conformance subtests: scale-up idempotency,
  per-ordinal volume persistence, ordinal hostname resolution.
- **Accept:** conformance green on all three adapters; killing one of N
  replicas is surfaced (not Healthy=false for the set unless quorum-relevant
  — the *provider* decides meaning); gate `HighAvailability` guards
  Replicas > 1 at validate.
- **Status (2026-07-21): implemented on branch
  `worktree-agent-ac3b0d7e379217021`** (worktree under `.claude/worktrees/`;
  commits `ff2127d` + review-fix `5fd4ac3`, with design note
  `docs/adr/004-replicas-and-identity.md`). Principles-reviewed
  2026-07-20: layering/Stage-F clean; the review's four Medium findings
  (shape-transition refusal, stable per-ordinal hashes, single-container
  ReadyReplicas contract, de-raced conformance assertions) are fixed in
  `5fd4ac3`. Merge pending owner decision; the gate-guard-at-validate
  Accept line is deliberately deferred to C2 (recorded in note 004), the
  live-Kubernetes conformance leg has not been re-run since the fixes, and
  the doc 04 §12 gate-table row will conflict with the 2026-07-20 table
  restructure on merge (trivial resolution).
- **Merged to main 2026-07-21** after the full
  `internal/adapters/runtime/kubernetes` integration suite (including the
  new replica and shape-transition conformance subtests) ran green against
  a live minikube cluster (302s). Design note 004 landed at
  `docs/adr/004-replicas-and-identity.md` per the ADR migration; the
  `HighAvailability` gate row was re-added in the current table format.
  C1 is done; the C2→C3/C4 chain is unblocked (§10 step 2).

### C2: Redpanda multi-broker clusters and replicated topics

- **Size:** L. **Depends:** C1.
- **Context:** Single-broker only today; `EventStream` has no replication
  factor; a broker restart pauses the pipeline.
- **Do:** `Provider(type: redpanda).spec.configuration.brokers: N` (default
  1) reconciled via C1's stable-identity replicas (seed list from ordinal
  hostnames); `EventStream.spec.replication: N` (schema + docs; default 1;
  validate refuses replication > brokers). Probe extends to cluster health
  (all brokers joined) and per-topic replication factor. Changing brokers
  1→3 is an in-place scale; 3→1 is refused (data-loss risk) without the
  destructive flag pair.
- **Accept:** integration test: 3-broker cluster to Ready; produce/consume
  during `docker kill`/pod delete of one broker succeeds; drift reports a
  missing broker; re-apply heals it; idempotent re-apply clean.

### C3: Distributed Kafka Connect workers

- **Size:** M. **Depends:** C1.
- **Context:** debezium and s3sink each run exactly one Connect worker;
  Connect is natively distributed (group.id + internal topics) — the
  single worker is the availability bottleneck for every Binding.
- **Do:** `spec.configuration.workers: N` on debezium/s3sink providers,
  realized as C1 replicas (no stable identity needed — Connect rebalances);
  connector REST calls go to any live worker (`internal/adapters/
  kafkaconnect` gains multi-address failover). Probe: connector RUNNING
  *and* worker count matches.
- **Accept:** integration test: 2 workers, kill one, Binding stays/returns
  RUNNING without `apply`; REST failover unit-tested; worker-count drift
  detected.

### C4: Object-store production posture: distributed MinIO or external S3

- **Size:** M. **Depends:** C1.
- **Context:** Single-node MinIO only. Production object storage is either
  distributed MinIO or (more commonly) an external S3/GCS/R2 endpoint.
- **Do:** Two halves: (1) verify and document the **external object store**
  path end-to-end — `Provider(type: s3)` with `external: true` +
  Connection/credentials against a real S3-compatible endpoint, Datasets and
  s3sink Bindings working with zero managed containers (this likely mostly
  works; make it a tested, documented first-class scenario). (2)
  `spec.configuration.nodes: N` for managed MinIO via C1 stable-identity
  replicas (erasure-coded pool, N∈{1,4+}).
- **Accept:** integration: external-mode suite against MinIO-as-external
  (simulating a cloud bucket); 4-node MinIO survives one node kill with
  sink traffic flowing; docs state the recommendation (external for prod,
  distributed MinIO for self-hosted).

### C5: Database HA posture — decision note

- **Size:** S (decision note only; implementation tasks spawn from it).
  **Depends:** —.
- **Context:** Managed Postgres/MySQL are single-container. Real HA
  databases (Patroni, Galera, cloud RDS) are operationally deep — likely
  *not* something platformctl should reimplement.
- **Do:** `docs/adr/005-database-ha-posture.md` deciding: managed
  databases are explicitly **single-node + backup/restore (C6) + fast
  drift-heal**, positioned for dev/staging and small production; production
  HA databases enter as `external: true` Sources through the Connection
  seam (already fully supported, CDC included). Enumerate what would change
  if a replication-capable managed mode is ever added (so nothing boxes it
  out). Update docs/planning/03 and README positioning accordingly.
- **Accept:** note committed; docs state the posture explicitly; no code.

### C6: Backup and restore capability

- **Size:** L (design in PR; the seam is new). **Depends:** A4 useful, not
  required.
- **Context:** No recovery story for data-bearing resources. Drift healing
  rebuilds infra; it cannot rebuild data.
- **Do:** New capability interface `reconciler.BackupCapableProvider`
  (`Backup(ctx, envelope, dest) (Manifest, error)` / `Restore(ctx, envelope,
  src) error`, dest/src being an object-store location + credentials).
  Implement for postgres and mysql (pg_dump/mysqldump streamed to the
  destination via a short-lived job container on the shared network) and s3
  (bucket sync). CLI: `platformctl backup <kind/name> --to <dataset|url>`
  and `platformctl restore`, honoring NFR-3-style flags for restore-over-
  existing. Scheduling stays external (cron/CI) — platformctl provides the
  primitive, not a scheduler. Gate `BackupRestore` (Alpha).
- **Accept:** integration: seed rows → backup → destroy → apply fresh →
  restore → rows present (postgres and mysql); s3 Dataset round-trip;
  backups never embed plaintext credentials; restore onto live data without
  flags refuses.
- **Status (2026-07-21): implemented on branch
  `worktree-agent-ad86992b28e68387f` (commit `309d165`) — reviewed
  2026-07-20, NOT merge-ready.** The interface shape, NFR-3 refusals,
  secret handling (0600 file mounts, nothing in env/argv/state/output), and
  layering are verified clean, but both accept-criterion round-trips fail
  against real Docker. Required before merge:
  1. dbjob's job containers break on entrypoint-bearing images (`minio/mc`'s
     ENTRYPOINT swallows `sh -c`): add a `ContainerSpec.Entrypoint` override
     to the runtime port (Docker `Config.Entrypoint`, K8s
     `container.Command` — the K1 mapping) with an F6 conformance subtest,
     and use it in dbjob.
  2. F1 violation: `engine/backup.go` hands the s3 adapter an in-network
     `http://<container>:9000` address that `remoteClient` dials from the
     CLI host — resolve through `EnsureReachable` via F4 endpoint facts
     (runtime name + container port), keeping the internal address for job
     containers only.
  3. F4 regression: the same engine block re-derives s3 conventions (port
     9000, scheme, secret key names, default network) — consume the
     provider's published endpoint facts instead.
  4. `dbjob.RunPipeline` must fail fast when either pipeline side exits
     nonzero (today the survivor blocks on the FIFO for the full 30-minute
     timeout).
  5. `docs/adr/007-backup-restore.md` is referenced five times but was
     never written — write it, covering the protect-vs-restore decision
     (prefer refusing protected targets), the restore-output `key`/`prefix`
     duplication, and the Docker-only mechanism (fail fast on other
     runtimes).

### C7: Ingress and HTTP routing on the Connection seam

- **Size:** L. **Depends:** B1 (K8s), C8 pairs naturally.
- **Context:** docs/adr/002 designated Connection as *the* ingress seam;
  proxy (socat TCP forward) is the only realization. HTTP endpoints (nessie,
  marquez, minio console, Connect REST) deserve hostname routing, not
  port-per-service.
- **Do:** `ingress` provider realizing managed `Connection`s with
  `spec.scheme: http|https`: on Docker, one shared reverse-proxy container
  (Caddy or Traefik, pinned) routing `Host(<connection-name>.<domain>)` →
  upstream, config reconciled per Connection; on Kubernetes, an Ingress (or
  Gateway API HTTPRoute — pick one in the PR, document why) per Connection.
  `ConnectionCapableProvider.SupportedConnectionSchemes()` gates schemes.
  Local-dev DNS: document `*.localhost` (resolves to loopback on modern
  systems). Gate `IngressProvider` (Alpha).
- **Accept:** integration: nessie REST reachable via
  `http://nessie.localhost:<port>` on Docker and via Ingress on K8s; two
  Connections route independently; inventory shows the routed URL; drift
  detects a mangled route and heals.

### C8: TLS termination and certificate handling

- **Size:** M. **Depends:** C7.
- **Context:** Every endpoint is plaintext with honest `Insecure` labeling
  (doc 07 §2.5); TLS was deferred as a Connection-provider capability.
- **Do:** Extend the ingress provider: `Connection.spec.tls: {secretRef}`
  (cert+key via SecretReference — file backend or K8s Secret) terminates
  TLS at the entrypoint; `spec.tls: {selfSigned: true}` provisions a local
  CA + per-host cert for dev (CA published in providerState for tool
  trust). On K8s, also accept cert-manager-managed Secrets by name
  (integration = referencing, not operating cert-manager). Endpoints served
  via TLS report `Insecure: false`; inventory renders https URLs. Gate
  `TLSTermination` (Alpha).
- **Accept:** integration: https endpoint with provided cert verifies
  against its CA; self-signed path works and inventory names the CA
  location; plaintext upstream is unreachable from outside when TLS mode is
  on (the entrypoint is the only route).

### C9: Monitoring stack provider

- **Size:** L. **Depends:** —. (Parallelizable with C1–C8.)
- **Context:** No metrics story. All core technologies expose Prometheus
  metrics natively (redpanda `/public_metrics`, Connect via JMX exporter,
  MinIO `/minio/v2/metrics`, postgres via postgres_exporter).
- **Do:** `prometheus` provider (managed Prometheus container/pod, pinned
  image; scrape config *generated from the platform's own endpoint
  inventory* — every provider adds a metrics `Endpoint` fact where the
  technology exposes one; postgres/mysql get sidecar exporter containers via
  their providers). Optional `grafana` provider provisioned with datasource
  + starter dashboards. Scrape what carries a metrics endpoint in state.
  `inventory --for prometheus` renders scrape config for users bringing
  their own Prometheus. Gate `MonitoringStackProvider` (Alpha).
- **Accept:** integration: managed Prometheus shows `up == 1` for broker,
  database exporter, connect, minio targets on the lakehouse example;
  `inventory --for prometheus` output is valid scrape config (parsed by
  promtool in the test); Grafana reaches Prometheus.

### C10: In-network reachability probes

- **Size:** M. **Depends:** —.
- **Context:** Doc 07 §2.4 deferral: host-side TCP probes report the host
  audience's truth; "can container A reach B" needs an in-network vantage
  point.
- **Do:** Add `ContainerRuntime.ProbeReachable(ctx, network, target) error`
  — Docker: `exec` a TCP dial inside an existing managed container on that
  network (or a transient probe container, pinned busybox image); K8s: an
  ephemeral pod (or exec) in the namespace. Engine uses it for external
  Connections *consumed by in-network Bindings*, reporting the audience
  distinctly (`ExternalEndpointUnreachableInNetwork` vs the existing
  host-side condition).
- **Accept:** conformance subtest on all three adapters; integration:
  a Connection reachable from the host but firewalled from the network
  reports the in-network condition specifically.

---

## 6. Stage D — Pipeline-infrastructure completeness

Theme: the infrastructure-level capabilities real production data pipelines
need beyond the CDC→lake spine: schema-carrying formats, replay/ingest,
serve-to-database, VPC reach, dead-letter handling, data lifecycle. All
provider work here lands on **existing seams** — no core-model rework is
expected; any task that discovers otherwise must stop and raise a design
note first (per doc 06 §3).

**Stage exit criteria:**
- [ ] A CDC pipeline lands **Parquet** in the lake, schema-evolved via a
      registry, queryable by Spark/Trino through the Nessie catalog.
- [ ] Lake data can be replayed into an EventStream (ingest) and an
      EventStream can be served into a relational Source (jdbc sink), both
      to Ready through `validate`-checked Bindings.
- [ ] A Binding across a WireGuard-tunneled Connection reaches a database
      only routable inside a private network, CDC RUNNING.
- [ ] Sink Bindings support declared dead-letter topics; poison messages
      land there without stopping the connector.

### D1: Schema registry support

- **Size:** L (this is doc 07 §2.3's named "real design chunk").
  **Depends:** —.
- **Context:** JSON-only sinks; Parquet/Avro blocked on schema-carrying
  converters (decision recorded in doc 07 §2.3). Redpanda ships a built-in
  Confluent-compatible schema registry — no new container is needed for the
  default path.
- **Do:** Design + implement: (1) `Provider(type: redpanda)` gains
  `configuration.schemaRegistry: enabled` — the provider enables/exposes the
  built-in registry and publishes its endpoint in providerState + inventory;
  (2) `Binding.spec.options.format/converter` block (validated by
  `BindingOptionsValidator`) selects json | avro | protobuf with registry
  URL auto-wired from the EventStream's provider (the user never types a
  registry URL for the managed case); (3) compatibility check: a Binding
  declaring a schema-carrying format against a provider chain without a
  registry endpoint fails at validate with the standard capability-error
  shape. A standalone registry provider (external/Confluent) is a follow-up,
  not this task. Gate `SchemaRegistrySupport` (Alpha).
- **Accept:** integration: Avro CDC end-to-end (Debezium Avro converters →
  subjects visible in the registry → consumer decodes); validate-time
  failure for format-without-registry; inventory shows the registry
  endpoint; docs/planning/03 documents the options block.
- **Status (2026-07-21): implemented on branch
  `worktree-agent-ac9cf81b40022f246`** (commits `0a9bc77` + review-fix
  `2a05bd4`). Principles-reviewed 2026-07-20: spec conformance, §5.2 error
  shape, schema↔doc sync, and Stage-F invariants all verified; the one
  Medium finding (checkSchemaFormat intercepted non-schema formats like
  parquet while the gate was disabled, breaching the Alpha/disabled
  convention) is fixed in `2a05bd4` (scoped to avro/protobuf). Merge
  pending owner decision + one live `TestAvroCDCEndToEnd` integration run;
  same doc 04 §12 merge-conflict note as C1.
- **Merged to main 2026-07-21.** The live run first failed twice, fixed in
  `6ec290a`: (a) the stock Debezium image ships no Confluent Avro
  converter — a version-pinned testdata image now adds the plugin (the
  s3sink pattern), documented as §7.3's worker-image requirement in
  doc 03; (b) DNS-label topic prefixes contain hyphens, illegal in Avro
  namespaces — the provider now sets
  `schema.name.adjustment.mode`/`field.name.adjustment.mode: avro` for
  schema-carrying formats. `TestAvroCDCEndToEnd` green against real
  Docker (33.5s). D2 (Parquet) is unblocked (§10 step 3).

### D2: Parquet sink format end-to-end

- **Size:** M. **Depends:** D1.
- **Context:** s3sink's parquet listing requires schema-carrying records
  (`SupportedSinkFormats` comment); the acceptance example deviates from the
  §6 sketch by using json (checkpoint open item 2).
- **Do:** With D1's converters, enable `Dataset.spec.format: parquet`
  through s3sink (connector parquet format class + registry converters;
  s3sink image docs updated for the required jars). Flip the acceptance
  example (`examples/cdc-attendance/`) to parquet, closing the deviation;
  keep a json variant fixture for the schemaless path.
- **Accept:** integration: parquet objects land and are readable
  (parquet-tools or Go parquet reader in-test asserts row content);
  acceptance example applies to Ready with parquet; format-change
  (json→parquet) updates the connector without recreating broker/db/store
  (existing Phase 4 exit-criterion bar).

### D3: JDBC database-sink provider (EventStream → Source)

- **Size:** M. **Depends:** D1 (schema-carrying records make JDBC sinks
  sane); checkpoint backlog item 1.
- **Context:** `DatabaseSinkCapableProvider` seam exists with no
  implementation (design note 001); pairing validates structurally then
  fails with the standard capability error.
- **Do:** `jdbcsink` provider over the existing Connect-worker pattern
  (`internal/adapters/kafkaconnect`), registering a JDBC sink connector
  (Confluent or Aiven connector class — pick, pin, document the image
  requirement like s3sink does) writing an EventStream into a
  postgres/mysql Source's database. Implements
  `DatabaseSinkCapableProvider` + `SpecValidator` +
  `BindingOptionsValidator`; secrets via the Source's/Connection's existing
  secretRef plumbing. Gate `JDBCSinkProvider` (Alpha).
- **Accept:** integration: topic → rows appearing in a managed postgres
  Source table (insert + upsert modes); capability error remains exact for
  non-capable providers; config drift detection matches the
  debezium/s3sink contract (key names only).

### D4: Object-store ingest provider (Dataset → EventStream)

- **Size:** M. **Depends:** D1 helpful; checkpoint backlog item 1.
- **Context:** `IngestCapableProvider` seam exists with no implementation —
  the replay/backfill direction (lake → stream) real pipelines use for
  reprocessing.
- **Do:** `s3source` provider on the Connect-worker pattern using an S3
  source connector, reading a Dataset's bucket/prefix (+format) into an
  EventStream topic. Same validator/capability/drift bars as D3. Gate
  `IngestProvider` (Alpha).
- **Accept:** integration: objects written by the sink suite are replayed
  into a fresh topic and consumed; ordering/offset semantics documented;
  validate rejects `mode: ingest` for non-capable providers with the
  standard error.

### D5: Tunnel provider (WireGuard) on the Connection seam

- **Size:** L. **Depends:** —. (checkpoint backlog item 5; doc 07 §2.4
  deferral.)
- **Context:** The Connection kind was explicitly designed so tunnels chain
  a managed Connection's egress additively (design note 002 addendum) — no
  schema change expected.
- **Do:** `wireguard` provider realizing a managed Connection whose
  upstream is only reachable through a WireGuard peer: one tunnel container
  (pinned image, NET_ADMIN documented) joining the peer network, with the
  existing proxy/forwarder chaining through it. Keys via SecretReference
  (private key never in state/inspect — file mount). Probe: handshake
  recency + upstream dial through the tunnel. Test rig: a "VPC" simulated
  by an isolated Docker network hosting a database + a WireGuard responder.
  Gate `TunnelProvider` (Alpha).
- **Accept:** integration: CDC Binding RUNNING against a database in the
  isolated network, unreachable without the tunnel (asserted); key rotation
  via SecretReference re-establishes the tunnel; destroy leaves no tunnel
  artifacts.

### D6: Dead-letter queue support for sink Bindings

- **Size:** S. **Depends:** —.
- **Context:** Kafka Connect sinks support `errors.tolerance` +
  `errors.deadletterqueue.topic.name`; platformctl neither models nor
  validates it — a poison message stops a production pipeline.
- **Do:** `Binding.spec.options.deadLetter: {stream: <EventStream name>,
  tolerance: all|none}` on sink-mode Bindings (schema + docs);
  compatibility resolves the named EventStream in-graph (ordering: DLQ
  topic exists before the connector); s3sink/jdbcsink translate to the
  connector's DLQ config; probe includes DLQ config in the drift diff.
- **Accept:** integration: sink with DLQ declared, one poison record →
  lands in the DLQ topic, connector stays RUNNING, valid records keep
  flowing; validate rejects a deadLetter naming a missing EventStream.

### D7: Dataset lifecycle and retention policies

- **Size:** S. **Depends:** —.
- **Context:** Buckets grow forever; ILM/versioning are unmanaged
  (doc 07 §2.1 lists them as not-yet-modeled).
- **Do:** `Dataset.spec.lifecycle: {expireAfterDays, versioning:
  enabled|suspended}` (schema + docs); s3 provider reconciles bucket
  lifecycle rules + versioning via the S3 API; probe diffs them; external
  Datasets: configure-only under the ExternalConfigurer contract.
- **Accept:** integration: lifecycle rule visible via S3 API after apply;
  out-of-band rule change detected as drift and healed; docs/planning/03
  updated.

### D8: First-class Catalog↔warehouse reference

- **Size:** M. **Depends:** —.
- **Context:** Doc 07 §2.3 deferral: `spec.nessie` can carry warehouse
  config today, but a first-class `warehouseRef` (Catalog → Dataset) needs
  the dependency graph to learn about refs inside engine blocks — ordering
  + validation, deliberately not bolted on ad hoc.
- **Do:** Add `Catalog.spec.warehouseRef` (top-level, kind-checked to
  Dataset — *not* inside the engine block, so the graph needs no
  engine-block introspection after all; record this simplification in the
  PR); graph orders Dataset before Catalog; nessie provider wires the
  warehouse location from the referenced Dataset's bucket/prefix +
  endpoint; validate rejects a warehouseRef to a non-Dataset.
- **Accept:** lakehouse example uses warehouseRef; Spark/Trino inventory
  snippets pick up the warehouse automatically; ambiguity/negative-path
  tests per the doc 07 §0.2 pattern.

### D9: Compute-engine infrastructure — decision note

- **Size:** S (decision note; at most one follow-up provider spawned from
  it). **Depends:** —.
- **Context:** Users querying the lakehouse need a compute engine (Trino,
  Spark, Flink). Provisioning *engine infrastructure* is in-scope
  infrastructure; jobs/queries remain out-of-scope orchestration (product
  boundary, doc 01). Today `inventory --for` assumes a user-operated
  engine.
- **Do:** `docs/adr/006-compute-engines.md`: decide whether platformctl
  ships a `trino` provider (coordinator + workers via C1 replicas,
  catalog auto-configured from Nessie/MinIO facts — the strongest
  UX win: `inventory --for trino` becomes "it's already running"), a
  `flink` session-cluster provider, both, or neither for now. Recommend:
  trino first (read path completes the lakehouse story; Flink is
  application-adjacent). The note defines the provider's scope line: engine
  *infrastructure* yes, job submission no.
- **Accept:** note committed with a decision and a task spec for the chosen
  provider (added to this backlog as D10 when accepted).

### D10: Trino compute-engine provider

- **Size:** L. **Depends:** C1 (coordinator/worker replicas); D8 helpful,
  not required (warehouseRef makes catalog-to-Dataset wiring first-class,
  but `spec.nessie` already carries warehouse config today per D8's
  context, so the Trino provider can read either shape).
- **Context:** design note `docs/adr/006-compute-engines.md` decided
  Trino first: read path completes the lakehouse story, catalog
  auto-configuration is the strongest inventory UX win, and Trino's
  stateless-of-its-own-data shape avoids the storage questions a stateful
  engine would raise. `inventory --for trino`
  (`cmd/platformctl/toolconfig.go:226-243`) today assumes a user-operated
  engine and only renders a paste-ready snippet.
- **Do:** `trino` provider: one coordinator container + N worker
  containers via C1's `ContainerSpec.Replicas` (`StableIdentity: false` —
  workers hold no durable per-replica state). New
  `Provider(type: trino).spec.configuration.catalogRef` (Catalog,
  kind-checked, same graph-ordering discipline as D8's warehouseRef —
  Catalog reconciles before the Trino Provider that reads it); the
  provider resolves the referenced Catalog's REST endpoint and its
  warehouse Dataset's S3/MinIO Provider endpoint + `SecretReference`,
  and writes `etc/catalog/lakehouse.properties` into the coordinator on
  reconcile (drift-checked: out-of-band catalog config edits are detected
  and healed, matching the debezium/s3sink config-drift bar). Probe:
  coordinator `/v1/info` reachable and `starting: false`, worker count in
  `ContainerState.ReadyReplicas` matches declared replicas. `inventory
  --for trino` gains a live-endpoint branch: when a `trino` Provider
  exists in the applied state, render the coordinator's JDBC URL / UI
  address instead of (or alongside) the paste-ready snippet — "it's
  already running" per the design note. Gate `TrinoProvider` (Alpha,
  disabled).
- **Accept:** integration: `trino` Provider + `catalogRef` to the
  lakehouse Catalog reaches Ready; a query against a table written by the
  existing CDC→Parquet path (D1/D2) returns rows through the coordinator;
  scale-up (1→3 workers) is in-place, no coordinator restart;
  out-of-band catalog config change reported as drift and healed;
  idempotent re-apply makes zero Docker API calls beyond probes;
  `inventory --for trino` reflects the live coordinator once the provider
  is applied; capability/negative-path test: `catalogRef` to a
  non-Catalog kind rejected at validate with the standard error.

---

## 7. Stage E — Effortless-pipeline DX and contribution readiness

Theme: the "worry about architecture, not configuration" promise, plus
doc 07 Gate 3 (public API and contribution readiness). Most tasks are
independent and parallelizable; E1/E2 deliver the largest direct UX value.

**Stage exit criteria:**
- [x] `platformctl init cdc-to-lake && platformctl apply` reaches Ready on
      a machine with only Docker installed, no manifest editing.
- [ ] Every schema-legal misconfiguration class in the negative-test corpus
      fails at `validate`, not apply.
- [ ] A third-party provider can be built from the provider-author guide +
      conformance suite alone (proven by an example provider PR that
      touches no core code).
- [ ] Versioned release artifacts (multi-platform binaries) build from CI.

### E1: Blueprint scaffolding — `platformctl init`

- **Size:** M. **Depends:** —.
- **Context:** New users start from `examples/`; nothing generates a
  tailored starting point. The fastest "headache-free" win.
- **Do:** `platformctl init <blueprint> [--dir]` writing a ready-to-apply
  manifest set + `.env` template + README from embedded templates.
  Blueprints v1: `cdc-to-lake` (postgres→debezium→redpanda→s3sink→minio),
  `lakehouse` (adds nessie/marquez/connection), `stream-basics`
  (redpanda + topics), `external-cdc` (external Source + Connection +
  managed CDC). Templates use auto-ports/default images (already supported)
  so zero edits are required; secrets pre-wired to the env backend with the
  `.env` template naming every key (`Preflight` reports any gap on first
  apply). `--list` enumerates blueprints machine-readably.
- **Accept:** e2e test per blueprint: init → validate green with no edits;
  cdc-to-lake init → apply → Ready in integration; README quickstart
  rewritten around `init`.

### E2: Configuration-minimization audit ("zero-boilerplate defaults")

- **Size:** M. **Depends:** —.
- **Context:** Auto host ports, versionprofile default images, and default
  namespace already exist. Nobody has audited what *remaining* fields users
  must type that the system could default or infer.
- **Do:** Audit every example manifest field-by-field: for each
  user-supplied value, classify {must-declare, could-default,
  could-infer-from-graph}. Implement the safe defaults (e.g. a Binding's
  `bootstrapServers` inferred from the target EventStream's provider —
  already resolvable in-graph; single-network default name; Connect worker
  image defaults per versionprofile). Every applied default must appear in
  `plan`/`status` output (explicit, not hidden). Deliverable includes the
  audit table in the PR.
- **Accept:** examples shrink measurably (line counts recorded in the PR);
  `plan -o json` exposes effective defaults; no default silently changes
  existing manifests' behavior (golden plans updated deliberately or
  unchanged).

### E3: Tool-config renderer expansion

- **Size:** S. **Depends:** (C9 for prometheus; D1 for registry-aware
  kafka snippets — otherwise independent).
- **Context:** `inventory --for spark|trino|dbt|psql|s3|kafka` exists;
  doc 07 §2.3 names Flink/Dagster/Metabase/Superset as addable without
  model changes.
- **Do:** Add renderers: `dagster` (resource config for postgres/s3/kafka),
  `flink` (SQL catalog + connector properties), `metabase`/`superset`
  (database connection details), `prometheus` (C9). Renderers read only
  observed endpoint facts; secrets stay referenced by name.
- **Accept:** golden-file test per renderer; `--for` list output enumerates
  all; each snippet syntactically valid for its tool (parse where a parser
  is cheap, golden otherwise).
- **Done (2026-07-21, merged):** dagster/flink/metabase/superset landed
  (`knownTools` map, golden/parse tests). `prometheus` remains with C9 —
  there is no metrics-endpoint fact to render until it lands.

### E4: Condition and error catalog — `platformctl explain`

- **Size:** S. **Depends:** —.
- **Context:** Conditions (`DriftDetected`, `SecretChanged`,
  `ExternalEndpointUnreachable`, `LineageEndpointDeclaredNotConsumed`, ...)
  and refusal errors are documented across scattered planning docs.
- **Do:** `platformctl explain <ConditionType|error-token>`: embedded
  catalog (one markdown fragment per condition: meaning, likely causes,
  remedy commands), sourced into the docs site build; `status` output
  footnotes point at `explain`. Catalog completeness enforced by a test
  that every `status.SetCondition` type string in the codebase has a
  catalog entry.
- **Accept:** completeness test green; `explain -o json` structured;
  docs site renders the catalog.

### E5: Provider-owned schema fragments and typed option validation

- **Size:** L. **Depends:** —. (Doc 07 §3.1 in full.)
- **Context:** Provider `spec.configuration`, engine blocks, and Binding
  `options` are open-ended objects; `SpecValidator` coverage is uneven and
  not schema-generated.
- **Do:** Each provider ships a JSON-Schema fragment for its configuration
  block (and per-engine Source/Catalog blocks, and per-mode Binding
  options), registered alongside the provider; `manifest.Load` composes
  fragments by discriminator (`type`/`engine`/`mode`+provider) into
  validation; `docsgen` renders per-provider reference sections from the
  fragments. Keep `SpecValidator` for cross-field/graph checks only.
  Negative-test corpus: one fixture per known apply-time misconfiguration
  class, asserted to fail at validate.
- **Accept:** every shipped provider has a fragment; corpus green;
  generated docs show per-provider option tables; unknown-key warnings on
  provider config blocks (typo protection — the single most common user
  error class).

### E6: Provider author contract — guide, conformance suite, exemplars

- **Size:** L. **Depends:** E5 (fragments are part of the contract); F5
  (the invocation contract must stabilize before it is documented and
  conformance-tested — doc 09 §3-F5).
- **Context:** Doc 07 §3.4. Contribution requires executable contracts, not
  conventions.
- **Do:** (1) `docs/contributing/provider-authoring.md`: lifecycle
  semantics, required/optional interfaces (Provider, SpecValidator,
  capability interfaces, ExternalConfigurer, VersionedProvider,
  BackupCapable...), providerState rules, endpoint publication, drift
  expectations, feature-gate procedure. (2)
  `internal/ports/reconciler/conformance`: a suite driving any provider
  through Reconcile/Probe/Destroy/idempotency/drift against the fake
  runtime, adopted by all shipped providers. (3) The compiled-in vs plugin
  decision note (Phase 8's seam): recommend compiled-in until the plugin
  protocol lands; record the criteria that would trigger building it.
- **Accept:** all nine-plus providers pass the reconciler conformance
  suite in CI; the guide is validated by using it to (re)build the noop
  provider from scratch in a test branch (recorded in the PR).

### E7: Documentation truth sweep

- **Size:** S. **Depends:** best done near stage close.
- **Context:** Doc 07 §3.3 residuals: roadmap-vs-checkpoint reconciliation
  (partially addressed by the doc 04 update landing with this plan),
  single-namespace-era prose, undeclared support levels of Alpha providers.
- **Do:** Sweep docs/planning + README + docs/reference for: stale
  namespace prose; a support-level statement per Alpha/Beta gate (what
  "Alpha" commits to: API may change, enabled-by-default or not, test
  coverage level); convert remaining stale roadmap checkboxes to
  historical-record notes; retire the test-only `ContainerProvider` gate
  (checkpoint item 4); verify every doc cross-reference resolves.
- **Accept:** schema-doc-sync agent clean; a support-level table exists in
  doc 04; no planning doc contradicts docs/history/checkpoint.md or this plan.

### E8: Release engineering and CI matrix

- **Size:** M. **Depends:** B8 (K8s CI leg), A10 (digest workflow).
- **Context:** CI runs unit + integration on one runner; releases are a
  local tag + `main.version` bump; no published binaries.
- **Do:** goreleaser (or equivalent) building linux/darwin × amd64/arm64
  binaries on tag, with `--version` stamped from the tag; CI matrix legs:
  unit+race, Docker integration (existing), Kubernetes (B8), machine-output
  harness (A7), example-validate; scheduled digest-refresh (A10) and a
  weekly full-matrix soak. Release checklist doc (gates table review,
  upgrade-notes check) — mechanical enough for an agent to execute.
- **Accept:** a tagged release produces downloadable binaries; CI matrix
  green; release checklist committed and exercised once.

---

## 7.5 Stage F — Segregation readiness (systemic fixes from live-testing findings)

Theme: close the five recurring bug classes that live Kubernetes/Docker
testing exposed (analysis and rationale:
`docs/planning/09-systemic-findings-and-segregation-readiness.md`) at the
**system level** — in `domain`/`ports`/`application` — so no current or
future provider has to implement, or can forget, the fix. This stage is the
hard prerequisite for E6 (provider-author contract) and Phase 8
(out-of-process plugins / non-container runtimes): its exit bar is doc 09
§5's segregation-readiness definition of done. No new feature gates — every
task is an internal contract change, invisible at the manifest surface
except where noted.

Class → task mapping (doc 09 §2): Class 1 (topology leaked into
dependents) → F1; Class 2 (exists ≠ ready ≠ reachable) → F3; Class 3
(under-declared intent) → F2 + F6; Class 4 (identity by convention) → F4;
Class 5 (contract tests don't prove translation) → F6; provider-contract
instability (the `*Aware` setter accretion) → F5.

**Stage exit criteria:**
- [x] The architecture test suite fails any provider or domain code that
      constructs a loopback/localhost address (allowlist: in-container
      healthcheck commands, runtime adapters).
- [x] `connection.DialAddress()`'s managed-Connection loopback guess no
      longer exists; external Connections expose `DeclaredAddress()` only.
- [x] All nine providers' admin-call retry loops go through one shared
      `WithReachable` helper that re-resolves the address per attempt.
- [x] `PortBinding` declares an explicit audience; the fake runtime refuses
      resolution of undeclared ports (strict interpreter), and the
      `HostPort: 0` magic value is retired.
- [x] The conformance suite contains entrypoint-faithfulness,
      delayed-listen readiness, immediate-dialability, and port-audience
      subtests, green on fake, Docker, and Kubernetes.
- [x] Providers receive all inputs through a single request-scoped struct;
      the `ProviderResourceAware`/`SecretsAware`/`ResourceSetAware` setter
      interfaces are removed; no provider holds cross-call state.

### F1: Reachability closure — addresses become unconstructible

- **Size:** M. **Depends:** —.
- **Context:** Doc 09 Class 1: ten independent call sites (8 providers +
  engine + domain) hardcoded `127.0.0.1:port`; all now route through
  `EnsureReachable` (B1, `81025c9`, `88b8329`) — but by convention only;
  nothing prevents call site number eleven.
- **Do:** (1) Split `connection.Connection.DialAddress()`: external
  Connections get `DeclaredAddress()`; the managed branch is deleted (a
  managed Connection's reachable address requires a runtime — domain
  cannot answer it, and the type system should say so). Audit
  `HostEndpoint()` identically. (2) Add
  `runtime.WithReachable(ctx, rt, name, port, opts, fn)` — resolve, call,
  close, and on retryable failure re-resolve a fresh address per attempt
  (the B8 nessie/openlineage fix, generalized), with a configurable
  ready-wait (default 30s, the docs/history/errors.md postgres fix generalized).
  Migrate every provider's hand-rolled wait/retry loop to it. (3) An
  architecture test (layering-grep style) banning loopback string literals
  in `internal/adapters/providers` and `internal/domain`, allowlisting
  in-container healthcheck commands and runtime adapters.
- **Accept:** arch test in CI, proven by a fixture violation failing it;
  grep for bespoke retry loops in providers comes back empty; full Docker +
  K8s integration suites green (behavioral no-op on both runtimes).

### F2: Explicit port audience in the runtime port

- **Size:** M. **Depends:** —.
- **Context:** Doc 09 Class 3 / K10: `HostPort: 0` was overloaded from
  "random ephemeral publish" to "in-network only" (B8); undeclared
  listeners worked on Docker and broke on Kubernetes (K8/K9).
- **Do:** Add `PortBinding.Audience: host | internal` (explicit; the
  `HostPort: 0` convention retired with a migration sweep over provider
  call sites). Document the port-contract rule: every listener a dependent
  may dial must be declared. Make the fake runtime the strict interpreter:
  it refuses `EnsureReachable`/in-network resolution of undeclared ports,
  so under-declaration fails in unit tests before any cluster sees it.
  Conformance subtests for both audiences on all three adapters.
- **Accept:** no `HostPort: 0` publishes remain; fake-strictness proven by
  a test that an undeclared-port dial fails on fake but the same declared
  spec passes everywhere; conformance green ×3.

### F3: Ready-means-serving in the port contract

- **Size:** S. **Depends:** F1 (the helper is the enforcement point).
- **Context:** Doc 09 Class 2: API-object-exists, process-healthy, and
  endpoint-reachable were repeatedly conflated (NodePort iptables race,
  port-forward/listen race, pg_isready socket-only window).
- **Do:** Harden `EnsureReachable`'s documented contract: the returned
  address must be currently dialable — adapters absorb asynchronous
  programming races (the K3 poll generalized). Conformance subtests:
  immediate-dialability after (re)creation; delayed-listen container
  (healthcheck green before TCP listen) documenting per-adapter
  healthy-vs-serving semantics.
- **Accept:** both subtests green on fake, Docker, and a live cluster;
  provider code contains no residual bespoke ready-waits (enforced by
  F1's migration).

### F4: Identity by handle — one naming authority

- **Size:** M. **Depends:** —.
- **Context:** Doc 09 Class 4 / K7: the Connection-forwarder name was
  guessed wrong twice, in two different sessions, because runtime object
  naming is an unwritten convention re-derived at each call site.
- **Do:** Centralize resource→runtime-object naming in one domain package
  used by both realizing providers and every consumer (engine probes,
  drift, gc, inventory). Extend endpoint facts
  (`internal/domain/endpoint`) with `(runtime object name, containerPort,
  audience)` and convert cross-resource dial sites (engine's Connection
  probe, Binding wiring) to fact-lookup instead of name re-derivation.
- **Accept:** grep shows no consumer-side name construction outside the
  naming package; engine's Connection probe resolves via facts; a unit
  test proves a renamed convention breaks exactly one package.

### F5: Provider invocation contract — request struct, stateless providers

- **Size:** L (short design note in the PR is enough; doc 09 §3-F5 is the
  design). **Depends:** F1 (so the new contract never carries an
  address-construction affordance).
- **Context:** Doc 09 §3-F5: inputs reach providers via accreting setter
  interfaces (`ProviderResourceAware`, `SecretsAware`, `ResourceSetAware`)
  and widening method signatures (`LineageAware.ConfigureLineage` grew an
  `rt` parameter in `81025c9`, breaking every implementor). Stateful
  set-then-call providers cannot become out-of-process plugins.
- **Do:** Introduce `reconciler.Request` (envelope, runtime, realizing
  Provider resource, resolved secrets, validated resource set — additive
  fields only) as the single input to Reconcile/Destroy/Probe and the
  capability methods. Migrate incrementally behind an engine-side shim
  (both shapes supported until all nine providers move; then delete the
  setter interfaces and the shim). Capability *marker* interfaces stay.
- **Accept:** setter interfaces gone; providers hold no cross-call state
  (constructor takes nothing but static config); every capability method
  takes the Request; engine special-cases for `*Aware` deleted; full
  suites green on both runtimes.

### F6: Conformance ratchet and translation-fidelity gate

- **Size:** S (policy + back-fill; the subtests land in F2/F3).
  **Depends:** F2, F3.
- **Context:** Doc 09 Class 5: most live-caught bugs did not leave a
  contract-level reproduction; the entrypoint-image (Cmd/Args) class still
  has no conformance subtest today.
- **Do:** Back-fill the entrypoint-faithfulness subtest (an image *with*
  an ENTRYPOINT; Cmd must append, not replace). Record the policy in
  doc 06 (agentic execution guide) and the provider/runtime authoring
  docs: a bug found only by live testing lands with a contract-level
  reproduction in the same commit, or a documented per-runtime-difference
  entry in doc 07 when it can't be expressed at the contract level.
  Formalize the runtime-parameterized real-examples suite (B8) as the
  acceptance bar for any future runtime adapter.
- **Accept:** entrypoint subtest green ×3; policy text committed; doc 07
  Cross-Runtime section notes the gate for future adapters.

---

## 7.6 Stage G — Structural debt (2026-07-21 survey)

Theme: seams the 2026-07-21 structural survey found that will degrade
under Stages C/D/E if left alone. None is a correctness bug; each is
task-shaped preventive work. Tasks are independent unless noted. Two
survey results are **dispositions, not tasks**: (a) the "providers/state
assume one instance per Provider" finding is the C1→C2/C3/C4 chain —
the runtime-level seam already exists on C1's branch (ADR 004); providers
and state adopt it per-task, and C2 must decide the
`ResourceState`-level replica representation before widening state. (b)
Patterns the survey verified as healthy and worth replicating unchanged:
the uniform `loadAndValidate` command path, `toolconfig.go`'s map-based
renderer dispatch, the 96-line registry indirection, and `meta.json`
`$ref` schema composition (low E5 risk).

### G1: Provider scaffolding kit (`providerkit`)

- **Size:** M. **Depends:** — (do before or alongside the next new
  provider; hard prerequisite for E6).
- **Context:** ~150–200 lines of near-identical scaffolding are
  copy-pasted across all seven technology providers: `hostPort()`
  (`internal/adapters/providers/postgres/postgres.go:65-76` ==
  `mysql/mysql.go:79-90`), `network()`, `reachableAddr()`, the
  reconcile-instance skeleton (profile → credentials → labels →
  EnsureNetwork → EnsureVolume → EnsureContainer → WaitHealthy), and the
  credential-rotation dance (try-desired → try-previous → rotate → retry,
  postgres `ensureSuperuser` vs mysql `ensureRootPassword`). A bug fix in
  one copy does not propagate; E6's author contract would document
  tribal knowledge instead of an SDK.
- **Do:** Extract `internal/adapters/providers/providerkit` (adapter-layer
  helper package — providers may import it; domain/ports must not)
  offering: `HostPort`, `Network`, `ReachableAddr`, a single-container
  ensure helper, and a generic credential-rotation helper. Migrate
  postgres and mysql first (reference pair), then the rest mechanically,
  one commit per provider.
- **Accept:** postgres and mysql each shrink by >100 lines; behavior
  identical (full unit + Docker integration suites green, zero test
  edits); archtest still green; E6's guide references providerkit as the
  documented shape.

### G2: Engine kind-dispatch table

- **Size:** S. **Depends:** —.
- **Context:** the engine's kind/lifecycle special cases (SecretReference,
  external-no-provider, external-with-provider) are re-checked as
  independent if-chains in four methods — `reconcileOne`
  (`internal/application/engine/engine.go:386-417`),
  `probeOneAgainstState` (:528-551), `applyDeleteOne` (:870-910), and
  `Destroy` (:986-1057). A future special-cased kind must be added in all
  four places with nothing enforcing coverage — a live correctness risk
  for any Stage-D/E kind that needs engine-level handling.
- **Do:** Introduce one internal `kindHandler` table (per-kind/lifecycle
  hooks for reconcile/probe/delete) that the four methods consult;
  register the existing special cases in it. Pure refactor — no behavior
  change.
- **Accept:** `engine_test.go` passes unchanged; adding a fake special
  kind in a test requires touching exactly one table; the four methods
  contain no kind-name string checks outside the table lookup.
- **Done (2026-07-21, merged):** `kindHandler` table in
  `internal/application/engine/kind_handler.go`; the four methods consult
  it; `TestFakeKindHandlerReachesAllFourDispatchPoints` proves the accept
  criterion. Two recorded findings from the refactor: (a) the historical
  per-method check order differed (SecretReference-first vs
  External-first) — harmless, since a validated SecretReference can never
  carry `spec.external` (schema `additionalProperties: false`); one table
  order now serves all four. (b) Defense-in-depth asymmetry: External is
  double-enforced (plan + engine, per NFR-3) but Imported teardown relies
  on plan-time exclusion only (`ComputeDestroy`); adding the engine-side
  Imported re-check is a candidate S-task if NFR-3's posture is ever
  extended to Imported.

### G3: Split `kubernetes.go` along its natural seams

- **Size:** S. **Depends:** **after the C1 branch merges** (C1 rewrites
  large parts of this file; splitting first guarantees painful
  conflicts).
- **Context:** `internal/adapters/runtime/kubernetes/kubernetes.go` is
  1197 lines spanning network/NetworkPolicy CRUD, volumes, container
  ensure, exec/logs, Services, and three reachability strategies;
  `convert.go` shows the split precedent. Stage C (StatefulSets) and
  C7/C8 (ingress) land more code exactly here.
- **Do:** Split into `network.go`, `volume.go`, `container.go`,
  `reachability.go`, `exec.go` (same package, zero behavior change).
- **Accept:** build + runtime conformance suite pass unchanged; no file
  exceeds ~400 lines.

### G4: Condition-reason catalog (E4 prerequisite)

- **Size:** M. **Depends:** — (must land before E4).
- **Context:** ~52 distinct condition `Reason` strings exist across ~156
  construction sites, but only 3 are named constants in
  `internal/domain/status`; the rest are inline literals with
  inconsistent spellings for the same semantic (postgres `WALNotLogical`
  vs mysql `BinlogNotRowFormat` are both "CDC precondition drifted").
  E4's `explain` catalog cannot be complete against unenumerable strings.
- **Do:** Declare every reason as a typed constant in
  `internal/domain/status`; migrate providers/engine to the constants
  (mechanical, one commit per package); add a test that fails on any
  `Reason:` string literal outside the status package. Deduplicate
  same-meaning reasons where doing so is not a user-visible break (note
  any rename in docs/upgrade-notes.md).
- **Accept:** literal-ban test green in CI; E4's later catalog
  completeness test can enumerate `status` package constants; no
  provider defines a reason locally.

### G5: Conformance suite per-area split

- **Size:** S. **Depends:** after the C1 branch merges (C1 adds ~200
  conformance lines).
- **Context:** `internal/ports/runtime/conformance/conformance.go` is one
  flat `Run` with 21+ inline subtests sharing implicit fixtures; the F6
  ratchet guarantees growth.
- **Do:** Split into per-area files (`network.go`, `volume.go`,
  `container.go`, `reachability.go`, `replicas.go`) each exposing a
  `run*` helper `Run` calls in order; make shared fixtures explicit
  parameters.
- **Accept:** identical subtest names/count pass on fake, Docker, and
  Kubernetes; no file exceeds ~250 lines.

### G6: Shared integration-test harness

- **Size:** S. **Depends:** —.
- **Context:** all 17 `cmd/platformctl/*_integration_test.go` files are
  correctly build-tagged, but setup/cleanup (runtime construction, env
  skips, container cleanup) is re-implemented per file (~21 helper
  copies).
- **Do:** Extract `cmd/platformctl/integration_harness_test.go`
  (`requireDocker(t)`, runtime construction, cleanup registration); new
  tests must use it; migrate existing files opportunistically, never as a
  big-bang rewrite.
- **Accept:** harness exists and ≥3 existing files migrated as the
  pattern-proof; doc 06 notes it as the convention for new integration
  tests.
- **Done (2026-07-21, merged):** `requireDocker`/`registerDockerCleanup`
  in `cmd/platformctl/integration_harness_test.go`; docker/redpanda/
  mariadb suites migrated and re-verified live; remaining
  close-to-the-shape suites (cdc, sink, avro_cdc, drift_config) are the
  opportunistic follow-up pass.

---

## 8. New feature gates introduced by this plan

Append to doc 04 §12 as each lands (Alpha/disabled unless stated):

| Gate | Stage/Task | Default | Graduation intent |
|---|---|---|---|
| `SharedStateBackend` | A4 | disabled | Beta once used by CI itself |
| `KubernetesSecretBackend` | B4 | disabled | Beta with KubernetesRuntime | *(happened: graduated Beta/enabled at B9 alongside `KubernetesRuntime` — doc 04 §12 is the master table)* |
| `KubernetesRuntime` (existing) | B close | **Beta/enabled at B**, GA target at C close | long hardening period honored |
| `HighAvailability` | C1 | disabled | Beta after C2/C3 soak |
| `IngressProvider` | C7 | disabled | Beta with TLSTermination |
| `TLSTermination` | C8 | disabled | Beta with IngressProvider |
| `MonitoringStackProvider` | C9 | disabled | Beta after lakehouse soak |
| `BackupRestore` | C6 | disabled | Beta after restore drills in CI |
| `SchemaRegistrySupport` | D1 | disabled | Beta with D2 landing |
| `JDBCSinkProvider` | D3 | disabled | Beta after soak |
| `IngestProvider` | D4 | disabled | Beta after soak |
| `TunnelProvider` | D5 | disabled | Beta after real-VPC validation |
| `TrinoProvider` | D10 | disabled | Beta after soak |
| Phase 6.5 gates (`MySQLProvider`, `NessieProvider`, `OpenLineageProvider`, `ProxyProvider`) | — | enabled (Alpha) | promote to Beta at Stage A close (their hardening period ends with the ops-hardening stage) |

## 9. Mapping to doc 07's open items

Every unresolved `[ ]` in doc 07 now has exactly one home:

| Doc 07 item | Task |
|---|---|
| §1.1 registry auth | A1 |
| §1.3 GC/orphan tooling (all bullets) | A2 |
| §1.4 state inspect/doctor/repair, migrations, remote-state decision | A3, A4 |
| §0.4 data-bearing protection | A5 |
| §0.3 ExternalConfigurer audit + docs | A6 |
| §0.5 / §3.2 generic output harness | A7 |
| §2.1 / §3.2 out-of-band config-drift tests | A8 |
| §3.2 MariaDB coverage | A9 |
| §2.5 image digests | A10 |
| Cross-Runtime: external reachability | B1 |
| Cross-Runtime: observed bind addresses (K8s) | B2 |
| Cross-Runtime: volume persistence coverage | B3 |
| Cross-Runtime: RBAC/ServiceAccount | B5 |
| Cross-Runtime: multi-replica/PDB/anti-affinity | C1 (deliberate scope change: now planned) |
| Cross-Runtime: Phase 7 doc/schema sync | B9 |
| §2.3 schema registry | D1 |
| §2.3 warehouseRef | D8 |
| §2.4 tunnel providers | D5 |
| §2.4 TLS termination / routing / auth proxy | C7, C8 |
| §2.4 in-network reachability probes | C10 |
| §3.1 provider schema fragments (all bullets) | E5 |
| §3.3 docs sweep residuals | E7 (+ doc 04 update alongside this plan) |
| §3.4 provider author contract (all bullets) | E6 |
| §0.6 Mermaid syntax validator (optional) | folded into A7's harness scope if cheap; otherwise remains optional |
| §0.1 display-label concept | remains revisit-on-evidence (no real manifest has hit the DNS-label limit) |
| §1.1 host-path mounts | **stays deliberately unsupported** (not portable across runtimes) |
| Cross-Runtime: Terraform/external adapter | **stays Phase 8** (out of this plan's scope; E6's plugin decision note records the seam) |

Checkpoint.md "Known open items" mapping: 1→D3/D4, 2→D2, 3→A10, 3b→A9,
4→E7 (retire `ContainerProvider` gate), 5→D5, 6→§8 graduations,
8→Stage B, 9/10→closed.

## 10. Execution order (updated 2026-07-21; A, B, F closed)

Historical sequencing for the closed stages is preserved in git history
and doc 10; what follows is the **current** order. Tasks parallelize along
dependency edges; one agent session per task (§2.1's protocol).

**Step 0 — land the in-flight branches (maintainer decision, blocking
step 1):**

1. Merge **C1** (`worktree-agent-ac3b0d7e379217021`, `ff2127d`+`5fd4ac3`)
   after its pending live-Kubernetes conformance leg runs green. Resolve
   the doc 04 §12 table conflict by re-adding its `HighAvailability` row
   in the current table format, and move its `docs/design/004-*.md` to
   `docs/adr/004-*.md` (the directory migrated 2026-07-21).
2. Merge **D1** (`worktree-agent-ac9cf81b40022f246`, `0a9bc77`+`2a05bd4`)
   after one green `TestAvroCDCEndToEnd` integration run; same doc 04 §12
   conflict note.
3. Rework **C6** on its branch per its status note's five required fixes
   (its ADR lands as `docs/adr/007-backup-restore.md`); re-run both
   round-trips live before merge. C6 is independent of C1/D1 except for
   trivial `main.go`/`reconciler.go` rebases — rebase it last.

**Step 1 — immediately, in parallel (all independent):**
- G1 (providerkit — before any new provider is written),
  G2 (engine kind table), G4 (reason catalog), G6 (integration harness).
- C9 (monitoring) and C10 (in-network probes) — independent of the C1
  chain.
- E2 (config-minimization audit), E3 (tool renderers), E4 **after G4**.

**Step 2 — after C1 merges:**
- C2 (Redpanda multi-broker) → then C3 (Connect workers) and C4 (MinIO
  nodes) in parallel. C2 also decides the state-level replica
  representation (Stage G's disposition note).
- G3 (kubernetes.go split) and G5 (conformance split) — mechanical, right
  after the merge, before C2 lands more code in those files.
- D10 (Trino) once C2 has proven the replica primitive against a real
  clustered technology.

**Step 3 — after D1 merges:**
- D2 (Parquet end-to-end) → then D3 (jdbcsink) and D4 (s3source) in
  parallel; D6 (DLQ) and D7 (Dataset lifecycle) independent, any time;
  D8 (warehouseRef) independent, any time; D5 (WireGuard tunnel)
  independent, any time.

**Step 4 — the reachable-endpoint chain:** C7 (ingress) → C8 (TLS); both
build on ADR 015's plane and benefit from G3 having landed.

**Step 5 — contribution readiness (strict order):**
E5 (schema fragments) → E6 (author guide + reconciler conformance suite;
requires G1 and F5 — F5 is done) → E7 (truth sweep) → E8 (release
engineering). E6's plugin decision note is the Phase 8 gateway.

**Standing rules:** C5/D9 are decided (ADR 005/006) — do not reopen them
without new evidence. Stage C closes when its five exit criteria hold;
`KubernetesRuntime` GA happens at C close, not before. Stage G tasks
never block a stage's exit criteria but G1 must precede E6 and G4 must
precede E4.

v-next milestones: **v1.1** = Stage A closed *(reached 2026-07-18 — tag
pending, binary still reports v1.0.0)*. **v1.2** = Stage B closed
*(reached 2026-07-19 — tag pending)*. **v1.3** = Stages C+D closed (HA +
pipeline completeness). **v2.0** = Stages E+F closed — the "production
data-pipeline platform, contribution-ready" declaration point; Stage F is
closed, so Phase 8 unblocks at E6's plugin decision note. Maintainer
action: cut the v1.1/v1.2 tags (or collapse them into one v1.2 release)
so `main.version` stops under-reporting. *(Done 2026-07-21: collapsed into
one `v1.2.0` tag — Stages A+B+F closed plus C1 and D1 merged;
`main.version` bumped.)*
