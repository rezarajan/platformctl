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

0. **Checkpoint continuously (M/L tasks, mandatory).** Before any other
   work, create `TASK_PROGRESS.md` in your working-tree root: the step
   plan for this task, one line per step with status
   (done / in-progress / next), file anchors, and verification results as
   they happen. Update it as you go and **commit after every completed
   increment** (WIP commits are fine — tidy at the end; if commit signing
   is unavailable, stage and record the state in `TASK_PROGRESS.md`
   instead). A session that dies mid-task must be resumable by a
   different session from `TASK_PROGRESS.md` + `git log` alone, without
   re-reading the full context or repeating completed work. On resume,
   read those two things first and continue from the first unfinished
   step.

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
   - Preferred over a blind full run: `scripts/test-impact.sh --base main`
     (doc 06 §10) — runs only the suites your diff affects, dedups
     content-states already proven green (shared ledger), and serializes
     on the shared daemon. Record the suite ids + timings in
     TASK_PROGRESS.md so the merge gate cites instead of re-runs.
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
- **Done (2026-07-22, merged):** ADR 017 records the mechanics and the
  state decision (aggregate state + published per-ordinal endpoint facts,
  no version bump); both runtimes' HA e2e green (Docker 16.5s, K8s 69s at
  brokers:3/replication:3 incl. kill/heal); ADR 004's gate-at-validate
  accept line closed (checkHighAvailabilityGate; §a.8 deviation). C3/C4
  and D10 are unblocked.
- **CI-caught follow-up (2026-07-22, F6 ratchet):** the healing apply
  returned Ready on broker-membership rejoin while partition leadership/
  metadata were still settling — a same-instant drift snapshot on a slow
  CI runner hit a transient ListTopics failure and reported ProbeFailed
  (never reproducible on fast local machines). Fix: `waitTopicSettled` —
  reconcileTopic now settles to a clean probe before Ready (F3's
  ready-means-serving applied at the topic level; zero added latency on
  a healthy cluster, honest timeout error otherwise) — and the
  DriftDetected condition now carries the probe error Message (an empty
  Message hid this root cause). Contract-level reproduction: the class
  is timing-dependent cluster convergence, not expressible as a
  deterministic conformance subtest — recorded here per the ratchet's
  secondary branch, with the HA e2e (which caught it in CI) as the
  standing live reproduction.
- **Done (2026-07-21, C2):** design note
  `docs/adr/017-redpanda-multibroker-and-replica-state.md` (both assigned
  questions: multi-broker mechanics on ADR 004's primitive, and the
  state-level replica representation — state stays one aggregate entry;
  per-ordinal facts ride as published `providerState` endpoint facts only;
  no state version bump). Declaring `configuration.brokers` opts into the
  ordinal-set shape at any N ≥ 1 (ADR 004 amended additively:
  `StableIdentity` selects the set shape at ReplicaCount()==1 too), which
  is what makes 1→3 a true same-shape in-place scale; 3→1 is refused at
  reconcile with a destroy-and-recreate remedy (destructive-flag plumbing
  judged disproportionate — recorded in ADR 017 §a.5 per this task's own
  fallback clause). C1's deferred gate-at-validate accept line is closed:
  `checkHighAvailabilityGate` in `loadAndValidate` (the
  checkSchemaRegistryGate mechanism; ADR 017 §a.8 records why it is not
  literally inside `SpecValidator`, which has no gate access by design).
  Verified live on **Docker** (`TestRedpandaHAEndToEnd`, 16.5s: 3-broker
  cluster Ready, RF-3 topic via admin API, produce/consume
  before/during/after an out-of-band broker kill, `BrokerMissing(<ordinal>)`
  drift, re-apply heal, idempotent re-apply, clean destroy) and on
  **Kubernetes** at the same brokers: 3 / replication: 3 sizing
  (`TestRedpandaHAKubernetesEndToEnd`, 69s, minimal-RBAC kubeconfig per
  deploy/kubernetes/rbac — never ambient admin; the StatefulSet controller
  performs the heal there, a documented per-runtime difference). Three
  live-caught defects fixed with conformance pins per the F6 ratchet:
  the StatefulSet builder dropped `Entrypoint`→`Command`
  (`ReplicaSet_EntrypointReplaces_OnSet`); ordinal short-name DNS did not
  actually resolve cross-pod on Kubernetes until per-ordinal Services
  (publishNotReadyAddresses) made ADR 004's claim real
  (`ReplicaSet_OrdinalInNetworkDNS`); and Redpanda refuses *even*
  replication factors ("must be odd", Raft quorum), now refused at
  validate. Stage-exit criterion 1 ("factor 3 keeps accepting
  produce/consume while one broker is killed, both runtimes") holds on a
  single-node minikube; "brokers spread across nodes when possible" (soft
  anti-affinity, C1) remains unexercised on multi-node clusters.

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
- **Done (2026-07-22, merged, bundled with D6):** workers: N via ADR
  004's Deployment-shaped branch (first real consumer); kafkaconnect REST
  failover across live workers (providerkit.ReachableURLs);
  ConnectWorkerMissing drift; the HA gate check generalized to a field
  list (brokers, workers). Two live-caught defects pinned: per-ordinal
  host-port collision (pin+workers refused at validate) and Connect's
  transient REST-forwarding failures (shared retryTransient at the
  client). D6 landed in the same commit: options.deadLetter with
  validate-time EventStream resolution (existence check — graph-edge
  introspection of options deliberately not taken, ordering story
  documented), DLQ keys in the drift diff, live poison-record proof.
- **Done (2026-07-21, C3; bundled with D6 — same files):**
  `spec.configuration.workers: N` on debezium/s3sink opts into ADR 004's
  `Replicas: N, StableIdentity: false` shape (the first real consumer of
  that branch; declaration is the opt-in, mirroring ADR 017 §a.1 —
  undeclared stays the single-container shape byte-for-byte).
  `internal/adapters/kafkaconnect` REST calls take `baseURLs []string` and
  try each live worker (`tryEach`); per-call addresses come from
  `providerkit.ReachableURLs` (per-ordinal EnsureReachable, skip-dead,
  error only at zero reachable — the clusterDial pattern). Probe for
  workers > 1 is per-ordinal presence via
  `providerkit.ProbeConnectWorkerSet` (drift reason
  `ConnectWorkerMissing(<ordinals>)`; Connect's REST API has no
  group-membership listing to check beyond presence — the rebalance
  protocol self-heals membership). `checkHighAvailabilityGate` now scans a
  field list (`brokers`, `workers`) and names whichever field triggered
  it. Two live-caught defects fixed with pins in the same commits:
  (i) workers > 1 with a pinned `connectPort` would give every ordinal the
  identical HostPort (ADR 004's known limitation) — ports are now
  auto-assigned per ordinal for the set shape and the pin combination is
  refused at validate, mirroring ADR 017 §a.4; (ii) `DeleteConnector` hit
  Connect's internal REST-forwarding window mid-rejoin (HTTP 500 "IO Error
  trying to forward REST request: Connection refused" / connection reset)
  — the shared `retryTransient` primitive now covers Delete as well as
  Put, pinned at the client level by
  `TestIsTransientConnectErrorRecognizesForwardingFailure`. Accept
  verified live on Docker (`TestConnectWorkersHAAndDeadLetterQueue`,
  58.5s: 2 workers Ready, kill one out-of-band, `drift` — not `apply` —
  reports the CDC Binding Ready=True via the survivor and
  `ConnectWorkerMissing` on the Provider, heal, clean destroy).

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
- **Status (2026-07-21): implemented and verified live on Docker (bundled
  with D7 — both land in the s3 provider).** Design finding recorded first:
  no `ExternalConfigurer` was needed for the Dataset half. Studying doc 03
  §3.3's table against ADR 005's "external Source + CDC already works with
  no ExternalConfigurer" precedent showed the same shape applies here — a
  `Provider(type: s3, external: true)` always takes the engine's generic
  no-provider path (`isExternalNoProvider`/`reconcileExternal`; its own
  `Reconcile`/`Probe`/`Destroy` are never called), while a Dataset naming it
  via an ordinary (non-`external`) `providerRef` reconciles through the
  normal path unchanged — the only real gap was teaching
  `reconcileDataset`/`Probe`/`Destroy` to resolve their realizing
  Provider's S3 endpoint from its own `spec.connectionRef` (a Connection or
  bare SecretReference, resolved from `req.Resources`) when that Provider
  is external, mirroring exactly how `debezium.buildDesiredConnector`
  already resolves an external Source's `connectionRef`. Half (2):
  `spec.configuration.nodes` opts an `s3` Provider into C2's StableIdentity
  ordinal-set shape (mirrors redpanda's `brokers` field-for-field);
  `nodes: 1` uses a single ordinal, `nodes: 4+` is a genuine distributed
  erasure-coded cluster (MinIO's own node list is static and identical on
  every ordinal, so — unlike redpanda's join protocol — no Entrypoint
  override/shell script was needed), `nodes: 2` or `3` refused at validate
  (no supported MinIO topology exists there), `nodes > 1` gated by
  `HighAvailability` (`checkHighAvailabilityGate` generalized from a single
  `brokers` check to a field list). `s3sink` Bindings needed zero changes
  for either half: its existing `options.endpoint` + Provider-level
  `configuration.credentialsSecretRef` already address any S3-compatible
  endpoint. Verified live against real Docker (`cmd/platformctl/
  s3_c4_d7_integration_test.go`): `TestS3ExternalDatasetEndToEnd` (65.7s) —
  apply against an out-of-band MinIO container simulating a cloud bucket
  creates zero containers for the store itself, a real sink Binding lands
  Kafka Connect traffic in it, destroy retains the external bucket.
  `TestS3DistributedMinIONodeKill` (51.5s) — a 4-node cluster reaches
  Ready, sink traffic lands both before and during an out-of-band
  single-node kill (the literal accept criterion), drift names the missing
  node, re-apply heals and is subsequently idempotent, destroy removes all
  four ordinals/volumes/network cleanly. Live-caught building the test: a
  raw (non-JSON) Kafka record value silently fails Kafka Connect's default
  JsonConverter — a test-fixture finding, not a product bug. docs/planning/
  03 updated (§4 Provider `nodes`/external examples, §8 Dataset external-
  Provider example).

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
- **Reworked and merged 2026-07-21** (`3c0f6dc` + merge): all five findings
  closed — `ContainerSpec.Entrypoint` (with the EntrypointReplaces
  conformance subtest), endpoint-fact Location resolution via
  `EnsureReachable`, the engine consuming published facts (port/scheme/
  secret-key names no longer hardcoded), fail-fast pipeline, and ADR 007
  (protect refusal, prefix fix, Docker-only fail-fast gate). Two further
  live-found fixes: `pg_dump --no-publications` (the dump collided with
  the Source's own publication) and per-store state isolation in the
  round-trip test. Merge-time adaptations: the backup helpers moved onto
  providerkit (G1); `resolveRequest`'s post-D1 state parameter passed nil
  (registry URLs are Binding-only); the conformance subtest re-inserted
  beside its sibling. Verified at merge: postgres/mysql/s3 round-trips +
  full Docker conformance live; unit + archtests green.

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
- **Done (2026-07-22, merged):** ADR 018 (Caddy via read-write admin API —
  route changes never restart the shared proxy; native Ingress on K8s;
  TLS deferred to C8 with the `Connection.spec.tls` seam noted); all
  accept items live on both runtimes; load-bearing fix: registry
  decorators must explicitly delegate optional runtime capabilities
  (interface embedding hides them — pinned). The agent-side RBAC
  deviation (K8s leg under ambient admin) was closed at the merge gate:
  the new `ingresses` verbs applied to the cluster and
  TestIngressKubernetesEndToEnd re-run green under a minted minimal-RBAC
  kubeconfig (35.9s).
- **Done (2026-07-21):** design note `docs/adr/018-ingress-routing.md`
  decides Caddy over Traefik on Docker (Caddy's admin API is read-write —
  `PATCH`/`POST`/`DELETE /id/<id>` — so per-Connection routes reconcile
  without ever touching `ContainerSpec.Files`, which participates in the
  Docker spec hash and would restart the shared proxy, dropping every other
  Connection's traffic; Traefik's API is read-only by design) and native
  `networking.k8s.io/v1 Ingress` over Gateway API `HTTPRoute` on Kubernetes
  (zero-install on every cluster; Gateway API CRDs are not guaranteed
  present, and this project's own minimal-RBAC posture already depends on
  well-known API groups). `internal/adapters/providers/ingress` implements
  `ConnectionCapableProvider{"http"}`: Docker/fake realize one shared Caddy
  container bootstrapped once via `EnsureContainer`, with every Connection's
  route reconciled through Caddy's admin API afterward; Kubernetes realizes
  one `Ingress` object per Connection via a new optional
  `runtime.IngressCapableRuntime` port capability
  (`internal/adapters/runtime/kubernetes/ingress.go`), which the provider
  type-asserts against — never an adapter-package import — after branching
  on `provider.Provider.RuntimeType` (a domain-layer fact, not adapter
  introspection). Gate `IngressProvider` (Alpha, disabled). RBAC:
  `deploy/kubernetes/rbac/role.yaml` + `preflight.go` +
  `deploy/kubernetes/rbac/README.md` gained `ingresses.networking.k8s.io`
  (get/create/update/delete/list), same-commit per doc 06 §8 rule 4.
  Verified live: **Docker** (`TestIngressRoutingEndToEnd`,
  `cmd/platformctl/ingress_integration_test.go`, ~11s) — nessie reachable at
  `http://nessie.localhost:<port>` and minio independently at
  `http://minio.localhost:<port>` through the one shared proxy container, an
  unrecognized Host routes to neither, `inventory` shows both routed URLs,
  an out-of-band admin-API edit to nessie's route is detected as
  `RouteConfigDrift` by `drift` and healed by the next `apply`, re-apply is
  idempotent (proxy container ID unchanged — confirming no route change
  ever restarts it), destroy is clean. **Kubernetes**
  (`TestIngressKubernetesEndToEnd`,
  `cmd/platformctl/ingress_kubernetes_integration_test.go`, ~32s, against a
  live minikube cluster with the `ingress-nginx` addon enabled) — the
  `Ingress` object is created with the correct Host/backend, idempotent
  re-apply, an out-of-band mangled `Ingress` heals on the next `apply`,
  clean destroy. One live-caught defect fixed with a conformance
  reproduction in the same commit (the F6 ratchet): `application/registry`'s
  `haGuardRuntime` wrapper embedded the `runtime.ContainerRuntime`
  *interface*, which only promotes that interface's own declared methods —
  so a provider's `req.Runtime.(runtime.IngressCapableRuntime)` assertion
  always failed for every runtime obtained through the registry, including a
  real Kubernetes adapter that genuinely implements it; the Kubernetes
  adapter's own fake-clientset unit tests never exercise this wrapper and so
  never caught it. Fixed by giving `haGuardRuntime` three explicit
  delegating methods; pinned by
  `TestRuntime_PromotesIngressCapableRuntime`
  (`internal/application/registry/registry_test.go`).
  **Deviation from doc 08 §2.1 step 5:** the live-cluster verification used
  the ambient (pre-existing, cluster-admin-bound) minikube kubeconfig, not a
  freshly minted minimal-RBAC one — minting one requires `kubectl apply` of
  `deploy/kubernetes/rbac/{serviceaccount,role,binding}.yaml`, which this
  session's environment blocked as a protected cluster-mutating action
  requiring interactive review. The RBAC manifests themselves were updated
  (the new `ingresses.networking.k8s.io` verbs), matching every other verb
  this adapter uses, but their *sufficiency* was not re-proven live the way
  B5's CI job proves it for the rest of the adapter — recorded here for the
  maintainer to verify with `kubectl apply -f deploy/kubernetes/rbac/` +
  `kubectl create token` per `deploy/kubernetes/rbac/README.md`.

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
- **Merged 2026-07-22.** Shipped: `Connection.spec.tls: {secretRef |
  selfSigned | secretName}` (exactly one, requires `scheme: https`),
  `SupportedConnectionSchemes()` → `["http", "https"]`. Docker: a second
  Caddy HTTP-app server (`srv1`, container-internal :443) with
  `tls_connection_policies` — found live, not by reading docs, that
  `automatic_https.disable: true` alone does **not** enable TLS on a
  listener; an explicit (even empty) policy is required — hosts every
  `https` route; certificates load exclusively through Caddy's admin API
  (`/config/apps/tls/certificates/load_pem`, `@id`-tagged like routes),
  never `ContainerSpec.Files` (a spec-hash-triggered container replace on
  every cert rotation would reproduce Decision 3's restart-blast-radius
  problem for the whole shared proxy — the label itself is a one-way hash
  and doesn't leak plaintext, a separate finding recorded in docs/adr/018's
  addendum). The Provider-scoped local CA is the one exception: it
  persists via `ContainerSpec.Files` using the exact read-before-
  regenerate pattern `postgres`'s superuser-password rotation established,
  because it changes as rarely as the bootstrap config itself. Kubernetes:
  `Ingress.spec.tls` referencing a `kubernetes.io/tls` Secret, via a new
  `IngressCapableRuntime.EnsureTLSSecret`/`GetTLSSecret`/`RemoveTLSSecret`
  capability — no new RBAC verb needed (`secrets` was already granted
  cluster-wide since A1, confirmed by inspection before writing any
  Secret-handling code). `secretName` (cert-manager) is referenced only —
  never created/updated/deleted by platformctl; a not-yet-issued Secret
  reports `Ready: false`/`CertMissing`, not an error, converging on the
  next `apply` once it exists (the same eventually-consistent posture
  `SchemaRegistryURL`/`CatalogFacts` already have for a not-yet-published
  upstream fact). Gate `TLSTermination` registered Alpha/disabled,
  independent of `IngressProvider`'s own gate; enforced by a new
  `registry.Registry.RequireGate` choke point in `engine.resolveRequest`
  (no existing choke point fit a manifest-declared per-*resource* field —
  mirrors `HighAvailability`'s own admitted-imperfect backstop-at-point-
  of-use pattern rather than inventing a second gating mechanism).
  `platformctl inventory` gained `certificateAuthorities` in `-o json/yaml`
  (the CA's public certificate only — never the private key) plus a
  human-readable pointer to it; https URL rendering needed no extra code
  (already generic via `Endpoint.Scheme`/`Insecure`). Verified live:
  `TestIngressTLSEndToEnd` (`cmd/platformctl/ingress_tls_integration_test.go`)
  against real Docker — provided-secretRef cert verifies against an
  independently test-generated CA (`resp.TLS.VerifiedChains`); self-signed
  path verified against the CA the provider itself published via
  `inventory -o json` (not a CA the test invented); a helper upstream
  created out-of-band with `Audience: internal` (zero host port) is
  reachable only through the entrypoint (`HostAddr` confirmed empty before
  and after apply); idempotent re-apply (shared proxy container ID
  unchanged); drift on a hand-mangled TLS route heals; clean destroy — all
  green (8.6–8.8s), alongside the full pre-existing `TestIngress*` Docker
  suite with no regression. `TestIngressTLSKubernetesEndToEnd`
  (`cmd/platformctl/ingress_tls_kubernetes_integration_test.go`) against a
  real cluster **under a minted minimal-RBAC kubeconfig** (doc 06 §8 rule
  4) — secretRef materializes a Secret holding the provided cert/key
  verbatim; the self-signed leaf cert chain-verifies (`crypto/x509`)
  against the Provider's own published CA Secret (the same object-level
  verification bar C7's `TestIngressKubernetesEndToEnd` already
  established for this runtime, not a live ingress-nginx round-trip — a
  deliberate scope match, not a shortcut); a cert-manager-style Connection
  converges from `Ready: false` to healthy once its Secret is simulated
  out-of-band; idempotent re-apply; drift heal; clean destroy — all green
  (~59–61s), alongside the full `TestIngress*` suite under the same minted
  kubeconfig with no regression, no leftover namespace. **Deferred**
  (explicit, not silently missing): a live end-to-end HTTPS round-trip
  through a real cluster ingress controller (this task's K8s leg verifies
  object/Secret correctness, matching C7's own established scope for this
  runtime); proactive certificate-expiry warnings between applies (Probe's
  structural check already fails Ready 24h before expiry, forcing reissue
  on the next `apply`, but there is no signal between applies — acceptable
  for Alpha/dev use, a production TLS story routes through
  `secretRef`/`secretName` rotation lifecycles it doesn't own).

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
- **Merged 2026-07-21** with one recorded convergence caveat: scrape
  targets come from *published* endpoint facts in state, and nothing
  orders the prometheus Provider after the providers it scrapes (no
  manifest ref, no graph edge) — on a fresh single apply it may reconcile
  before some targets have published, reaching Ready with the
  then-current subset and converging on the next apply (its probe
  regenerates the config and reports the drift). Acceptable for
  Alpha/disabled; the follow-up seam if two-apply convergence proves
  annoying is an explicit `configuration.scrapeRefs` (graph-ordered, the
  D8/D10 ref discipline) — recorded here so it isn't re-derived.
- **Status (2026-07-21): core slice implemented** on branch
  `worktree-agent-a9601d0ee08ea8bae`. Shipped: a `prometheus` provider
  (`internal/adapters/providers/prometheus`, nessie-shaped: one
  single-container instance, no dependent kind) reconciling a managed
  Prometheus container whose scrape config is generated purely from
  currently-published metrics endpoint facts, via a new engine-resolved
  `reconciler.Request.MetricsTargets` field (mirrors the existing
  `SchemaRegistryURL` D1 pattern — the engine scans state for every
  Provider's published `"metrics"`-named endpoint and hands the provider
  already-resolved job/target/path triples; the provider itself never
  constructs an address, ADR 015). Metrics endpoint facts added to
  `redpanda` (its admin API, previously unpublished at all — now also
  exposes `/public_metrics`) and `s3`/`minio` (`/minio/v2/metrics/cluster`,
  reusing the already-published API port; `MINIO_PROMETHEUS_AUTH_TYPE:
  public` set so the endpoint scrapes with no bearer token). Config is
  written via `ContainerSpec.Files` and regenerated + diffed on Probe
  against Prometheus's own `/api/v1/status/config`, reporting drifted job
  *names* only (the debezium `connectorConfigDrift` bar). Ready requires
  `/-/ready` 200 **and** `/api/v1/targets`'s `activeTargets` count to match
  the configured target count — found live, not by reasoning: Prometheus's
  target-discovery sync lags `/-/ready` by a few seconds at startup even for
  a purely static config, so `Reconcile`'s own convergence wait
  (`waitReady`) blocks on both, not just `/-/ready`, so `apply` returning
  success actually means Ready (ADR 015 F3). Per-target up-ness is
  Prometheus's own concern, never part of this Ready gate.
  `inventory --for prometheus` (`cmd/platformctl/toolconfig.go`) renders
  the same scrape config (literally the same `prometheus.RenderScrapeConfig`
  call) from host-published metrics endpoints, for a bring-your-own
  Prometheus; a parse-based unit test (`gopkg.in/yaml.v3`, already a repo
  dependency) covers it — promtool was out of this slice's scope. Gate
  `MonitoringStackProvider` registered Alpha/disabled. Verified live:
  `TestPrometheusMonitoringStackEndToEnd`
  (`cmd/platformctl/prometheus_integration_test.go`) against real Docker —
  apply (redpanda + EventStream + minio, then prometheus added on top) to
  `up == 1` for both targets within a 30s deadline, idempotent re-apply
  (unchanged container ID), clean destroy — green in ~15s per run, plus the
  existing `TestRedpandaEndToEnd`/`TestSinkEndToEnd` suites re-run live to
  confirm the shared redpanda/s3 provider changes (the new admin-API port,
  the new metrics endpoint facts) introduced no regression. **Deferred**
  (explicit, not silently missing): postgres/mysql sidecar exporter
  containers (no native metrics endpoint to publish yet — `configuration`
  keying for a sidecar shape is a larger change than this slice), a
  standalone `grafana` provider, and live Kubernetes-runtime verification
  (the provider is runtime-agnostic by construction — no Docker-specific
  API used beyond the shared `providerkit`/`ContainerRuntime` port — but
  untested against a real cluster).
- **Status (2026-07-22): C9 completion — exporter sidecars + `grafana`
  provider shipped**, closing the three deferrals recorded just above
  (postgres/mysql sidecar exporters and the standalone `grafana` provider;
  live Kubernetes-runtime verification remains open, unchanged from the
  original slice — neither new package uses anything beyond the shared
  `providerkit`/`ContainerRuntime` port). `postgres`/`mysql`/`mariadb` gain
  an opt-in `configuration.metrics: enabled | disabled` (default disabled,
  same `MonitoringStackProvider` gate, enforced at validate by the CLI's
  `checkMonitoringMetricsGate` — a `SpecValidator` has no gate access by
  design, docs/adr/017 §a.8): a second, independent
  `postgres_exporter`/`mysqld_exporter` container reconciled alongside the
  instance (docs/adr/004 — a sidecar, mirroring `openlineage`'s Marquez+db
  two-container shape, never a replica; the instance's own `ContainerSpec`
  is untouched code, not just untouched behavior — proven by
  `TestInstanceContainerSpecUnaffectedByMetrics` in both providers' test
  suites). The exporter's port is `Audience: internal` — never
  host-published — and authenticates as a dedicated least-privilege
  monitoring role/user (`pg_monitor`; `PROCESS, REPLICATION CLIENT,
  SELECT`) this provider creates and password-manages itself at reconcile,
  never the admin/root credential and never a user-declared
  `SecretReference`; the password never touches env either
  (`DATA_SOURCE_PASS_FILE` for postgres_exporter, a fully file-mounted
  `--config.my-cnf` for mysqld_exporter — both verified live against the
  real images, no env-based deviation needed for either exporter, unlike
  the task brief's anticipated fallback). Its `"metrics"` endpoint fact
  publishes exactly like redpanda's/s3's, so the `prometheus` provider
  scrapes it with **zero** changes to `internal/adapters/providers/prometheus`
  (asserted by inspection: that package's `git diff` for this task is
  empty; `resolveMetricsTargets` keys `JobName` off the owning `Provider`
  resource's own name, confirmed by reading
  `internal/application/engine/engine.go`). The new `grafana` provider
  (nessie-shaped: one container, no dependent kind) reuses the existing
  `MonitoringStackProvider` gate rather than a new one (recorded design
  choice: it gates the monitoring stack as a class), is provisioned
  entirely via `ContainerSpec.Files` (Grafana's own file-based provisioning
  mechanism) with a Prometheus datasource resolved from a `prometheus`
  Provider's own published `"prometheus"` endpoint fact — a new
  `reconciler.Request.PrometheusURL` field, resolved in `engine.go`'s
  `resolvePrometheusURL` the same explicit-ref-or-sole-candidate-inference
  rule `resolveCatalogFacts`'s `warehouseProviderRef` already established
  (`configuration.prometheusRef`, optional; ambiguous when unset with more
  than one candidate — never guessed, ADR 015) — and a minimal starter
  broker+database overview dashboard (three stat panels: `sum(up)`,
  `sum(pg_up)`, `sum(mysql_up)`). Admin credentials are a required
  `SecretReference` (`ValidateSpec`-checked), mounted via
  `GF_SECURITY_ADMIN_USER` (env) + `GF_SECURITY_ADMIN_PASSWORD__FILE`
  (file — verified live: Grafana's own double-underscore `__FILE`
  convention); anonymous access is explicitly set off rather than relied
  on as Grafana's own default. **Deviation, recorded not solved**: Grafana
  only applies its admin-credential env vars the first time it creates the
  admin user in its own on-disk database (a documented Grafana limitation)
  — unlike postgres/mysql's `CredentialRotation` state machine, a
  `SecretReference` value changed after the first apply is not rotated
  into a live Grafana container; this needed no new code because Grafana
  itself doesn't support it, not because it was out of scope. **Finding**:
  the task's Accept criteria asked for both the exporter's `Audience:
  internal` fact (no host-published address) and for `inventory --for
  prometheus` to include the exporter targets — in tension as literally
  written, since `gatherToolFacts` uniformly skipped any endpoint fact with
  no host-published address. Resolved (not routed to the maintainer, a
  small and clearly-scoped fix): the `"metrics"` case in
  `cmd/platformctl/toolconfig.go`'s `gatherToolFacts` now falls back to the
  endpoint's in-network address when no host address is published — still
  a legitimate bring-your-own-Prometheus target for a Prometheus container
  joined to the same runtime network, just not from an arbitrary external
  host; pinned by `TestGatherToolFactsFallsBackToInternalForMetrics`.
  Verified live: `TestMonitoringStackCompletionEndToEnd`
  (`cmd/platformctl/monitoring_completion_integration_test.go`) against
  real Docker — a three-tier apply (infra: redpanda+EventStream+minio+
  postgres(metrics)+mysql(metrics); + prometheus; + grafana, each against
  the same state file, mirroring the recorded convergence caveat) to
  `/api/v1/targets` showing all four jobs (`mon-redpanda`, `mon-minio`,
  `mon-postgres`, `mon-mysql` — job names are the realizing Provider's own
  resource name, not the exporter container's `-exporter` name)
  `up == 1` within a 30s deadline; Grafana's own
  `/api/datasources/uid/prometheus/health` reports `OK` and
  `/api/dashboards/uid/datascape-overview` 200s; `inventory --for
  prometheus` names both exporter jobs and their in-network target
  addresses; idempotent re-apply (all eight managed containers' IDs
  unchanged, including both exporter sidecars and grafana); clean destroy
  — green in ~40s per run. `TestPrometheusMonitoringStackEndToEnd`
  re-run live to confirm no regression (~14s, unchanged from its own
  status note). Unit tests added in both exporter-providing packages
  (`metrics_test.go`) and the new `grafana` package (`grafana_test.go`,
  `internal/application/engine/engine_test.go`'s
  `TestResolvePrometheusURL*`); `go test ./...`/`gofmt`/`go vet` all
  green.

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
- **Status (2026-07-21): implemented** on branch
  `worktree-agent-a43c800e89e71c432`. `ProbeReachable` added to
  `ContainerRuntime`; Docker execs into an existing managed container on the
  network when one exists, else runs a transient probe container (pinned
  `busybox:1.36`, added to `scripts/pinned-images.txt`); Kubernetes mirrors
  it with exec-into-existing-pod / an ephemeral probe pod in the namespace
  (RBAC: `pods` gained `create`/`delete`, kept in sync with
  `preflight.go`); the fake is the strict interpreter (reachable only for a
  declared port on a fake-managed container on that network). Engine wires
  it into `externalConnectionStatus` for a genuinely External Connection
  (its address is real enough to dial in-network; a managed Connection's
  resolved address is a host-audience tunnel and is not), reporting
  `ExternalEndpointUnreachableInNetwork` distinctly from the host-side
  reason. Verified live: the Docker and Kubernetes (minikube)
  conformance legs both green, including the new
  `ProbeReachable_InNetwork_reachable_and_undeclared_errors` subtest. The
  engine-condition case is covered by a unit test against the strict fake
  (`TestExternalConnectionInNetworkReachability`, both conditions); no live
  rig of "reachable from host, firewalled in-network" was attempted — noted
  as deferred to the merge gate.

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
- [x] Lake data can be replayed into an EventStream (ingest) and an
      EventStream can be served into a relational Source (jdbc sink), both
      to Ready through `validate`-checked Bindings.
- [x] A Binding across a WireGuard-tunneled Connection reaches a database
      only routable inside a private network, CDC RUNNING.
- [x] Sink Bindings support declared dead-letter topics; poison messages
      land there without stopping the connector.
      (Exit-criterion evidence, D3/D4, 2026-07-22: criterion 2 —
      `TestJDBCSinkEndToEnd` (63.3s) / `TestS3SourceIngestEndToEnd`
      (246.9s), both live against real Docker, see those tasks' own status
      notes above. Criterion 4 — `s3sink` live-verified at D6
      (`TestConnectWorkersHAAndDeadLetterQueue`); `jdbcsink` translates
      `options.deadLetter` identically, unit-tested
      (`TestDeadLetterConfigTranslation`/
      `TestDeadLetterConfigReplicationFromEventStream` in
      `internal/adapters/providers/jdbcsink`), closing D6's own "jdbcsink
      half is N/A — D3 open" note now that D3 has landed.)
      (Exit-criterion evidence, D5, 2026-07-22: criterion 3 —
      `TestWireGuardTunnelEndToEnd`, live against real Docker, two
      consecutive green runs (26–29s); see D5's own status note above for
      the full topology and the two bugs found live.)

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
- **Done (2026-07-21, merged):** parquet through s3sink via D1's registry
  (Dataset parquet implies avro stream serialization unless
  options.format overrides); ADR-009-shaped validate failure for a
  registry-less parquet chain; acceptance example flipped to parquet with
  json fixtures retained; rows read back in-test
  (github.com/parquet-go/parquet-go, integration-only). At merge, the
  `SchemaRegistrySupport` gate executed its recorded graduation
  (Beta/enabled — doc 04 §12). Follow-up (small): with the gate now
  enabled by default, the cdc-to-lake blueprint can flip to parquet on
  its zero-flag path — needs the E1 blueprint e2e re-run before flipping.

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
- **Done (2026-07-21/22, merged):** `jdbcsink` provider implementing
  `DatabaseSinkCapableProvider` (`SupportedSinkEngines`: `postgres`, `mysql`
  — exactly the engines with shipped providers) over Confluent's
  kafka-connect-jdbc v10.9.6 (`io.confluent.connect.jdbc.JdbcSinkConnector`;
  the zip bundles the PostgreSQL driver, mysql-connector-j 9.7.0 added
  separately — testdata/jdbcsink-image). Target Source address/credential
  resolution mirrors `debezium.buildDesiredConnector`'s SOURCE-side
  resolution applied to the Binding's TARGET side, exactly as this task
  specified. `options.mode` (insert/upsert, validated), `options.table`
  (name override), `options.pkFields` (upsert), `options.unwrap` (Debezium's
  own envelope-unwrap SMT — necessary plumbing beyond the task's own option
  list, documented in the provider's doc comment and docs/planning/03 §7.2:
  a CDC-sourced topic's records are Debezium's before/after/op envelope, not
  a flat row). DLQ (D6) reuse unit-tested identically to s3sink's own
  coverage. **Finding, recorded per this task's own protocol:**
  `ValidateBindingOptions` requires `options.format` to be `avro`/`protobuf`
  — stronger than every other provider's optional-format treatment,
  verified against kafka-connect-jdbc's own `FieldsMetadata.extract`
  (v10.9.6): a schemaless (json) record contributes zero value columns and
  `pk.mode: record_key` throws outright with no key schema. This is a
  genuine technical constraint of the chosen connector, not a design
  preference (docs/planning/03 §7.2 documents it in full).
  `TestJDBCSinkEndToEnd` green against real Docker (63.3s, after fixing two
  bugs found live — recorded here per protocol, not silently corrected):
  (a) `buildDesiredConnector` initially set a literal `topics: <EventStream
  name>`, but Debezium writes CDC records to a per-table topic
  (`<prefix>.<schema>.<table>`), never the bare name — fixed to
  `topics.regex`, mirroring `s3sink.desiredConnectorConfig`'s identical
  pattern; (b) even with the regex fixed, the sink connector's consumer
  only discovers newly-matching topics on its next metadata refresh
  (Kafka's 5-minute default) — `s3sink.reconcileWorker`'s own doc comment
  already names this exact gotcha and sets
  `CONNECT_CONSUMER_METADATA_MAX_AGE_MS=10000`; `jdbcsink.reconcileWorker`
  was missing the identical setting, added. With both fixes: insert mode
  lands CDC rows in a second managed postgres Source's table; a
  manifest-only mode switch to upsert updates the connector in place (no
  container recreated, `insert.mode`/`pk.mode` drift-diff-visible);
  a source `UPDATE` lands as an in-place row update and a fresh `INSERT` as
  a new row, both via `pk.mode: record_key` with no `pkFields` override
  (the CDC key IS the row's primary key); idempotent re-apply; clean
  destroy. `TestJDBCSinkValidateCapabilityErrorExact` confirms the exact
  ADR 009 error shape. Gate `JDBCSinkProvider` registered Alpha/disabled
  (doc 04 §12 row appended). Bundled with D4 (s3source) in the same
  commit/PR — see that task's own status note.

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
- **Done (2026-07-21, merged):** `s3source` provider implementing
  `IngestCapableProvider` (`SupportedIngestFormats`: `jsonl`, `avro`,
  `parquet` — deliberately not the literal `json` value, see docs/planning/03
  §7.2's update note) over Aiven's s3-source-connector-for-apache-kafka
  v3.4.2 (`io.aiven.kafka.connect.s3.source.S3SourceConnector`; the repo
  s3sink's own plugin comes from was archived 2024-09-11, development moved
  to `Aiven-Open/cloud-storage-connectors-for-apache-kafka`, this provider's
  first use of it). Same Kafka-Connect-worker/`ValidateSpec`/
  `ValidateBindingOptions`/config-drift bars as `s3sink`; ordering/offset
  semantics documented in docs/planning/03 §7.2 (lexicographical
  `ListObjectsV2` order, `startAfter`-cursor-in-offsets-topic resumption,
  never-reprocess). `TestS3SourceIngestEndToEnd` green against real Docker
  (246.9s): records produced directly to a topic (bypassing CDC), landed by
  the existing `s3sink` provider in MinIO, replayed by `s3source` into a
  fresh topic with content asserted; idempotent re-apply ("no changes");
  clean destroy, no orphans. `TestS3SourceValidateCapabilityErrorExact`
  confirms the exact ADR 009 error shape for a non-ingest-capable provider.
  Gate `IngestProvider` registered Alpha/disabled in `cmd/platformctl/
  main.go`, matching the `IngressProvider`/`TrinoProvider` posture (doc 04
  §12 row appended). Bundled with D3 (jdbcsink) — see that task's own
  status note for the shared commit/PR.

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
- **Done (2026-07-22, D5):** `wireguard` provider (docs/adr/023, decisions
  recorded there) — a `ConnectionCapableProvider` (scheme `tcp`) realizing
  a tunnel-mediated Connection directly (see the ADR's "Scope" section for
  why `Connection.spec.via` chaining through an *existing* `proxy`/
  `ingress` Connection is schema-complete but not wired this task — the
  file fence marks `proxy` read-only reference). One tunnel container per
  Connection (`linuxserver/wireguard`, digest-pinned, `NET_ADMIN`), driving
  `wg-quick` via a custom entrypoint; the forwarder is an `iptables` DNAT
  rule baked into the same container's boot script (no socat — the pinned
  image ships neither socat nor nc, and installing one at apply time would
  break image pinning). Private key file-mounted only (never env/state/
  inspect); rotation is a container recreate via the existing spec-hash
  mechanism. New `ContainerSpec.Sysctls` runtime-port field (Docker-wired;
  Kubernetes intentionally unimplemented, documented — a cluster-operator
  sysctls-allowlist decision this task cannot make unilaterally): found
  live that `net.ipv4.ip_forward=1` must be set at container-create time.
  Gate `TunnelProvider` registered Alpha/disabled. **Docker only — no K8s
  leg**: `NET_ADMIN` + the `ip_forward` sysctl on Kubernetes is explicit
  future work (docs/adr/023 follow-ups), not silently missing.
  Verified live: `TestWireGuardTunnelEndToEnd`
  (`cmd/platformctl/wireguard_integration_test.go`, 26–29s per run, two
  consecutive green runs against real Docker) — a raw (unmanaged) isolated
  "VPC" Docker network hosts a plain `postgres:16` (no host publish) and a
  hand-configured WireGuard responder fixture (same pinned image, server
  role); negative reachability proven via `runtime.ProbeReachable` from the
  shared platform network *before* the tunnel exists (a host-side dial
  would not have been a real negative proof, since the test process runs
  on the same host as the daemon regardless of Docker network topology);
  after `apply`, the CDC connector reaches `RUNNING` through the tunnel
  (Debezium never joins the VPC or transit networks, only the tunnel
  container's own `name:port` on the shared network); idempotent re-apply
  (unchanged container ID); key rotation (new `SecretReference` value +
  the responder accepting the new peer key, the real VPC-operator-side
  half of a rotation) recreates the tunnel container and the connector
  stays `RUNNING`; `destroy` removes the tunnel container and the shared
  platform network, leaving only the still-attached raw responder fixture
  holding the peer network open (`RemoveNetwork`'s own documented
  refuse-while-attached contract — correct, not a leak). Two bugs found
  and fixed live, both recorded in `docs/adr/023`/commit history: (a) the
  pinned image symlinks `/etc/wireguard` -> `/config/wg_confs`, which
  doesn't exist pre-boot — `ContainerSpec.Files` writes there failed
  opaquely; moved the config to `/etc/datascape/wg0.conf` (`wg-quick`
  accepts any absolute path); (b) a design correction, found by reading
  `debezium.go`'s Connection-resolution code before finishing
  implementation: a first draft used one shared container per *Provider*
  (baking every Connection's DNAT rule into one boot script); every
  existing managed-Connection consumer dials `naming.RuntimeObjectName`
  of the *Connection* resource itself, so the shared-container shape was
  unresolvable by any real consumer — reworked to one tunnel container
  per Connection, matching `proxy`'s own shape exactly.

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
- **Done (2026-07-21, D6; bundled with C3 — same files):**
  `Binding.spec.options.deadLetter: {stream, tolerance: all|none}` parsed
  and shape-validated in `internal/domain/binding` (sink-mode only,
  tolerance defaults `all`); `compatibility.Check` verifies the named
  EventStream exists in-set (`checkDeadLetterQueue`, the standard
  does-not-resolve error family — the Accept's "rejects a deadLetter
  naming a missing EventStream" line, unit-pinned). Deviation from the
  "in-graph with ordering" wording, recorded in the check's doc comment
  and docs/planning/03 §7.4: no dependency edge is added —
  `graph.Build`'s refFields are a fixed generic top-level list, and
  special-casing one nested, mode-scoped, provider-consumed options field
  is the engine-block introspection this plan avoids. The existence check
  plus Kafka Connect's own DLQ-topic auto-creation (RF resolved from the
  named EventStream's `spec.replication` via `req.Resources`, else 1)
  covers the ordering window; the platform-managed EventStream's config
  wins once it reconciles. s3sink translates to `errors.tolerance` +
  `errors.deadletterqueue.topic.name`/`.topic.replication.factor`/
  `.context.headers.enable`; the DLQ keys ride in `desiredConnectorConfig`
  so Probe's existing drift diff covers them. jdbcsink half is N/A (no
  shipped provider — D3 open). Accept verified live on Docker
  (`TestConnectWorkersHAAndDeadLetterQueue`, 58.5s: poison record lands in
  the DLQ topic with the connector RUNNING throughout; valid records land
  in MinIO before and after the poison).

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
- **Status (2026-07-21): implemented and verified live on Docker (bundled
  with C4 — see that task's status note for the shared design/verification
  detail).** `Dataset.spec.lifecycle: {expireAfterDays, versioning}` reconciled
  via minio-go's lifecycle/versioning API — one managed rule keyed by a
  deterministic per-Dataset ID (`ensureLifecycle`, read-modify-write so a
  shared bucket's other Datasets' rules survive the S3 lifecycle PUT's
  full-replace semantics) and bucket versioning, each independently and
  only when declared; probe diffs both (`probeLifecycleDrift`) and reports
  `LifecycleRuleDrift`/`VersioningDrift`. External Datasets get this for
  free — no ExternalConfigurer/configure-only special case was needed (see
  C4's status note): the same `reconcileDataset`/`Probe` code path runs
  regardless of whether the realizing Provider is managed or external,
  because externality is resolved once, in the endpoint-dial step, not in
  the lifecycle logic. Verified live in both C4 integration tests: the rule
  and versioning state are visible via the real S3 API immediately after
  apply, and (in `TestS3ExternalDatasetEndToEnd`) an out-of-band
  `SetBucketLifecycle` call is caught as `LifecycleRuleDrift` by `drift`
  and healed by the next `apply`.

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
- **Done (2026-07-21, D8):** `Catalog.spec.warehouseRef` (top-level,
  kind-checked to Dataset via the plain `refFields` pass — the "no
  engine-block introspection" simplification the task text predicted,
  recorded in graph.go's doc comment; D10's `configRefFields` untouched).
  Reconciliation design + deviations recorded in docs/adr/006's
  "Implementation notes (D8, added post-implementation)": nessie's
  warehouse config is container-create-time env, but warehouseRef's facts
  only resolve after nessie's own Provider-kind reconcile — so the derived
  config is applied from the *Catalog*-kind reconcile
  (`ensureDerivedWarehouseConfig` re-`EnsureInstance`s with corrected env;
  idempotency rides entirely on `EnsureContainer`'s existing spec-hash —
  recreate once on fact change, zero API calls when unchanged, unit-pinned
  via the fake runtime's MutationCount). Explicit
  `configuration.defaultWarehouseLocation`/`warehouseS3*` (D10) kept and
  always win — additive coexistence. Engine seam:
  `reconciler.Request.WarehouseFacts` via `resolveWarehouseFacts`
  (published-facts-only, ADR 015); trino's `resolveCatalogFacts` now
  prefers the Catalog's warehouseRef chain, then `warehouseProviderRef`,
  then sole-S3-Provider inference (resolution order unit-pinned). Accept
  verified live on Docker: lakehouse example adopts warehouseRef with no
  explicit warehouse fields (`TestLakehouse` 67.89s; bonus
  `TestLakehouseExampleOnKubernetes` 135.82s green too); `docker inspect`
  confirms the derived
  `NESSIE_CATALOG_WAREHOUSES_WAREHOUSE_LOCATION=s3://warehouse/iceberg/`
  and `/iceberg/v1/config` answers 200 (500 pre-D8 in this example);
  `inventory --for spark|trino` reflect the warehouse facts (verified
  live; `gatherToolFacts` needed no change — already fact-driven); trino
  e2e green with warehouseRef + warehouseProviderRef coexisting
  (`TestTrinoComputeEngineEndToEnd` 117.93s, the scenario's explicit
  nessie warehouse fields replaced by a `trn-warehouse` Dataset);
  negative path: warehouseRef to a non-Dataset rejected at validate with
  graph's standard does-not-resolve shape
  (`TestCatalogWarehouseRefRejectsWrongKind`; ambiguity is impossible for
  a kind-checked ref beyond the generic duplicate/ambiguity rules already
  unit-pinned — recorded in that test's doc comment).

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
- **Done (2026-07-22, merged):** every accept item live on Docker
  (97.84s e2e; drift/heal on the catalog properties file via ReadFile;
  in-place worker scale with coordinator ID pinned). Recorded
  deviations (ADR 006 implementation notes): `configRefFields` — the
  graph's first configuration-level ref rule (kind-checked structurally,
  the D8-anticipated mechanism); `warehouseProviderRef` as D10's own
  disambiguator (no D8 dependency); query round-trip proven via schema
  ops + system catalog (Iceberg-table write path through Nessie STATIC
  auth is the follow-up); scale proof is 2→3 (Replicas<=1 is
  single-container by the port's own shape contract). Two additive
  nessie options (defaultWarehouseLocation, warehouseS3*) were
  prerequisites.
- **Status (2026-07-21): implemented, live-verified green.** `trino`
  provider (coordinator + worker replica set via C1/C2's primitive,
  `StableIdentity: false`), `configuration.catalogRef`/
  `warehouseProviderRef` (new nested-ref graph extraction,
  `internal/domain/graph/graph.go`'s `configRefFields`), catalog config
  auto-generation + drift-heal, `TrinoProvider` gate (Alpha/disabled).
  `TestTrinoComputeEngineEndToEnd` green on real Docker (97.84s) after 8
  live-caught bugs across 12 attempts — full writeup in
  `docs/adr/006-compute-engines.md`'s "Implementation notes" section,
  including two accept-item deviations recorded there and in the test's
  own doc comment: (a) the query-round-trip proves the wiring via
  `CREATE/SHOW SCHEMA` + a `system` catalog `SELECT`, not a literal
  CDC-parquet-table read (D1/D2 write plain Parquet, not Iceberg tables;
  Nessie's `STATIC`-auth Iceberg REST write path additionally does not
  support table creation, tracked as a follow-up); (b) the scale-up
  scenario is 2->3, not 1->3 (`Replicas <= 1` with `StableIdentity: false`
  is always the single-container shape, a shape transition to any
  `Replicas > 1` refused in place by the runtime port's own contract).
  Two prerequisite `nessie` provider fields added as a live-caught,
  necessary fix (`configuration.defaultWarehouseLocation`,
  `warehouseS3Endpoint`/`warehouseS3SecretRef` — optional, additive,
  `TestLakehouse` re-verified green).

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
- **Done (2026-07-21):** `.goreleaser.yaml` (linux/darwin x amd64/arm64,
  `CGO_ENABLED=0 -trimpath`, `-X main.version={{ .Tag }}` — the full
  `vX.Y.Z` tag, not goreleaser's v-stripped `.Version`, to match
  `main.go`'s existing default shape) + `.github/workflows/release.yml`
  (`v*.*.*` tag push → `verify` job re-running the unit job's own checks
  against the tagged commit → `goreleaser release --clean`). Validated
  locally (network install, goreleaser v2.17.0): `goreleaser check` clean;
  `goreleaser release --snapshot --clean` built all four targets,
  archived, checksummed (64-hex sha256, verified); the produced binary's
  `--version` printed the stamped tag exactly. Found and removed a
  `before.hooks: go mod tidy` step from an initial draft — it silently
  rewrote the working tree's `go.mod`/`go.sum` (pulling in other in-flight
  branches' indirect test deps as direct requires); a release pipeline
  must not mutate the tree it's building, so module tidiness stays the
  unit job's job. CI matrix: confirmed already-shipped legs need no
  duplication — example-validate covers both `examples/` dirs (unit job)
  and every blueprint (`TestInitBlueprintValidatesWithNoEdits`, plain
  `go test ./...`); the A7 machine-output harness
  (`output_contract_harness_test.go`) likewise runs under plain
  `go test ./...`; B8's `integration-k8s` and A10's `refresh-digests.yml`
  already exist. Added: a `Race detector` step (`go test -race ./...`,
  ~41s measured locally vs ~5.5s plain — scoped to the same
  non-integration-tagged set) to the `unit` job for the "unit+race" leg;
  a `schedule: cron: "0 7 * * 1"` trigger on `ci.yml` for the weekly
  full-matrix soak — no job restructuring needed, since the existing
  `integration` job's G7 branch already falls through to
  `test-impact --full` for any non-`pull_request` event and
  `integration-k8s` has no event gating at all. `docs/releasing.md`
  written and linked from `docs/README.md`'s Process section; the
  version-stamp proof re-run standalone with
  `go build -ldflags "-X main.version=v9.9.9-test"` → `--version` printed
  `v9.9.9-test` exactly. Not exercised: an actual tag push through GitHub
  Actions (this is local/config validation only, as the task's own
  fallback language anticipates for network-gated tools) — the release
  checklist's step-by-step is otherwise unexercised against a real tag
  until the next real milestone cut.

---

### E9: Interactive composition — add / wire / expose (ADR 024)

- **Size:** L (ADR 024 is the design — implement it literally).
  **Depends:** H1 (emitted patches must lint clean; co-evolves with the
  lint fixture bar). E1 blueprints are the template stock.
- **Do:** `internal/application/compose` (headless engine: composite
  definitions with attachment points, graph-aware candidate computation
  via the loadAndValidate front-end in tolerant mode, manifest-patch
  generation at blueprint quality with provenance comments, idempotent
  re-generation, collision-safe naming); `platformctl add <composite>`
  (`source`, `pipeline`, `sink`, `catalog`, `monitoring`),
  `platformctl wire <mode> --from --to`, `platformctl expose
  <Kind>/<name>` (Connection realization selected by scheme: tcp→proxy,
  http(s)→ingress); every prompt flag-covered, non-TTY+incomplete-flags
  a hard error, `--dry-run` exact; interactive layer via charm.land/huh
  v2 confined to cmd/platformctl+cliutil by a new archtest; machine
  output per the A7 harness for --dry-run/-o json.
- **Accept:** the owner scenario end-to-end as an integration test:
  init → add pipeline → add pipeline (second run's candidate lists
  include the first broker/Dataset; prefix override emits a second sink
  Binding to the same bucket) → expose Source/<first> → the resulting
  set validates green, lints clean, applies to Ready on Docker, and
  re-running each add with identical answers proposes zero changes;
  TUI-confinement archtest proven by a fixture violation.
- **Gate:** none (file-generation only, the init precedent — recorded in
  ADR 024).
- **Done (2026-07-22):** `internal/application/compose` (headless engine:
  LoadTolerant, candidates, patch/collision/idempotency, the five
  composites, wire, expose); `platformctl add source|pipeline|sink|
  catalog|monitoring`, `wire <mode>`, `expose <Kind>/<name>`
  (cmd/platformctl); huh/v2 prompt helpers confined to internal/cliutil,
  proven by internal/archtest's charm-confinement test (fixture violation,
  not committed). Owner-scenario integration test
  (`TestComposeOwnerScenario`) ran live on Docker: init → engine-level
  reuse-candidate assertion → add pipeline (reusing broker+sink worker,
  `--sink-prefix other/`) → expose Source/app-db --scheme tcp → validates
  green → applies to Ready → zero-drift plan → idempotent re-add →
  destroy clean. Lint-clean is deferred-pending-H1: `internal/application/
  lint` has not merged into this tree as of this task.
- **Done addendum (2026-07-22, at the E9→main merge, H1 already merged):**
  lint-clean closed — the generators gained explicit
  `deletionPolicy: retain` on emitted Dataset/Source (DL020 flagged them;
  ADR 024's co-evolution bar makes that a composition bug, fixed in the
  merge). Verified with the real binary: init → add pipeline (reuse
  broker + sink, prefix override) → validate green, `lint` reports zero
  findings, re-add still proposes no changes.

### E10: Visual composer (optional; spin-off candidate)

- **Size:** L. **Depends:** E9 (builds on the engine/TUI seam).
- **Do:** deliberately not designed yet (ADR 024 §interaction layer):
  a Bubble Tea v2 canvas over the compose engine — live graph view,
  select-to-wire. Revisit once E9 usage shows what a canvas must do;
  candidate for a separate repository consuming the compose engine as a
  library, which would force the engine's API to be clean (the same
  forcing-function argument as Phase 8 plugins).
- **Accept:** n/a until designed; placement here records intent only.

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
- **Done (2026-07-21, merged), with one criterion revised by evidence:**
  the kit exists (Network/HostPort/ReachableAddr/ResolveCredential/
  EnsureInstance/CredentialRotation, 262 lines) and all nine providers
  migrated where the helpers honestly apply, net −536 provider lines and
  full-sweep green — but the ">100 lines per reference provider" number
  was an overestimate (postgres −47, mysql −43): the remaining per-file
  bulk is engine-specific reconcile/probe/destroy logic the task itself
  excludes, and forcing it through knob-laden helpers is what the task
  prohibits. E6 should document providerkit as the authored shape; the
  E6 reference to this note stands.

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
- **Done (2026-07-21, merged):** seven files, largest 400 lines
  (container.go split into ensure + container_remove.go halves — recorded
  deviation); --color-moved proves 3155 moved vs 74 new lines; live K8s
  suite green under minimal RBAC as the gate.

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
- **Done (2026-07-21, merged):** 52 constants declared in
  `internal/domain/status/reasons.go`, grouped by area; all 156
  `status.Condition{Reason: ...}` construction sites plus 5 dynamic
  reason-building call sites (redpanda `probeTopic`'s 3 return statements,
  nessie's 2-way reason selection) migrated — zero string-value changes,
  confirmed by full-suite green with no test edits beyond constant
  references. `internal/archtest/reason_literal_test.go` (`TestNoConditionReasonStringLiterals`)
  bans any `Reason: "literal"` outside `internal/domain/status` and
  `_test.go` files; verified to fail on an injected violation, then
  reverted. No dedup/renames were needed — every reused reason string
  (`InstanceHealthy`, the `ConnectWorker*`/`Connector*` Kafka Connect set,
  the postgres/mysql CDC-source set) was already spelled identically
  across providers, so no `docs/upgrade-notes.md` entry was required.
  Debezium/s3sink's `"ConnectorState" + state` and redpanda's
  `PartitionCountMismatch`/`RetentionMismatch` stay intentionally dynamic
  (constant prefix + runtime-observed suffix, documented at each call
  site) to preserve their existing diagnostic detail without a behavior
  change.

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
- **Done (2026-07-21, merged):** eight per-area files, largest 259 lines,
  explicit fixtures struct, cross-subtest dependencies documented;
  27/27 subtests byte-identical in name/count/order vs main (fake +
  Docker legs diffed live).

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

### G7: Integration-test economy hardening

- **Size:** S. **Depends:** — (the mechanism shipped 2026-07-22:
  `scripts/test-impact.sh` + doc 06 §10; this task hardens it).
- **Context:** the suite↔scope map inside the script is a hand-maintained
  contract; nothing fails when a new integration test lands unmapped, and
  the ledger has no expiry/pruning.
- **Do:** (1) a completeness guard test: every `Test*` in
  `cmd/platformctl/*_integration_test.go` (and integration-tagged
  packages) must be matched by exactly one suite's `-run` pattern, or be
  on an explicit exemption list with a reason — fails CI otherwise;
  (2) `--prune` for ledger entries older than N days; (3) a CI leg that
  uses `test-impact.sh --base origin/main` for PR builds and `--full`
  for main/nightly, replacing the always-full PR sweep.
- **Accept:** guard test proven by a deliberately unmapped fixture test
  failing it; PR CI time drops measurably for docs-only/scoped changes;
  full sweep still runs on main.
- **Done (2026-07-22, merged, with E4):** completeness guard
  (`internal/archtest`, parses the map out of the script — single source
  of truth), `--prune`, CI adoption (PR = `--base origin/main`, main
  push = `--full`; K8s job untouched). Finding: the guard surfaced 23
  pre-existing unmapped integration tests, exempted in-test with
  per-cause reasons (accounting in the branch checkpoint) — widening the
  map's `-run` patterns to absorb the exemption list is the small
  follow-up, best done when the current provider wave's map rows have
  all merged.

---

## 7.7 Stage H — Guardrails & zero trust (ADRs 020/021/022)

Theme: the owner-directed guardrail programme — design-quality lints
(detection), organizational policy (enforcement), and identity-aware
mediation (runtime zero trust) — delivered in that order because each
layer consumes the previous one's output. Designs are accepted in ADR
020 (lints), ADR 021 (policy), ADR 022 (domains/identity/mediation);
tasks here carry sizes/acceptance only — the design content lives in the
ADRs and is not restated.

**Stage exit criteria:**
- [x] `platformctl lint` reports the ADR 020 built-in set deterministically
      (byte-identical on identical input), waivers are auditable, every
      shipped blueprint lints clean in CI, and every lint code resolves in
      `platformctl explain`.
- [ ] A policy file can deny a manifest set at validate/plan/apply with
      the standard error shape; the zero-trust pack ships and passes
      against the lakehouse example (with documented, justified waivers
      where the example is deliberately dev-flavored).
- [ ] The owner's scenario holds end-to-end: a cdc Binding whose source
      and sink chains carry different `metadata.domain` values is denied
      at validate by a cross-domain policy; with an allow policy, the
      same manifest reconciles the path through a MediatedConnection and
      the mediator's own logs/policy state show the identity-checked
      dial; removing the allow and re-applying severs it.
- [ ] Undeclared domains remain byte-identical to today's behavior
      (no segmentation, no mediation) — pinned by tests.

### H1: Lint engine and built-in set (ADR 020)

- **Size:** M. **Depends:** — (E4's explain catalog is merged).
- **Do:** `internal/application/lint` (pure functions over the resolved
  graph + provider implementations, reusing compatibility's index);
  the DL001–DL021 built-in set from ADR 020 §4; severities warning/info;
  `platformctl lint [path]` with `-o json` + `--strict`; the one-line
  validate summary; waiver annotations (`lint.datascape.io/waive`) with
  mandatory reasons (empty reason is itself a warning); every code in the
  E4 explain catalog (extend the completeness guard to lint codes).
  Findings sorted (severity, code, key); golden test for determinism.
- **Accept:** stage-criterion 1's lint half; blueprint lint-clean CI
  test; a fixture manifest exhibiting every DL code, golden-verified.
- **Gate:** `DesignLints` (Alpha, **enabled** — read-only reporting; the
  gate exists to switch the validate summary off, not to hide the
  command).

### H2: Provider-contributed lints (ADR 020 §5)

- **Size:** S. **Depends:** H1.
- **Do:** `reconciler.DesignLinter` optional capability (pure,
  validate-time); first implementors: debezium (N connectors × one
  database = N replication slots; overlapping table captures), redpanda
  (replication vs broker count shape hints), s3sink (prefix-collision
  refinement). Codes namespaced `DL-<type>-NNN`, catalog entries
  included.
- **Accept:** each shipped lint has a positive+negative fixture; the
  catalog completeness guard covers namespaced codes.

### H3: Policy engine core (ADR 021 §§1–3)

- **Size:** L (ADR 021 is the design note; no new ADR needed unless the
  vocabulary must deviate — then amend 021 additively). **Depends:** H1
  (findings are policy facts).
- **Do:** `internal/domain/policy` (kind `policy.datascape.io/v1alpha1`,
  closed rule vocabulary per ADR 021 §2, JSON Schema); loading from
  `--policies`/`.datascape/policies/` (never from the governed set);
  deny-wins evaluation wired into loadAndValidate (after compatibility +
  lint) and into plan/apply/destroy for plan-scoped rules; exemption
  annotations honored only when the rule declares `exemptible: true`;
  `platformctl policy test`; rule ids in the explain catalog; machine
  output per the A7 harness.
- **Accept:** stage-criterion 2's engine half; a deny names rule id,
  message, resource, and exits via the standard validation path;
  determinism golden.
- **Gate:** `PolicyEngine` (Alpha, disabled).

### H4: Zero-trust policy pack (ADR 021 §4)

- **Size:** S. **Depends:** H3; C8 (the TLS rules reference shipped
  mechanisms only).
- **Do:** `platformctl policy init zero-trust` writing the versioned
  starter pack (every rule cites its mechanism ADR); onboarding
  §governance section; pack evaluated in CI against the examples with
  recorded waivers where dev-flavored.
- **Accept:** stage-criterion 2's pack half.

### H5: Domains and cross-domain policy (ADR 022 Rings 0–1)

- **Size:** M. **Depends:** H3.
- **Do:** `metadata.domain` (meta.json, DNS-label, default `default`;
  doc 03 §2 additive); policy vocabulary gains domain/edge selectors
  (`crossDomain: {from, to}` over graph edges); Ring-0 validate denial;
  Ring-1 compilation — per-domain network segmentation (Docker: network
  per domain; Kubernetes: the existing B7 walls per domain-namespace
  mapping) with allowed cross-domain paths compiling to exactly the
  mediated entrypoint's holes; undeclared-domain no-op pinned
  byte-identical.
- **Accept:** the owner-scenario's validate half (stage criterion 3,
  first sentence); segmentation integration test on both runtimes.
- **Gate:** rides `PolicyEngine` (domains without policies are inert
  labels; no separate gate).

### H6: Mediated connections — OpenZiti mesh provider (ADR 022 Ring 2)

- **Size:** L (ADR 022 is the design). **Depends:** H5.
- **Do:** a `mesh`-class provider (OpenZiti: pinned controller + router
  images) realizing MediatedConnections on the Connection seam
  (ConnectionCapableProvider); workload identities minted per
  participant from the F4 naming authority (SPIFFE-aligned URI form);
  ADR 021/H5 policies compiled to Ziti dial/bind service policies;
  the raw-TCP proof is a Postgres path (a cdc Binding dialing its
  cross-domain source only through the mediated entrypoint, source dark
  on the shared networks); mediator mTLS is Ziti's own (no hand-rolled
  certs); teardown removes identities/policies. Consul-intentions
  alternative and sidecar `meshMode` recorded as follow-ups in ADR 022 —
  not built here.
- **Accept:** stage criterion 3 end-to-end + criterion 4; drift on
  out-of-band Ziti policy edits detected and healed (the debezium
  config-drift bar applied to mediator state).
- **Gate:** `MediatedConnections` (Alpha, disabled).

## 7.8 Stage I — Production-review remediations (doc 11, 2026-07 owner review)

Findings from docs/planning/11-production-review-2026-07.md promoted to
sequenced tasks. Stage I tasks are independent of Stage H ordering
unless a dependency is stated.

### I1: Consume `Connection.spec.via` — VPC-behind-VPN egress, blast-minimized (ADR 023 completion)

- **Size:** M. **Depends:** D5 (merged). **Why:** the owner's named
  production scenario — a database reachable only through a VPN into a
  VPC — is exactly the `via` seam ADR 023 left schema-complete but
  unconsumed. Zero-trust framing: only the Connection's own forwarder
  may egress through the tunnel; nothing else on the shared network
  gains a route (blast-minimized).
- **Do:** the proxy (connection-capable) provider realizes a managed
  Connection whose `spec.via` names a tunnel-capable Provider by (1)
  resolving the tunnel Provider's published endpoint fact (ADR 015
  discipline — engine-resolved from state, never provider-constructed),
  (2) attaching ONLY the Connection's forwarder container to the tunnel
  transit network (the wireguard container's DNAT surface), never the
  consumer workloads, and (3) probing reachability of `spec.target`
  through the tunnel before Ready (settledness bar, I3). Drift: a
  forwarder found attached to networks beyond [shared, transit] is
  drift (excess attachment = widened blast radius). Destroy detaches
  before removal. Validate-time: `via` naming a non-tunnel-capable
  provider already errors (D5); add the pairing error when `via` is set
  on a Connection whose realizing provider is not via-capable —
  capability-interface message format per doc 02 §4.2.
- **Accept:** extend the D5 e2e: a consumer on the shared network can
  reach the private DB ONLY through the via'd Connection entrypoint;
  direct dial of the tunnel network from a non-forwarder container
  fails (negative proof); CDC RUNNING through the via'd Connection;
  key rotation mid-stream recovers; destroy leaves no transit
  attachments.
- **Gate:** reuses `TunnelProvider` (Alpha, disabled) — no new gate.
- **Interim (2026-07-22):** until I1 lands, `validate` REFUSES a
  Connection declaring `spec.via` (compatibility check + test) — a
  declared-but-inert egress control applied as a plain forwarder is a
  silent security failure (doc 11 Phase A finding). I1 deletes the
  refusal and replaces its test with the realization contract.

### I2: Outbound database TLS — reach TLS-requiring (cloud-managed) databases

- **Size:** M. **Depends:** none (independent of I1). **Why:**
  `sslmode=disable` is hardcoded at
  internal/adapters/providers/postgres/sql.go:27,
  internal/adapters/providers/debezium/debezium.go:620 (preflight; the
  Debezium connector config sets no `database.sslmode` at all), and
  internal/adapters/providers/jdbcsink/jdbcsink.go:657; mysql/mariadb
  DSNs carry no tls parameter. Every cloud-managed engine (RDS, Cloud
  SQL, Azure Database) requires or defaults to TLS — the owner's
  simplest production scenario cannot connect today, and plaintext is
  silently used everywhere else.
- **Do:** declare TLS posture once on the consumption seam and thread
  it to every consumer: `Connection.spec.tls` gains an external-side
  meaning via a new `spec.tls.mode` for `external: true` Connections
  (`require` | `verify-ca` | `verify-full`, plus optional
  `caSecretRef` for private/RDS CA bundles; absent = current plaintext
  behavior, preserving back-compat for local dev). Domain validation:
  external+tls requires mode (the existing exactly-one-of applies only
  to managed termination). Consumers updated: postgres admin conn,
  debezium preflight AND connector properties (`database.sslmode`,
  `database.ssl.*` for mysql), jdbcsink JDBC URL, mysql/mariadb DSNs
  (`tls=` param with a registered custom CA config when caSecretRef is
  set). CA material resolves through the existing secretRefs discipline
  (named SecretReference, listed in the realizing provider's
  spec.secretRefs; never logged, fingerprint only). Schema + doc 03
  §8.2 in the same commit; explain-catalog entries for new failure
  reasons (CA parse failure, verify failure).
- **Accept:** e2e against a TLS-required Postgres (server cert from a
  test CA): connect refused without tls declared (the real error
  surfaced, not swallowed), succeeds with `verify-full` + caSecretRef;
  CDC RUNNING against the TLS DB; negative: wrong CA fails with a
  named reason at preflight (validate-time completeness, ADR 011 —
  never mid-apply). Unit: DSN/property construction for all four
  consumers, all modes.
- **Gate:** new `ExternalDatabaseTLS` (Alpha, enabled — additive
  opt-in field; absent field = unchanged behavior).

### I3: Settledness NFR + async-correctness audit backlog

- **Size:** S (doc) + audit findings sized separately. **Depends:**
  none. **Why:** the "Ready means settled and serving" invariant exists
  only as F3 folklore + point fixes (93fbf14); doc 01's NFR table
  (NFR-1..10) never states it, so nothing holds new providers to it.
- **Do:** add NFR-11 (Settledness): "A resource reported Ready answers
  its declared protocol at that moment, and a probe immediately after
  apply returns no drift; wait loops poll condition-based with an
  overall deadline and never sleep-fixed-duration-and-assume" — doc 01
  NFR table + doc 02 engineering-rules section (additive). Phase B's B1
  audit (doc 11) then verifies every provider against NFR-11; each
  violation becomes a fix task referencing it.
- **Accept:** NFR-11 present in docs 01+02; B1 audit report cites it
  per finding.

### I4: Reconcile/Probe symmetry — Ready must use the probe's own serving check (B1 findings 1–3)

- **Size:** S–M. **Depends:** none. **Why:** the B1 audit (doc 11) found
  one recurring class: three connection-realizing providers set Ready in
  Reconcile from a WEAKER signal than their own Probe verifies —
  wireguard (container healthcheck = interface exists, vs Probe's
  handshake + dial-through-forwarder; CONFIRMED, the redpanda-93fbf14
  signature), ingress-docker (route written via admin API, vs Probe's
  dial-through-route), proxy (container Running — no healthcheck even
  configured — vs Probe's dial-through-forwarder). NFR-11 names exactly
  this: Ready means serving NOW, and an immediate drift probe reports
  clean.
- **Do:** in each provider, before setting Ready in reconcile, run the
  SAME serving check Probe uses (extract/reuse the existing functions:
  wireguard's handshake+`dialUpstream`, ingress-docker's
  `probeThroughRoute`/`probeThroughRouteTLS`, proxy's
  `probeThroughForwarder`) inside a bounded condition-poll with an
  honest timeout error naming the last observed state (the
  `waitTopicSettled` pattern). No new signals — symmetry with Probe is
  the whole fix. Proxy additionally gains a real container HealthCheck.
- **Accept:** unit: reconcile fails (honest error) when the upstream
  never answers; e2e: existing wireguard + ingress suites stay green,
  and each gains an assertion that `drift` immediately after `apply`
  reports zero drift (the NFR-11 acceptance bar).
- **Deferred sibling (recorded, not scheduled):** ingress-kubernetes is
  symmetric-shallow (neither Reconcile nor Probe dials through the
  ingress controller) — no false-Ready flap, and a serving probe would
  hardcode assumptions about which ingress controller the cluster runs;
  revisit if a live K8s ingress flake ever surfaces (B1 finding 4).
- **Done (2026-07-22):** wireguard's `reconcileConnection` extracted
  Probe's Connection-case check into shared `probeTunnelServing`
  (container health + `handshakeAge` + `dialUpstream`), added
  `waitTunnelServing` (a `tunnelSettleTimeout`/`tunnelSettlePoll`-bounded
  poll, vars not consts so tests can shrink them) before setting Ready;
  ingress-docker's `reconcileConnectionDocker` gained `waitRouteServing`
  (reuses `probeThroughRoute`/`probeThroughRouteTLS` via
  `routeSettleTimeout`/`routeSettlePoll`) before Ready; proxy's
  `reconcileConnection` gained `waitForwarderServing` (reuses
  `probeThroughForwarder` via `forwarderSettleTimeout`/
  `forwarderSettlePoll`) before Ready, plus a real container `HealthCheck`
  (a self-dial through the socat listener). No new status reasons: a
  settle-timeout returns a bare error naming the last observed state,
  mirroring how redpanda's own `waitTopicSettled` surfaces `(st, err)`
  rather than a new Ready=False reason. Unit tests added per provider
  (wireguard_test.go, proxy_test.go — neither existed before —
  ingress/route_settle_test.go) proving reconcile fails honestly when the
  upstream never answers; e2e `drift` immediately after `apply` asserted
  zero-drift in wireguard_integration_test.go and
  ingress_integration_test.go (NFR-11). `go test ./...` exit 0; gofmt/vet
  clean; `test-impact.sh --base main` selected lakehouse+ingress+wireguard
  (see TASK_PROGRESS.md for the sweep log).
- **Done, follow-up (2026-07-22) — ordering gap the settle-poll exposed:**
  the first I4 sweep failed the ingress (and lakehouse-K8s) suites for a
  REAL pre-existing gap, not a settle-poll bug: a managed Connection's
  `spec.target` is a plain "host:port" string, so `graph.Build` created
  no dependency edge to the in-set resource the host names — Connection
  "minio" (target `ing-test-minio:9000`) reconciled at level [4/6] while
  Provider "ing-test-minio" waited at [6/6]; ordering was arbitrary and
  only survived before I4 because reconcile set Ready blind. Fix (in
  `internal/domain/graph`): for each managed (external: false)
  Connection, the target's host part now resolves against every in-set
  resource's runtime object name (`naming.RuntimeObjectName`; metadata
  name indexed too against future divergence) in the Connection's own
  namespace, adding a Connection→upstream edge — the D8 warehouseRef
  precedent, differing only in resolving a plain host string instead of
  a NameRef. Deliberately lenient where refFields is strict: a host
  matching nothing is a genuinely external address (the entire point of
  managed Connections) and adds no edge and no error; a self-naming
  target adds no self-edge; an edge that closes a loop is NOT silently
  skipped — the existing cycle detection reports it (a Connection whose
  upstream depends back on it is a design error the user must see).
  Ordering visibly changes in: ingress-scenario (nessie→ing-test-nessie,
  minio→ing-test-minio — the observed failure), ingress-k8s-scenario,
  ingress-tls-scenario (nessie-provided/nessie-selfsigned→ing-tls-nessie;
  internal-upstream's target is an out-of-set test fixture, correctly no
  edge), ingress-tls-k8s-scenario (all three Connections→
  ingk8stls-nessie). examples/ + blueprint templates: every Connection
  target there is an external placeholder host — no edges, no ordering
  change. Unit tests: graph_test.go TestManagedConnectionTarget* (+
  external/no-match/self-edge/cycle negative pins).
- **Done, follow-up 2 (2026-07-22) — K8s settle bar matches Probe's:**
  round-2 sweep was 15/16 green; the one K8s failure
  (TestLakehouseExampleOnKubernetes, Connection/orders-db: "forwarder has
  no published host address yet" for the full 45s) exposed that proxy's
  `waitForwarderServing` treated an empty `ctr.HostAddr` as a wait state
  — but on Kubernetes under the default ClusterIP/port-forward access
  mode, Inspect NEVER reports a HostIP/HostPort (only NodePort/
  LoadBalancer Services get one), so the address could never appear.
  Proxy's Probe guards its own dial-through with `if addr != ""` and
  skips it in exactly this case; the fix makes the settle poll mirror
  that guard verbatim — on a runtime with no published host binding, the
  serving bar is container health, the same as Probe's. Deliberately NOT
  a per-attempt port-forward dial: that would make reconcile STRICTER
  than Probe (breaking I4's symmetry in the opposite direction) and would
  wrongly fail Connections whose target is a genuinely external,
  unresolvable-from-the-cluster host (the lakehouse example's placeholder
  upstream) — serving means "the forwarder accepts and forwards";
  upstream reachability on such runtimes stays Probe/drift's job, as
  before I4. Docker behavior unchanged (address exists immediately;
  dial-through always runs — its suites stayed green). Unit pin:
  proxy_test.go TestReconcileConnectionReadyWhenRuntimePublishesNoHostAddress
  (a no-host-addr fake wrapper simulating the K8s Inspect shape).
  Targeted re-run of only the failed lakehouse suite (not a full
  re-sweep; 15 green suites' content-scope unchanged except proxy —
  coordinator-authorized deviation, see TASK_PROGRESS.md) launched under
  the shared flock with the minted minimal-RBAC kubeconfig.

### I5: Duplication debt with drift risk (B4 findings 2+3)

- **Size:** M. **Depends:** none (files disjoint from I4).
- **Do:** (1) extract debezium's ~60-line Source/Connection endpoint+
  credential resolution block (mirrored near-verbatim in jdbcsink, keyed
  `replicationSecretRef` vs `credentialsSecretRef`) into a providerkit
  helper parameterized by config-key name; both providers call it.
  (2) Hoist the duplicated ProbeReachable machinery (pinned probe image,
  dial script, exec-then-ephemeral-probe algorithm) out of
  runtime/docker and runtime/kubernetes into one shared package, keeping
  only the transport adapter-specific — and fix the already-drifted
  divergence: kubernetes' `dialable` ignores ctx and hardcodes a 2s
  dial; it gets docker's ctx-aware signature (deadline-capped, refuses
  when expired).
- **Accept:** behavior-preserving for docker (its ctx semantics are the
  keeper); unit-covered helper; both runtimes' conformance suites green;
  cdc + jdbcsink integration suites green (the two consumers of the
  extracted resolution helper).
- **Gate:** none (refactor).
- **Done (2026-07-22):** (1) `providerkit.ResolveEndpoint` +
  `providerkit.ResolveEndpointCredentials`
  (internal/adapters/providers/providerkit/endpoint.go), called from
  both debezium.go and jdbcsink.go; diffing the two original ~60-line
  blocks first found no semantic divergence beyond the expected
  config-key name and each caller's own error wording — no drift bug.
  (2) `internal/adapters/runtime/probe` (Image, TCPDialScript, ExecArgs,
  Command, ctx-aware Dialable) hoisted out of runtime/docker and
  runtime/kubernetes; kubernetes' `dialable` now shares docker's
  deadline-capped ctx semantics (the one deliberate behavior change —
  it previously ignored ctx and hardcoded a 2s dial). Unit tests added
  for both (`providerkit/endpoint_test.go`,
  `runtime/probe/probe_test.go`); `scripts/test-impact.sh`'s
  docker-conformance/k8s-adapter scopes extended to cover the new probe
  package.

### I6: KubernetesRuntime GA-parity evidence (B2 gaps 1+2)

- **Size:** M. **Depends:** none (test-only; no production code).
- **Why:** the B2 audit (doc 11) found the two evidence gaps standing
  between KubernetesRuntime and an UNCONDITIONAL GA claim: no K8s
  analogue of `TestChaosApplyKilledMidRun` (crash-mid-apply +
  re-converge is proven on Docker only), and no K8s variant of
  `TestConnectWorkersHAAndDeadLetterQueue` (DLQ + worker-HA is GA-gated
  functionality, unverified on the GA-candidate runtime).
- **Do:** (1) a K8s mid-apply-kill test: run apply against the cluster,
  SIGKILL it mid-run, re-apply, assert convergence + clean drift —
  mirror the Docker chaos test's structure; add a `chaos-k8s` suite row
  (scope: internal/adapters/runtime/kubernetes + SHARED_CORE).
  (2) a K8s Connect-HA/DLQ test mirroring the Docker one's assertions
  (worker kill → task rebalance; poison message → DLQ topic) on the
  minimal-RBAC cluster; extend the `connect-ha-dlq` suite row's scope
  with the K8s adapter dir. Both run under the minted minimal-RBAC
  kubeconfig (doc 06 §8.4) — RBAC verbs they surface as missing get the
  role.yaml+preflight+README treatment in the same commit.
- **Accept:** both tests green twice on the live cluster; suite rows
  selected by a K8s-adapter-only change (`test-impact.sh --print`
  proof). GA graduation of KubernetesRuntime (owner decision) can then
  be unconditional.
- **Gate:** none (evidence for an existing gate's graduation).
- **Done (2026-07-22, test-only):**
  `TestKubernetesChaosApplyKilledMidRun`
  (cmd/platformctl/chaos_kubernetes_integration_test.go +
  testdata/chaos-k8s-scenario, the runtime:kubernetes mirror of
  testdata/cdc-scenario) — build binary, apply on the live cluster,
  SIGKILL on the first "✓" progress line, state valid, re-apply
  converges, status all-Ready, drift clean, destroy clean. First live
  pass 66.63s (test), 130s wall.
  `TestKubernetesConnectDeadLetterQueueAndWorkerResilience`
  (cmd/platformctl/connect_ha_dlq_kubernetes_integration_test.go +
  testdata/connect-ha-dlq-k8s-scenario) — D6's DLQ assertions in full
  (pre-poison record lands in MinIO, poison record routed to the
  declared DLQ topic, sink connector RUNNING throughout, post-poison
  record still lands, live connector config carries the DLQ keys) plus
  an out-of-band worker-pod delete healed by the Deployment controller
  with drift (never apply) observing the recovery. Suite rows:
  `chaos-k8s` added; `connect-ha-dlq` scope + -run extended
  (scripts/test-impact.sh); archtest suite-map completeness green;
  unfiltered `go test ./...` exit 0. Both tests' two-green-runs
  evidence: see `i6-live-runs.log` at the I6 worktree root (runs were
  queued behind other agents' sweeps on the shared flock at commit
  time; the merge gate transcribes the four timings here).
  **Two live findings, recorded not worked around (§2.1 deviation
  clause):** (1) C3's `workers > 1` Connect-worker set has NO working
  Kubernetes leg — `providerkit.ReachableURLs`' per-ordinal addressing
  (`runtime.OrdinalName` → `EnsureReachable`) resolves only
  StatefulSet-ordinal names, which the Deployment-shaped
  (`StableIdentity: false`, docs/adr/004) worker sets never get, so a
  Binding on a `workers: 2` debezium/s3sink Provider fails at apply
  with `no member of "<name>" (2 ordinals) is currently reachable`
  (reproduced live; full entry in doc 07's per-runtime differences).
  The K8s DLQ test therefore runs single-worker and proves
  Deployment-controller self-heal, not C3's two-worker failover claim.
  (2) A host-side Kafka produce/consume against the legacy
  single-broker redpanda shape on Kubernetes must redirect dials away
  from the broker's advertised loopback sentinel
  (redpanda.advertisedAddr, docs/adr/017 §a.4) — metadata-only calls
  work via the seed, but produce follows the advertised address; the
  test now uses the same dialer-redirect trick as the provider's own
  admin client (proven live: a plain client's ProduceSync hangs until
  deadline). No new RBAC verbs: role.yaml/preflight already cover both
  scenarios. **Consequence for the GA decision (owner's call):**
  KubernetesRuntime GA-parity evidence is complete for the
  mid-apply-kill (gap 1) and DLQ (gap 2's D6 half) claims; an
  unconditional GA claim should either exclude Connect-worker HA
  (`workers > 1`) on Kubernetes or wait for the finding-(1) fix.




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
| `DesignLints` | H1 | **enabled** (read-only reporting) | Beta once blueprints + examples are lint-clean for a release |
| `PolicyEngine` | H3 | disabled | Beta after the zero-trust pack soaks in this repo's own CI |
| `MediatedConnections` | H6 | disabled | Beta after the owner-scenario e2e soaks on both runtimes |
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

**Step 6 — guardrails & zero trust (Stage H, added 2026-07-22 by owner
direction; ADRs 020/021/022):** H1 → H2 in parallel with H3's start; H3 →
H4 and H5; H6 last (needs H5's domains). H1 is independent of every other
stage and may start immediately; H4 waits for C8; H6 is the only L-size
item and should not start before the C/D provider wave has merged (it
touches the Connection seam those tasks also exercise). Stage H does not
block Stage C/D/E closure — it is an additive programme.
E9 (composition, ADR 024) slots after H1 merges and pairs naturally with
H1's lint bar; E10 is unscheduled intent.

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
