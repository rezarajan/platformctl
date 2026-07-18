# Production-Grade Docker Runtime Gap Analysis

Review date: 2026-07-15.

Purpose: document the work still needed before platformctl should be treated as a
production-grade Docker-runtime foundation for data lakehouse and pipeline
infrastructure, and before the project invites broad third-party open-source
contribution. This is intentionally a work backlog, not an implementation note.

**Task tracking (2026-07-17):** every open (`[ ]`) item in this document is
now sliced into an actionable, stage-gated task in
[08-production-readiness-plan.md](08-production-readiness-plan.md) — see its
§9 for the item-by-item mapping. Work new items there; this document remains
the analysis record and the home of the per-area detail the tasks reference.

Validation signal during review:

- `GOCACHE=/tmp/platformctl-go-build go test ./...` passes outside the sandbox.
- `GOCACHE=/tmp/platformctl-go-build go test -race ./...` passes outside the sandbox.
- The same unit run fails inside the managed sandbox because
  `TestProbeTCPReachable` opens loopback sockets; this is an environment
  limitation, but the test should still be made clearer or skippable in
  restricted runners.

## Executive Summary

platformctl has a strong core: schema validation, dependency ordering,
idempotent reconciliation, drift probing, Docker ownership labels, real CDC and
sink providers, endpoint inventory, and architecture graph rendering are all in
place. The remaining risks are mostly production contract gaps rather than
missing demo functionality.

The highest-priority work is:

1. Define and enforce stable identity: names, namespaces, state keys, Docker
   object names, provider object names, URL path segments, secret paths, and
   graph node ids must all share a canonical policy.
2. Make external lifecycle enforcement real for all apply paths, not only
   destroy.
3. Stop silently orphaning state/runtime objects when resources are removed or
   renamed in manifests.
4. Harden the Docker runtime port and adapter around bind addresses,
   ownership checks, restart/resource/security policies, image pinning, logs,
   and endpoint discovery.
5. Make machine-readable outputs valid for every command and make Mermaid/DOT
   rendering robust against special characters and id collisions.
6. Upgrade provider drift checks from "is it alive" to "does it still match the
   declared spec".
7. Fill data-engineering completeness gaps: persistent catalog configuration,
   table/warehouse contracts, schema-aware formats, ingress/egress, TLS/auth,
   and generated endpoint config for tools such as Spark, Trino, Dagster,
   dbt, BI tools, and SQL/S3 clients.

## Cross-Runtime Portability (Docker + Kubernetes + Terraform)

**Added 2026-07-16.** This gap analysis is scoped to the Docker runtime, but
the project's own design goal (docs/planning/01-product-requirements.md
G6, docs/planning/04-roadmap-and-feature-gates.md §10) is that the
provider/runtime split lets a second runtime adapter — Kubernetes, then
Terraform/external — be added *without changing any existing resource kind
or provider*. That claim was previously untested: Phase 7 (Kubernetes) and
Phase 8 (Terraform/external) were both listed as "future, not started," and
`registry.PlannedRuntimes` rejected `kubernetes` at construction time with
"planned but not yet available."

A real Kubernetes `ContainerRuntime` adapter now exists
(`internal/adapters/runtime/kubernetes`, Alpha, behind the
`KubernetesRuntime` gate — disabled by default) and:

- Passes the exact same conformance suite the Docker adapter passes
  (`internal/ports/runtime/conformance`), run against a real cluster
  (`minikube`, verified live during this work — not mocked).
- Was exercised through the real CLI with the **unmodified** `redpanda`
  provider (`internal/adapters/providers/redpanda`) — `platformctl apply`
  against a manifest with `spec.runtime.type: kubernetes` created a real
  Deployment + Service, pulled the image, ran the translated health probe,
  and reported the Provider healthy in ~2.5s. Zero provider code changes.

This is the strongest evidence available that the port boundary in
`internal/ports/runtime` is genuinely runtime-agnostic, not just designed to
look that way. Building it surfaced concrete facts worth recording — both
things that needed fixing and things that turned out to be genuine,
justified per-runtime differences (per this project's own "never overfit
unless truly technology-specific" standard):

### Mapping decisions that worked cleanly (no port change needed)

- **Docker network → Kubernetes Namespace.** A Docker network is a shared
  addressing+isolation domain that lets containers resolve each other by
  name; a Kubernetes Namespace plus a same-name Service does the same job
  (Service DNS gives every pod in the namespace `<name>` resolution, exactly
  matching Docker's embedded per-network DNS). `EnsureNetwork` creates the
  Namespace; every container/volume naming that network lands inside it.
  Every provider already names exactly one network per container
  (verified: `grep -rn "Networks:" internal/adapters/providers/*/*.go` — 11
  call sites, all single-element slices), so this required no provider
  change at all.
- **RestartPolicy, SecurityContext field names.** These were deliberately
  named after Kubernetes' own vocabulary when added in this session's
  earlier Gate 1.1 slice (restartPolicy Always/OnFailure/Never;
  runAsUser/readOnlyRootFilesystem/capabilities), specifically so a future
  Kubernetes adapter wouldn't need translation. That bet paid off — the
  Kubernetes adapter maps these fields almost directly.
- **Health checks.** Docker's `HealthCheck.Test` convention (`["CMD-SHELL",
  "<cmd>"]` / `["CMD", "<argv>"]`, used identically across all 7 providers
  that set one) translates cleanly to a Kubernetes `ExecAction`, applied to
  both readiness and liveness probes (Docker's single healthcheck gates both
  "accepting traffic" and "should be restarted").

### A real port-boundary defect found and fixed

- **`VolumeSpec` had no namespace concept.** Docker volumes are
  cluster-global; a Kubernetes `PersistentVolumeClaim` is namespace-scoped
  and can only be mounted by a Pod in the *same* namespace. `VolumeSpec`
  carried no hint of which namespace a volume belonged to, so the
  Kubernetes adapter had no way to know where to place the PVC before the
  container that mounts it is created. Fixed by adding `VolumeSpec.Networks
  []string` (Docker ignores it; Kubernetes requires exactly one, mirroring
  `ContainerSpec.Networks`) — a small, mechanical change to `runtime.go` and
  the 6 provider call sites (`mysql`, `s3`, `postgres`, `redpanda`,
  `openlineage`, `placeholder`), plus the conformance suite. This is
  exactly the kind of "design defect in the port boundary" §10 of the
  roadmap doc anticipated finding, found by actually building the second
  adapter rather than reasoning about it abstractly.

### A real bug found only by running a real, unmodified provider

- **`Cmd` maps to Docker's CMD (appended after ENTRYPOINT), not a full
  command replacement** — the Kubernetes adapter's first version set
  `container.Command = spec.Cmd`, which is Kubernetes' ENTRYPOINT-replacing
  field, not its CMD-appending one (`container.Args`). This went undetected
  by the conformance suite (which uses a bare `alpine sleep`, no image
  ENTRYPOINT) and was only caught by running the real `redpanda` provider
  end-to-end: its image has `Entrypoint: ["/entrypoint.sh"]`
  (`docker image inspect docker.redpanda.com/redpandadata/redpanda:v24.2.1`
  confirms this), and replacing it skipped whatever bootstrap the entrypoint
  script does, surfacing as `unrecognised option '--node-id'` from the raw
  `redpanda` binary. Fixed: `spec.Cmd` → `container.Args`, `container.Command`
  left unset so the image's own entrypoint (if any) still runs — this is
  the standard, well-known Docker→Kubernetes CMD/ENTRYPOINT mapping.
  **Lesson for this backlog generally: a synthetic conformance suite proves
  the *port contract*; only a real provider against a real cluster proves
  the *translation* is faithful.** Both are needed.

### Genuine per-runtime differences (not bugs — documented, not forced)

- **RestartPolicy.MaxRetries and non-`Always` modes.** Kubernetes
  Deployments require Pod `restartPolicy: Always` — there is no Pod-level
  "give up after N restarts" the way Docker's `on-failure` +
  `MaxRetries` has (that's a Job concept, not a Deployment one, and our
  containers are long-running Deployments). The Kubernetes adapter accepts
  `RestartPolicy` but only actually applies `Always` at the Pod level;
  `Mode` values other than `always`/`unless-stopped` and `MaxRetries` are
  silently not enforced under Kubernetes. This is a real platform
  difference, not something to fake.
- **`LogConfig` has no Kubernetes equivalent** — log driver selection is a
  node/kubelet concern in Kubernetes, not a per-Pod API field. Ignored by
  the Kubernetes adapter.
- **`SecurityContext.SecurityOpt`** is a Docker-specific escape hatch
  (e.g. `no-new-privileges`) with no generic Kubernetes translation.
  Ignored by the Kubernetes adapter.
- **CPU reservation.** Kubernetes has a real, portable reservation concept
  (`resources.requests.cpu`); Docker does not (CPU shares are a relative
  weight). `Resources.CPUReservation` was added to the port specifically
  because building the Kubernetes adapter proved it has a genuine
  runtime-specific meaning worth exposing, even though Docker can only
  approximate it.

### Still open (this adapter is an early, Alpha proof, not production-ready)

- [x] ~~Namespace-collision refusal message names no remedy~~ resolved
      (2026-07-17, `docs/remediation/F-009`): `EnsureNetwork`'s unmanaged-
      namespace refusal now names `spec.runtime.network` as the fix and
      notes that colliding with a cluster's pre-existing system namespaces
      (`default`, `kube-system`, ...) is expected, not a bug. Verified live
      against a real cluster.
- [ ] **External reachability.** A container's Service is ClusterIP-only;
      nothing outside the cluster network can reach it, including
      `platformctl` itself when run from outside the cluster — verified
      live: applying the redpanda Provider succeeded (Deployment healthy in
      ~2.5s), but the dependent EventStream's own topic-management call
      failed with `dial tcp 127.0.0.1:19093: connect: connection refused`,
      because that call runs from the CLI process, not from in-cluster. This
      is the Kubernetes-adapter instance of Gate 1.1's "host bind address
      and published-port inspection" item, and is the single biggest
      remaining gap before this adapter is useful for anything beyond a
      Provider with no CLI-side control-plane calls. Closing it needs a
      deliberate design choice: NodePort exposure, `kubectl port-forward`-
      style tunneling built into the adapter (client-go's
      `tools/portforward` package), or documenting that `platformctl`
      targeting Kubernetes must run in-cluster.
  - [ ] `ContainerState`/`Inspect` do not yet report actual bind
        addresses for the Kubernetes adapter either (same gap as Docker's
        Gate 1.1 item).
- [ ] No conformance/integration coverage yet for volumes actually
      persisting data across a Deployment update (PVC reuse is exercised by
      idempotency tests, not by writing-then-reading real data).
- [ ] No RBAC/ServiceAccount design — the adapter uses whatever permissions
      the ambient kubeconfig grants; a production posture needs a documented
      minimal ClusterRole.
- [ ] No coverage of multi-replica scenarios, PodDisruptionBudgets, or
      anti-affinity — this adapter targets "one Pod per managed container,"
      matching Docker's model, deliberately not more.
- [ ] Terraform/external adapters remain untouched — this work only
      addresses the Kubernetes half of the goal. `registry.PlannedRuntimes`
      still rejects `external`/`terraform` construction.
- [ ] `docs/planning/04-roadmap-and-feature-gates.md` Phase 7 status and
      `schemas/v1alpha1/provider.json`'s `runtime.type` description need
      updating now that `kubernetes` is a real (Alpha) adapter, not merely
      schema-accepted-for-forward-compatibility.

## Stage Gates

Use these gates to split future work. A later gate should not start until the
earlier gate's acceptance criteria are either complete or explicitly waived in
the tracking issue.

### Gate 0: Foundation Correctness

Required before broader contribution or more provider work.

**Status (2026-07-16): implementation complete for all six; see Gate 0
Details below for the specific test-coverage items still open per area.**
Checked here means the mechanism is implemented and at least indirectly
tested; it does not mean every edge case in that area's detail section has
dedicated coverage.

- [x] Canonical resource identity policy is implemented and documented
      (namespaced `resource.Key`, DNS-label schema pattern, escaped
      structured state keys, project-scoped Docker labels — see 0.1).
- [x] External lifecycle resources cannot be mutated accidentally through any
      provider path (`ExternalConfigurer` contract enforced centrally in the
      engine — see 0.3).
- [x] Resource removal and rename behavior is explicit, tested, and safe
      (authoritative apply-delete + legacy-orphan refusal — see 0.4; rename
      and provider-type-change as distinct scenarios still need dedicated
      tests).
- [x] Machine-readable command output is valid JSON/YAML for every command
      (structured payload always to stdout, prose to stderr — see 0.5).
      **Audit correction (2026-07-17, `docs/remediation/F-001`):** this
      checkbox was previously unsupported by the code — `graph -o json`,
      `validate -o json`, and `inventory --for -o json` all emitted
      non-JSON prose to stdout despite the claim. Fixed; all three now
      verified to emit exactly one parseable document. A generic
      per-command × per-path harness remains open (0.5).
- [x] Mermaid/DOT renderers are collision-resistant and escaping-safe
      (hash-based ids, separated labels — see 0.6; adversarial-input golden
      tests still open).
- [x] Docker bind addresses and object ownership checks are safe by default
      (127.0.0.1 default, managed-by label refusal — see 0.7; actual
      published-port inspection still open, tracked with Gate 1.1).

### Gate 1: Docker Production Runtime

Required before positioning the Docker runtime as production-grade.

**Status (2026-07-16): all four acceptance criteria complete** (close-out
pass, incremental commits; per-area detail and residual items in 1.1–1.4
below — the notable explicit deferrals are registry auth for private
images, and the 1.3/1.4 operator tooling dispositioned past this gate).

- [x] Runtime specs cover restart policy, bind address, resource limits,
      security context, network aliases, logs, image pull policy, and digest
      pinning (see 1.1 — every named item done; registry auth for private
      registries is the one explicit deferral in this area).
- [x] Docker adapter refuses or adopts pre-existing networks and volumes
      consistently with the container ownership policy (see 0.7 — refuses,
      does not yet adopt; no adopt flow exists for networks/volumes).
- [x] Endpoint discovery reports actual published ports and bind addresses,
      not only deterministic intent — `ContainerState.Ports` (observed, from
      inspect) + `HostAddr()`; all nine endpoint-publishing providers build
      their inventory Host from the observed binding, and report
      "(in-network only)" honestly on runtimes without host publishing.
- [x] Secret material is not exposed through inspectable container
      environment variables where a runtime-supported safer path exists —
      postgres, mysql/mariadb, and minio bootstrap passwords now ride
      `ContainerSpec.Files` + the images' native `*_FILE` env convention;
      rotation recovery reads the file back via `ContainerRuntime.ReadFile`
      (env fallback for containers created before the change). Verified by
      the full CDC, sink, and lakehouse (rotation) integration suites.
      Out of scope for this checkbox (no runtime-supported safer path /
      different channel): Kafka Connect connector credentials travel in
      connector REST configs (Gate 2.2), Marquez's fixed internal Postgres
      credentials (Gate 2.5).

### Gate 2: Lakehouse And Pipeline Completeness

Required before claiming full data-engineer lakehouse/pipeline coverage.

**Status (2026-07-16): all four acceptance criteria complete** (close-out
pass, incremental commits; each area's residual items are explicitly
deferred with reasons in 2.1–2.5 — the notable ones: schema-registry design
before Parquet/Avro production claims, tunnel/TLS-termination providers on
the designed Connection seam, image digests, and out-of-band config-change
integration tests).

- [x] Catalog, warehouse, object store, and table format resources form a
      usable contract for Spark/Trino/Dagster/dbt-style tooling —
      `inventory --for <tool>` renders paste-ready config from observed
      endpoints (catalog REST URI + branch, S3, databases, kafka); Iceberg
      tables are declared external-tool-produced (decision recorded in
      2.3); json is the supported production sink format until a schema
      registry ships (decision recorded, tracked).
- [x] CDC, sink, ingest, and database-sink pairings either have real
      providers or are clearly hidden from production docs — cdc (debezium)
      and sink→Dataset (s3sink) are real; sink→Source and ingest are now
      explicitly documented in docs/planning/03 §7.2 as capability seams
      with **no shipped provider** (validate fails with the standard
      capability error; they were never silently pretend-available).
- [x] External ingress and egress are represented through first-class
      Connections, tunnels, TLS/auth, and reachability policies — the
      Connection seam is the recorded design (docs/design/002): managed =
      platform-owned entrypoint, external = declared egress, tunnels chain
      additively; endpoints carry explicit TLS labeling; host-audience
      reachability is probed end-to-end (engine + through-forwarder).
      Tunnel/TLS-termination *providers* are deferred provider work on the
      existing seam (2.4).
- [x] Drift detection verifies full provider configuration, not only
      liveness — per-provider equivalence table in 2.1; connector probes
      diff live config against manifest-derived config; database probes
      check CDC-readiness settings and credential validity; every
      deliberately unmanaged field carries its reason.

### Gate 3: Public API And Contribution Readiness

Required before inviting third-party provider/runtime contribution.

- [ ] Provider SDK contracts have conformance tests beyond the current fake and
      Docker runtime tests.
- [ ] JSON schemas, generated docs, README, planning docs, and examples are
      synchronized.
- [ ] CI covers unit, race, schema/render fuzz or golden tests, machine-output
      validation, Docker integration, and representative negative cases.
- [ ] Release artifacts pin dependency/image versions or document their
      mutability and support window.

## Gate 0 Details

**Status update (2026-07-16):** the bulk of Gate 0's *implementation* landed
in commit `a2c1484` ("harden Gate 0 reconciliation contracts"), but that
commit did not update this checklist. The subsections below have been
corrected to match the current code, verified by direct inspection
(user-confirmed edit). Any remaining `[ ]` items are genuine gaps — mostly
missing dedicated test coverage for mechanics that already exist, plus the
still-open Gate 1+ work.

### 0.1 Canonical Names, Namespaces, And Stable IDs

Resolved:

- Namespaced-resources policy chosen: `resource.Key` is
  `Namespace/Kind/Name`; `schemas/v1alpha1/meta.json` allows
  `metadata.namespace` (DNS-label pattern, defaults to `default`).
- `metadata.name`, `metadata.namespace`, and `nameRef.name`/`.namespace` all
  carry a DNS-label schema pattern (`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`,
  maxLength 63), enforced in both the JSON Schema and
  `resource.ValidateDNSLabel` (Go-side, so `import`/CLI-constructed
  envelopes are covered too, not just manifest-loaded ones).
- State serialization is no longer delimiter-fragile:
  `state.KeyString`/`state.ParseKey` URL-path-escape each of
  namespace/kind/name before joining on `/`, with a versioned migration path
  (`state.CurrentVersion = 2`, `parseV1Key` fallback) in
  `internal/ports/state/state.go`.
- Docker labels carry project scope: `runtime.ManagedLabels` sets
  `io.datascape.{managed-by,generation,namespace,kind,name,project}` on every
  created object (`internal/ports/runtime/runtime.go`).

Still open:

- [ ] Separate display-label/title concept distinct from `metadata.name`
      (not yet needed — no reported case of the DNS-label policy being too
      restrictive for a real manifest; revisit if one shows up).
- [ ] Add dedicated unit tests for name-policy edge cases: invalid names,
      slash/dot/colon names, long (>63 char) names, Unicode, and
      cross-namespace vs. same-namespace collisions. There is no
      `internal/domain/resource/resource_test.go` today — `ValidateDNSLabel`
      and `Envelope.Validate` are only exercised indirectly through
      higher-level tests.

Design notes:

- Rejecting unsafe names is safer than trying to make every downstream system
  accept arbitrary strings.
- If arbitrary display names are needed, put them in annotations or a `title`
  field and keep `metadata.name` operational.
- Docker labels should carry at least project id, namespace, kind, name,
  generation/spec hash, and managed-by.

### 0.2 Ambiguous Bare-Name References

Current risk:

Resolved:

- `graph.Build` rejects ambiguous refs for every ref field (kind-filtered
  candidate matching, error names the field and match count) — this now also
  covers `metadata.observers` (`resource.Envelope.Validate` DNS-validates
  observer names; `graph.Build` resolves them through the same ambiguity path
  as other refs).
- `compatibility.Check`'s ref index (`byName map[string][]resource.Envelope`
  in `internal/application/compatibility/compatibility.go`) is a slice
  keyed by namespace+name, not an overwriting map; `idx.resolve` returns an
  explicit `ambiguous` bool checked at every call site (sourceRef, targetRef,
  connectionRef).
- `archview.Build` no longer resolves by bare name at all — it consumes
  `graph.Build`'s already-validated, already-disambiguated key set
  (`resolveByRef` in `internal/application/archview/archview.go` looks up by
  `resource.Key`, not by name), so ambiguity is rejected once, upstream, by
  the same command path (`loadAndValidate`) every subcommand shares.

Still open:

- [ ] Add explicit negative-path unit tests asserting the ambiguity error for
      cross-kind duplicate names on each ref field (sourceRef, targetRef,
      connectionRef, observers) — the mechanism is shared and exercised
      indirectly by existing tests, but there's no test enumerating all ref
      fields the way this item originally called for.

### 0.3 External Lifecycle Must Be Enforced On Apply

Current risk:

Resolved:

- `reconciler.ExternalConfigurer` is a distinct interface from `Reconcile`
  (`internal/ports/reconciler`); the engine (`internal/application/engine/
  engine.go`) checks `isExternal(env)` and requires the provider to implement
  `ExternalConfigurer` — `fmt.Errorf("... is External with providerRef, but
  provider type %q does not implement ExternalConfigurer")` if not. A normal
  `Reconcile` is never called for an External resource with a providerRef;
  this is enforced centrally in the engine, not by provider convention.
  Covered by `TestExternalProviderRefUsesConfigureExternal` in
  `engine_test.go`.
- External-with-no-providerRef still takes the connection-resolvable-only
  path (`reconcileExternal`), unchanged from before.

Still open:

- [ ] Confirm every resource kind that plausibly supports `external: true`
      either implements `ExternalConfigurer` on its provider(s) or is
      documented as not supporting the external lifecycle. Not audited
      kind-by-kind.
- [ ] `docs/planning/03-resource-model-reference.md` should state the
      `ExternalConfigurer` contract explicitly wherever `external: true` is
      documented per-kind (currently the contract lives only in code +
      `docs/planning/02-architecture.md`).

### 0.4 Removed Or Renamed Resources Are Orphaned

Current risk:

Resolved:

- `plan.computeApplyDeletes` (`internal/application/plan/plan.go`) diffs
  state against desired and emits `ActionDelete` entries (authoritative,
  in-apply deletion — the "delete belongs in normal apply" question was
  answered in favor of Terraform-style authoritative apply, not a separate
  prune command) using `state.ResourceState.LastApplied` +
  `.Dependencies`/`.Provider` to reconstruct enough identity to destroy
  without the manifest present.
- Legacy pre-rename state (no `LastApplied` recorded) surfaces as
  `ActionOrphanUnknown` rather than being silently skipped or guessed at;
  `Engine` refuses to act on it until state is repaired
  (`TestApplyRefusesLegacyOrphanUnknown`).
- Covered by `TestComputePlansAuthoritativeDeletes` and
  `TestComputeReportsLegacyOrphanUnknown` (`plan_test.go`).

- [x] Dedicated tests for a **rename** (old-name delete + new-name create
      in the same apply — `TestComputePlansRenameAsDeleteAndCreate`) and a
      **provider-type change** (same resource name, different
      `providerRef` target — `TestComputePlansProviderTypeChangeAsUpdate`)
      as scenarios distinct from a plain removal, in `plan_test.go`.

Still open:

- [ ] Confirm data-bearing kinds (Dataset, Source, Catalog) get the "stronger
      protection than ephemeral connectors" the design note called for, or
      that authoritative apply-delete is an accepted risk for them too
      (no `--protect`/`prevent-destroy`-style opt-out exists today).

### 0.5 Machine-Readable Output Contract

Current risk:

Resolved (`cmd/platformctl/root.go`, `cmd/platformctl/toolconfig.go`):

- `isStructured(output)`/`humanWriter(cmd, output)` route every prompt and
  human summary line to stderr when `-o json|yaml` is active; stdout gets
  exactly one `cliutil.WriteOutput` call per exit path — success, no-op,
  drift-heal, cancelled — via typed payloads (`applyOutput`, `destroyOutput`,
  `driftOutput`, `inventoryOutput`, `importOutput`).
- `inventory -o json` with zero endpoints returns `{"endpoints": []}` (via
  `inventoryOutput{Endpoints: data}` with `data` typed as a slice, not a bare
  `nil`/text branch).
- `graph` takes its own `--format tree|dot|mermaid|json` flag, independent of
  the root `-o table|json|yaml`, whose help text is now accurate — **and (audit
  fix, 2026-07-17, `docs/remediation/F-001`) `-o json|yaml` now overrides
  `--format` and emits the same node/edge document `--format json` produces,
  instead of ignoring `-o` and writing tree text regardless.** A stderr
  warning fires when both an explicit non-default `--format` and a structured
  `-o` are given, since `-o` wins.
- **`validate -o json|yaml`** (audit fix, F-001) emits `{"valid": true,
  "resources": N}` instead of the prose line unconditionally; the prose
  stays the default-output behavior.
- **`inventory --for <tool> -o json|yaml`** (audit fix, F-001) emits
  `{"tool": "<tool>", "config": "<rendered snippet>"}` instead of writing
  the raw prose snippet regardless of `-o`.
- [x] Command-level tests that parse stdout as JSON/YAML now exist for the
      three paths above (`cmd/platformctl/output_contract_test.go`:
      `TestGraphStructuredOutput`, `TestValidateStructuredOutput`,
      `TestInventoryForStructuredOutput`, plus default-output regression
      guards for each). Full apply/destroy/drift path × exit-path coverage
      (success/no-op/drifted/cancelled) remains a generic-harness gap — see
      below.

Still open:

- [ ] Add a generic command-level harness that parses stdout as JSON/YAML
      for every command × every exit path (success, no-op, changed,
      drifted, empty, cancelled) — `apply`/`destroy`/`drift`/`status` are
      verified correct by inspection and by the three commands' dedicated
      tests above, but no single harness sweeps all of them systematically.

### 0.6 Mermaid, DOT, And Architecture Rendering Escaping

Current risk:

Resolved (`internal/application/archview/render.go`):

- `graphID(k)` generates node/edge ids from a **hex** encoding of the full
  `resource.Key` string (namespace+kind+name) — collision-resistant, and
  decoupled from the human-readable label. This was originally shipped using
  `base64.RawURLEncoding`, which was found and fixed in this pass: the
  base64url alphabet legally includes `-`, and DOT ids are emitted
  **unquoted**, where a bare `-` is only valid as a numeral's sign per the
  DOT grammar — any resource name containing a hyphen (a normal, schema-legal
  DNS-label character, e.g. `orders-db`) could have produced a
  Graphviz-invalid `.dot` file. Hex (`[0-9a-f]`) is safe unquoted in both DOT
  and Mermaid and is equally collision-resistant. Locked in by
  `TestGraphIDIsSafeUnquotedIdentifier` and
  `TestRenderDOTQuotesAndEscapesAdversarialLabels`
  (`render_escaping_test.go`).
- `mermaidEscape` replaces backslash, quote, CR/LF, and pipe (the characters
  that break Mermaid node/edge syntax); labels and ids are no longer the same
  string (`nodeLabel` is display-only, `graphID` is the wire id).
- `archview.resolveByName`-style nondeterministic lookup no longer exists —
  see 0.2 above; archview resolves through already-disambiguated keys.
- [x] Golden/adversarial-input tests added
      (`internal/application/archview/render_escaping_test.go`): a
      Connection target containing quotes, backticks, braces, angle brackets,
      and pipes renders without corrupting DOT ids, without an unescaped
      embedded quote in a DOT label, without a raw pipe breaking a Mermaid
      edge label, and the JSON output still parses.

Still open:

- [ ] No lightweight Mermaid syntax validator is wired into tests (optional,
      lower priority now that labels/ids are structurally separated and
      covered by the adversarial-input tests above).

### 0.7 Docker Bind Address And Ownership Safety

Current risk:

Resolved (`internal/ports/runtime/runtime.go`,
`internal/adapters/runtime/docker/docker.go`):

- `runtime.PortBinding.HostIP` exists; the Docker adapter defaults it to
  `127.0.0.1` whenever a provider leaves it empty
  (`TestPortMapsDefaultHostIPLocalhost`), and honors an explicit override
  (`TestPortMapsHonorsExplicitHostIP`) for the rare case a provider needs
  wider exposure.
- `EnsureNetwork`/`EnsureVolume` both check
  `Labels[runtime.LabelManagedBy] != runtime.ManagedByValue` on any
  same-name existing object and refuse to adopt it silently (no
  import/adopt flow exists yet for networks/volumes specifically — refusal
  is the current behavior, which is the safe default this item asked for).

- [x] Integration tests against a real Docker daemon
      (`internal/adapters/runtime/docker/docker_integration_test.go`):
      `TestEnsureNetworkRefusesUnmanagedExisting` and
      `TestEnsureVolumeRefusesUnmanagedExisting` create a same-name
      network/volume out-of-band (no ownership label) via the raw client and
      assert `EnsureNetwork`/`EnsureVolume` refuse to reuse it.
      `TestPublishedPortBindsToLoopbackByDefault` creates a real container
      with a `PortBinding{HostPort: ...}` (no `HostIP` set) and asserts via
      `ContainerInspect` that Docker actually bound the published port to
      `127.0.0.1`, not `0.0.0.0`. Writing these surfaced that `RemoveNetwork`/
      `RemoveVolume` apply the *same* ownership guard on teardown, which is
      correct but means test cleanup for a deliberately-unmanaged object must
      go through the raw client, not `RemoveNetwork`/`RemoveVolume`.

Still open:

- [ ] Inventory/endpoint reporting still constructs the host address from
      the *configured* port (`hostport.Resolve` + a hardcoded `127.0.0.1`
      per provider), not from an actual Docker inspect of the published
      port. `runtime.ContainerState` has no `Ports`/bind-address field, so
      there's no way yet for a provider to report what Docker actually
      bound versus what was requested. This is the same gap as Gate 1.1's
      "host bind address and published-port inspection" — closing 1.1
      closes this too.

## Gate 1 Details

### 1.1 Expand The ContainerRuntime Contract

**Status update (2026-07-16):** the core production controls landed in
`internal/ports/runtime/runtime.go` (types: `RestartPolicy`, `Resources`,
`SecurityContext`, `LogConfig`, all added to `ContainerSpec`) and are wired
into `internal/adapters/runtime/docker/docker.go`'s `EnsureContainer`
(restart policy, CPU/memory limits+reservation, user/read-only-rootfs/
capabilities/security-opt, log driver+options) — verified against a real
Docker daemon (`conformance.go`'s new
`EnsureContainer_productionFields_idempotent` subtest, run via both the fake
and `docker_integration_test.go`). Field names deliberately follow whichever
vocabulary (Docker/Compose for restart policy, Kubernetes for security
context) is more portable, per the design note below, so a future Kubernetes
runtime adapter can consume the same `ContainerSpec` fields directly instead
of needing Docker-shaped ones translated.

Resolved:

- [x] Restart policy (`RestartPolicy{Mode, MaxRetries}`).
- [x] CPU and memory limits/reservation (`Resources{CPULimit,
      MemoryLimitBytes, MemoryReservationBytes}` — no CPU *reservation* field:
      Docker's CPU shares are a relative weight, not a portable absolute
      reservation, so it was deliberately left unmodeled rather than faked).
- [x] User, read-only root filesystem, capabilities, security options
      (`SecurityContext{User, ReadOnlyRootFS, CapAdd, CapDrop, SecurityOpt}`).
- [x] Log driver and options (`LogConfig{Driver, Options}`; rotation is a
      log-driver *option*, e.g. `json-file`'s `max-size`/`max-file`, so it
      rides on `Options` rather than needing a separate field).
- [x] Container log retrieval for diagnostics: `ContainerRuntime.Logs(ctx,
      name, tail)`, backed by the same `ContainerLogs` call the engine
      already used internally for failure messages (`tailLogs`, now a thin
      wrapper over the shared `fetchLogs` helper).

Resolved in the Gate 1 close-out pass (2026-07-16, incremental commits):

- [x] Network aliases (`ContainerSpec.Aliases`): Docker per-network endpoint
      aliases; Kubernetes one ClusterIP Service per alias selecting the same
      pod. Docker integration test proves a peer container resolves the
      alias (`TestNetworkAliasResolvesInNetwork`).
- [x] Host bind address and published-port *inspection*:
      `ContainerState.Ports` reports observed bindings from Docker inspect
      (`portsFromInspect`); `ContainerState.HostAddr(containerPort)` is the
      provider-facing accessor. Conformance subtest
      `Inspect_reports_observed_ports` runs on all three adapters.
- [x] File mounts (`ContainerSpec.Files` + `ContainerRuntime.ReadFile`):
      literal file content placed before PID 1 runs (Docker:
      CopyToContainer pre-start; Kubernetes: per-container Secret with
      subPath mounts). Conformance proves process-visibility, ReadFile
      round-trip, and that content never appears in Inspect env.
- [x] Image pull policy (`ContainerSpec.PullPolicy`: if-not-present default
      / always / never) and digest pinning (any `repo@sha256:...` ref works
      through the existing inspect/pull path; now the documented way to pin).
      Kubernetes maps the default to IfNotPresent explicitly so `:latest`
      behaves identically on both runtimes. `PullNever` fails fast
      (`TestPullPolicyNeverFailsFastOnAbsentImage`).

Still open (deliberately deferred):

- [ ] Registry auth for private images — `ImagePull` sends no RegistryAuth
      header; only daemon-level/ambient credentials work today.
- [ ] Host-path mounts — `FileMount` covers literal content; mounting an
      arbitrary host directory remains unsupported (deliberate: host paths
      are not portable across runtimes).

Design notes:

- Keep the port provider-friendly, but do not pretend Docker-specific
  operational facts do not exist. A Docker runtime can expose Docker-grade
  controls while higher-level resource kinds stay provider-agnostic.

### 1.2 Runtime Drift Equivalence

**Status update (2026-07-16):** the specific complaint about the fake
runtime is resolved; the broader "runtime-level desired-vs-actual
equivalence" design question is still open.

Resolved:

- [x] The fake runtime's `containerSpecEqual` no longer hand-picks fields —
      it's `reflect.DeepEqual(a, b)` over the whole `ContainerSpec`
      (`internal/adapters/runtime/fake/fake.go`), so command, ports,
      volumes, health checks, restart policy, resources, and security
      context are all covered automatically as the struct grows, rather
      than needing a matching update here every time a field is added.
- [x] Conformance tests for command, env, ports, networks, labels, health
      checks, restart policy, and resource limits — the pre-existing
      `EnsureContainer_idempotent` plus the new
      `EnsureContainer_productionFields_idempotent` subtest, which also
      asserts changing `RestartPolicy` alone (not name/image/labels/env/
      networks) is detected as drift, not silently ignored.
- The real Docker adapter's mechanism (`specHash` = `sha256(json.Marshal(
  ContainerSpec))`, stored as a label, compared on next `EnsureContainer`)
  already covered every field structurally without needing a matching
  update — new `ContainerSpec` fields are automatically part of the hash.

Resolved in the Gate 1 close-out pass (2026-07-16):

- [x] `ContainerState` expanded with observed `Ports` (bind address +
      host/container port + protocol from Docker inspect), with conformance
      coverage on all three adapters (`Inspect_reports_observed_ports`).

Still open:

- [ ] Provider probes (per-provider `Probe` methods) still mostly check
      liveness, not full desired-configuration equivalence — this is Gate
      2.1's concern, not Gate 1's, but the two are related.
- [ ] A full `ProbeContainerSpec` capability (field-by-field desired vs.
      observed diffing beyond ports/env/labels) remains future work; the
      spec-hash label plus observed ports covers the practical drift cases
      today.

Design notes:

- A provider should not have to hand-roll Docker inspect diffing.
- Spec-hash labels are useful, but they do not catch out-of-band mutation if
  labels remain stale.

### 1.3 Garbage Collection And Orphan Inspection

**Disposition (2026-07-16 Gate 1 close-out): explicitly deferred past the
Gate 1 stage-gate**, then resolved as its own task (2026-07-18,
`docs/planning/08` A2). This was inspection/operator tooling, not a
correctness or safety gap: ownership labels are on every created object,
unlabeled objects are never touched, unmanaged same-name objects are
refused, and authoritative apply already deletes state-tracked orphans.
Nothing here could damage a shared daemon by its absence — it only made
cleanup of *pre-crash* stale objects more manual.

Resolved:

- [x] Managed network and volume listing:
      `ContainerRuntime.ListManagedNetworks`/`ListManagedVolumes`
      (`internal/ports/runtime/runtime.go`), implemented by all three
      adapters (docker, kubernetes, fake) and covered by a new conformance
      subtest (`ListManagedNetworks_and_Volumes_only_labeled`).
- [x] `platformctl gc plan` (read-only: every labeled container/network/
      volume whose namespace/kind/name has no matching state entry,
      grouped and reported as `gcOrphan{Object, Namespace, Kind, Name}`)
      and `platformctl gc apply` (removes exactly that list; refuses
      without `--yes-i-understand-this-is-destructive`, mirroring
      `destroy`'s NFR-3 pattern) — `cmd/platformctl/gc.go`.
- [x] Non-destructive (`gc plan`) and destructive (`gc apply`) flows with
      `-o table|json|yaml` output, verified live against a real Docker
      daemon: `TestGCPlanAndApply` (`cmd/platformctl/gc_integration_test.go`)
      creates a labeled container+network+volume out-of-band (no state
      entry), asserts `gc plan` lists exactly them, `gc apply` without the
      flag refuses and removes nothing, `gc apply` with the flag removes
      exactly them.

Design notes:

- Open-source users will run this on shared Docker daemons. They need to know
  exactly what will be touched.

### 1.4 State Durability And Recoverability

Resolved in the Gate 1 close-out pass (2026-07-16):

- [x] Fsync the state directory after rename (best-effort where the
      platform allows opening directories) — `localfile.Save`.

Resolved (2026-07-18, `docs/planning/08` A3):

- [x] Migration scaffolding formalized: `internal/ports/state/state.go`'s
      `migrations` is now an ordered, named chain
      (`[]migration{FromVersion, Name, Apply}`) applied in
      `State.Normalize`, replacing the inline `version < 2` branch — the
      v1→v2 key migration is its first (and so far only) entry.
      `TestMigrationChainHasNoGaps` (`internal/ports/state/migration_test.go`)
      is the template/contiguity guard a future migration must satisfy:
      append one entry, never touch the decode loop.
- [x] `platformctl state inspect` (dump normalized state, read-only),
      `state doctor` (reports: stale on-disk format version, legacy orphan
      entries with no last-applied manifest, corrupt entries whose state
      key disagrees with their own manifest's key, and Provider entries
      whose backing container the runtime reports gone — exits 1 when
      anything is found), and `state repair` (persists a migrated format
      and drops confirmed-gone Provider entries, with confirmation unless
      `--yes`; never touches legacy-orphan or corrupt entries, which have
      no safe automatic fix; a no-op — no write — on healthy state) —
      `cmd/platformctl/state.go`. Covered by a doctor/repair round-trip
      fixture test exercising every defect class in one pass
      (`cmd/platformctl/state_test.go`) and registered in the output-contract
      harness (A7).

**Disposition for the remainder: tracked as `docs/planning/08` A4.**

- [ ] Decide whether remote/shared state is in scope for Docker production
      (`docs/planning/08` A4 — design note first, then the S3-backed
      implementation).

Design notes:

- Local file state is acceptable for a Docker-first runtime, but recovery
  tooling matters because data systems are long-lived.

## Gate 2 Details

### 2.1 Provider Drift Must Check Desired Configuration

**Status update (2026-07-16 Gate 2 close-out):** probes upgraded across the
board; the equivalence table below is the authoritative record of what each
probe checks and what is deliberately not drift-managed (with reasons —
per this section's own required-work wording, a field may be intentionally
excluded if the exclusion is explained).

Per-provider drift equivalence table:

| Provider | Resource | Checked by Probe | Deliberately not drift-managed |
|---|---|---|---|
| redpanda | Provider | container found + healthy | — |
| redpanda | EventStream | topic exists; partition count; `retention.ms` vs declared (when declared; an undeclared retention is not managed) | other topic configs (not declared in the manifest model) |
| postgres | Provider | container found + healthy | — |
| postgres | Source | database exists; `wal_level=logical` (the CDC-readiness this provider declares); replication credentials still authenticate (when `replicationSecretRef` declared) | publication membership, grants beyond authentication (provisioned superuser-side; a grants-equivalence check needs a declared grants model first) |
| mysql/mariadb | Provider | container found + healthy | — |
| mysql/mariadb | Source | database exists; `binlog_format=ROW`; replication credentials still authenticate (when declared) | binlog retention, grant list (same reason as postgres) |
| s3/minio | Provider | container found + healthy | — |
| s3/minio | Dataset | bucket exists; prefix listable with declared credentials | versioning/lifecycle policy and format contract (not part of the Dataset model; format is enforced at the sink connector) |
| debezium | Binding | connector RUNNING **and** live config == manifest-derived config (all keys the provider sets; drifted key *names* reported, values never leaked; `openlineage.*` keys excluded — engine-managed post-registration) | Connect-added default keys beyond the desired set |
| s3sink | Binding | connector RUNNING **and** live config == manifest-derived config (same contract as debezium) | same |
| proxy | Connection | forwarder container healthy **and** upstream answers through it (dial the published port; socat closes the accepted session immediately on upstream connect failure, so an immediate EOF = upstream unreachable, a held-open session = alive) | — |
| nessie | Catalog | REST API answers; declared branch exists | warehouse/object-store wiring (not yet part of the Catalog model — tracked in 2.3) |
| SecretReference | — | resolvable; one-way fingerprint vs last applied | — |
| external (no provider) | any | Connection resolvable + TCP-reachable | — |

Resolved:

- [x] Define per-provider drift equivalence tables (above).
- [x] Probe full desired configuration or explain why a field is
      intentionally not drift-managed (above; every exclusion carries its
      reason).
- [x] Detect SecretReference material drift via one-way fingerprints and
      reconcile dependents when the resolved value changes.
- [x] Support admin-password rotation for Docker-managed Postgres and
      MySQL/MariaDB when either the new credential already works or the
      previous managed-container bootstrap env is still available.
- [x] Store observed provider facts in status for `status -o json`: probe
      results carry `ProviderState`, merged by the engine under
      `providerState.observed` (never clobbering the reconcile-written
      providerState); condition messages carry observed-vs-desired detail
      (e.g. `wal_level is "replica", want "logical"`); drifted connector
      config reports key names only — values may carry credentials.

Still open:

- [ ] Add integration tests for manual out-of-band config changes (e.g.
      ALTER a topic's retention.ms out-of-band, assert `drift` reports
      RetentionMismatch). The mechanisms are unit-covered and the existing
      chaos suite covers out-of-band *liveness* changes; config-level
      out-of-band coverage is additive test work.

Design notes:

- "Running" is not enough. A connector can be RUNNING with the wrong topic,
  table filter, credentials, sink bucket, or lineage endpoint.

### 2.2 Provider-Specific Bugs To Fix

**Status update (2026-07-17, remediation audit):** all seven items were
fixed in the Gate 2 close-out (commit `09e1b61`), but this checklist was
never ticked at the time — the Gate 2 stage-gate summary referenced the
work without updating the detail section it summarized. Re-verified against
the current code below (audit finding, cross-linked from
`docs/remediation/F-006`).

Resolved:

- [x] URL-escape Kafka Connect connector names in REST paths —
      `kafkaconnect.connectorPath` (`internal/adapters/kafkaconnect/connect.go`),
      used by all five REST calls.
- [x] Escape Kafka topic names when generating `topics.regex` for the S3
      sink — `regexp.QuoteMeta(b.SourceRef)`
      (`internal/adapters/providers/s3sink/s3sink.go`).
- [x] Build Postgres connection strings with URL-safe credential handling —
      `net/url`-based `connString`
      (`internal/adapters/providers/postgres/sql.go`), round-trip tested
      against the real `pgx` parser with `@:/#`-and-quote-laden credentials.
- [x] Build MySQL DSNs through `mysql.Config` rather than string
      formatting — `godriver.NewConfig()` + `FormatDSN()`
      (`internal/adapters/providers/mysql/sql.go`), same round-trip test
      treatment against the real driver.
- [x] Generate unique MySQL/MariaDB Debezium `database.server.id` values per
      connector — FNV-1a over the connector name (`serverID`,
      `internal/adapters/providers/debezium/debezium.go`). **Upgrade note:**
      this is a behavioral migration — pre-existing connectors report a
      one-time `ConnectorConfigDrift` until the next `apply`; see
      `docs/upgrade-notes.md`.
- [x] Add validation for provider-specific option blocks such as Debezium
      table lists, snapshot mode, external database host/port overrides, and
      S3 sink endpoint — `reconciler.BindingOptionsValidator`, implemented
      by both `debezium` and `s3sink`.
- [x] Make destroy behavior explicit for data-bearing subresources such as
      Datasets and Sources — `spec.deletionPolicy: retain|delete`
      (`internal/domain/dataset`, `internal/domain/source`, adopted by the
      `s3`/`postgres`/`mysql` providers' `Destroy`). Schema + `docs/planning/03`
      updated in the same commit per the schema-change rule.

Design notes:

- Secrets commonly contain `@`, `:`, `/`, `#`, spaces, quotes, and backslashes.
  Credential handling must not rely on lucky demo passwords.

### 2.3 Lakehouse Contract Completeness

**Status update (2026-07-16 Gate 2 close-out):**

Resolved:

- [x] Decide whether Iceberg tables are resources or produced by external
      tools: **produced by external tools** (Spark/Trino/dbt against the
      catalog's Iceberg REST endpoint). platformctl provisions the catalog,
      warehouse store, and branches; table DDL belongs to the tools that own
      table semantics. Making tables resources would re-implement each
      engine's DDL surface inside providers — overfitting the model to one
      table format's lifecycle.
- [x] Generated config views: `platformctl inventory --for
      spark|trino|dbt|psql|s3|kafka` renders paste-ready snippets from the
      recorded (observed) endpoints — catalog REST URI + branch, S3
      endpoint, database addresses. Secret values are never rendered;
      snippets name the SecretReference and env keys. Flink, Dagster, and
      Metabase/Superset consume the same postgres/s3/kafka facts and can be
      added as renderers without model changes.
- [x] TLS/auth and branch in inventory JSON: `insecure` on every endpoint
      (see 2.5); the Catalog publishes its own endpoints with
      `defaultBranch`/`icebergUri` in providerState. Region is a per-sink
      connector setting (aws.s3.region), not yet a modeled fact.
- Parquet/Avro **decision recorded**: not production-supported until
      schema-carrying converters ship — the pipeline runs schemaless JSON
      converters, so `json` is the supported production sink format (the
      acceptance example deliberately uses json; s3sink's parquet listing
      requires schema-carrying records at runtime, documented in its
      SupportedSinkFormats comment). A schema-registry design is the
      prerequisite, tracked below.

Still open (deferred with reasons):

- [ ] Schema registry / schema-carrying converter support (the blocker for
      Parquet/Avro production claims) — a real design chunk: registry
      provider kind or converter config on Bindings, plus image
      implications for the Connect workers.
- [ ] First-class Catalog↔warehouse(Dataset) modeling. The seam exists
      today without core-schema change (engine blocks are open:
      `spec.nessie` can carry warehouse config), but a first-class
      `warehouseRef` needs the dependency graph to learn about refs inside
      engine blocks (ordering + validation), which is deliberately not
      being bolted on ad hoc.

Design notes:

- Inventory should answer "what exact config do I paste into my tool?"
  without requiring users to reverse-engineer provider conventions.

### 2.4 Ingress, Egress, And External Reachability

**Status update (2026-07-16 Gate 2 close-out):**

Resolved:

- [x] First-class ingress/egress design around Connection: recorded in
      docs/design/002 (+ addendum) — Connection is *the* seam; a managed
      Connection is the platform-owned entrypoint, an external one the
      declared egress; tunnel providers chain a managed Connection's egress
      additively (no schema change). This close-out affirms that design
      rather than inventing a parallel one.
- [x] Network aliases (Gate 1.1): stable internal names decoupled from
      container names on both runtimes.
- [x] TLS metadata on endpoints: `Endpoint.Insecure`, set by every provider,
      rendered by inventory (see 2.5). Connection TLS termination remains a
      future provider capability, not metadata absence.
- [x] Host-audience reachability probes: the engine TCP-probes external
      Connections (`ExternalEndpointUnreachable`), and the proxy's
      Connection probe now dials *through* the forwarder to verify the
      upstream (see 2.1).

Still open (deferred with reasons):

- [ ] Tunnel-capable providers (WireGuard/SSH/SOCKS) for VPC reach — pure
      provider work on the designed Connection seam (checkpoint backlog
      item); deferred because it needs real target infrastructure to test
      honestly, not because the model lacks the seam.
- [ ] TLS termination / HTTP routing / auth proxy — same seam, same
      reasoning: additive Connection-provider capabilities.
- [ ] In-network-audience reachability probes: verifying that container A
      can reach B requires an in-network vantage point (a probe container
      or exec), a deliberate runtime capability addition rather than a
      host-side approximation that would report the wrong audience's truth.

Design notes:

- A data engineer needs stable internal and external addresses, but also
  controlled exposure. Reachability and exposure are different facts.

### 2.5 Production Security Baseline

**Status update (2026-07-16 Gate 2 close-out):**

Resolved:

- [x] File-mount support where images allow it (Gate 1 checkbox 4):
      postgres, mysql/mariadb, and minio bootstrap passwords ride
      `ContainerSpec.Files` + native `*_FILE` env; rotation recovery via
      `ReadFile`.
- [x] Secret-bearing configs are not persisted or logged — audited:
      state stores `lastApplied` manifests (secret *references* only, never
      values — the schema rejects inline values), provider `providerState`
      carries names/addresses/ids only, connector configs with credentials
      live solely in Connect's own storage, and drift reporting for
      connector config emits key *names* only (2.1). No code path writes a
      resolved secret value to state, logs, or command output.
- [x] Explicit "local only, insecure" labeling per endpoint:
      `Endpoint.Insecure` + the inventory SECURITY column ("plaintext
      (local only)"), set by all nine providers. TLS *support* remains
      future Connection-provider work (2.4).
- [x] Default images pinned by version (minio release tag, nessie 0.108.1,
      marquez 0.51.1, socat 1.8.0.3) across providers, examples, and
      testdata; postgres/mysql/mariadb were already version-pinned through
      the immutable versionprofile catalogs, which are the documented
      support windows for the database engines.
- [x] Marquez internal Postgres credentials: **fixed-by-image, documented as
      such** — marquez.dev.yml hardcodes user/password/dbname (only
      host/port are substitutable via env), and the metadata store is a
      dedicated, never-published, in-network-only container. Generating
      credentials the image cannot consume would be theater; revisit if the
      image gains credential env support.

Still open (deferred with reasons):

- [ ] Digests (not just version tags) for release-tested images in
      CI/examples — mechanical follow-up; needs a digest-refresh workflow so
      pins don't rot.
- [ ] TLS/auth *support* per provider endpoint — tracked with 2.4's
      Connection-provider capabilities.

Design notes:

- Docker-local does not automatically mean safe. Published ports and Docker
  inspect access are real exposure paths.

## Gate 3 Details

### 3.1 Schema And Provider Configuration Validation

Current gap:

- Provider `spec.configuration` is open-ended.
- Source and Catalog engine-specific blocks are open-ended objects.
- Binding `options` is open-ended.
- Many required provider options are checked by `SpecValidator`, but option
  validation is uneven and mostly not schema-generated.

Required work:

- [ ] Add provider-owned schema fragments or a typed validation registry.
- [ ] Validate engine blocks based on `spec.engine`.
- [ ] Validate Binding options based on provider type, mode, and endpoint
      kinds.
- [ ] Generate docs from those provider schemas.
- [ ] Add negative tests that `validate` catches every known apply-time
      misconfiguration class.

Design notes:

- Extensibility is useful, but production UX requires errors at validate time.

### 3.2 Test Coverage Gaps

**Status update (2026-07-17):** most items resolved across the Gate 0/1/2
close-out passes and the remediation audit (`docs/remediation/`); checked
against the actual test files below, not just prior claims.

Resolved:

- [x] Add tests for name policy and renderer escaping —
      `internal/domain/resource/resource_test.go`,
      `internal/application/archview/render_escaping_test.go`.
- [x] Add removed-resource/orphan tests —
      `TestComputePlansAuthoritativeDeletes`,
      `TestComputeReportsLegacyOrphanUnknown`,
      `TestComputePlansRenameAsDeleteAndCreate`,
      `TestComputePlansProviderTypeChangeAsUpdate`
      (`internal/application/plan/plan_test.go`).
- [x] Add unmanaged Docker network/volume collision tests —
      `TestEnsureNetworkRefusesUnmanagedExisting`,
      `TestEnsureVolumeRefusesUnmanagedExisting`
      (`internal/adapters/runtime/docker/docker_integration_test.go`, run
      live against a real daemon).
- [x] Add host bind-address tests — `TestPortMapsDefaultHostIPLocalhost`
      (`docker_test.go`), `TestPublishedPortBindsToLoopbackByDefault`
      (`docker_integration_test.go`, live).
- [x] Add lakehouse integration coverage for MySQL and Postgres admin-secret
      rotation through SecretReference changes.
- [x] Add tests for special-character secrets and URL/DSN escaping —
      `internal/adapters/providers/postgres/sql_test.go`,
      `internal/adapters/providers/mysql/sql_test.go` (round-trip through
      the real `pgx`/`go-sql-driver` parsers).
- [x] Expand fake runtime conformance to compare every spec field —
      `fake.containerSpecEqual` is `reflect.DeepEqual` over the whole
      `ContainerSpec` (`internal/adapters/runtime/fake/fake.go`).
- [x] Make `TestProbeTCPReachable` skip or self-describe when loopback
      listen is blocked by a restricted runner (`docs/remediation/F-007`,
      resolved).
- [x] Fix `just check`; `gofmt -l .` alone does not fail when files need
      formatting (`docs/remediation/F-008`, resolved — reproduced the
      failure-to-fail, fixed, reproduced the fix catching it).
- [x] Add command-output tests that parse `-o json`/`-o yaml` paths for
      `graph`, `validate`, `inventory --for`
      (`cmd/platformctl/output_contract_test.go`, `docs/remediation/F-001`)
      — partial: see "still open" below for the remaining generic sweep.

Still open:

- [ ] A generic command-output harness parsing every command × every exit
      path (success/no-op/changed/drifted/empty/cancelled) — the three
      previously-broken paths (graph/validate/inventory --for) now have
      dedicated tests, and apply/destroy/drift/status/import were verified
      correct by inspection, but no single harness sweeps all of them.
- [ ] Add full provider config drift tests — the per-provider equivalence
      table (§2.1) and connector-config-diff mechanism are tested; targeted
      out-of-band config-change integration tests (e.g. ALTER a topic's
      retention.ms out-of-band, assert `drift` reports it) remain additive
      work (§2.1's own "still open" note).
- [ ] Add MariaDB integration coverage (still genuinely untested — no test
      applies a `type: mariadb` Provider).

Design notes:

- CI already has valuable Docker integration coverage. The main missing
  coverage is around edge inputs, output contracts, and negative paths.

### 3.3 Docs And Public Surface Sync

**Status update (2026-07-17, remediation audit `docs/remediation/`):**

Resolved:

- [x] Regenerate reference docs after schema changes — `docs/reference/`
      regenerated (`docs/remediation/F-002`); the generator now preserves
      multi-paragraph Kind-level descriptions as real paragraphs
      (`docsgen.description`) while table cells stay single-line
      (`docsgen.firstParagraph` for the index summary column), so hand-owned
      narrative content (e.g. SecretReference's rotation-behavior notes)
      moved into the schema description instead of living only in the
      committed markdown, where a future regeneration would have silently
      deleted it. A drift guard (`TestGeneratedReferenceInSync`,
      `internal/application/docsgen/generated_sync_test.go`) now fails CI
      if `docs/reference/` and the schemas disagree, with the exact fix
      command in the failure message.
- [x] Update SecretReference docs: Vault is implemented behind a gate;
      Kubernetes is still unavailable — `docs/planning/03`'s SecretReference
      example comment corrected (was `vault (future)`; vault has shipped
      since Phase 6, gated `VaultSecretBackend`, Alpha/disabled).
- [x] Update README command descriptions: `graph` now renders architecture
      (`docs/remediation/F-003`, resolved) — table corrected to describe
      `--format tree|dot|mermaid|json` and the post-F-001 `-o json|yaml`
      contract; `inventory` (with `--for <tool>`) added, previously absent
      from the table entirely. Verified against live `--help` output.

Still open:

- [ ] Reconcile roadmap checkboxes with checkpoint status or convert old
      roadmap sections into historical records.
- [ ] Reconcile schema/docs/code around namespace support (namespaces
      themselves are documented — §0.1 — this item is about sweeping any
      remaining single-namespace-era prose).
- [ ] Document the exact support level of Alpha-enabled providers.

Design notes:

- For open-source contribution, stale docs are architectural debt because
  contributors will copy the wrong patterns.

### 3.4 Contributor-Facing Provider Runtime Contract

Required work:

- [ ] Write provider author docs with lifecycle semantics, required
      interfaces, validation responsibilities, state/providerState rules,
      endpoint publication rules, and drift expectations.
- [ ] Add a provider conformance suite for Reconcile/Probe/Destroy behavior.
- [ ] Add examples of a source provider, sink provider, catalog provider, and
      connection provider.
- [ ] Decide whether third-party providers are compiled in, plugins, or both.

Design notes:

- The current code is understandable, but contribution requires executable
  contracts, not only conventions.

## Cross-Cutting Acceptance Checklist

Use this checklist for each future issue created from this document.

- [ ] Does the change preserve deterministic `plan` output?
- [ ] Does `validate` catch misconfiguration before `apply`?
- [ ] Does the JSON/YAML output remain parseable on every path?
- [ ] Does the graph renderer handle special characters and id collisions?
- [ ] Does `drift` detect both liveness and spec drift?
- [ ] Does `apply` heal only resources it is allowed to mutate?
- [ ] Does `destroy` avoid data loss unless the user explicitly opted in?
- [ ] Are Docker objects labeled with enough identity to audit and clean up?
- [ ] Are host and in-network endpoints both represented accurately?
- [ ] Are secrets referenced without leaking through logs, state, or output?
- [ ] Are unit, race, and relevant Docker integration tests updated?

## Suggested Issue Slicing

1. Identity and name policy epic.
2. Output contract and renderer escaping epic.
3. External lifecycle and orphan/prune epic.
4. Docker runtime hardening epic.
5. Provider drift-equivalence epic.
6. Data-engineering lakehouse contract epic.
7. Security baseline epic.
8. Contribution and conformance epic.

Each epic should start with a design note and a failing test suite before code
changes. Keep provider-specific fixes small once the shared contracts are in
place.
