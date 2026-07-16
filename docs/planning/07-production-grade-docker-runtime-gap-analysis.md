# Production-Grade Docker Runtime Gap Analysis

Review date: 2026-07-15.

Purpose: document the work still needed before platformctl should be treated as a
production-grade Docker-runtime foundation for data lakehouse and pipeline
infrastructure, and before the project invites broad third-party open-source
contribution. This is intentionally a work backlog, not an implementation note.

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
      (structured payload always to stdout, prose to stderr — see 0.5;
      generic per-path parse tests still open).
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

- [ ] Catalog, warehouse, object store, and table format resources form a
      complete usable contract for Spark/Trino/Dagster/dbt-style tooling.
- [ ] CDC, sink, ingest, and database-sink pairings either have real providers
      or are clearly hidden from production docs.
- [ ] External ingress and egress are represented through first-class
      Connections, tunnels, TLS/auth, and reachability policies.
- [ ] Drift detection verifies full provider configuration, not only liveness.

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

Resolved (`cmd/platformctl/root.go`):

- `isStructured(output)`/`humanWriter(cmd, output)` route every prompt and
  human summary line to stderr when `-o json|yaml` is active; stdout gets
  exactly one `cliutil.WriteOutput` call per exit path — success, no-op,
  drift-heal, cancelled — via typed payloads (`applyOutput`, `destroyOutput`,
  `driftOutput`, `inventoryOutput`, `importOutput`).
- `inventory -o json` with zero endpoints returns `{"endpoints": []}` (via
  `inventoryOutput{Endpoints: data}` with `data` typed as a slice, not a bare
  `nil`/text branch).
- `graph` takes its own `--format tree|dot|mermaid|json` flag, independent of
  the root `-o table|json|yaml`, whose help text is now accurate.

Still open:

- [ ] Add command-level tests that actually parse stdout as JSON/YAML for
      each command × each path (success, no-op, changed, drifted, empty,
      cancelled). Today this is verified by inspection of `root.go` and by
      `archview`'s own `TestRenderFormats` (which does assert `json.Unmarshal`
      round-trips), but there is no generic harness asserting, e.g., `apply
      -o json` on a cancelled run still emits a single valid JSON document on
      stdout with nothing else mixed in.

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
Gate 1 stage-gate.** This is inspection/operator tooling, not a correctness
or safety gap: ownership labels are on every created object, unlabeled
objects are never touched, unmanaged same-name objects are refused, and
authoritative apply already deletes state-tracked orphans. Nothing here can
damage a shared daemon by its absence — it only makes cleanup of *pre-crash*
stale objects more manual. Track as its own epic (issue slicing #4).

Remaining work (unchanged):

- [ ] Add managed network and volume listing.
- [ ] Add `platformctl doctor` or `platformctl gc plan` to show orphaned
      Docker objects by project/namespace/resource labels.
- [ ] Add non-destructive and destructive cleanup flows with dry-run output.

Design notes:

- Open-source users will run this on shared Docker daemons. They need to know
  exactly what will be touched.

### 1.4 State Durability And Recoverability

Resolved in the Gate 1 close-out pass (2026-07-16):

- [x] Fsync the state directory after rename (best-effort where the
      platform allows opening directories) — `localfile.Save`.

**Disposition for the rest: explicitly deferred past the Gate 1 stage-gate.**
Migration scaffolding already has a working precedent (the v1→v2 key
migration in `state.Normalize`/`parseV1Key` with tests); formalizing it, the
`state inspect/doctor/repair` helpers, and the remote-state decision are
operator-tooling work tracked with the same epic as 1.3.

- [ ] Add migration scaffolding and tests before changing state format
      (beyond the existing v1→v2 path).
- [ ] Add `platformctl state inspect`, `state doctor`, and `state repair`
      helpers for corrupted or stale state.
- [ ] Decide whether remote/shared state is in scope for Docker production.

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

Required work:

- [ ] URL-escape Kafka Connect connector names in REST paths.
- [ ] Escape Kafka topic names when generating `topics.regex` for the S3 sink.
- [ ] Build Postgres connection strings with URL-safe credential handling.
- [ ] Build MySQL DSNs through `mysql.Config` rather than string formatting.
- [ ] Generate unique MySQL/MariaDB Debezium `database.server.id` values per
      connector; the current formula is effectively constant.
- [ ] Add validation for provider-specific option blocks such as Debezium
      table lists, snapshot mode, external database host/port overrides, and
      S3 sink endpoint.
- [ ] Make destroy behavior explicit for data-bearing subresources such as
      Datasets and Sources.

Design notes:

- Secrets commonly contain `@`, `:`, `/`, `#`, spaces, quotes, and backslashes.
  Credential handling must not rely on lucky demo passwords.

### 2.3 Lakehouse Contract Completeness

Current gap:

The repo can stand up a useful local lakehouse-shaped stack, but the resource
model does not yet fully describe what external tools need to consume it.

Required work:

- [ ] Model the relationship between Catalog, Dataset, warehouse location,
      object-store endpoint, branch, table namespace, and table format.
- [ ] Decide whether Iceberg tables are resources or produced by external
      tools.
- [ ] Add schema registry or schema-carrying converter support before claiming
      Parquet/Avro production support.
- [ ] Add generated config views for common tools:
      Spark, Trino, Flink, Dagster, dbt, Metabase/Superset, psql, mysql,
      kafka clients, and S3 clients.
- [ ] Include endpoint auth, TLS, region, warehouse path, and branch in
      inventory JSON.

Design notes:

- Inventory should answer "what exact config do I paste into my tool?"
  without requiring users to reverse-engineer provider conventions.

### 2.4 Ingress, Egress, And External Reachability

Current gap:

- Managed `Connection` is TCP forwarding through socat.
- There is no TLS termination, HTTP routing, auth proxy, SOCKS/SSH/WireGuard
  tunnel, egress policy, or private-network connector.
- In-network DNS is just container names on one shared Docker network.

Required work:

- [ ] Add a first-class ingress/egress design around Connection.
- [ ] Add tunnel-capable providers for VPC/private-network reach.
- [ ] Add TLS and authentication metadata to endpoints and Connections.
- [ ] Support network aliases so stable internal names do not have to equal
      runtime container names.
- [ ] Add reachability probes for both host and in-network audiences.

Design notes:

- A data engineer needs stable internal and external addresses, but also
  controlled exposure. Reachability and exposure are different facts.

### 2.5 Production Security Baseline

Current gap:

- Secret values are commonly injected into container env, which is inspectable
  through Docker APIs.
- Several services run over plaintext localhost HTTP/TCP.
- Latest image tags are used in examples and providers.
- Marquez internal Postgres credentials are fixed.

Required work:

- [ ] Add Docker secret/file-mount support where images allow it.
- [ ] Avoid persisting or logging provider configs that contain secret values.
- [ ] Add TLS/auth support or explicit "local only, insecure" labeling for
      each provider endpoint.
- [ ] Pin default images by version and document image support windows.
- [ ] Prefer digests for release-tested images in CI/examples.
- [ ] Replace fixed internal credentials with generated or secret-backed
      credentials where practical.

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

Required work:

- [ ] Add tests for name policy and renderer escaping.
- [ ] Add command-output tests that parse every `-o json` and `-o yaml` path.
- [ ] Add removed-resource/orphan tests.
- [ ] Add unmanaged Docker network/volume collision tests.
- [ ] Add host bind-address tests.
- [x] Add lakehouse integration coverage for MySQL and Postgres admin-secret
      rotation through SecretReference changes.
- [ ] Add full provider config drift tests.
- [ ] Add MariaDB integration coverage.
- [ ] Add tests for special-character secrets and URL/DSN escaping.
- [ ] Expand fake runtime conformance to compare every spec field.
- [ ] Make `TestProbeTCPReachable` skip or self-describe when loopback listen
      is blocked by a restricted runner.
- [ ] Fix `just check`; `gofmt -l .` alone does not fail when files need
      formatting.

Design notes:

- CI already has valuable Docker integration coverage. The main missing
  coverage is around edge inputs, output contracts, and negative paths.

### 3.3 Docs And Public Surface Sync

Required work:

- [ ] Update README command descriptions: `graph` now renders architecture,
      not the raw dependency DAG.
- [ ] Reconcile roadmap checkboxes with checkpoint status or convert old
      roadmap sections into historical records.
- [ ] Reconcile schema/docs/code around namespace support.
- [ ] Update SecretReference docs: Vault is implemented behind a gate;
      Kubernetes is still unavailable.
- [ ] Regenerate reference docs after schema changes.
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
