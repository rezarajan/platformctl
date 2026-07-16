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

## Stage Gates

Use these gates to split future work. A later gate should not start until the
earlier gate's acceptance criteria are either complete or explicitly waived in
the tracking issue.

### Gate 0: Foundation Correctness

Required before broader contribution or more provider work.

- [ ] Canonical resource identity policy is implemented and documented.
- [ ] External lifecycle resources cannot be mutated accidentally through any
      provider path.
- [ ] Resource removal and rename behavior is explicit, tested, and safe.
- [ ] Machine-readable command output is valid JSON/YAML for every command.
- [ ] Mermaid/DOT renderers are collision-resistant and escaping-safe.
- [ ] Docker bind addresses and object ownership checks are safe by default.

### Gate 1: Docker Production Runtime

Required before positioning the Docker runtime as production-grade.

- [ ] Runtime specs cover restart policy, bind address, resource limits,
      security context, network aliases, logs, image pull policy, and digest
      pinning.
- [ ] Docker adapter refuses or adopts pre-existing networks and volumes
      consistently with the container ownership policy.
- [ ] Endpoint discovery reports actual published ports and bind addresses,
      not only deterministic intent.
- [ ] Secret material is not exposed through inspectable container environment
      variables where a runtime-supported safer path exists.

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

### 0.1 Canonical Names, Namespaces, And Stable IDs

Current risk:

- `metadata.name` only has `minLength: 1` in `schemas/v1alpha1/meta.json`.
- `resource.Key` is `Kind/Name`; `state.parseKey` splits on `/`, so a name
  containing `/` corrupts state round-trips.
- `Metadata.Namespace` exists in Go, but the schema does not allow
  `metadata.namespace`, and state keys ignore it.
- The same name flows into Docker container/network/volume names, Kafka topics,
  Kafka Connect connector names, REST URL path segments, env var names, file
  secret paths, host-port hashes, Mermaid ids, Graphviz ids, and state keys.
- Docker object names are not scoped by project/workspace. Two checkouts or two
  users on the same Docker daemon can collide.

Required work:

- [ ] Choose one v1 policy:
  - Strict single namespace: remove `Namespace` from Go/docs and reject it
    explicitly.
  - Namespaced resources: include namespace in `resource.Key`, state,
    labels, Docker object names, inventory, graph output, and import/destroy.
- [ ] Add a schema pattern for `metadata.name` and all `nameRef` values.
      Prefer a conservative DNS-label-style policy unless there is a clear
      reason to allow arbitrary names.
- [ ] Introduce separate concepts for display label, stable resource key, and
      runtime object name.
- [ ] Add a project or stack id used in Docker names and labels. It must be
      stable across runs and explicit enough for multi-checkout use.
- [ ] Make state serialization structured rather than delimiter-based, or
      escape keys with a tested reversible encoding.
- [ ] Add tests for invalid names, cross-workspace collisions, slash/dot/colon
      names, long names, Unicode, and name collisions after sanitization.

Design notes:

- Rejecting unsafe names is safer than trying to make every downstream system
  accept arbitrary strings.
- If arbitrary display names are needed, put them in annotations or a `title`
  field and keep `metadata.name` operational.
- Docker labels should carry at least project id, namespace, kind, name,
  generation/spec hash, and managed-by.

### 0.2 Ambiguous Bare-Name References

Current risk:

- `manifest.Validate` permits `Provider/foo` and `Source/foo` in the same set.
- `graph.Build` rejects ambiguous refs for normal ref fields, but
  `metadata.observers` takes the first matching name and does not reject
  ambiguity.
- `compatibility.Check` builds `byName map[string]Envelope`, so duplicate names
  across kinds overwrite each other before capability checks.
- `archview.resolveByName` iterates a map and can choose nondeterministically.

Required work:

- [ ] Either make names globally unique in a manifest set or make refs typed
      (`kind` + `name`) everywhere.
- [ ] Apply the same ambiguity rules to observers, Binding refs, Connection
      refs, compatibility checks, inventory, import, and graph rendering.
- [ ] Add negative tests for cross-kind duplicate names and ambiguous observer
      targets.

Design notes:

- Global uniqueness is simpler for v1 and aligns with current bare `nameRef`.
- Typed refs are more extensible, but they are a schema and UX change.

### 0.3 External Lifecycle Must Be Enforced On Apply

Current risk:

- `resource.LifecycleOf` marks `spec.external: true` as External.
- Plan emits `configure` for external resources.
- The engine only special-cases external resources with no `providerRef`.
- If an external resource also has a `providerRef`, the engine calls the
  provider's normal `Reconcile`, and most providers do not know this is a
  configure-only lifecycle. Example class: an external Dataset with a
  providerRef can still cause bucket creation.

Required work:

- [ ] Define a provider contract for External resources:
  - no mutation at all, only validation/status, or
  - explicit `Configure` method separate from `Reconcile`.
- [ ] Enforce the contract in the engine, not by provider convention.
- [ ] Add tests for `external: true` with and without `providerRef` for every
      resource kind that supports external lifecycle.
- [ ] Update docs to make "external" behavior exact.

Design notes:

- Destroy already has an engine-level external guard. Apply needs the same
  level of central enforcement.
- If "configure an external system" is supported later, it should be an
  explicit capability with a destructive/mutating policy.

### 0.4 Removed Or Renamed Resources Are Orphaned

Current risk:

- `plan.Compute` iterates desired envelopes only. State entries that are no
  longer present in manifests are not surfaced.
- Removing a manifest can leave the state entry and Docker object alive until
  the user reconstructs the old manifest or cleans up manually.
- Renames are indistinguishable from create-new plus orphan-old.

Required work:

- [ ] Add a safe orphan detection model:
  - `plan` reports state resources missing from desired.
  - `apply` does not delete by default unless a clear prune flag/policy exists.
  - `destroy` can operate from state for missing desired resources, using
    stored provider/runtime identity.
- [ ] Decide whether `delete` actions belong in normal apply or only in a
      `prune`/`gc` command.
- [ ] Persist enough provider/runtime identity in state to destroy resources
      after their manifests are removed.
- [ ] Add tests for deletion, rename, provider type change, and state-only
      resources.

Design notes:

- Terraform-style deletion on apply is powerful but dangerous for data
  systems. A separate explicit prune command may be a safer first step.
- Data-bearing resources need stronger protection than ephemeral connectors.

### 0.5 Machine-Readable Output Contract

Current risk:

- `drift -o json` and `drift -o yaml` write structured rows and then append
  human summary text to stdout, making the output invalid as JSON/YAML.
- `destroy -o json` writes a plan and then human prompts/summaries to stdout.
- `inventory -o json` with no endpoints prints plain text instead of a JSON
  empty result.
- `apply -o json` can print a plan, prompt text, or no-change text to stdout.
- The root `-o` help says `table|json|yaml`, but `graph` accepts
  `tree|dot|mermaid|json` through the same flag.

Required work:

- [ ] Define per-command stdout/stderr contracts.
- [ ] For `json` and `yaml`, stdout must contain exactly one parseable
      document for every exit path.
- [ ] Move human summaries and prompts to stderr or include them inside the
      structured payload.
- [ ] Give `graph` its own format flag or make global output help accurate.
- [ ] Add command-level tests that parse JSON/YAML for success, no-op,
      changed, drifted, empty, validation-error, and cancelled paths.

Design notes:

- CI and external tools should never need to parse human prose.
- Exit codes can carry changed/drifted status; structured payload should carry
  counts and reasons.

### 0.6 Mermaid, DOT, And Architecture Rendering Escaping

Current risk:

- `archview.mermaidID` replaces only `/`, `-`, `.`, space, and `:`.
- Different resource keys can collapse to the same Mermaid id.
- Mermaid labels only replace `"`, but labels can contain newlines, brackets,
  pipes, slashes, backticks, braces, and Mermaid control characters.
- Pipeline and reaches edge labels are embedded directly in Mermaid edge
  syntax.
- Synthetic external node names come from raw `Connection.spec.target`, which
  can contain punctuation.
- `archview.resolveByName` can pick nondeterministically when names are
  duplicated across kinds.

Required work:

- [ ] Implement renderer-specific escaping for Mermaid node ids, labels, edge
      labels, DOT ids, DOT labels, and tree output.
- [ ] Generate renderer ids from a stable opaque encoding or hash of the full
      resource key, then keep display labels separate.
- [ ] Add golden tests for special characters, duplicate-looking sanitized
      names, multiline labels, synthetic external targets, and all graph
      formats.
- [ ] Add an optional Mermaid syntax validation step in tests if a lightweight
      validator is available.

Design notes:

- The safest Mermaid pattern is stable internal ids plus quoted/escaped labels.
- A name policy can reduce the input space, but renderer escaping is still
  required for details such as external targets and endpoint URLs.

### 0.7 Docker Bind Address And Ownership Safety

Current risk:

- `runtime.PortBinding` has no `HostIP`.
- Docker `nat.PortBinding` is created with only `HostPort`; Docker commonly
  binds published ports on all interfaces when `HostIP` is empty.
- Providers report host endpoints as `127.0.0.1:<port>`, which can understate
  the actual exposure.
- `EnsureNetwork` returns success when a same-name network already exists,
  without verifying platformctl ownership labels.
- `EnsureVolume` returns success when a same-name volume already exists,
  without verifying platformctl ownership labels.

Required work:

- [ ] Add `HostIP` or `BindAddress` to the runtime port; default to
      `127.0.0.1` for local development.
- [ ] Inventory must report the actual bind address.
- [ ] EnsureNetwork and EnsureVolume must refuse unmanaged same-name objects,
      adopt only through explicit import/adopt flow, or use project-scoped
      names that avoid collisions.
- [ ] Add integration tests proving ports are not exposed beyond the intended
      interface and unmanaged networks/volumes are not reused.

Design notes:

- This is a security issue, not a UX polish item.
- Host tools can still connect to localhost; remote access should be explicit.

## Gate 1 Details

### 1.1 Expand The ContainerRuntime Contract

Current gap:

The runtime port supports only network, volume, container, env, command,
ports, health checks, labels, and simple mounts. Production Docker operation
needs more controls.

Required work:

- [ ] Restart policy.
- [ ] CPU and memory limits/reservations.
- [ ] User, group, read-only root filesystem, capabilities, security options.
- [ ] Network aliases and explicit DNS names.
- [ ] Host bind address and published-port inspection.
- [ ] Log driver and log rotation.
- [ ] Config/file mounts, not only named volumes.
- [ ] Image pull policy, registry auth, digest pinning, and local-only mode.
- [ ] Container events/log retrieval for diagnostics.

Design notes:

- Keep the port provider-friendly, but do not pretend Docker-specific
  operational facts do not exist. A Docker runtime can expose Docker-grade
  controls while higher-level resource kinds stay provider-agnostic.

### 1.2 Runtime Drift Equivalence

Current gap:

- Docker `EnsureContainer` uses a spec-hash label to decide replacement.
- `Inspect` returns only name, id, image, running, healthy, and labels.
- Provider probes mostly check liveness, not spec equivalence.
- The fake runtime's `containerSpecEqual` ignores command, ports, volumes,
  health checks, and other fields, so conformance can miss drift-sensitive
  changes.

Required work:

- [ ] Define runtime-level desired-vs-actual equivalence.
- [ ] Expand `ContainerState` or add a `ProbeContainerSpec` capability.
- [ ] Make the fake runtime compare every meaningful `ContainerSpec` field.
- [ ] Add conformance tests for command, env, ports, bind address, volumes,
      networks, labels, health checks, restart policy, and resource limits.

Design notes:

- A provider should not have to hand-roll Docker inspect diffing.
- Spec-hash labels are useful, but they do not catch out-of-band mutation if
  labels remain stale.

### 1.3 Garbage Collection And Orphan Inspection

Current gap:

`ListManaged` lists managed containers only. Networks, volumes, and stale
runtime objects are not surfaced as a first-class inventory/GC view.

Required work:

- [ ] Add managed network and volume listing.
- [ ] Add `platformctl doctor` or `platformctl gc plan` to show orphaned
      Docker objects by project/namespace/resource labels.
- [ ] Add non-destructive and destructive cleanup flows with dry-run output.

Design notes:

- Open-source users will run this on shared Docker daemons. They need to know
  exactly what will be touched.

### 1.4 State Durability And Recoverability

Current gap:

- Local state uses atomic temp-file rename, but does not fsync the directory.
- State has no migration framework beyond version rejection.
- Locking is local advisory `flock`; there is no stale lock diagnosis beyond
  writing a PID to the lock file.

Required work:

- [ ] Fsync the state directory after rename where supported.
- [ ] Add migration scaffolding and tests before changing state format.
- [ ] Add `platformctl state inspect`, `state doctor`, and `state repair`
      helpers for corrupted or stale state.
- [ ] Decide whether remote/shared state is in scope for Docker production.

Design notes:

- Local file state is acceptable for a Docker-first runtime, but recovery
  tooling matters because data systems are long-lived.

## Gate 2 Details

### 2.1 Provider Drift Must Check Desired Configuration

Current provider drift gaps:

- Redpanda topic probe checks existence and partition count, but not
  `retention.ms`.
- Postgres Source probe checks database existence, but not logical WAL,
  publication, replication role, grants, or credential validity.
- MySQL/MariaDB Source probe checks database existence, but not binlog mode,
  replication user, grants, or credential validity.
- S3 Dataset probe checks bucket existence, but not prefix reachability,
  format contract, versioning/lifecycle policy, or credentials beyond root.
- Debezium and s3sink Binding probes check connector RUNNING, but not whether
  connector config matches the desired manifest.
- Proxy Connection probe checks the forwarder container only, not upstream
  reachability.
- Nessie Catalog probe checks branch existence, but not warehouse/object-store
  configuration.

Required work:

- [ ] Define per-provider drift equivalence tables.
- [ ] Probe full desired configuration or explain why a field is intentionally
      not drift-managed.
- [ ] Store observed provider facts in status for `status -o json`.
- [ ] Add integration tests for manual out-of-band config changes.

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
