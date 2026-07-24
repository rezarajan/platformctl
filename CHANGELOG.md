# Changelog

All notable user-facing changes to `platformctl` are documented here,
release by release. Entries are written for someone deciding whether to
upgrade — what's new to use, what changed in behavior, what still needs a
`--feature-gates` flag to try. See `docs/upgrade-notes.md` for
operator-facing migration detail on any entry marked **(migration)**, and
`docs/planning/11-production-review-2026-07.md` for the full evidence
trail behind every claim below.

## v1.3.0 — 2026-07-23

Two releases' worth of production-readiness work: a systematic review of
everything shipped since v1.2.0 (Stage I), structural hardening of the
test suite and runtime lifecycle contracts (Stage J), the first two
zero-trust access layers built on the graph-scoped foundation (Stage K),
and the groundwork for making mediated (identity-attested) connectivity
the platform's default transport rather than an opt-in (Stage L).

**Fifteen proven features graduate Alpha → Beta this release** —
maturity only (production-ready quality), with each gate's opt-in default
preserved. See "Gate graduation review" below for the full list, evidence,
and the three gates deliberately held at Alpha.

### Production-review remediations (Stage I)

A systematic pass (docs/planning/11) auditing everything shipped through
Stage H against its own claims, closing every gap it found:

- **Reconcile/Probe symmetry (I4).** `Ready` now always reflects the same
  check a `Probe` call would make — previously a handful of providers
  (ingress, wireguard, proxy) could report `Ready` from a container
  health check alone while `Probe` used a stricter dial-through check,
  so `plan` immediately after `apply` could show phantom drift. Fixed
  for every affected provider; a graph-ordering bug this surfaced
  (`Connection` reconciling out of order relative to a Provider it
  depends on) fixed alongside via a new dependency edge in `graph.Build`.
- **Outbound database TLS (I2).** `jdbcsink` and the CDC path can now
  reach a TLS-requiring external database (e.g. a cloud-managed
  Postgres/MySQL) — full `sslmode`/`sslrootcert` (postgres) and
  `sslMode` (mysql/mariadb) support, live-verified end to end against a
  real TLS-required Postgres (`ExternalDatabaseTLS` gate, Alpha/enabled
  — unchanged this release, already additive-only).
- **VPC-behind-VPN egress via WireGuard (I1).** `Connection.spec.via`
  is now fully consumed: traffic to a private-network target routes
  through a WireGuard tunnel container with a negative proof (a direct
  dial from outside the forwarder fails) and key-rotation-mid-stream
  recovery, both live-verified.
- **Kubernetes GA-parity gap closure (I6/I7/I8).** Two real evidence
  gaps the B2 audit found — no Kubernetes chaos/mid-apply-kill test, no
  Kubernetes DLQ/Connect-HA test — are now closed, live, on a real
  cluster. Closing them surfaced and fixed two real defects: Kafka
  Connect worker HA (`workers > 1`) failing hard at apply on Kubernetes
  (I7, an ordinal-free/any-member addressing fix), and a contention-
  fragile produce-during-kill race in Redpanda's Kubernetes HA path
  (I8).
- **Verify-then-promote restore (I13).** Restore now writes to a scratch
  database and only atomically swaps it into place after content
  verification — a failed/corrupt restore attempt never touches the live
  target. Proven live on both Postgres and MySQL with byte-identical
  fingerprint assertions on the untouched target.
- **Backup/restore on Kubernetes (I15).** The same backup/restore
  pipeline now runs as Kubernetes Jobs, not just Docker containers —
  reached live green only after five real-bug iterations (RBAC, RFC
  1123 naming, and a masked "0-byte backup reported as success" defect
  the drill itself caught).
- **dbjob pipeline hardening (I12).** Fault-injection drills
  (producer-killed-mid-stream, consumer-rejects-instantly, corrupt exit
  file) each now end in a named error and an empty-bucket-listing
  assertion — two of the drills' own first drafts were themselves caught
  as dishonest (silently passing without really exercising the fault)
  and fixed.
- **Grafana admin credential rotation (I14)**, **duplication/drift-risk
  cleanup (I5)** — shared endpoint-resolution and dial-probe code
  extracted out of per-provider duplication into `providerkit`/
  `runtime/probe`, closing a real behavioral divergence (Kubernetes'
  probe previously ignored context deadlines; it now shares Docker's
  deadline-capped semantics).
- **Structured logging (I11).** Every reconciliation action now logs
  through `log/slog` behind `--log-format text|json` (text remains
  byte-compatible with prior output); `json` emits one parseable event
  per action with resource/action/outcome/duration fields.
- **Generalized cross-provider facts (I9).** `Request` gained a generic,
  read-only facts-query surface so a new provider can consume another
  provider's published endpoint facts without a core-package change —
  the bespoke per-fact fields (`SchemaRegistryURL`, `KafkaBootstrapServers`,
  etc.) are now thin deprecated wrappers over it, byte-identical output.
- **Fragment-completeness sweep (I10).** A new archtest walks every
  shipped manifest (testdata, examples, blueprints) against the JSON
  Schema fragments and fails on any configuration key a fragment would
  reject — closing the exact blind spot that let `httpsPort` escape
  validation until a live integration run.
- **NFR-11 (Settledness), audited (I3/I4).** "A resource reported Ready
  answers its declared protocol at that moment" is now a named
  requirement (doc 01), not folklore — audited across every provider.

### Test/structure hardening (Stage J)

- **Three-tier testing (J1, ADR 028).** `just test` is now a genuinely
  fast (≤1 minute), budget-guarded local loop; anything requiring
  Docker/Kubernetes/wall-clock time moved behind the `integration` build
  tag. CI enforces the budget by parsing `go test -json` output and
  failing on any fast-tier test over 60s.
- **Residue-free lifecycle (ADR 029).** `Remove` is now contractually
  required to leave zero derived residue, not just the object it was
  asked to remove — conformance-enforced. **(migration)** — see
  `docs/upgrade-notes.md`'s 2026-07-23 entry: Docker's image-declared
  anonymous volumes and Kubernetes' derived Services/files-Secret/
  PodDisruptionBudget are now cleaned up on teardown, closing a leak
  that had existed since the project began (one sweep found and pruned
  3,853 dangling volumes, 8.4GB, from pre-fix residue on the
  maintainers' own development host). `testkit.Janitor` now owns
  cleanup order/loudness across the integration suite; every hand-rolled
  test cleanup closure in the repo has been converted to it.
- **Runtime object naming authority (ADR 030).** All derived runtime
  object names (backup Jobs, per-ordinal replicas, etc.) are now minted
  through one `naming.Derived`/`naming.Timestamp` authority — lowercase
  RFC 1123, a 63-character hash-truncation bound — rather than
  concatenated ad hoc at each call site; `archtest` now forbids per-site
  name concatenation. **(migration)** — backup timestamps changed format
  (see `docs/upgrade-notes.md`).
- **Provider diagnostics channel (ADR 031).** Providers report
  informational findings through `Request.Warnf` — routed through the
  engine's own presentation layer — rather than writing directly to
  `os.Stderr`/`os.Stdout`; `archtest` now forbids process-global stream
  writes from adapter and application code (caught one previously
  undocumented site: the Kubernetes adapter's `networkPolicy: none`
  warning, now emitted at the correct chokepoint).
- **Backup orchestration dedup (J4).** Postgres and MySQL backup/restore
  now share one `dbbackup` orchestration harness (two profiles) instead
  of ~580 duplicated lines maintained in lockstep across two providers.
- **Resource requests/limits from spec to scheduler (J5).** Heavyweight
  example providers (Nessie, Marquez/OpenLineage — real JVMs) now carry
  resource requests/limits reaching the Kubernetes scheduler, closing a
  real CI eviction-churn bug where these pods were recreated repeatedly
  under memory pressure.
- **Provider-author contract (E6).** A new `internal/ports/reconciler/
  conformance` suite (Settledness, Idempotency, Probe honesty, Destroy
  convergence, Request statelessness, endpoint-publication, capability-
  error-format subtests) and `docs/contributing/provider-authoring.md`
  make the provider contract executable, not just conventional. All
  thirteen-plus shipped providers now carry conformance coverage
  (`internal/adapters/providers/*/conformance_test.go`), sub-second even
  under `-race`.

### Zero-trust access, layer 3: label-scoped moderation (Stage K, ADR 033)

Building on Stage H's graph-scoped access (ADR 026) — which grants
exactly what the declared reference graph asks for — Stage K gives the
**policy** vocabulary the same label-level granularity:

- **Label grammar and selector vocabulary (K1/K2).** Policy rules can now
  target `matchEdge.selector`/`matchResource.selector` (label selectors),
  not only bare Kind/name/domain matches — resolved with the same
  label-matching machinery the runtime's own graph-scoped access already
  uses.
- **Selector-scoped wide grants (K3).** A `spec.access` grant can now be
  scoped to resources carrying a label, not only "this whole domain" —
  narrowing the blast radius of an intentional wide grant without
  retreating to per-edge Bindings.
- **Decision audit trail (K5, ADR 033 decision 5).** `platformctl
  policy audit` reports, for every declared edge, why it is permitted or
  denied — grant, no-matching-deny, or exemption — with a non-empty
  justification required for every row. **(migration)** validate/plan/
  apply/destroy now also emit a structured policy-decision log line on
  stderr per evaluated rule, when `PolicyEngine` is enabled and a
  `--policies` directory is supplied; see `docs/upgrade-notes.md`.

`LabelScopedAccess` (the gate for K1-K3's selector vocabulary) ships this
release **Alpha, disabled by default** — the shape is real and
exercised, but the composed cross-provider scenario ADR 033's own
graduation bar names has not yet run to completion (K4, mediator
attribute enforcement, remains open). `GraphScopedAccess` likewise stays
Alpha/disabled: its Kubernetes-side negative proof is a structured skip
on the shared development cluster's CNI (which doesn't enforce
`NetworkPolicy`), not a pass — the dual-runtime negative-proof bar its
own gate row names is met on Docker only.

### Mediation transport infrastructure (Stage L, ADR 034)

The groundwork for making mediated (identity-attested, per-edge
authorized) connectivity the platform's *default* transport, inverting
Stage H's opt-in boundary (ADR 027) — a `Binding`/`Connection` will
eventually opt an edge *out* via `spec.transport: direct`, rather than
opting one in:

- **L1: the transport seam.** The engine now substitutes a mediated
  address into `SchemaRegistryURL`/`KafkaBootstrapServers` when the
  `MediatedTransport` gate is on, proven against a fake
  `mediation.AddressResolver` — a design-spike seam with no real fabric
  behind it yet.
- **L2: platform-owned fabric.** The engine can now ensure a mediation
  fabric (an OpenZiti mesh) the same way it ensures a network — state
  gained a `mediationFabric` field to record it (additive; see
  `docs/upgrade-notes.md`). A real leak found live during this work —
  the fabric's state briefly appeared as an orphan-sweepable resource,
  self-destructing on the next unrelated `apply` — was fixed before
  merge.
- **L2a: mediation port conformance suite.** A technology-independent
  conformance suite for the `mediation.Fabric` port (CRI/CNI-adapter-
  contract style) — found and fixed a real `RevokeIdentity`
  posture-decay defect live.

**`MediatedTransport` ships Alpha, disabled by default, and is
byte-identical-off** — no manifest that doesn't opt in observes any
change. Per ADR 034's own addendum, `Engine.Mediation` stays `nil` in
production until L3's three sub-parts (L3a/L3b/L3c: the atomic
default-flip mechanics — address-resolver wiring, consumer tunneler
injection, dark-target handling) all land together; that work is
**in progress, not shipped this release**. `MediatedConnections` (the
existing, separate, per-Connection opt-in mediation mechanism from Stage
H) **graduates to Beta** this release (still opt-in/disabled by default) —
see "Gate graduation review" below.

### Mediation hardening (H9/H10)

- **H9: the composed cross-domain scenario.** Every zero-trust component
  (domains, crossDomain policy, graph-scoped access, mediated
  connections) had been proven in isolation; H9 is the first end-to-end
  scenario combining all of them on one cross-domain edge — deny without
  exemption, apply with exemption, positive mediator-state assertion,
  re-deny on exemption removal, and full manifest-driven teardown.
  Composing them found three real defects invisible to any single
  component's own tests: a missing cross-domain address-qualification
  capability on Kubernetes (now `runtime.AddressQualifier`), a
  mediator drift-detection filter silently broken since Stage H by an
  incompatible controller API filter, and an NFR-3 interaction on
  teardown of an External Source. All three fixed.
- **H10: controller CA pinning + enrollment JWT off Env.** The OpenZiti
  management client no longer dials the controller with
  `InsecureSkipVerify` (the bootstrap CA is now fetched and pinned into
  the client's trust store); the one-time enrollment JWT is now
  file-mounted (0600) rather than passed through `ContainerSpec.Env`,
  matching the WireGuard provider's existing private-key convention.

### Gate graduation review

Fifteen proven features graduate **Alpha → Beta** this release (owner
directive: all proven features ship as Beta at this stage). Beta here is a
**maturity/quality** signal — production-ready, live-exercised — with each
gate's **opt-in default preserved**: most are require-gates (`validate`
refuses the feature unless you enable it), so default-enabling would both
remove a deliberate guard and change behavior for existing deployments.

Graduated to Beta (disabled by default, except `DesignLints` which stays
enabled): `HighAvailability`, `BackupRestore`, `MonitoringStackProvider`,
`IngressProvider`, `TLSTermination`, `TrinoProvider`, `JDBCSinkProvider`,
`IngestProvider`, `TunnelProvider`, `SharedStateBackend`,
`MediatedConnections`, `PolicyEngine`, `GraphScopedAccess`,
`LabelScopedAccess`, `DesignLints`. Every one is backed by a dedicated
live suite; the four zero-trust gates are additionally proven end-to-end
by the new `examples/zero-trust-lakehouse` capstone (27/27 resources
Ready, 3/3 zero-trust proofs live).

The GA promotions `docs/planning/11` had queued (`KubernetesRuntime`,
`BackupRestore`, `ExternalResourceConfiguration`) are **deferred** — this
release consolidates the platform at Beta; GA is a later, separate owner
decision.

Held at **Alpha** on evidence: `MediatedTransport` (the L3 default-flip is
incomplete — ADR 034 addendum records it as atomic and not shipped;
graduating it would be a defect), `ParallelReconciliation` (data-race
history, no production soak), and `VaultSecretBackend` (no strong live
evidence this cycle).

`docs/planning/04-roadmap-and-feature-gates.md` §12's gate table is
synced to `cmd/platformctl/main.go`.


### Upgrade notes

See `docs/upgrade-notes.md` for full detail on every entry marked
**(migration)** above, plus:

- State gains an additive `mediationFabric` top-level key (nil/absent
  unless a mediation fabric has been ensured — not reachable by default
  this release).

No breaking changes ship in this release; every behavioral change above
is either purely additive or a one-time, self-healing visible effect
documented in `docs/upgrade-notes.md`.

## v1.2.0 — 2026-07-21

Operational hardening, Kubernetes Beta, and segregation readiness. See
`docs/planning/08-production-readiness-plan.md` Stages A, B, and F, and
the C1/D1 merges, for detail — this changelog starts detailed coverage at
v1.3.0.
