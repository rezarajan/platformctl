# C9 completion — postgres/mysql exporters + grafana provider

Task: docs/planning/08-production-readiness-plan.md §5 C9, the explicitly
deferred remainder (see C9's two status notes: 2026-07-21 "core slice
implemented" and the "Merged 2026-07-21" convergence-caveat note).

Working in worktree `agent-af345ef3a7bea0dc6`. Protocol: doc 08 §2.1.

(Replaces this file's previous contents, which were the already-merged
C4+D7 task's checkpoint — see `git log --oneline -- TASK_PROGRESS.md` if
ever needed. That task's own note already established the "replace, don't
append" convention for this file.)

## Step plan

- [x] Step 0: this file created; `git merge main --no-edit` done (fast-forward
      to `39efc80`, brought in C2 redpanda multi-broker + G7 test-impact
      economy + the C4/D7 TASK_PROGRESS.md this file replaces).
- [x] Step 1: read CLAUDE.md, C9 entry + both status notes, ADR 004/015/016,
      prometheus/postgres/mysql/openlineage packages in full, reconciler.go,
      runtime.go, providerkit (instance/credential), engine.go's
      resolveMetricsTargets/resolveCatalogFacts/resolveSchemaRegistryURL,
      status/reasons.go, main.go gate registration, root.go gate-check
      pattern (checkSchemaRegistryGate/checkHighAvailabilityGate),
      docsgen.go, scripts/test-impact.sh, scripts/pinned-images.txt +
      refresh-digests.sh.
- [x] Step 1.5 (spike, not committed): live-verified against real Docker
      that quay.io/prometheuscommunity/postgres-exporter supports
      DATA_SOURCE_URI (env, non-secret) + DATA_SOURCE_USER (env) +
      DATA_SOURCE_PASS_FILE (file — password never touches env); confirmed
      prom/mysqld-exporter's --config.my-cnf fully avoids env for MySQL
      (user+password both in a mounted file); confirmed grafana/grafana's
      provisioning dirs (datasources/dashboards) + GF_SECURITY_ADMIN_
      PASSWORD__FILE + /api/datasources/uid/:uid/health + /api/dashboards/
      uid/:uid all work as expected. Resolved and pinned 3 new images
      (scripts/pinned-images.txt): postgres-exporter v0.17.1, mysqld-exporter
      v0.15.1, grafana 11.4.0.
- [x] Step 2: map to interfaces — reconciler.Request (add PrometheusURL,
      mirroring SchemaRegistryURL's bare-string style, not a new struct),
      runtime.ContainerSpec/PortBinding/FileMount (no port changes needed).
- [x] Step 3: Size L, ADR needed? — decided NOT a new ADR: this is additive
      capability inside two existing providers (openlineage's two-container
      pattern, already an accepted precedent per ADR 004's "what a second
      container per Provider means") plus one new nessie-shaped provider
      reusing the existing MonitoringStackProvider gate. No new port/adapter
      shape. Recording the decision here per doc 08 step 3 ("if a design
      note exists" — ADR 004 already covers the sidecar-vs-replica
      question this task named explicitly).
- [x] Step 4: implement.
  - [x] internal/ports/reconciler/reconciler.go: `PrometheusURL string` field.
  - [x] internal/application/engine/engine.go: `resolvePrometheusURL` +
        engine_test.go's `TestResolvePrometheusURL*`.
  - [x] internal/domain/status/reasons.go: `ReasonExporterHealthy/
        Unhealthy`, `ReasonDatasourceUnhealthy`, `ReasonDashboardMissing`,
        `ReasonPrometheusUnresolved`.
  - [x] internal/adapters/providers/postgres: metrics.go (new), sql.go
        (`ensureMonitoringUser`), postgres.go wiring (reconcileInstance/
        Destroy/Probe/ValidateSpec), metrics_test.go (metricsEnabled table,
        `TestInstanceContainerSpecUnaffectedByMetrics` byte-identity pin).
  - [x] internal/adapters/providers/mysql: same shape (my.cnf-only exporter
        credentials — no env deviation needed for mysql either).
  - [x] internal/adapters/providers/grafana: new package (grafana.go,
        provisioning.go, grafana_test.go).
  - [x] cmd/platformctl/main.go: register grafana provider (gate
        MonitoringStackProvider); updated the C9 gate-registration comment.
  - [x] cmd/platformctl/root.go: `checkMonitoringMetricsGate` (mirrors
        checkSchemaRegistryGate), wired into loadAndValidate.
  - [x] cmd/platformctl/toolconfig.go: fixed a real Accept-criteria tension
        (see Design record's "Finding" below) — `gatherToolFacts` falls
        back to `ep.Internal` for the "metrics" case; pinned by
        `TestGatherToolFactsFallsBackToInternalForMetrics`.
  - [x] schemas/v1alpha1/provider.json: additive (grafana in x-known-values,
        metrics/grafana configuration prose). Guard hook allowed it.
  - [x] docs/planning/03-resource-model-reference.md: additive field note +
        example manifests. Guard hook allowed it (matches C4/D7's own
        finding that it isn't unconditional for a pure-insertion diff).
  - [x] docs/planning/08-production-readiness-plan.md: additive C9-
        completion status note under the existing C9 entry, with live
        verification evidence + the toolconfig.go finding recorded. Guard
        hook allowed it.
  - [x] scripts/test-impact.sh: ONE new `monitoring` line in the suites
        heredoc only — diffed to confirm nothing else in the file touched.
  - [x] scripts/pinned-images.txt + refresh-digests.sh run: 3 new images
        pinned; also caught an already-stale postgres:18 digest (unrelated,
        normal side effect of running the shared script) — docs/reference
        regenerated after.
  - [x] Integration test: cmd/platformctl/monitoring_completion_integration_test.go
        + testdata/monitoring-completion-scenario/{infra,plus-prometheus,full}.
- [x] Step 4b (coordinator-directed, 2026-07-22): second `git merge main
      --no-edit` (main moved to 7e68444: D5 wireguard, H1/H2 lint, E9
      composition, 93fbf14 redpanda settle fix). Conflicts resolved keeping
      BOTH sides: scripts/pinned-images.txt (both-append: 3 exporter/grafana
      images + wireguard), schemas/v1alpha1/provider.json (main's newer
      type/configuration descriptions with this task's grafana + metrics
      prose spliced in; grafana AND wireguard/jdbcsink/s3source all in
      x-known-values; JSON re-validated), docs/reference regenerated from
      the merged schema (now includes explain.md), TASK_PROGRESS.md kept
      ours (main deletes it at each wave merge). Post-merge finding: main's
      E4 explain-catalog completeness archtest required entries for this
      task's five new reasons — added to
      internal/domain/status/catalog.go (metrics-exporter + grafana areas)
      and docs/reference/explain.md regenerated.
- [x] Step 4c (coordinator-directed): `platformctl lint` (H1, new on main)
      run over all three monitoring-completion-scenario tiers with the
      gate enabled: info-severity findings only (DL010 unbound
      EventStream, DL012 unresolved Providers — inherent to a monitoring
      scenario that deliberately has no data-flow Bindings; no
      Dataset/Source exists so no deletionPolicy findings), `--strict`
      exit 0. Nothing to fix; no waivers needed. No blueprint references
      monitoring (grep: zero hits), so no blueprint lint leg.
- [x] Step 5: verify — see Verification log below. All green, including
      post-merge unfiltered `go test ./...` exit 0 (not grep-filtered).
- [x] Step 6: doc sync done (schema + doc 03 + doc 08 status note in the
      commits above).
- [x] Step 7: final commit (orchestrator-directed, 2026-07-22): squashed
      via the C4/D7-precedent `git reset --soft main` + single task
      commit. Per the orchestrator's standing rule, the post-merge
      `scripts/test-impact.sh --base main` run was NOT waited on as a
      commit precondition — the branch gate was already satisfied (live
      accept green twice: 40.3s pre-merge, 37.7s in the earlier sweep;
      unfiltered `go test ./...` exit 0 post-merge; lint --strict exit 0)
      and the sweep is re-verification the orchestrator's merge gate
      consumes from the shared ledger. Sweep still running at commit
      time: background task id `b57tylbfz`, log
      /tmp/claude-1000/-home-cascadura-git-platformctl/
      3ff96d5f-6a0c-4676-8628-0810b1d9fe68/tasks/b57tylbfz.output,
      18 suites selected (redpanda, cdc, sink, connect-ha-dlq, acceptance,
      lakehouse, chaos, prometheus, monitoring, ingress, blueprints,
      object-store-posture, trino, jdbcsink, s3source, wireguard, compose;
      backup ledger-deduped), queued on the shared flock behind a
      concurrent agent's wireguard suite when last observed. An earlier
      pre-merge full sweep (task `brnjtuewj`) ran 13/14 green including
      monitoring 37.7s and prometheus 14.4s; its only failure (sink) was a
      host-port collision with that same concurrent sweep ("Bind for
      127.0.0.1:18582 ... port is already allocated"), not a regression.

## Design record (so a resuming session doesn't re-derive it)

- **Exporter credential design**: a dedicated `<name>-monitor` role/user
  created (and password-rotated-on-demand) at reconcile time by the admin
  connection — never the admin credential itself, never a user-declared
  SecretReference. Password generated once (crypto/rand, 24 bytes,
  base64.RawURLEncoding), persisted by reading it back from the exporter
  container's own FileMount (mirrors liveSuperuser/liveRootPassword's
  read-back-for-idempotency pattern) so re-apply doesn't recreate the
  exporter container or rotate the SQL password pointlessly.
  - postgres: `GRANT pg_monitor TO <user>` (predefined least-privilege
    monitoring role, PG10+). Exporter env: DATA_SOURCE_URI (host:port/db,
    non-secret) + DATA_SOURCE_USER (non-secret) + DATA_SOURCE_PASS_FILE
    (file mount) — postgres_exporter's own file-based credential support
    (verified live), so NO deviation needed for postgres: the password
    never touches env.
  - mysql/mariadb: `GRANT PROCESS, REPLICATION CLIENT, SELECT ON *.* TO
    <user>` (mysqld_exporter's documented minimum). Exporter given
    `--config.my-cnf=<path>`, a fully file-mounted my.cnf ([client]
    host/port/user/password) — mysqld_exporter's own file-based credential
    support (verified live). NO deviation needed for mysql either: neither
    user nor password touches env.
  - Both exporter ports: `Audience: AudienceInternal` (task's explicit
    instruction) — no host publish; only prometheus (same network) scrapes
    them.
- **Prometheus stays assert-only**: confirmed by reading
  internal/application/engine.go's resolveMetricsTargets — JobName is
  keyed by the *Provider resource's own name* (`key.Name`), and the scrape
  target is whatever `Endpoint.Internal` the provider published under
  `Name == "metrics"`. postgres/mysql publish one more such fact
  (pointing at the exporter's internal address); nothing in
  internal/adapters/providers/prometheus changes.
- **Grafana → Prometheus wiring (ADR 015)**: new `Request.PrometheusURL`
  field (bare string, mirrors SchemaRegistryURL, not a new struct — only
  one fact is needed unlike CatalogFacts's three). Resolved in engine.go
  the same way resolveCatalogFacts resolves its warehouse Provider: an
  explicit `configuration.prometheusRef` wins; otherwise the sole
  `type: prometheus` Provider in the namespace; ambiguous (0 or >1
  candidates without an explicit ref) leaves it unresolved (empty string).
  Never constructed by the grafana provider itself.
- **Grafana admin credential**: required SecretReference (ValidateSpec
  enforces `secretRefs` is non-empty), mounted via GF_SECURITY_ADMIN_USER
  (env, non-secret) + GF_SECURITY_ADMIN_PASSWORD__FILE (file mount) —
  Grafana's own file-based admin-credential support (verified live).
  Deviation recorded: Grafana only applies GF_SECURITY_ADMIN_PASSWORD
  (__FILE) when first creating the admin user in its own sqlite db (a
  documented Grafana limitation, not this codebase's gap) — unlike
  postgres/mysql's rotation state machine, a changed SecretReference after
  first apply is NOT rotated live; recorded as a known limitation rather
  than solved (out of this slice's scope). Anonymous access left at
  Grafana's own default (off); not explicitly set to keep the container
  config minimal.
- **Gate reuse**: grafana registered under the existing
  `MonitoringStackProvider` gate (no new gate) — same registry.Provider()
  mechanism prometheus already uses. postgres/mysql's `metrics: enabled`
  additionally requires this gate too, enforced the same way
  HighAvailability/SchemaRegistrySupport are (cmd/platformctl's
  loadAndValidate, since a SpecValidator has no feature-gate access by
  design — docs/adr/017 §a.8).

## Design record — the "Finding" (Accept-criteria tension, resolved not routed)

The task's Accept list asks for both the exporter's `Audience: internal`
fact (never host-published, per the task's own explicit instruction) and
for `inventory --for prometheus` to include the exporter targets. As
written these are in tension: `cmd/platformctl/toolconfig.go`'s
`gatherToolFacts` uniformly skipped any endpoint fact whose `Host` field is
empty, for every endpoint kind, before this task. Resolved (small, scoped,
in-family with the existing code, so fixed directly rather than surfaced as
a blocking question): the `"metrics"` case now falls back to `ep.Internal`
when `ep.Host` is empty — a legitimate bring-your-own-Prometheus target for
a Prometheus container joined to the same runtime network, just not from
an arbitrary external host. Pinned by
`TestGatherToolFactsFallsBackToInternalForMetrics`
(`cmd/platformctl/toolconfig_test.go`); every other endpoint kind's
behavior (s3/kafka/postgres/mysql/trino) is unchanged.

## Verification log

- `gofmt -l .`: empty (repeatedly, after every increment).
- `go build ./...`, `go build -tags integration ./...`: clean.
- `go vet ./...`, `go vet -tags integration ./...`: clean.
- `go test ./...`: green throughout (unit + archtest, including the
  loopback and reason-literal arch tests, which this task's CMD-SHELL
  healthchecks and new `status` reasons pass).
- Live (real Docker), each run individually green:
  - `TestMonitoringStackCompletionEndToEnd`
    (`cmd/platformctl/monitoring_completion_integration_test.go`): 40.3s.
    Three-tier apply (infra → +prometheus → +grafana against the same
    state file); `/api/v1/targets` shows `mon-redpanda`/`mon-minio`/
    `mon-postgres`/`mon-mysql` all `up`; Grafana's
    `/api/datasources/uid/prometheus/health` reports `OK`; the starter
    dashboard (`uid: datascape-overview`) 200s; `inventory --for
    prometheus` names both exporter jobs and their in-network targets;
    idempotent re-apply (all 8 managed containers' IDs unchanged); clean
    destroy.
  - `TestPrometheusMonitoringStackEndToEnd` (regression check, unmodified):
    13.8s, still green — confirms zero prometheus-package behavior change.
  - `TestCDCEndToEnd`/`TestAvroCDCEndToEnd`/`TestMariaDBCDCEndToEnd`/
    `TestCDCAttendanceExampleOnKubernetes`: 171.7s combined, all green —
    postgres/mysql with `metrics` unset (the default/existing-manifest
    path) unaffected on both Docker and Kubernetes runtimes.
  - `TestBackupRestorePostgresRoundTrip`/`TestBackupRestoreMySQLRoundTrip`/
    `TestBackupRestoreS3DatasetRoundTrip`: 76.8s combined, all green —
    postgres/mysql's `BackupCapableProvider` path unaffected.
- `scripts/test-impact.sh --base main` (full sweep, in progress at the time
  of the final commit — result recorded in the commit body, not repeated
  here to avoid a stale duplicate).
