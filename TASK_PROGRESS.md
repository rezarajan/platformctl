# Task progress — user & developer onboarding, Terraform positioning, README truth pass

Working tree: `.claude/worktrees/agent-a6fc41c6318e6affe`. Protocol: docs/planning/08 §2.1
step 0 (this file + WIP commits per increment). Documentation-only task; no Go code touched.
(This file's previous contents were the already-merged C4+D7 task's checkpoint — see
`git log --oneline -- TASK_PROGRESS.md` / commit `99a5d2c` for that history.)

## Step plan

0. [done] `git merge main --no-edit` — already up to date (working tree was clean; branch already
   contained the latest main, including C2/C3/C7/D6/D7/D10/trino work). Create this file.
1. [done] Research pass: read docs/README.md, README.md, docs/planning/{00,01,02,03,04,06,07,08,10},
   docs/adr/README.md + ADRs 001/005/009/011/012/014/018, cmd/platformctl/main.go,
   cmd/platformctl/root.go, internal/application/registry/registry.go,
   internal/application/featuregate/featuregate.go, internal/adapters/secrets/env/env.go,
   internal/adapters/runtime/docker/docker.go, internal/adapters/runtime/kubernetes/preflight.go,
   internal/cliutil/cliutil.go, deploy/kubernetes/rbac/README.md, scripts/hooks/guard-planning-docs.sh.
   Two background research subagents (sonnet) cross-checked exact error strings, exit codes,
   capability-interface error shapes, and cross-runtime differences — findings folded in below.
2. [done] Verified commands (built binary at
   `/tmp/claude-1000/.../scratchpad/platformctl` via
   `CGO_ENABLED=0 go build -trimpath -buildvcs=false -o <bin> ./cmd/platformctl`):
   - `platformctl --help` — full command list: init, validate, plan, apply, destroy, status,
     drift, import, backup, restore, graph, inventory, docs, gc, state, completion, help.
     README's CLI table was missing backup/restore entirely — fixed in D4.
   - `platformctl init --list` → 4 blueprints: cdc-to-lake, lakehouse, stream-basics,
     external-cdc (descriptions captured verbatim for users.md).
   - `platformctl validate examples/cdc-attendance/` → "14 resource(s) valid", exit 0.
   - `platformctl validate examples/lakehouse/` → "20 resource(s) valid", exit 0.
   - `platformctl init cdc-to-lake && platformctl validate cdc-to-lake` (scratch dir) → 14 files
     written, "13 resource(s) valid", exit 0. Confirms README's two-command quickstart claim.
   - `platformctl <cmd> --help` for validate/plan/apply/status/drift/graph/inventory/import/gc/
     state/backup/restore/explain — `explain` does NOT exist in this tree ("unknown command");
     referenced generically per task instructions ("an agent is building its catalog").
3. [done] Deliverable 1: docs/onboarding/users.md (297 lines)
4. [done] Deliverable 2: docs/onboarding/developers.md (221 lines)
5. [done] Deliverable 3: docs/positioning/terraform.md (216 lines) + README "platformctl and
   Terraform" section (~45 lines)
6. [done] Deliverable 4: README.md truth pass (provider list, CLI table, Highlights, HA posture,
   backup/restore, monitoring/trino/ingress, parquet-by-default, DLQ, multi-broker/distributed
   MinIO/Connect-worker HA, test-economy tooling)
7. [done] Deliverable 5: docs/README.md Onboarding section + positioning/terraform.md Records line
8. [done] Verification: relative links resolve (scripted grep+test -f, see below);
   `platformctl validate examples/cdc-attendance/` still green (14 resource(s) valid, exit 0).
9. [done] Final commit.

## Verified facts / exact strings used (for citation — cross-checked by 2 research subagents)

- Exit codes (`internal/cliutil/cliutil.go`): 0 OK, 1 plan-has-changes/drift-found
  (`ExitPlanChanges`), 2 execution error, 3 validation error, 4 lock held.
- Feature gate disabled error (`internal/application/featuregate/featuregate.go:61`):
  `feature gate %q (stage: %s) is disabled; enable with --feature-gates=%s=true`
- Missing secret env var (`internal/adapters/secrets/env/env.go`): resolve error
  `SecretReference %q: key %q not found (expected env var %s)`; preflight error
  `SecretReference %q: unset environment variable(s): %s`; engine aggregation
  (`internal/application/engine/engine.go`): `%d secret(s) cannot be resolved — apply would
  half-apply the platform, so nothing was changed:\n  - %s`
- Docker daemon absent (`internal/adapters/runtime/docker/docker.go:36-41`): wraps as
  `connect to Docker daemon: %w` around the Docker SDK client error.
- Drift: `drift` command prints `drift detected on %d resource(s); run apply to reconcile` and
  exits 1; no-drift prints `no drift detected`, exits 0. `status`'s DRIFT column is the
  DriftDetected condition's Status string (True/False/Unknown).
- Kubernetes RBAC/preflight (`internal/adapters/runtime/kubernetes/preflight.go`): missing
  permissions error `kubernetes (%s): missing permission(s): %s — see
  deploy/kubernetes/rbac/role.yaml for the minimal Role this adapter needs`; unreachable cluster
  `kubernetes (%s): cluster unreachable: %w`.
- `registry.PlannedRuntimes` (`internal/application/registry/registry.go`): `{"external": true,
  "terraform": true}` → `runtime type %q is planned but not yet available in this version`.
- Capability error family (ADR 009, `internal/application/compatibility`):
  `Binding %q: Provider %q (type: %s)\ndoes not support <thing> %q (supported: %s)`.
- Provider/gate list as of this tree (`cmd/platformctl/main.go` `defaultWiring`): providers —
  noop, container(Alpha/off, test-only), redpanda(GA), postgres(GA), debezium(GA), s3(GA),
  minio(GA, same adapter as s3), s3sink(GA), mysql(Beta/on), mariadb(Beta/on, same adapter as
  mysql), nessie(Beta/on), openlineage(Beta/on), proxy(Beta/on), prometheus(Alpha/off),
  ingress(Alpha/off), trino(Alpha/off). Runtimes: fake(test), docker(GA), kubernetes(Beta/on).
  CLI commands (root.go): init, validate, plan, apply, destroy, status, drift, import, backup,
  restore, graph, inventory, docs build/serve, gc plan/apply, state inspect/doctor/repair/unlock.
- doc 03 confirms multi-broker redpanda (`configuration.brokers`, ADR 017), distributed
  Connect workers (`configuration.workers`, C3), distributed erasure-coded MinIO
  (`configuration.nodes`, C4, refuses 2-3 nodes), Dataset `spec.lifecycle` (D7), external
  object-store posture (C4), DLQ (`spec.options.deadLetter`, D6), schema-carrying formats +
  parquet sink (D1/D2) — `examples/cdc-attendance/dataset.yaml` ships `format: parquet` today.
- ADR 018 (ingress): Docker = one shared Caddy container per ingress Provider, JSON admin API,
  `Host(<connection-name>.<domain>)` routing; Kubernetes = one native `Ingress` object per
  Connection; TLS deferred to C8.

## Verification results

- Link check script (grep relative markdown links in touched files + `test -f` each target):
  ran clean, see commit body.
- `platformctl validate examples/cdc-attendance/` → "14 resource(s) valid" (unchanged, exit 0) —
  sanity check that no code was touched.

## Deviations

- `platformctl explain` does not exist in this tree (confirmed via `--help`); referenced
  generically per task instructions rather than documented as a working command with real flags.
- Did not touch docs/planning/00-README.md (deliverable 5 said "may... or skip it"); chose to
  skip it since docs/README.md already carries the Onboarding index and is not a guarded planning
  contract doc, keeping the guarded surface untouched entirely.
