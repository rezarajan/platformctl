# Upgrade notes

Dated entries for behavioral migrations — a change that is correct and
intentional, but produces a one-time visible effect (a drift report, a
restart, a config rewrite) the first time a binary crossing the change is
run against pre-existing state. Ordinary bug fixes and new features don't
need an entry here; only migrations that would otherwise look like a
regression to an operator who wasn't told.

## 2026-07-16 — MySQL/MariaDB CDC `database.server.id` is now unique per connector

**Affects:** any MySQL or MariaDB `Binding(mode: cdc)` registered by a
`platformctl` binary built before commit `09e1b61`.

**What changed:** the Debezium provider's `database.server.id` used to be
computed as `184000 + len(connectorClass)` — effectively constant per
database engine, so two MySQL/MariaDB CDC connectors registered against the
same server carried the *same* replication `server_id`, which is invalid
for MySQL replication (every replication client must have a distinct,
non-zero `server_id`; connectors sharing one can silently kick each other's
binlog session). It now derives from the connector (Binding) name via
FNV-1a (`internal/adapters/providers/debezium/debezium.go`, `serverID`),
which is unique per connector and deterministic (plan output stays
reproducible).

**What you'll see:** the first `platformctl drift` run against existing
state after upgrading reports `ConnectorConfigDrift` on `database.server.id`
for every pre-existing MySQL/MariaDB CDC Binding (this is the new
config-equivalence probe added alongside — see `docs/planning/07` §2.1 —
correctly noticing the live connector's `server_id` no longer matches the
manifest-derived one). Running `apply` (with `DriftDetection` enabled, the
default) heals it: the connector config is re-PUT with the new `server_id`.

**What it does *not* do:** no snapshot is re-run and no data is
re-streamed. Debezium resumes streaming from its recorded binlog offset;
the connector's replication session restarts once (a few seconds of no new
CDC events), not a resync.

**Action required:** none — this is self-healing on the next `apply`. If
you run `drift` in CI and treat any nonzero drift count as a failure,
expect exactly one such report per pre-existing MySQL/MariaDB CDC Binding
on the first run after upgrading, and none afterward.

See `docs/planning/07-production-grade-docker-runtime-gap-analysis.md` §2.2
for the original defect this fixed, and `docs/remediation/F-006-serverid-migration-drift-note.md`
for the audit finding that flagged the missing note.

## 2026-07-23 — state gains a top-level `mediationFabric` key

**Affects:** anyone reading/parsing state files with external tooling, or
diffing state files across an upgrade.

**What changed:** `State` (`internal/ports/state/state.go`) gained a new
top-level field, `MediationFabric *MediationFabricState` (`json:
"mediationFabric,omitempty"`), recording the engine-owned platform
mediation fabric introduced by docs/planning/08 Stage L2. It is
deliberately not a `resources` entry — putting it there was tried and
found, live, to self-destruct on the next unrelated `plan`/`apply`/
`destroy` (the orphan sweep would treat it as a deleted user resource).

**What you'll see:** every state file predating L2, and every state file
where `MediatedTransport` is never used, keeps `mediationFabric` absent
(`omitempty`) — a byte-identical `resources` block, and old state files
deserialize with this field simply nil. The key appears for the first
time only once a mediation fabric is actually ensured (which requires the
Alpha/disabled `MediatedTransport` gate, itself still a no-op in this
release — see the L1-L2a note in `docs/adr/034`).

**Action required:** none for this release — `MediatedTransport` ships
Alpha/disabled and the fabric-provisioning path it guards is not reachable
by default. External tooling that asserts a *closed* schema on state
files (rejecting unknown top-level keys) will need updating before this
gate is ever turned on in a later release.

## 2026-07-23 — `Remove` now also cleans up derived residue (ADR 029)

**Affects:** any operator with monitoring/dashboards that count Docker
volumes or Kubernetes objects, or automation that lists these objects
directly rather than through `platformctl inventory`/`gc`.

**What changed:** per docs/adr/029 (residue-free lifecycle), a managed
container's teardown now also removes objects it derived rather than
declared: on Docker, image-declared anonymous volumes created alongside
the container (previously leaked on every `Remove`); on Kubernetes,
derived `Service`s (including alias Services), the per-container files
`Secret`, and (for StatefulSets) the `PodDisruptionBudget` are now deleted
by `Remove`/`removeDeployment`/`removeStatefulSet`/
`removeCommonContainerObjects` (`internal/adapters/runtime/kubernetes/
container_remove.go`), not just the Deployment/StatefulSet itself.

**What you'll see:** on the first `platformctl apply`/`destroy`/`gc` cycle
after upgrading that removes or recreates any managed container, disk
usage frees up and `docker volume ls` / `kubectl get secrets,services,pdb`
shrink — residue accumulated by pre-upgrade binaries gets swept as part of
ordinary teardown, not silently left behind. This is a one-time visible
drop, not a recurring one: going forward, residue simply never
accumulates.

**Action required:** none — this is a strict improvement (less orphaned
state). If you have alerting on Docker volume count or Kubernetes object
count trending down sharply after an upgrade, expect it once.

## 2026-07-23 — policy decisions now log structured events on stderr

**Affects:** operators who scrape/parse `platformctl`'s stderr output
(e.g. shipping it to a log pipeline) when the `PolicyEngine` gate is
enabled and a `--policies` directory is supplied.

**What changed:** docs/planning/08 Stage K5 (ADR 033 decision 5) added a
structured decision-audit event, logged on the same `--log-format`
seam reconciliation actions already use, for every policy rule evaluated
during `validate`/`plan`/`apply`/`destroy` — carrying `resource`, `rule`,
`effect`, `outcome`, `exempted`, and `msg` fields (JSON with
`--log-format json`; a text line with the default format).

**What you'll see:** with `PolicyEngine=true` and a policies directory
passed, new lines appear on stderr, one per evaluated rule, in addition
to any existing warn/error output. With the gate off (the default) or no
policies directory, output is unaffected — zero decision events are
emitted, matching the pre-upgrade byte-for-byte. **stdout — the
machine-output contract for `--output json`/`yaml` — is completely
unchanged** by this feature; only stderr gains lines, and only when
`PolicyEngine` is already turned on.

**Action required:** none unless you enable `PolicyEngine` and parse
stderr with a strict line-format assumption — in that case, expect one
new JSON (or text) line per evaluated policy rule per command run.

## 2026-07-23 — backup timestamps are now lowercase (`20060102t150405z`)

**Affects:** any operator or tooling keyed on the previous uppercase
timestamp format in backup Job names or backup object keys (both database
backup providers: postgres, mysql).

**What changed:** the live Kubernetes Job backup path failed outright —
`'...-backup-20260723T071734Z-producer-files' is not a valid RFC 1123
subdomain` (Kubernetes object names must be lowercase). The fix
(`internal/adapters/providers/{postgres,mysql}/backup.go`,
`internal/ports/runtime/job.go`) changes the timestamp format used in
both backup Job names *and* backup object keys (kept in sync for
greppable parity) from an uppercase `RFC3339`-ish layout to
`internal/domain/naming.TimestampFormat` (`20060102t150405z`, always
lowercase, always UTC).

**What you'll see:** every backup taken by a `platformctl` binary built
at or after this commit uses the new lowercase timestamp in its object
key and (on Kubernetes) its Job name. Backups taken by older binaries
keep their old uppercase object keys — nothing renames or migrates
existing objects. On Kubernetes specifically, this is not just a
formatting change but a correctness fix: the old format could not
actually create a Job at all, so this was a hard blocker there, not a
cosmetic difference.

**Action required:** if you have scripts, retention tooling, or restore
automation that parses backup object-key timestamps with a
case-sensitive or uppercase-specific pattern, update it to accept
`20060102t150405z` (lowercase) going forward; existing uppercase-keyed
objects from before this upgrade are unaffected and remain restorable by
name.

<!-- Gate-graduation-driven migrations (e.g. any newly-enabled-by-default
     feature gate) for v1.3.0 are pending a separate, in-progress
     graduation pass (owned outside this document's authoring session) —
     entries will be appended here once that list is final. -->
