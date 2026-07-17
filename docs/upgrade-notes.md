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
