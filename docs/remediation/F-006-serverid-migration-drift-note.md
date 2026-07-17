# F-006: `database.server.id` formula change makes pre-existing MySQL connectors report config drift (undocumented migration behavior)

**Severity:** Low (self-healing; but the first `drift` after upgrading looks
like a regression to an operator who wasn't told).
**Status:** RESOLVED (2026-07-17). `docs/upgrade-notes.md` created;
cross-linked from docs/planning/07 §2.2 and checkpoint.md. Implementation
also surfaced that §2.2's whole checklist (all seven items, this one
included) had never been ticked despite being fixed in the Gate 2
close-out — corrected in the same pass.

## Evidence

- Old formula (removed in `09e1b61`): `184000 + len(connectorClass)` —
  constant per engine.
- New: `serverID(connectorName)` (FNV-1a, `internal/adapters/providers/
  debezium/debezium.go`), and the Binding probe now diffs live connector
  config against the manifest-derived config (`connectorConfigDrift`,
  landed `5367d76`).
- Consequence: any MySQL/MariaDB CDC connector registered by an older
  binary carries the old `database.server.id`; the first `platformctl
  drift` with the new binary reports `ConnectorConfigDrift` naming
  `database.server.id`, and the next `apply` (with DriftDetection enabled)
  re-PUTs the config — restarting the connector's binlog session once.

This is correct behavior (the old ids were genuinely broken for multiple
connectors), but no release note or doc records the one-time drift +
connector restart.

## Root cause

Behavioral migration shipped without an operator-facing note.

## Required behavior

Documentation only (no code change):

1. Add a "Upgrade notes" entry — location: `errors.md` style is for
   incidents; use `checkpoint.md`'s hardening section *and* a new
   `docs/upgrade-notes.md` (create it, one dated section per behavioral
   migration) stating: after upgrading past `09e1b61`, existing MySQL
   CDC Bindings report `ConnectorConfigDrift` on `database.server.id` once;
   running `apply` heals it and restarts the connector's replication
   session (snapshot is not re-run; streaming resumes from the recorded
   binlog offset).
2. Cross-link from `docs/planning/07` §2.2 (the server-id bullet).

## Exact files and symbols

- New: `docs/upgrade-notes.md`.
- `docs/planning/07-production-grade-docker-runtime-gap-analysis.md` §2.2
  server-id bullet: add "(see docs/upgrade-notes.md for the one-time drift
  on upgrade)".
- Reference only: `internal/adapters/providers/debezium/debezium.go`
  `serverID`, `connectorConfigDrift`.

## Implementation constraints

- Docs only. Do not add code to suppress the one-time drift — masking real
  config differences is exactly what §2.1 prohibits.

## Tests / validation commands

None beyond doc review; optionally the out-of-band config-change
integration test tracked as open in §2.1 would cover this scenario
naturally when written.

## Dependencies / ordering

None.

## Risk

None (docs).

## Escalation conditions

Escalate if a release/versioning scheme exists that should carry upgrade
notes instead (none found in-repo beyond `main.version`).
