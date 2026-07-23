# J4 progress

Task: docs/planning/08-production-readiness-plan.md "### J4: Database
backup orchestration dedup — one harness in providerkit" — extract
postgres/backup.go and mysql/backup.go's ~580 near-duplicated lines of
verify-then-promote backup/restore orchestration into one implementation.

## Setup
- Worktree merged to main tip 0456b72 — fast-forward, already current, no
  conflicts.
- Read docs/adr/007-backup-restore.md in full (original decision + I12/I13/
  I15 addenda + the 2026-07-23 merge-gate amendment), both backup.go files,
  dbjob (untouched, per instructions), providerkit, and the integration test
  suite (cmd/platformctl/backup_integration_test.go,
  backup_kubernetes_integration_test.go) to pin every string/behavior that
  must stay byte-identical.

## Design
- New package `internal/adapters/providers/dbbackup` (sibling to dbjob, not
  providerkit — providerkit already covers unrelated concerns (endpoint
  resolution, TLS, instance mgmt); dbbackup mirrors dbjob's own naming and
  scope exactly).
- `dbbackup.EngineProfile` parameterizes: ProviderType/Format (manifest
  fields), BuildDumpSide/BuildReplaySide (dbjob.Side builders), Port/DBHost/
  Image/DataMount (headroom + dial), AdminConn/EnsureDatabase/DropDatabase
  (admin SQL), PromoteAndCleanup (the one genuinely divergent tail — mysql's
  RENAME TABLE batch needs an aside schema created first and leaves two
  leftovers to drop; postgres's ALTER DATABASE RENAME pair needs neither).
- `dbbackup.Backup`/`dbbackup.Restore` own everything identical in shape:
  objectKey derivation, the dump/manifest-persist pipeline (Backup), and
  manifest-read -> headroom -> scratch-restore -> verify (Restore).
- Byte-identical-behavior trick: every error-wrap that named the engine
  ("Source %q: postgres backup: %w" vs mysql's dynamic cfg.Type) now
  uniformly uses `cfg.Type` — provably identical for postgres because the
  registry only ever instantiates postgres.Provider under type name
  "postgres" (main.go's RegisterProvider("postgres", ...)), so cfg.Type is
  always the literal "postgres" there too. The ONE asymmetry NOT unified:
  the "spec.<X>.database is required" message uses a literal "postgres" for
  postgres but the Source's own declared `src.Engine` for mysql (this was
  already inconsistent pre-extraction, in mysql.go's reconcileSource too —
  preserved via each wrapper computing dbName/its own error text itself,
  BEFORE calling into dbbackup, so dbbackup never needs to know about it).
- postgres/backup.go and mysql/backup.go now: decode cfg/src, validate
  dbName (their own pre-existing error text), resolve credentials + version
  profile (unchanged per-package helpers), then build one EngineProfile and
  call dbbackup.Backup/Restore.
- dbjob unchanged (not touched, per instructions).

## Status
- [x] Read all required docs/ADR/precedent files.
- [x] internal/adapters/providers/dbbackup/dbbackup.go written.
- [x] postgres/backup.go shrunk to profile + engine SQL helpers (in sql.go,
      unchanged).
- [x] mysql/backup.go shrunk to profile + engine SQL helpers (sql.go
      unchanged); dbSide/dumpTool/restoreTool/clientCnf kept (used by the
      profile closures, still covered by
      TestDBSideDoesNotInterpolateDatabaseName).
- [x] gofmt clean; `go build ./...` clean; `go vet ./...` and
      `go vet -tags integration ./...` clean.
- [x] `go test ./...` green (62 ok packages, 0 FAIL) — archtest
      (naming-authority, adapter-streams, wrapper-completeness) all green.
- [x] golangci-lint v2.12.2 clean (both scoped and full-repo run).
- [ ] Live: `go test -tags integration -count=1 -run 'TestBackupRestore'
      -timeout 1800s ./cmd/platformctl/` under flock — queued behind another
      session's sweep, running now.
- [ ] Doc 08 J4 Done-note (additive).
- [ ] Final commit.
