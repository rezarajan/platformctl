package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/rezarajan/platformctl/internal/adapters/providers/dbbackup"
	"github.com/rezarajan/platformctl/internal/adapters/providers/dbjob"
	"github.com/rezarajan/platformctl/internal/adapters/providers/providerkit"
	"github.com/rezarajan/platformctl/internal/domain/backup"
	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/source"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// pgpassPath is where the job containers mount a .pgpass file so the
// superuser password never rides an env var or a command-line argument
// (docs/planning/07 Gate 1 checkbox 4's convention, applied to pg_dump/psql
// the same way reconcileInstance applies it to the server itself).
const pgpassPath = "/run/datascape/.pgpass"

// escapePgpass applies the .pgpass file's own escaping rule (PostgreSQL
// docs: backslash and colon are escaped with a backslash) so a password
// containing either character round-trips correctly.
func escapePgpass(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `:`, `\:`)
	return r.Replace(s)
}

// pgpassFile renders the .pgpass line pg_dump/psql authenticate with,
// shared by Backup's dump side and Restore's replay side (the only
// difference between the two is which database name the line grants).
func pgpassFile(dbHost, dbName, user, pass string) []byte {
	return []byte(fmt.Sprintf("%s:5432:%s:%s:%s", escapePgpass(dbHost), escapePgpass(dbName), escapePgpass(user), escapePgpass(pass)))
}

// Backup implements reconciler.BackupCapableProvider: pg_dump streamed to
// dest via the shared job-container pipeline
// (internal/adapters/providers/dbbackup, internal/adapters/providers/dbjob)
// on the Source's own database network — pg_dump never runs as this
// process, and the dump's bytes never pass through it either.
func (p *Provider) Backup(ctx context.Context, req reconciler.Request, dest backup.Location) (backup.Manifest, error) {
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return backup.Manifest{}, err
	}
	src, err := source.FromEnvelope(req.Resource)
	if err != nil {
		return backup.Manifest{}, err
	}
	dbName, _ := src.EngineConfig["database"].(string)
	if dbName == "" {
		return backup.Manifest{}, fmt.Errorf("Source %q: spec.postgres.database is required", req.Resource.Metadata.Name)
	}
	suUser, suPass, err := superuser(cfg, req.Secrets, naming.RuntimeObjectName(req.Provider))
	if err != nil {
		return backup.Manifest{}, err
	}
	prof, err := profile(cfg)
	if err != nil {
		return backup.Manifest{}, err
	}
	dbHost := naming.RuntimeObjectName(req.Provider)

	return dbbackup.Backup(ctx, req, dest, dbName, cfg, dbbackup.EngineProfile{
		ProviderType: p.Type(),
		Format:       "postgres/pg_dump-plain",
		BuildDumpSide: func(dbName string) dbjob.Side {
			return dbjob.Side{
				Image:    prof.Image,
				Networks: []string{providerkit.Network(cfg)},
				Env: map[string]string{
					"PGHOST":     dbHost,
					"PGPORT":     "5432",
					"PGUSER":     suUser,
					"PGDATABASE": dbName,
					"PGPASSFILE": pgpassPath,
				},
				Files: []runtime.FileMount{{Path: pgpassPath, Content: pgpassFile(dbHost, dbName, suUser, suPass), Mode: 0o600}},
				// --no-publications: a publication (e.g. "dbz_publication",
				// Debezium's default) is provider-managed infrastructure —
				// reconcileDatabase's own ensurePublication (postgres/sql.go)
				// idempotently recreates it on a fresh database, same as any
				// other reconciled object. Without this flag, pg_dump's plain
				// SQL dump embeds a CREATE PUBLICATION statement for it too,
				// which then collides ("publication ... already exists") with
				// the one the Source's own reconcile already created on the
				// freshly re-applied database, aborting the restore partway
				// through (found live: restore replayed the table data
				// successfully, then failed on this line, per ON_ERROR_STOP=1).
				ShellCmd: "pg_dump -h \"$PGHOST\" -p \"$PGPORT\" -U \"$PGUSER\" -d \"$PGDATABASE\" -F p --no-publications",
			}
		},
	})
}

// Restore implements reconciler.BackupCapableProvider: verify-then-promote
// (docs/adr/007-backup-restore.md addendum 2, docs/planning/08 I13) via the
// shared dbbackup orchestration — streams src back through the same job
// mechanism in reverse (mc reads the object, psql replays it) into a
// SCRATCH database, never the live target; only once the streamed content's
// checksum is verified good does an atomic rename-swap promote the scratch
// database over the target. On any failure — disk headroom, pipeline, or
// integrity check — the scratch database is dropped and the target is left
// completely untouched. The restore-over-existing-data safety gate remains
// the engine's job, enforced before Restore is ever called.
func (p *Provider) Restore(ctx context.Context, req reconciler.Request, src backup.Location) error {
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return err
	}
	dbSrc, err := source.FromEnvelope(req.Resource)
	if err != nil {
		return err
	}
	dbName, _ := dbSrc.EngineConfig["database"].(string)
	if dbName == "" {
		return fmt.Errorf("Source %q: spec.postgres.database is required", req.Resource.Metadata.Name)
	}
	suUser, suPass, err := superuser(cfg, req.Secrets, naming.RuntimeObjectName(req.Provider))
	if err != nil {
		return err
	}
	dbHost := naming.RuntimeObjectName(req.Provider)
	prof, err := profile(cfg)
	if err != nil {
		return err
	}

	return dbbackup.Restore(ctx, req, src, dbName, cfg, dbbackup.EngineProfile{
		Port:           5432,
		DBHost:         dbHost,
		Image:          prof.Image,
		DataMount:      prof.DataMount,
		EnsureDatabase: ensureDatabase,
		DropDatabase:   dropDatabase,
		AdminConn: func(addr string) string {
			return connStringAddr(addr, suUser, suPass, "postgres", nil)
		},
		BuildReplaySide: func(scratchName string) dbjob.Side {
			return dbjob.Side{
				Image:    prof.Image,
				Networks: []string{providerkit.Network(cfg)},
				Env: map[string]string{
					"PGHOST":     dbHost,
					"PGPORT":     "5432",
					"PGUSER":     suUser,
					"PGDATABASE": scratchName,
					"PGPASSFILE": pgpassPath,
				},
				Files:    []runtime.FileMount{{Path: pgpassPath, Content: pgpassFile(dbHost, scratchName, suUser, suPass), Mode: 0o600}},
				ShellCmd: "psql -h \"$PGHOST\" -p \"$PGPORT\" -U \"$PGUSER\" -d \"$PGDATABASE\" -v ON_ERROR_STOP=1",
			}
		},
		// PromoteAndCleanup: postgres's ALTER DATABASE RENAME pair is fully
		// transactional (docs/adr/007-backup-restore.md addendum 2) — no
		// aside database needs creating first, and only the one aside-
		// renamed pre-restore database is left to best-effort drop
		// afterward (known limitation (e): a failed drop here is a
		// harmless, named leftover, never this call's own failure).
		PromoteAndCleanup: func(ctx context.Context, admin, resourceName, dbName, scratchName, oldName string, warnf func(string, ...any)) error {
			if err := promoteDatabase(ctx, admin, dbName, scratchName, oldName); err != nil {
				_ = dropDatabase(ctx, admin, scratchName)
				return fmt.Errorf("Source %q: postgres restore: %w", resourceName, err)
			}
			if err := dropDatabase(ctx, admin, oldName); err != nil {
				warnf("Source %q: postgres restore: promoted successfully, but dropping the pre-restore database %q failed (harmless leftover, drop it by hand): %v", resourceName, oldName, err)
			}
			return nil
		},
	})
}
