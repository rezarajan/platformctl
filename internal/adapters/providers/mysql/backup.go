package mysql

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

// clientCnfPath is where the job containers mount a --defaults-extra-file
// so the root password never rides an env var or a command-line argument
// (docs/planning/07 Gate 1 checkbox 4's convention, applied to
// mysqldump/mysql the same way reconcileInstance applies it to the server
// itself).
const clientCnfPath = "/run/datascape/client.cnf"

// escapeCnf quotes a my.cnf option value, escaping backslash and the
// closing quote so a password containing either survives intact.
func escapeCnf(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `"`, `\"`)
	return `"` + r.Replace(s) + `"`
}

func clientCnf(host, user, pass string) []byte {
	return []byte(fmt.Sprintf("[client]\nhost=%s\nport=3306\nuser=%s\npassword=%s\n", host, user, escapeCnf(pass)))
}

// dumpTool/restoreTool pick the engine-native binary name: recent
// MariaDB images ship mariadb-prefixed client tools, mirroring the same
// engine-vs-adminTool branch reconcileInstance's healthcheck already uses.

// dbSide builds the mysql/mariadb end of a backup/restore pipeline. The
// database name rides in an env var and is expanded quoted by the shell —
// never interpolated into the sh -c text: a manifest-declared database
// name containing shell metacharacters must not execute inside a job
// container that holds root DB and object-store credentials (doc 11 B4
// finding 1 — postgres's PGDATABASE pattern, applied here; pinned by
// TestDBSideDoesNotInterpolateDatabaseName).
func dbSide(tool string, cfg provider.Provider, image, dbHost, rootPass, dbName string) dbjob.Side {
	return dbjob.Side{
		Image:    image,
		Networks: []string{providerkit.Network(cfg)},
		Env:      map[string]string{"DATASCAPE_BACKUP_DATABASE": dbName},
		Files:    []runtime.FileMount{{Path: clientCnfPath, Content: clientCnf(dbHost, "root", rootPass), Mode: 0o600}},
		ShellCmd: fmt.Sprintf("%s --defaults-extra-file=%s \"$DATASCAPE_BACKUP_DATABASE\"", tool, clientCnfPath),
	}
}

func dumpTool(cfg provider.Provider) string {
	if mariadb(cfg) {
		return "mariadb-dump"
	}
	return "mysqldump"
}

func restoreTool(cfg provider.Provider) string {
	if mariadb(cfg) {
		return "mariadb"
	}
	return "mysql"
}

// Backup implements reconciler.BackupCapableProvider: mysqldump/mariadb-dump
// streamed to dest via the shared job-container pipeline
// (internal/adapters/providers/dbbackup, internal/adapters/providers/dbjob).
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
		return backup.Manifest{}, fmt.Errorf("Source %q: spec.%s.database is required", req.Resource.Metadata.Name, src.Engine)
	}
	rootPass, err := rootPassword(cfg, req.Secrets, naming.RuntimeObjectName(req.Provider))
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
		Format:       cfg.Type + "/" + dumpTool(cfg) + "-sql",
		BuildDumpSide: func(dbName string) dbjob.Side {
			return dbSide(dumpTool(cfg), cfg, prof.Image, dbHost, rootPass, dbName)
		},
	})
}

// Restore implements reconciler.BackupCapableProvider: verify-then-promote
// (docs/adr/007-backup-restore.md addendum 2, docs/planning/08 I13) via the
// shared dbbackup orchestration — streams src back through the same job
// mechanism in reverse (mc reads the object, mysql/mariadb replays it) into
// a SCRATCH schema, never the live target; only once the streamed content's
// checksum is verified good does an atomic RENAME TABLE batch promote the
// scratch schema's tables over the target's. On any failure — disk
// headroom, pipeline, or integrity check — the scratch schema is dropped
// and the target is left completely untouched. The restore-over-existing-
// data safety gate remains the engine's job, enforced before Restore is
// ever called.
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
		return fmt.Errorf("Source %q: spec.%s.database is required", req.Resource.Metadata.Name, dbSrc.Engine)
	}
	rootPass, err := rootPassword(cfg, req.Secrets, naming.RuntimeObjectName(req.Provider))
	if err != nil {
		return err
	}
	dbHost := naming.RuntimeObjectName(req.Provider)
	prof, err := profile(cfg)
	if err != nil {
		return err
	}

	return dbbackup.Restore(ctx, req, src, dbName, cfg, dbbackup.EngineProfile{
		Port:           3306,
		DBHost:         dbHost,
		Image:          prof.Image,
		DataMount:      prof.DataMount,
		EnsureDatabase: ensureDatabase,
		DropDatabase:   dropDatabase,
		AdminConn: func(addr string) string {
			return dsnAddr(addr, "root", rootPass, "", nil)
		},
		BuildReplaySide: func(scratchName string) dbjob.Side {
			// Consumer replays into the SCRATCH schema, never dbName — the
			// verify-then-promote guarantee depends entirely on this:
			// nothing above this line has written to the live target at
			// all.
			return dbSide(restoreTool(cfg), cfg, prof.Image, dbHost, rootPass, scratchName)
		},
		// PromoteAndCleanup: unlike postgres's single ALTER DATABASE
		// RENAME pair, MySQL's RENAME TABLE batch needs an aside schema
		// created first to receive the target's tables, and leaves TWO
		// leftovers to best-effort drop afterward — the aside-renamed
		// pre-restore schema and the now-emptied scratch schema (known
		// limitation (e): a failed drop here is a harmless, named
		// leftover, never this call's own failure).
		PromoteAndCleanup: func(ctx context.Context, admin, resourceName, dbName, scratchName, oldName string, warnf func(string, ...any)) error {
			if err := ensureDatabase(ctx, admin, oldName); err != nil {
				_ = dropDatabase(ctx, admin, scratchName)
				return fmt.Errorf("Source %q: %s restore: create promote-aside database: %w", resourceName, cfg.Type, err)
			}
			if err := promoteDatabase(ctx, admin, dbName, scratchName, oldName); err != nil {
				_ = dropDatabase(ctx, admin, scratchName)
				_ = dropDatabase(ctx, admin, oldName)
				return fmt.Errorf("Source %q: %s restore: %w", resourceName, cfg.Type, err)
			}
			if err := dropDatabase(ctx, admin, oldName); err != nil {
				warnf("Source %q: %s restore: promoted successfully, but dropping the pre-restore schema %q failed (harmless leftover, drop it by hand): %v", resourceName, cfg.Type, oldName, err)
			}
			if err := dropDatabase(ctx, admin, scratchName); err != nil {
				warnf("Source %q: %s restore: promoted successfully, but dropping the now-empty scratch schema %q failed (harmless leftover, drop it by hand): %v", resourceName, cfg.Type, scratchName, err)
			}
			return nil
		},
	})
}
