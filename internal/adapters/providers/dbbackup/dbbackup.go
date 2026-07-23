// Package dbbackup is the shared verify-then-promote backup/restore
// orchestration postgres's and mysql's reconciler.BackupCapableProvider
// implementations both use (docs/planning/08 J4; docs/adr/
// 007-backup-restore.md, its I12/I13/I15 addenda and their merge-gate
// amendment). Before this package existed, postgres/backup.go and
// mysql/backup.go carried ~580 near-duplicated lines of the same
// orchestration around a small engine-specific core — every backup fix that
// cycle (RuntimeType threading, Warnf wording, naming.Derived adoption)
// touched both files in lockstep. This package now implements that
// orchestration exactly once:
//
//   - Backup: dump -> objectKey -> dbjob pipeline -> manifest persist.
//   - Restore: manifest read -> disk-headroom precheck -> scratch restore ->
//     integrity verify -> atomic promote -> best-effort old-drop.
//
// Everything genuinely engine-specific — dump/replay command construction,
// admin-connection SQL, and promote semantics (postgres's single ALTER
// DATABASE RENAME pair vs. mysql's RENAME TABLE batch, which needs an aside
// schema created first and leaves two leftovers to drop instead of one) —
// is supplied by the caller as an EngineProfile, built from already-resolved
// engine-specific inputs (credentials, version profile, dump command)
// postgres/mysql resolve themselves before calling into this package. This
// package imports no engine-specific SQL of its own; postgres/mysql shrink
// to an EngineProfile plus their own engine-specific SQL helpers
// (internal/adapters/providers/postgres/sql.go,
// internal/adapters/providers/mysql/sql.go).
//
// The error-wrapping convention below (`"Source %q: %s backup/restore: %w"`
// with cfg.Type as the engine label) reproduces both engines' pre-extraction
// text exactly: cfg.Type is always "postgres" for a postgres Provider (the
// registry only ever instantiates postgres.Provider under that type name),
// so using cfg.Type uniformly here is byte-identical to postgres's old
// hardcoded "postgres" literal while remaining mysql's existing dynamic
// "mysql"/"mariadb" behavior unchanged.
package dbbackup

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/rezarajan/platformctl/internal/adapters/providers/dbjob"
	"github.com/rezarajan/platformctl/internal/adapters/providers/providerkit"
	"github.com/rezarajan/platformctl/internal/domain/backup"
	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// EngineProfile supplies everything Backup/Restore need that is genuinely
// engine-specific. Callers (postgres.Provider.Backup/Restore,
// mysql.Provider.Backup/Restore) resolve credentials, the version profile,
// and dbName themselves — in the same order and with the same error
// behavior their pre-extraction code had — before building one of these and
// calling into this package.
type EngineProfile struct {
	// ProviderType is the realizing Provider's own p.Type() ("postgres" or
	// "mysql" — mysql's is always "mysql" even for a mariadb Provider,
	// matching the pre-extraction manifest.ProviderType value exactly).
	ProviderType string
	// Format is backup.Manifest.Format for a successful Backup
	// ("postgres/pg_dump-plain"; mysql's own cfg.Type+"/"+tool+"-sql").
	Format string
	// BuildDumpSide builds Backup's producer Side (pg_dump/mysqldump/
	// mariadb-dump), given the already-validated database name.
	BuildDumpSide func(dbName string) dbjob.Side

	// Port is the engine's server port (postgres 5432, mysql/mariadb 3306),
	// used by Restore's providerkit.ReachableAddr dial and disk-headroom
	// precheck volume naming.
	Port int
	// DBHost is the running instance's own runtime object name
	// (naming.RuntimeObjectName(req.Provider)) — Restore's dial target and
	// the disk-headroom precheck's data-volume/instance name.
	DBHost string
	// Image/DataMount are the resolved version profile's image and data
	// mount path, used only by the disk-headroom precheck (I13).
	Image     string
	DataMount string
	// AdminConn builds an admin connection string from a freshly dialable
	// "host:port" — the engine's own DSN shape and admin database (postgres
	// connects to the "postgres" maintenance database; mysql's root has no
	// default database to name).
	AdminConn func(addr string) string
	// EnsureDatabase/DropDatabase are the engine's own admin-SQL helpers —
	// used identically by both engines for the scratch database's
	// lifecycle: created up front, best-effort dropped on any failure path.
	EnsureDatabase func(ctx context.Context, admin, name string) error
	DropDatabase   func(ctx context.Context, admin, name string) error
	// BuildReplaySide builds Restore's consumer Side (psql/mysql/mariadb),
	// wired to replay into scratchName rather than the live target — the
	// verify-then-promote guarantee depends entirely on this.
	BuildReplaySide func(scratchName string) dbjob.Side
	// PromoteAndCleanup performs the atomic promote plus every step
	// specific to how each engine models "database" — postgres's ALTER
	// DATABASE RENAME pair needs no aside database created first and
	// leaves one leftover to best-effort drop; mysql's RENAME TABLE batch
	// needs an aside schema created (ensureDatabase) before the rename and
	// leaves two leftovers (the aside pre-restore schema and the
	// now-emptied scratch schema) to best-effort drop, each its own Warnf
	// call. Deliberately not unified further: the two engines take a
	// genuinely different number of steps here, not just different SQL
	// text. warnf is req.Warnf's own signature — any post-success
	// cleanup-failure must be reported through it, never returned as this
	// call's own error (docs/adr/007-backup-restore.md addendum 2's known
	// limitation (e)).
	PromoteAndCleanup func(ctx context.Context, admin, resourceName, dbName, scratchName, oldName string, warnf func(format string, args ...any)) error
}

// Backup implements the shared dump -> objectKey -> dbjob pipeline ->
// manifest-persist flow (docs/adr/007-backup-restore.md). dbName is the
// caller's already-validated spec.<engine>.database value; cfg is the
// realizing Provider's already-decoded envelope.
func Backup(ctx context.Context, req reconciler.Request, dest backup.Location, dbName string, cfg provider.Provider, prof EngineProfile) (backup.Manifest, error) {
	started := time.Now().UTC()
	jobName := naming.Derived(naming.RuntimeObjectName(req.Resource), "backup", naming.Timestamp(started))
	objectKey := strings.TrimSuffix(dest.Prefix, "/") + "/" + req.Resource.Metadata.Name + "-" + naming.Timestamp(started) + ".sql"
	objectKey = strings.TrimPrefix(objectKey, "/")

	mcConfig, err := dbjob.MCConfig(dest)
	if err != nil {
		return backup.Manifest{}, err
	}
	consumerNetworks := []string(nil)
	if dest.Network != "" {
		consumerNetworks = []string{dest.Network}
	}
	labels := runtime.ManagedLabels(req.Provider.Metadata.Namespace, "Source", req.Resource.Metadata.Name, jobName)
	mcSide := dbjob.Side{
		Image:    dbjob.MCImage,
		Networks: consumerNetworks,
		Env:      map[string]string{"MC_CONFIG_DIR": dbjob.MCConfigDir},
		Files:    []runtime.FileMount{{Path: dbjob.MCConfigPath, Content: mcConfig, Mode: 0o600}},
	}

	spec := dbjob.PipelineSpec{
		RuntimeType: cfg.RuntimeType,
		JobName:     jobName,
		Namespace:   providerkit.Network(cfg),
		Labels:      labels,
		Producer:    prof.BuildDumpSide(dbName),
		Consumer: func() dbjob.Side {
			s := mcSide
			s.ShellCmd = fmt.Sprintf("mc pipe %s/%s/%s", dbjob.MCAlias, dest.Bucket, objectKey)
			return s
		}(),
		// Cleanup: a producer killed mid-stream can still leave the
		// consumer having completed an upload of the truncated bytes it
		// received before the FIFO's write end closed — mc rm --force is
		// idempotent whether or not that happened, so any failure of this
		// pipeline never leaves a partial object behind (docs/adr/
		// 007-backup-restore.md's I12 addendum).
		Cleanup: func() *dbjob.Side {
			s := mcSide
			s.ShellCmd = fmt.Sprintf("mc rm --force %s/%s/%s", dbjob.MCAlias, dest.Bucket, objectKey)
			return &s
		}(),
	}
	result, err := dbjob.RunPipeline(ctx, req.Runtime, spec)
	if err != nil {
		return backup.Manifest{}, fmt.Errorf("Source %q: %s backup: %w", req.Resource.Metadata.Name, cfg.Type, err)
	}

	manifest := backup.Manifest{
		Kind:         req.Resource.Kind,
		Name:         req.Resource.Metadata.Name,
		Namespace:    req.Resource.Metadata.Namespace,
		ProviderType: prof.ProviderType,
		Format:       prof.Format,
		Destination:  backup.RefOf(dest, objectKey),
		StartedAt:    started,
		CompletedAt:  time.Now().UTC(),
		Checksum:     "sha256:" + result.SHA256,
		Bytes:        result.Bytes,
	}
	if err := dbjob.PersistManifest(ctx, req.Runtime, cfg.RuntimeType, jobName, providerkit.Network(cfg), labels, dest, objectKey, manifest); err != nil {
		return backup.Manifest{}, fmt.Errorf("Source %q: %s backup: dump uploaded but its integrity manifest was not: %w", req.Resource.Metadata.Name, cfg.Type, err)
	}
	return manifest, nil
}

// Restore implements the shared verify-then-promote flow (docs/adr/
// 007-backup-restore.md addendum 2, docs/planning/08 I13): fetch the
// backup's integrity manifest, run the I13 disk-headroom precheck, stream
// src back through the job mechanism in reverse into a SCRATCH database,
// verify its checksum, and only then atomically promote the scratch over
// the live target via prof.PromoteAndCleanup. On any failure — disk
// headroom, pipeline, or integrity check — the scratch database is dropped
// and the target is left completely untouched. The restore-over-existing-
// data safety gate remains the engine's job, enforced before Restore is
// ever called. dbName is the caller's already-validated
// spec.<engine>.database value; cfg is the realizing Provider's
// already-decoded envelope.
func Restore(ctx context.Context, req reconciler.Request, src backup.Location, dbName string, cfg provider.Provider, prof EngineProfile) error {
	restoreTS := naming.Timestamp(time.Now())
	jobName := naming.Derived(naming.RuntimeObjectName(req.Resource), "restore", restoreTS)
	// Unlike Backup's dest.Prefix (a directory-like prefix Backup appends a
	// generated filename under), src.Prefix for Restore names the exact
	// object to read back — the CLI/engine resolves --from plus --object
	// into this before calling Restore (docs/adr/007-backup-restore.md).
	objectKey := strings.TrimPrefix(src.Prefix, "/")
	if objectKey == "" {
		return fmt.Errorf("Source %q: restore source must name a specific backup object, not a bare bucket", req.Resource.Metadata.Name)
	}

	mcConfig, err := dbjob.MCConfig(src)
	if err != nil {
		return err
	}
	producerNetworks := []string(nil)
	if src.Network != "" {
		producerNetworks = []string{src.Network}
	}
	labels := runtime.ManagedLabels(req.Provider.Metadata.Namespace, "Source", req.Resource.Metadata.Name, jobName)

	// Fetch the backup's integrity manifest before streaming anything back —
	// a missing sidecar refuses outright rather than silently skipping
	// verification (docs/adr/007-backup-restore.md's I12 addendum).
	wantManifest, err := dbjob.ReadManifest(ctx, req.Runtime, cfg.RuntimeType, jobName, providerkit.Network(cfg), labels, src, objectKey)
	if err != nil {
		return fmt.Errorf("Source %q: %s restore: %w", req.Resource.Metadata.Name, cfg.Type, err)
	}

	// I13 disk-headroom precheck: 2x the recorded backup size must be free
	// on the instance's own data volume before anything else starts.
	if err := dbjob.CheckDiskHeadroom(ctx, req.Runtime, cfg.RuntimeType, labels, jobName, providerkit.Network(cfg), prof.DBHost, prof.Image, prof.DBHost+"-data", prof.DataMount, wantManifest.Bytes); err != nil {
		return fmt.Errorf("Source %q: %s restore: %w", req.Resource.Metadata.Name, cfg.Type, err)
	}

	addr, closeAddr, err := providerkit.ReachableAddr(ctx, req.Runtime, prof.DBHost, prof.Port)
	if err != nil {
		return fmt.Errorf("Source %q: %s restore: %w", req.Resource.Metadata.Name, cfg.Type, err)
	}
	defer closeAddr()
	admin := prof.AdminConn(addr)

	scratchName := dbName + "_restore_" + restoreTS
	if err := prof.EnsureDatabase(ctx, admin, scratchName); err != nil {
		return fmt.Errorf("Source %q: %s restore: create scratch database: %w", req.Resource.Metadata.Name, cfg.Type, err)
	}

	spec := dbjob.PipelineSpec{
		RuntimeType: cfg.RuntimeType,
		JobName:     jobName,
		Namespace:   providerkit.Network(cfg),
		Labels:      labels,
		Producer: dbjob.Side{
			Image:    dbjob.MCImage,
			Networks: producerNetworks,
			Env:      map[string]string{"MC_CONFIG_DIR": dbjob.MCConfigDir},
			Files:    []runtime.FileMount{{Path: dbjob.MCConfigPath, Content: mcConfig, Mode: 0o600}},
			ShellCmd: fmt.Sprintf("mc cat %s/%s/%s", dbjob.MCAlias, src.Bucket, objectKey),
		},
		// Consumer replays into the SCRATCH database, never dbName — the
		// verify-then-promote guarantee depends entirely on this: nothing
		// above this line has written to the live target at all.
		Consumer: prof.BuildReplaySide(scratchName),
	}
	result, err := dbjob.RunPipeline(ctx, req.Runtime, spec)
	if err != nil {
		_ = prof.DropDatabase(ctx, admin, scratchName)
		return fmt.Errorf("Source %q: %s restore: %w", req.Resource.Metadata.Name, cfg.Type, err)
	}
	if err := dbjob.VerifyIntegrity(req.Resource.Metadata.Name, src.Bucket, objectKey, wantManifest, result); err != nil {
		_ = prof.DropDatabase(ctx, admin, scratchName)
		return err
	}

	// Verified good — atomically promote the scratch database over the
	// live target, plus whatever aside-database/cleanup steps this engine
	// needs around that (docs/adr/007-backup-restore.md addendum 2).
	oldName := dbName + "_old_" + restoreTS
	return prof.PromoteAndCleanup(ctx, admin, req.Resource.Metadata.Name, dbName, scratchName, oldName, req.Warnf)
}
