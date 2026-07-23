package postgres

import (
	"context"
	"fmt"
	"github.com/rezarajan/platformctl/internal/adapters/providers/providerkit"
	"strings"
	"time"

	"github.com/rezarajan/platformctl/internal/adapters/providers/dbjob"
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

// Backup implements reconciler.BackupCapableProvider: pg_dump streamed to
// dest via two short-lived job containers (internal/adapters/providers/dbjob)
// on the Source's own database network — pg_dump never runs as this
// process, and the dump's bytes never pass through it either.
func (p *Provider) Backup(ctx context.Context, req reconciler.Request, dest backup.Location) (backup.Manifest, error) {
	started := time.Now().UTC()
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
		Producer: dbjob.Side{
			Image:    prof.Image,
			Networks: []string{providerkit.Network(cfg)},
			Env: map[string]string{
				"PGHOST":     dbHost,
				"PGPORT":     "5432",
				"PGUSER":     suUser,
				"PGDATABASE": dbName,
				"PGPASSFILE": pgpassPath,
			},
			Files: []runtime.FileMount{{Path: pgpassPath, Content: []byte(fmt.Sprintf("%s:5432:%s:%s:%s", escapePgpass(dbHost), escapePgpass(dbName), escapePgpass(suUser), escapePgpass(suPass))), Mode: 0o600}},
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
		},
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
		return backup.Manifest{}, fmt.Errorf("Source %q: postgres backup: %w", req.Resource.Metadata.Name, err)
	}

	manifest := backup.Manifest{
		Kind:         req.Resource.Kind,
		Name:         req.Resource.Metadata.Name,
		Namespace:    req.Resource.Metadata.Namespace,
		ProviderType: p.Type(),
		Format:       "postgres/pg_dump-plain",
		Destination:  backup.RefOf(dest, objectKey),
		StartedAt:    started,
		CompletedAt:  time.Now().UTC(),
		Checksum:     "sha256:" + result.SHA256,
		Bytes:        result.Bytes,
	}
	if err := dbjob.PersistManifest(ctx, req.Runtime, cfg.RuntimeType, jobName, providerkit.Network(cfg), labels, dest, objectKey, manifest); err != nil {
		return backup.Manifest{}, fmt.Errorf("Source %q: postgres backup: dump uploaded but its integrity manifest was not: %w", req.Resource.Metadata.Name, err)
	}
	return manifest, nil
}

// Restore implements reconciler.BackupCapableProvider: verify-then-promote
// (docs/adr/007-backup-restore.md addendum 2, docs/planning/08 I13) —
// streams src back through the same job mechanism in reverse (mc reads the
// object, psql replays it) into a SCRATCH database, never the live target;
// only once the streamed content's checksum is verified good does an
// atomic rename-swap promote the scratch database over the target. On any
// failure — disk headroom, pipeline, or integrity check — the scratch
// database is dropped and the target is left completely untouched. The
// restore-over-existing-data safety gate remains the engine's job, enforced
// before Restore is ever called.
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
		return fmt.Errorf("Source %q: postgres restore: %w", req.Resource.Metadata.Name, err)
	}

	// I13 disk-headroom precheck: 2x the recorded backup size must be free
	// on the instance's own data volume before anything else starts.
	if err := dbjob.CheckDiskHeadroom(ctx, req.Runtime, cfg.RuntimeType, labels, jobName, providerkit.Network(cfg), dbHost, prof.Image, dbHost+"-data", prof.DataMount, wantManifest.Bytes); err != nil {
		return fmt.Errorf("Source %q: postgres restore: %w", req.Resource.Metadata.Name, err)
	}

	addr, closeAddr, err := providerkit.ReachableAddr(ctx, req.Runtime, dbHost, 5432)
	if err != nil {
		return fmt.Errorf("Source %q: postgres restore: %w", req.Resource.Metadata.Name, err)
	}
	defer closeAddr()
	admin := connStringAddr(addr, suUser, suPass, "postgres", nil)

	scratchName := dbName + "_restore_" + restoreTS
	if err := ensureDatabase(ctx, admin, scratchName); err != nil {
		return fmt.Errorf("Source %q: postgres restore: create scratch database: %w", req.Resource.Metadata.Name, err)
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
		Consumer: dbjob.Side{
			Image:    prof.Image,
			Networks: []string{providerkit.Network(cfg)},
			Env: map[string]string{
				"PGHOST":     dbHost,
				"PGPORT":     "5432",
				"PGUSER":     suUser,
				"PGDATABASE": scratchName,
				"PGPASSFILE": pgpassPath,
			},
			Files:    []runtime.FileMount{{Path: pgpassPath, Content: []byte(fmt.Sprintf("%s:5432:%s:%s:%s", escapePgpass(dbHost), escapePgpass(scratchName), escapePgpass(suUser), escapePgpass(suPass))), Mode: 0o600}},
			ShellCmd: "psql -h \"$PGHOST\" -p \"$PGPORT\" -U \"$PGUSER\" -d \"$PGDATABASE\" -v ON_ERROR_STOP=1",
		},
	}
	result, err := dbjob.RunPipeline(ctx, req.Runtime, spec)
	if err != nil {
		_ = dropDatabase(ctx, admin, scratchName)
		return fmt.Errorf("Source %q: postgres restore: %w", req.Resource.Metadata.Name, err)
	}
	if err := dbjob.VerifyIntegrity(req.Resource.Metadata.Name, src.Bucket, objectKey, wantManifest, result); err != nil {
		_ = dropDatabase(ctx, admin, scratchName)
		return err
	}

	// Verified good — atomically promote the scratch database over the
	// live target (docs/adr/007-backup-restore.md addendum 2).
	oldName := dbName + "_old_" + restoreTS
	if err := promoteDatabase(ctx, admin, dbName, scratchName, oldName); err != nil {
		_ = dropDatabase(ctx, admin, scratchName)
		return fmt.Errorf("Source %q: postgres restore: %w", req.Resource.Metadata.Name, err)
	}
	// Best-effort cleanup of the aside-renamed pre-restore database — the
	// promote already fully succeeded at this point; a failure here is a
	// harmless, named leftover, never this call's own failure (ADR
	// addendum 2, known limitation (e)).
	if err := dropDatabase(ctx, admin, oldName); err != nil {
		req.Warnf("Source %q: postgres restore: promoted successfully, but dropping the pre-restore database %q failed (harmless leftover, drop it by hand): %v", req.Resource.Metadata.Name, oldName, err)
	}
	return nil
}
