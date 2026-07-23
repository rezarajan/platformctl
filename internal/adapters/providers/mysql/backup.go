package mysql

import (
	"context"
	"fmt"
	"github.com/rezarajan/platformctl/internal/adapters/providers/providerkit"
	"os"
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
// streamed to dest via two short-lived job containers
// (internal/adapters/providers/dbjob) on the Source's own database network.
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
	jobName := naming.RuntimeObjectName(req.Resource) + "-backup-" + started.Format("20060102T150405Z")
	objectKey := strings.TrimPrefix(strings.TrimSuffix(dest.Prefix, "/")+"/"+req.Resource.Metadata.Name+"-"+started.Format("20060102T150405Z")+".sql", "/")

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
		Producer:    dbSide(dumpTool(cfg), cfg, prof.Image, dbHost, rootPass, dbName),
		Consumer: func() dbjob.Side {
			s := mcSide
			s.ShellCmd = fmt.Sprintf("mc pipe %s/%s/%s", dbjob.MCAlias, dest.Bucket, objectKey)
			return s
		}(),
		// Cleanup: mc rm --force is idempotent whether or not a producer
		// killed mid-stream left the consumer having completed an upload
		// of the truncated bytes it received — any failure of this
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
		ProviderType: p.Type(),
		Format:       cfg.Type + "/" + dumpTool(cfg) + "-sql",
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

// Restore implements reconciler.BackupCapableProvider: verify-then-promote
// (docs/adr/007-backup-restore.md addendum 2, docs/planning/08 I13) —
// streams src back through the same job mechanism in reverse (mc reads the
// object, mysql/mariadb replays it) into a SCRATCH schema, never the live
// target; only once the streamed content's checksum is verified good does
// an atomic RENAME TABLE batch promote the scratch schema's tables over the
// target's. On any failure — disk headroom, pipeline, or integrity check —
// the scratch schema is dropped and the target is left completely
// untouched. The restore-over-existing-data safety gate remains the
// engine's job, enforced before Restore is ever called.
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
	restoreTS := time.Now().UTC().Format("20060102T150405Z")
	jobName := naming.RuntimeObjectName(req.Resource) + "-restore-" + restoreTS
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
	if err := dbjob.CheckDiskHeadroom(ctx, req.Runtime, cfg.RuntimeType, labels, jobName, providerkit.Network(cfg), dbHost, prof.Image, dbHost+"-data", prof.DataMount, wantManifest.Bytes); err != nil {
		return fmt.Errorf("Source %q: %s restore: %w", req.Resource.Metadata.Name, cfg.Type, err)
	}

	addr, closeAddr, err := providerkit.ReachableAddr(ctx, req.Runtime, dbHost, 3306)
	if err != nil {
		return fmt.Errorf("Source %q: %s restore: %w", req.Resource.Metadata.Name, cfg.Type, err)
	}
	defer closeAddr()
	admin := dsnAddr(addr, "root", rootPass, "", nil)

	scratchName := dbName + "_restore_" + restoreTS
	if err := ensureDatabase(ctx, admin, scratchName); err != nil {
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
		// Consumer replays into the SCRATCH schema, never dbName — the
		// verify-then-promote guarantee depends entirely on this: nothing
		// above this line has written to the live target at all.
		Consumer: dbSide(restoreTool(cfg), cfg, prof.Image, dbHost, rootPass, scratchName),
	}
	result, err := dbjob.RunPipeline(ctx, req.Runtime, spec)
	if err != nil {
		_ = dropDatabase(ctx, admin, scratchName)
		return fmt.Errorf("Source %q: %s restore: %w", req.Resource.Metadata.Name, cfg.Type, err)
	}
	if err := dbjob.VerifyIntegrity(req.Resource.Metadata.Name, src.Bucket, objectKey, wantManifest, result); err != nil {
		_ = dropDatabase(ctx, admin, scratchName)
		return err
	}

	// Verified good — atomically promote the scratch schema's tables over
	// the live target's (docs/adr/007-backup-restore.md addendum 2).
	oldName := dbName + "_old_" + restoreTS
	if err := ensureDatabase(ctx, admin, oldName); err != nil {
		_ = dropDatabase(ctx, admin, scratchName)
		return fmt.Errorf("Source %q: %s restore: create promote-aside database: %w", req.Resource.Metadata.Name, cfg.Type, err)
	}
	if err := promoteDatabase(ctx, admin, dbName, scratchName, oldName); err != nil {
		_ = dropDatabase(ctx, admin, scratchName)
		_ = dropDatabase(ctx, admin, oldName)
		return fmt.Errorf("Source %q: %s restore: %w", req.Resource.Metadata.Name, cfg.Type, err)
	}
	// Best-effort cleanup of the aside-renamed pre-restore tables' schema
	// and the now-empty scratch schema — the promote already fully
	// succeeded at this point; a failure here is a harmless, named
	// leftover, never this call's own failure (ADR addendum 2, known
	// limitation (e)).
	if err := dropDatabase(ctx, admin, oldName); err != nil {
		fmt.Fprintf(os.Stderr, "warning: Source %q: %s restore: promoted successfully, but dropping the pre-restore schema %q failed (harmless leftover, drop it by hand): %v\n", req.Resource.Metadata.Name, cfg.Type, oldName, err)
	}
	if err := dropDatabase(ctx, admin, scratchName); err != nil {
		fmt.Fprintf(os.Stderr, "warning: Source %q: %s restore: promoted successfully, but dropping the now-empty scratch schema %q failed (harmless leftover, drop it by hand): %v\n", req.Resource.Metadata.Name, cfg.Type, scratchName, err)
	}
	return nil
}
