package postgres

import (
	"context"
	"fmt"
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
	jobName := naming.RuntimeObjectName(req.Resource) + "-backup-" + started.Format("20060102T150405Z")
	objectKey := strings.TrimSuffix(dest.Prefix, "/") + "/" + req.Resource.Metadata.Name + "-" + started.Format("20060102T150405Z") + ".sql"
	objectKey = strings.TrimPrefix(objectKey, "/")

	mcConfig, err := dbjob.MCConfig(dest)
	if err != nil {
		return backup.Manifest{}, err
	}
	consumerNetworks := []string(nil)
	if dest.Network != "" {
		consumerNetworks = []string{dest.Network}
	}

	spec := dbjob.PipelineSpec{
		JobName: jobName,
		Labels:  runtime.ManagedLabels(req.Provider.Metadata.Namespace, "Source", req.Resource.Metadata.Name, jobName),
		Producer: dbjob.Side{
			Image:    prof.Image,
			Networks: []string{network(cfg)},
			Env: map[string]string{
				"PGHOST":     dbHost,
				"PGPORT":     "5432",
				"PGUSER":     suUser,
				"PGDATABASE": dbName,
				"PGPASSFILE": pgpassPath,
			},
			Files:    []runtime.FileMount{{Path: pgpassPath, Content: []byte(fmt.Sprintf("%s:5432:%s:%s:%s", escapePgpass(dbHost), escapePgpass(dbName), escapePgpass(suUser), escapePgpass(suPass))), Mode: 0o600}},
			ShellCmd: "pg_dump -h \"$PGHOST\" -p \"$PGPORT\" -U \"$PGUSER\" -d \"$PGDATABASE\" -F p",
		},
		Consumer: dbjob.Side{
			Image:    dbjob.MCImage,
			Networks: consumerNetworks,
			Env:      map[string]string{"MC_CONFIG_DIR": dbjob.MCConfigDir},
			Files:    []runtime.FileMount{{Path: dbjob.MCConfigPath, Content: mcConfig, Mode: 0o600}},
			ShellCmd: fmt.Sprintf("mc pipe %s/%s/%s", dbjob.MCAlias, dest.Bucket, objectKey),
		},
	}
	if err := dbjob.RunPipeline(ctx, req.Runtime, spec); err != nil {
		return backup.Manifest{}, fmt.Errorf("Source %q: postgres backup: %w", req.Resource.Metadata.Name, err)
	}

	return backup.Manifest{
		Kind:         req.Resource.Kind,
		Name:         req.Resource.Metadata.Name,
		Namespace:    req.Resource.Metadata.Namespace,
		ProviderType: p.Type(),
		Format:       "postgres/pg_dump-plain",
		Destination:  backup.RefOf(dest, objectKey),
		StartedAt:    started,
		CompletedAt:  time.Now().UTC(),
	}, nil
}

// Restore implements reconciler.BackupCapableProvider: streams src back
// through the same two-container job mechanism in reverse (mc reads the
// object, psql replays it), unconditionally overwriting the database's
// current contents — the restore-over-existing-data safety gate is the
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
	jobName := naming.RuntimeObjectName(req.Resource) + "-restore-" + time.Now().UTC().Format("20060102T150405Z")
	// Unlike Backup's dest.Prefix (a directory-like prefix Backup appends a
	// generated filename under), src.Prefix for Restore names the exact
	// object to read back — the CLI/engine resolves --from plus --object
	// into this before calling Restore (docs/design/007).
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

	spec := dbjob.PipelineSpec{
		JobName: jobName,
		Labels:  runtime.ManagedLabels(req.Provider.Metadata.Namespace, "Source", req.Resource.Metadata.Name, jobName),
		Producer: dbjob.Side{
			Image:    dbjob.MCImage,
			Networks: producerNetworks,
			Env:      map[string]string{"MC_CONFIG_DIR": dbjob.MCConfigDir},
			Files:    []runtime.FileMount{{Path: dbjob.MCConfigPath, Content: mcConfig, Mode: 0o600}},
			ShellCmd: fmt.Sprintf("mc cat %s/%s/%s", dbjob.MCAlias, src.Bucket, objectKey),
		},
		Consumer: dbjob.Side{
			Image:    prof.Image,
			Networks: []string{network(cfg)},
			Env: map[string]string{
				"PGHOST":     dbHost,
				"PGPORT":     "5432",
				"PGUSER":     suUser,
				"PGDATABASE": dbName,
				"PGPASSFILE": pgpassPath,
			},
			Files:    []runtime.FileMount{{Path: pgpassPath, Content: []byte(fmt.Sprintf("%s:5432:%s:%s:%s", escapePgpass(dbHost), escapePgpass(dbName), escapePgpass(suUser), escapePgpass(suPass))), Mode: 0o600}},
			ShellCmd: "psql -h \"$PGHOST\" -p \"$PGPORT\" -U \"$PGUSER\" -d \"$PGDATABASE\" -v ON_ERROR_STOP=1",
		},
	}
	if err := dbjob.RunPipeline(ctx, req.Runtime, spec); err != nil {
		return fmt.Errorf("Source %q: postgres restore: %w", req.Resource.Metadata.Name, err)
	}
	return nil
}
