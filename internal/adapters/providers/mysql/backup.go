package mysql

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

	spec := dbjob.PipelineSpec{
		JobName: jobName,
		Labels:  runtime.ManagedLabels(req.Provider.Metadata.Namespace, "Source", req.Resource.Metadata.Name, jobName),
		Producer: dbjob.Side{
			Image:    prof.Image,
			Networks: []string{providerkit.Network(cfg)},
			Files:    []runtime.FileMount{{Path: clientCnfPath, Content: clientCnf(dbHost, "root", rootPass), Mode: 0o600}},
			ShellCmd: fmt.Sprintf("%s --defaults-extra-file=%s %s", dumpTool(cfg), clientCnfPath, dbName),
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
		return backup.Manifest{}, fmt.Errorf("Source %q: %s backup: %w", req.Resource.Metadata.Name, cfg.Type, err)
	}

	return backup.Manifest{
		Kind:         req.Resource.Kind,
		Name:         req.Resource.Metadata.Name,
		Namespace:    req.Resource.Metadata.Namespace,
		ProviderType: p.Type(),
		Format:       cfg.Type + "/" + dumpTool(cfg) + "-sql",
		Destination:  backup.RefOf(dest, objectKey),
		StartedAt:    started,
		CompletedAt:  time.Now().UTC(),
	}, nil
}

// Restore implements reconciler.BackupCapableProvider: streams src back
// through the same two-container job mechanism in reverse (mc reads the
// object, mysql/mariadb replays it), unconditionally overwriting the
// database's current contents — the restore-over-existing-data safety gate
// is the engine's job, enforced before Restore is ever called.
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
	jobName := naming.RuntimeObjectName(req.Resource) + "-restore-" + time.Now().UTC().Format("20060102T150405Z")
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
			Networks: []string{providerkit.Network(cfg)},
			Files:    []runtime.FileMount{{Path: clientCnfPath, Content: clientCnf(dbHost, "root", rootPass), Mode: 0o600}},
			ShellCmd: fmt.Sprintf("%s --defaults-extra-file=%s %s", restoreTool(cfg), clientCnfPath, dbName),
		},
	}
	if err := dbjob.RunPipeline(ctx, req.Runtime, spec); err != nil {
		return fmt.Errorf("Source %q: %s restore: %w", req.Resource.Metadata.Name, cfg.Type, err)
	}
	return nil
}
