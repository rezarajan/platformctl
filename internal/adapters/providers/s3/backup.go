package s3

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/rezarajan/platformctl/internal/domain/backup"
	"github.com/rezarajan/platformctl/internal/domain/dataset"
	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// remoteClient builds an S3 client for an arbitrary backup.Location, dialing
// from *this process* (not a job container on the shared network — this
// provider's own Backup/Restore run in-process, unlike postgres/mysql's
// dbjob mechanism). loc.Endpoint is only valid from inside the runtime's own
// network; a Location resolved from an in-platform Dataset instead carries
// RuntimeName/ContainerPort (docs/planning/08 F4) and this resolves a
// currently-dialable address from them via ContainerRuntime.EnsureReachable
// — the exact pattern this provider's own admin calls use (s3.go's
// reachableAddr) — rather than dialing Endpoint directly, which is what
// caused "no such host" against a real runtime (C6 review finding 2;
// docs/adr/007-backup-restore.md). A raw-URL Location (RuntimeName empty:
// real AWS S3, or any other externally routable endpoint) dials Endpoint as
// before — it needs no runtime resolution. The returned close func must
// always be called, even when it's a no-op.
func remoteClient(ctx context.Context, rt runtime.ContainerRuntime, loc backup.Location) (*minio.Client, func() error, error) {
	ep := strings.TrimPrefix(strings.TrimPrefix(loc.Endpoint, "https://"), "http://")
	closeAddr := func() error { return nil }
	if loc.RuntimeName != "" {
		addr, cf, err := rt.EnsureReachable(ctx, loc.RuntimeName, loc.ContainerPort)
		if err != nil {
			return nil, nil, fmt.Errorf("resolve a reachable address for %q port %d: %w", loc.RuntimeName, loc.ContainerPort, err)
		}
		ep, closeAddr = addr, cf
	}
	cl, err := minio.New(ep, &minio.Options{
		Creds:  credentials.NewStaticV4(loc.AccessKey, loc.SecretKey, ""),
		Secure: !loc.Insecure,
	})
	if err != nil {
		_ = closeAddr()
		return nil, nil, fmt.Errorf("s3 client for %q: %w", ep, err)
	}
	return cl, closeAddr, nil
}

func joinKey(prefix, suffix string) string {
	prefix = strings.Trim(prefix, "/")
	suffix = strings.TrimPrefix(suffix, "/")
	switch {
	case prefix == "":
		return suffix
	case suffix == "":
		return prefix
	default:
		return prefix + "/" + suffix
	}
}

// ensureRemoteBucket best-effort creates the destination bucket — a Dataset
// destination already has one (its own provider reconciled it), but a raw
// URL destination may not.
func ensureRemoteBucket(ctx context.Context, cl *minio.Client, bucket string) error {
	exists, err := cl.BucketExists(ctx, bucket)
	if err != nil {
		return fmt.Errorf("check bucket %q: %w", bucket, err)
	}
	if exists {
		return nil
	}
	if err := cl.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
		resp := minio.ToErrorResponse(err)
		if resp.Code == "BucketAlreadyOwnedByYou" || resp.Code == "BucketAlreadyExists" {
			return nil
		}
		return fmt.Errorf("create bucket %q: %w", bucket, err)
	}
	return nil
}

// syncObjects copies every object under fromBucket/fromPrefix into
// toBucket/toPrefix, preserving each object's path relative to fromPrefix.
// Each object streams directly from the source GET into the destination PUT
// — this process holds at most one object in flight, never the whole
// dataset.
func syncObjects(ctx context.Context, from *minio.Client, fromBucket, fromPrefix string, to *minio.Client, toBucket, toPrefix string) (int, error) {
	count := 0
	for obj := range from.ListObjects(ctx, fromBucket, minio.ListObjectsOptions{Prefix: fromPrefix, Recursive: true}) {
		if obj.Err != nil {
			return count, fmt.Errorf("list %q: %w", fromBucket, obj.Err)
		}
		rc, err := from.GetObject(ctx, fromBucket, obj.Key, minio.GetObjectOptions{})
		if err != nil {
			return count, fmt.Errorf("read %s/%s: %w", fromBucket, obj.Key, err)
		}
		destKey := joinKey(toPrefix, strings.TrimPrefix(obj.Key, fromPrefix))
		_, err = to.PutObject(ctx, toBucket, destKey, rc, obj.Size, minio.PutObjectOptions{ContentType: obj.ContentType})
		_ = rc.Close()
		if err != nil {
			return count, fmt.Errorf("write %s/%s: %w", toBucket, destKey, err)
		}
		count++
	}
	return count, nil
}

// Backup implements reconciler.BackupCapableProvider: a bucket/prefix sync
// using the S3 API directly (no job container — this provider already
// speaks S3 in-process via bucket.go's minio-go client).
func (p *Provider) Backup(ctx context.Context, req reconciler.Request, dest backup.Location) (backup.Manifest, error) {
	started := time.Now().UTC()
	ds, err := dataset.FromEnvelope(req.Resource)
	if err != nil {
		return backup.Manifest{}, err
	}
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return backup.Manifest{}, err
	}
	name := naming.RuntimeObjectName(req.Provider)
	user, pass, err := rootCredentials(cfg, req.Secrets, name)
	if err != nil {
		return backup.Manifest{}, err
	}
	addr, closeAddr, err := reachableAddr(ctx, req.Runtime, name)
	if err != nil {
		return backup.Manifest{}, err
	}
	defer closeAddr()
	srcClient, err := newClient(addr, user, pass)
	if err != nil {
		return backup.Manifest{}, err
	}
	destClient, closeDest, err := remoteClient(ctx, req.Runtime, dest)
	if err != nil {
		return backup.Manifest{}, err
	}
	defer closeDest()
	if err := ensureRemoteBucket(ctx, destClient, dest.Bucket); err != nil {
		return backup.Manifest{}, err
	}
	if _, err := syncObjects(ctx, srcClient, ds.Bucket, ds.Prefix, destClient, dest.Bucket, dest.Prefix); err != nil {
		return backup.Manifest{}, fmt.Errorf("Dataset %q: s3 backup: %w", req.Resource.Metadata.Name, err)
	}

	return backup.Manifest{
		Kind:         req.Resource.Kind,
		Name:         req.Resource.Metadata.Name,
		Namespace:    req.Resource.Metadata.Namespace,
		ProviderType: p.Type(),
		Format:       "s3/sync",
		Destination:  backup.RefOf(dest, ""),
		StartedAt:    started,
		CompletedAt:  time.Now().UTC(),
	}, nil
}

// Restore implements reconciler.BackupCapableProvider: syncs src back into
// the Dataset's own bucket/prefix, unconditionally overwriting whatever
// objects already exist under it — the restore-over-existing-data safety
// gate is the engine's job, enforced before Restore is ever called.
func (p *Provider) Restore(ctx context.Context, req reconciler.Request, src backup.Location) error {
	ds, err := dataset.FromEnvelope(req.Resource)
	if err != nil {
		return err
	}
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return err
	}
	name := naming.RuntimeObjectName(req.Provider)
	user, pass, err := rootCredentials(cfg, req.Secrets, name)
	if err != nil {
		return err
	}
	if err := ensureBucket(ctx, req.Runtime, name, apiPort, user, pass, ds.Bucket); err != nil {
		return err
	}
	addr, closeAddr, err := reachableAddr(ctx, req.Runtime, name)
	if err != nil {
		return err
	}
	defer closeAddr()
	destClient, err := newClient(addr, user, pass)
	if err != nil {
		return err
	}
	srcClient, closeSrc, err := remoteClient(ctx, req.Runtime, src)
	if err != nil {
		return err
	}
	defer closeSrc()
	if _, err := syncObjects(ctx, srcClient, src.Bucket, src.Prefix, destClient, ds.Bucket, ds.Prefix); err != nil {
		return fmt.Errorf("Dataset %q: s3 restore: %w", req.Resource.Metadata.Name, err)
	}
	return nil
}
