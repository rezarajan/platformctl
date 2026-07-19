package s3

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

func newClient(endpoint, user, pass string) (*minio.Client, error) {
	cl, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(user, pass, ""),
		Secure: false,
	})
	if err != nil {
		return nil, fmt.Errorf("s3 client for %q: %w", endpoint, err)
	}
	return cl, nil
}

func bucketExists(ctx context.Context, cl *minio.Client, bucket string) (bool, error) {
	exists, err := cl.BucketExists(ctx, bucket)
	if err != nil {
		return false, fmt.Errorf("check bucket %q: %w", bucket, err)
	}
	return exists, nil
}

// ensureBucket waits for the store to accept API calls, then creates the
// bucket if it doesn't already exist. Container health and host-port
// reachability are not the same instant on every runtime; the wait uses
// runtime.WithReachable (docs/planning/09 Class 2 / F1) so a stale
// port-forward tunnel opened right after WaitHealthy — before MinIO is
// actually accepting connections — gets re-resolved on retry rather than
// reused for the whole wait.
func ensureBucket(ctx context.Context, rt runtime.ContainerRuntime, name string, port int, user, pass, bucket string) error {
	var exists bool
	opts := runtime.ReachableOptions{Timeout: 30 * time.Second}
	err := runtime.WithReachable(ctx, rt, name, port, opts, func(ctx context.Context, addr string) error {
		cl, err := newClient(addr, user, pass)
		if err != nil {
			return err
		}
		e, err := bucketExists(ctx, cl, bucket)
		if err != nil {
			return err
		}
		exists = e
		return nil
	})
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	addr, closeAddr, err := rt.EnsureReachable(ctx, name, port)
	if err != nil {
		return err
	}
	defer closeAddr()
	cl, err := newClient(addr, user, pass)
	if err != nil {
		return err
	}
	if err := cl.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
		return fmt.Errorf("create bucket %q: %w", bucket, err)
	}
	return nil
}

// removeDataset deletes every object under bucket/prefix, then removes the
// bucket itself unless other prefixes still hold data (a Dataset owns its
// prefix; the bucket may be shared).
func removeDataset(ctx context.Context, cl *minio.Client, bucket, prefix string) error {
	exists, err := bucketExists(ctx, cl, bucket)
	if err != nil || !exists {
		return err
	}
	for obj := range cl.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if obj.Err != nil {
			return fmt.Errorf("list objects in %q: %w", bucket, obj.Err)
		}
		if err := cl.RemoveObject(ctx, bucket, obj.Key, minio.RemoveObjectOptions{}); err != nil {
			return fmt.Errorf("remove object %s/%s: %w", bucket, obj.Key, err)
		}
	}
	if err := cl.RemoveBucket(ctx, bucket); err != nil {
		// Shared bucket with remaining data outside this prefix: leave it.
		if strings.Contains(err.Error(), "not empty") {
			return nil
		}
		return fmt.Errorf("remove bucket %q: %w", bucket, err)
	}
	return nil
}

// prefixListable verifies the declared credentials can list under
// bucket/prefix — probe support for permission/policy drift
// (docs/planning/07 §2.1). An empty listing is fine; an error is not.
func prefixListable(ctx context.Context, cl *minio.Client, bucket, prefix string) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	for obj := range cl.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, MaxKeys: 1}) {
		if obj.Err != nil {
			return obj.Err
		}
		break
	}
	return nil
}
