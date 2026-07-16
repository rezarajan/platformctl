package s3

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
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

func ensureBucket(ctx context.Context, cl *minio.Client, bucket string) error {
	// Container health and host-port reachability are not the same instant
	// on every runtime; tolerate a short gap after WaitHealthy.
	exists, err := bucketExists(ctx, cl, bucket)
	for deadline := time.Now().Add(30 * time.Second); err != nil && time.Now().Before(deadline); {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
		exists, err = bucketExists(ctx, cl, bucket)
	}
	if err != nil || exists {
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
