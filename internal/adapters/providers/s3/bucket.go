package s3

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	minlifecycle "github.com/minio/minio-go/v7/pkg/lifecycle"

	"github.com/rezarajan/platformctl/internal/domain/dataset"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// newClient constructs an S3 API client. secure selects HTTPS (a real
// external endpoint, docs/planning/08 C4) vs. the plain-HTTP convention
// every managed instance/node uses on the shared network.
func newClient(endpoint, user, pass string, secure bool) (*minio.Client, error) {
	cl, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(user, pass, ""),
		Secure: secure,
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
// reused for the whole wait. name is the dial target: the legacy
// single-container name, or a StableIdentity node set's ordinal 0
// (docs/planning/08 C4) — both are managed instances platformctl may have
// just started, so both get the same boot-race tolerance.
func ensureBucket(ctx context.Context, rt runtime.ContainerRuntime, name string, port int, user, pass, bucket string) error {
	var exists bool
	opts := runtime.ReachableOptions{Timeout: 30 * time.Second}
	err := runtime.WithReachable(ctx, rt, name, port, opts, func(ctx context.Context, addr string) error {
		cl, err := newClient(addr, user, pass, false)
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
	cl, err := newClient(addr, user, pass, false)
	if err != nil {
		return err
	}
	return createBucket(ctx, cl, bucket)
}

// ensureBucketAt is ensureBucket's external counterpart (docs/planning/08
// C4): a single attempt, no boot-race wait — an externally-operated store
// is assumed already up, so there is nothing for platformctl to wait on.
func ensureBucketAt(ctx context.Context, cl *minio.Client, bucket string) error {
	exists, err := bucketExists(ctx, cl, bucket)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	return createBucket(ctx, cl, bucket)
}

func createBucket(ctx context.Context, cl *minio.Client, bucket string) error {
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

// --- D7: Dataset lifecycle (expiration + versioning) -----------------------

// lifecycleRuleID is deterministic from the Dataset's own name, so
// re-applying the same spec produces byte-identical XML (idempotent, no
// spurious PUTs) and probe/heal can find this Dataset's own rule again by ID
// without disturbing another Dataset's rule on a shared bucket/different
// prefix.
func lifecycleRuleID(datasetName string) string {
	return "datascape-" + datasetName
}

// desiredLifecycleRule builds the single managed lifecycle rule for a
// Dataset declaring spec.lifecycle.expireAfterDays.
func desiredLifecycleRule(id string, ds dataset.Dataset) minlifecycle.Rule {
	return minlifecycle.Rule{
		ID:         id,
		Status:     "Enabled",
		RuleFilter: minlifecycle.Filter{Prefix: ds.Prefix},
		Expiration: minlifecycle.Expiration{Days: minlifecycle.ExpirationDays(ds.Lifecycle.ExpireAfterDays)},
	}
}

// getLifecycleConfig fetches the bucket's current lifecycle configuration,
// treating "no configuration at all" (a bucket with zero rules answers this
// error, not an empty document) as an empty Configuration rather than an
// error — the natural starting point for a bucket whose lifecycle
// platformctl has never touched.
func getLifecycleConfig(ctx context.Context, cl *minio.Client, bucket string) (*minlifecycle.Configuration, error) {
	cfg, err := cl.GetBucketLifecycle(ctx, bucket)
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchLifecycleConfiguration" {
			return minlifecycle.NewConfiguration(), nil
		}
		return nil, fmt.Errorf("get bucket %q lifecycle: %w", bucket, err)
	}
	if cfg == nil {
		return minlifecycle.NewConfiguration(), nil
	}
	return cfg, nil
}

// findRule returns the rule with the given ID, or nil.
func findRule(cfg *minlifecycle.Configuration, id string) *minlifecycle.Rule {
	for i := range cfg.Rules {
		if cfg.Rules[i].ID == id {
			return &cfg.Rules[i]
		}
	}
	return nil
}

// ruleMatches reports whether a live rule already matches the desired one on
// every field this platform manages (status, prefix filter, expiration
// days) — probe's drift check and reconcile's idempotency check share this
// so "no changes" never issues a redundant PUT.
func ruleMatches(got *minlifecycle.Rule, want minlifecycle.Rule) bool {
	return got != nil &&
		got.Status == want.Status &&
		got.RuleFilter.Prefix == want.RuleFilter.Prefix &&
		got.Expiration.Days == want.Expiration.Days
}

// ensureLifecycle reconciles this Dataset's managed lifecycle rule and
// bucket versioning (docs/planning/08 D7), each independently and only when
// spec.lifecycle declares it — a bucket lifecycle PUT replaces every rule
// (S3 semantics), so this always reads the live configuration first and
// writes back every existing rule plus this Dataset's own (by ID), never
// clobbering a sibling Dataset's rule on the same bucket/a different
// prefix. Idempotent: a matching live rule/versioning state makes zero API
// calls beyond the two reads.
func ensureLifecycle(ctx context.Context, cl *minio.Client, ds dataset.Dataset, ruleID string) error {
	if ds.Lifecycle.HasExpiration() {
		cfg, err := getLifecycleConfig(ctx, cl, ds.Bucket)
		if err != nil {
			return err
		}
		want := desiredLifecycleRule(ruleID, ds)
		if !ruleMatches(findRule(cfg, ruleID), want) {
			replaced := false
			for i := range cfg.Rules {
				if cfg.Rules[i].ID == ruleID {
					cfg.Rules[i] = want
					replaced = true
					break
				}
			}
			if !replaced {
				cfg.Rules = append(cfg.Rules, want)
			}
			if err := cl.SetBucketLifecycle(ctx, ds.Bucket, cfg); err != nil {
				return fmt.Errorf("set bucket %q lifecycle rule %q: %w", ds.Bucket, ruleID, err)
			}
		}
	}
	if ds.Lifecycle.HasVersioning() {
		want := versioningStatus(ds.Lifecycle.Versioning)
		live, err := cl.GetBucketVersioning(ctx, ds.Bucket)
		if err != nil {
			return fmt.Errorf("get bucket %q versioning: %w", ds.Bucket, err)
		}
		if live.Status != want {
			if err := cl.SetBucketVersioning(ctx, ds.Bucket, minio.BucketVersioningConfiguration{Status: want}); err != nil {
				return fmt.Errorf("set bucket %q versioning: %w", ds.Bucket, err)
			}
		}
	}
	return nil
}

// versioningStatus maps the model's enabled|suspended vocabulary to
// minio-go's Enabled/Suspended constants.
func versioningStatus(v string) string {
	if v == dataset.VersioningEnabled {
		return minio.Enabled
	}
	return minio.Suspended
}

// probeLifecycleDrift diffs the live bucket lifecycle rule (by
// lifecycleRuleID) and versioning state against spec.lifecycle — rule
// names/values only, never secrets (there are none in a lifecycle rule).
// Returns (drift, reason); reason is one of status.ReasonLifecycleRuleDrift
// / status.ReasonVersioningDrift when drift is true.
func probeLifecycleDrift(ctx context.Context, cl *minio.Client, ds dataset.Dataset, ruleID string) (bool, string, error) {
	if ds.Lifecycle.HasExpiration() {
		cfg, err := getLifecycleConfig(ctx, cl, ds.Bucket)
		if err != nil {
			return false, "", err
		}
		if !ruleMatches(findRule(cfg, ruleID), desiredLifecycleRule(ruleID, ds)) {
			return true, status.ReasonLifecycleRuleDrift, nil
		}
	}
	if ds.Lifecycle.HasVersioning() {
		live, err := cl.GetBucketVersioning(ctx, ds.Bucket)
		if err != nil {
			return false, "", fmt.Errorf("get bucket %q versioning: %w", ds.Bucket, err)
		}
		if live.Status != versioningStatus(ds.Lifecycle.Versioning) {
			return true, status.ReasonVersioningDrift, nil
		}
	}
	return false, "", nil
}
