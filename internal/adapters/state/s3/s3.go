// Package s3 implements StateStore against S3-compatible object storage
// (MinIO tested) for teams that need one shared source of truth across
// operators/CI rather than a single local file. See
// docs/design/003-shared-state.md for the design and locking protocol.
package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/rezarajan/platformctl/internal/ports/state"
)

// DefaultLeaseTTL is used when Config.LeaseTTL is zero. It must outlast the
// longest apply/destroy run in practice (docs/design/003's documented
// simplification: no lease renewal/heartbeat).
const DefaultLeaseTTL = 15 * time.Minute

type Config struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Bucket    string
	// Prefix is prepended to both object keys. Empty is legal; a non-empty
	// value should end in "/" (not enforced — a bare prefix like "team-a"
	// just concatenates directly, which is valid but probably not intended).
	Prefix   string
	Secure   bool
	Region   string
	LeaseTTL time.Duration
	// Holder identifies this process in the lock's lease record and any
	// "locked by" error. Defaults to "<hostname>:<pid>".
	Holder string
}

type Store struct {
	client   *minio.Client
	bucket   string
	prefix   string
	holder   string
	leaseTTL time.Duration
}

func New(cfg Config) (*Store, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("s3 state backend: bucket is required")
	}
	cl, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.Secure,
		Region: cfg.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("s3 state client for %q: %w", cfg.Endpoint, err)
	}
	holder := cfg.Holder
	if holder == "" {
		host, _ := os.Hostname()
		holder = fmt.Sprintf("%s:%d", host, os.Getpid())
	}
	ttl := cfg.LeaseTTL
	if ttl <= 0 {
		ttl = DefaultLeaseTTL
	}
	return &Store{client: cl, bucket: cfg.Bucket, prefix: cfg.Prefix, holder: holder, leaseTTL: ttl}, nil
}

func (s *Store) stateKey() string { return s.prefix + "state.json" }
func (s *Store) lockKey() string  { return s.prefix + "state.lock" }

func (s *Store) Load(ctx context.Context) (state.State, error) {
	st := state.State{Version: state.CurrentVersion}
	obj, err := s.client.GetObject(ctx, s.bucket, s.stateKey(), minio.GetObjectOptions{})
	if err != nil {
		return st, fmt.Errorf("get state object s3://%s/%s: %w", s.bucket, s.stateKey(), err)
	}
	defer obj.Close()
	data, err := io.ReadAll(obj)
	if err != nil {
		if isNoSuchKey(err) {
			st.Normalize()
			return st, nil
		}
		return st, fmt.Errorf("read state object s3://%s/%s: %w", s.bucket, s.stateKey(), err)
	}
	if len(data) == 0 {
		st.Normalize()
		return st, nil
	}
	if err := json.Unmarshal(data, &st); err != nil {
		return st, fmt.Errorf("parse state object s3://%s/%s: %w", s.bucket, s.stateKey(), err)
	}
	if st.Version > state.CurrentVersion {
		return st, fmt.Errorf("state object s3://%s/%s has version %d, newer than this binary supports (%d) — upgrade platformctl", s.bucket, s.stateKey(), st.Version, state.CurrentVersion)
	}
	st.Normalize()
	return st, nil
}

// RawVersion reports the state object's on-disk version without going
// through Load's Normalize (which always reports state.CurrentVersion once
// loaded into memory) — the same "was this actually persisted at the
// migrated format" check localfile.Store.RawVersion provides. Absent
// object = CurrentVersion (nothing to migrate).
func (s *Store) RawVersion(ctx context.Context) (int, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, s.stateKey(), minio.GetObjectOptions{})
	if err != nil {
		return 0, fmt.Errorf("get state object s3://%s/%s: %w", s.bucket, s.stateKey(), err)
	}
	defer obj.Close()
	data, err := io.ReadAll(obj)
	if err != nil {
		if isNoSuchKey(err) {
			return state.CurrentVersion, nil
		}
		return 0, fmt.Errorf("read state object s3://%s/%s: %w", s.bucket, s.stateKey(), err)
	}
	if len(data) == 0 {
		return state.CurrentVersion, nil
	}
	var probe struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		return 0, fmt.Errorf("parse state object s3://%s/%s: %w", s.bucket, s.stateKey(), err)
	}
	if probe.Version == 0 {
		return 1, nil
	}
	return probe.Version, nil
}

func (s *Store) Save(ctx context.Context, st state.State) error {
	st.Version = state.CurrentVersion
	st.Flatten()
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return fmt.Errorf("encode state: %w", err)
	}
	if _, err := s.client.PutObject(ctx, s.bucket, s.stateKey(), bytes.NewReader(data), int64(len(data)),
		minio.PutObjectOptions{ContentType: "application/json"}); err != nil {
		return fmt.Errorf("put state object s3://%s/%s: %w", s.bucket, s.stateKey(), err)
	}
	return nil
}

// lease is the lock object's content — a fixed-TTL claim, not a
// continuously-renewed one (docs/design/003's documented simplification).
type lease struct {
	Holder     string    `json:"holder"`
	AcquiredAt time.Time `json:"acquiredAt"`
	ExpiresAt  time.Time `json:"expiresAt"`
}

// Lock acquires the lease via a create-only-if-absent conditional PUT
// (MinIO's SetMatchETagExcept("*") extension — the reason this backend
// targets MinIO specifically, see docs/design/003). An existing, unexpired
// lease fails fast naming its holder; an expired one is reclaimed via a
// conditional PUT matched to its own ETag, so two clients racing to reclaim
// the same expired lease can't both succeed.
func (s *Store) Lock(ctx context.Context) (func() error, error) {
	now := time.Now()
	mine := lease{Holder: s.holder, AcquiredAt: now, ExpiresAt: now.Add(s.leaseTTL)}
	data, err := json.Marshal(mine)
	if err != nil {
		return nil, err
	}

	createOpts := minio.PutObjectOptions{ContentType: "application/json"}
	createOpts.SetMatchETagExcept("*")
	if _, err := s.client.PutObject(ctx, s.bucket, s.lockKey(), bytes.NewReader(data), int64(len(data)), createOpts); err == nil {
		return s.releaseFunc(mine), nil
	} else if !isPreconditionFailed(err) {
		return nil, fmt.Errorf("acquire state lock s3://%s/%s: %w", s.bucket, s.lockKey(), err)
	}

	existing, etag, err := s.readLease(ctx)
	if err != nil {
		return nil, fmt.Errorf("inspect existing state lock s3://%s/%s: %w", s.bucket, s.lockKey(), err)
	}
	if now.Before(existing.ExpiresAt) {
		return nil, fmt.Errorf("state is locked by %q (expires %s); if that process died, run `platformctl state unlock`, or wait for the lease to expire",
			existing.Holder, existing.ExpiresAt.Format(time.RFC3339))
	}

	reclaimOpts := minio.PutObjectOptions{ContentType: "application/json"}
	reclaimOpts.SetMatchETag(etag)
	if _, err := s.client.PutObject(ctx, s.bucket, s.lockKey(), bytes.NewReader(data), int64(len(data)), reclaimOpts); err != nil {
		return nil, fmt.Errorf("reclaim expired state lock s3://%s/%s (lost the race to another process): %w", s.bucket, s.lockKey(), err)
	}
	return s.releaseFunc(mine), nil
}

// releaseFunc deletes the lock object only if it still holds the lease this
// call acquired — a lease that already expired and was reclaimed by
// someone else must never be deleted out from under them.
func (s *Store) releaseFunc(mine lease) func() error {
	return func() error {
		ctx := context.Background()
		current, _, err := s.readLease(ctx)
		if err != nil {
			if isNoSuchKey(err) {
				return nil // already gone
			}
			return fmt.Errorf("read state lock before release: %w", err)
		}
		if current.Holder != mine.Holder || !current.AcquiredAt.Equal(mine.AcquiredAt) {
			return nil // reclaimed by someone else; not ours to delete
		}
		if err := s.client.RemoveObject(ctx, s.bucket, s.lockKey(), minio.RemoveObjectOptions{}); err != nil {
			return fmt.Errorf("release state lock s3://%s/%s: %w", s.bucket, s.lockKey(), err)
		}
		return nil
	}
}

// ForceUnlock removes the lock object unconditionally — the
// `platformctl state unlock` escape hatch for a lease whose holder process
// died before the TTL lapsed.
func (s *Store) ForceUnlock(ctx context.Context) error {
	if err := s.client.RemoveObject(ctx, s.bucket, s.lockKey(), minio.RemoveObjectOptions{}); err != nil && !isNoSuchKey(err) {
		return fmt.Errorf("force-unlock state lock s3://%s/%s: %w", s.bucket, s.lockKey(), err)
	}
	return nil
}

func (s *Store) readLease(ctx context.Context) (lease, string, error) {
	var l lease
	obj, err := s.client.GetObject(ctx, s.bucket, s.lockKey(), minio.GetObjectOptions{})
	if err != nil {
		return l, "", err
	}
	defer obj.Close()
	info, err := obj.Stat()
	if err != nil {
		return l, "", err
	}
	data, err := io.ReadAll(obj)
	if err != nil {
		return l, "", err
	}
	if err := json.Unmarshal(data, &l); err != nil {
		return l, "", fmt.Errorf("parse lock object s3://%s/%s: %w", s.bucket, s.lockKey(), err)
	}
	return l, info.ETag, nil
}

func isNoSuchKey(err error) bool {
	resp := minio.ToErrorResponse(err)
	return resp.Code == "NoSuchKey"
}

func isPreconditionFailed(err error) bool {
	resp := minio.ToErrorResponse(err)
	return resp.Code == "PreconditionFailed"
}
