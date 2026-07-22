//go:build integration

package s3

import (
	"context"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/rezarajan/platformctl/internal/ports/state"
	"github.com/rezarajan/platformctl/internal/ports/state/conformance"
)

const (
	testMinioContainer = "datascape-test-state-minio"
	testMinioPort      = "19110"
	testMinioUser      = "datascape-state-test"
	testMinioPass      = "datascape-state-test-pw"
)

// startTestMinio brings up a real MinIO instance for the duration of the
// test file's run and returns its endpoint. Shared across tests in this
// file via TestMain so each test doesn't pay the container-startup cost.
func startTestMinio(t *testing.T) string {
	t.Helper()
	endpoint := "127.0.0.1:" + testMinioPort
	if out, err := exec.Command("docker", "run", "-d", "--name", testMinioContainer,
		"-p", testMinioPort+":9000",
		"-e", "MINIO_ROOT_USER="+testMinioUser,
		"-e", "MINIO_ROOT_PASSWORD="+testMinioPass,
		"minio/minio:RELEASE.2025-04-22T22-12-26Z@sha256:a1ea29fa28355559ef137d71fc570e508a214ec84ff8083e39bc5428980b015e",
		"server", "/data").CombinedOutput(); err != nil {
		if !strings.Contains(string(out), "already in use") {
			t.Fatalf("start minio: %v\n%s", err, out)
		}
	}
	t.Cleanup(func() {
		_ = exec.Command("docker", "rm", "-f", testMinioContainer).Run()
	})
	waitForMinio(t, endpoint, 30*time.Second)
	return endpoint
}

func testMinioClient(t *testing.T, endpoint string) *minio.Client {
	t.Helper()
	cl, err := minio.New(endpoint, &minio.Options{
		Creds: credentials.NewStaticV4(testMinioUser, testMinioPass, ""),
	})
	if err != nil {
		t.Fatalf("minio client: %v", err)
	}
	return cl
}

func waitForMinio(t *testing.T, endpoint string, timeout time.Duration) {
	t.Helper()
	cl := testMinioClient(t, endpoint)
	deadline := time.Now().Add(timeout)
	for {
		if _, err := cl.ListBuckets(context.Background()); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("minio at %s did not become ready within %s", endpoint, timeout)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func ensureTestBucket(t *testing.T, endpoint, bucket string) {
	t.Helper()
	cl := testMinioClient(t, endpoint)
	exists, err := cl.BucketExists(context.Background(), bucket)
	if err != nil {
		t.Fatalf("check bucket %q: %v", bucket, err)
	}
	if !exists {
		if err := cl.MakeBucket(context.Background(), bucket, minio.MakeBucketOptions{}); err != nil {
			t.Fatalf("create bucket %q: %v", bucket, err)
		}
	}
}

// TestConformance runs the same suite localfile passes, against a real
// MinIO instance — docs/adr/003's own accept criterion.
func TestConformance(t *testing.T) {
	endpoint := startTestMinio(t)
	bucket := "datascape-state-conformance"
	ensureTestBucket(t, endpoint, bucket)

	n := 0
	conformance.Run(t, func(t *testing.T) state.StateStore {
		t.Helper()
		n++
		// Each subtest gets its own prefix so they don't share a lock/state
		// object with each other (conformance.Factory's "fresh, empty
		// store per invocation" contract).
		store, err := New(Config{
			Endpoint:  endpoint,
			AccessKey: testMinioUser,
			SecretKey: testMinioPass,
			Bucket:    bucket,
			Prefix:    "conformance-" + strconv.Itoa(n) + "/",
			LeaseTTL:  5 * time.Second,
		})
		if err != nil {
			t.Fatalf("new s3 state store: %v", err)
		}
		return store
	})
}

// TestLockReclaimsAfterExpiry proves the expired-lease reclaim path
// specifically (docs/adr/003): a lease that has expired must be
// reclaimable by a different holder, not just by the original one.
func TestLockReclaimsAfterExpiry(t *testing.T) {
	endpoint := startTestMinio(t)
	bucket := "datascape-state-lock-expiry"
	ensureTestBucket(t, endpoint, bucket)

	first, err := New(Config{
		Endpoint: endpoint, AccessKey: testMinioUser, SecretKey: testMinioPass,
		Bucket: bucket, Prefix: "expiry/", LeaseTTL: 1 * time.Second, Holder: "first-holder",
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := New(Config{
		Endpoint: endpoint, AccessKey: testMinioUser, SecretKey: testMinioPass,
		Bucket: bucket, Prefix: "expiry/", LeaseTTL: 30 * time.Second, Holder: "second-holder",
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	unlock, err := first.Lock(ctx)
	if err != nil {
		t.Fatalf("first Lock: %v", err)
	}
	_ = unlock // release is deliberately never called

	if _, err := second.Lock(ctx); err == nil {
		t.Fatal("second Lock succeeded while first's lease is still live")
	} else if !strings.Contains(err.Error(), "first-holder") {
		t.Errorf("lock-held error does not name the holder: %v", err)
	}

	// Simulate the holder DYING: a live holder renews its lease (doc 11
	// production review), so merely skipping release would keep the lease
	// alive forever — correctly. Death takes the renewal goroutine too.
	first.stopRenewal()
	time.Sleep(1500 * time.Millisecond) // let first's 1s lease expire un-renewed

	unlock2, err := second.Lock(ctx)
	if err != nil {
		t.Fatalf("second Lock after first's lease expired: %v", err)
	}
	if err := unlock2(); err != nil {
		t.Fatalf("unlock: %v", err)
	}
}

// TestForceUnlock covers the `state unlock` escape hatch.
func TestForceUnlock(t *testing.T) {
	endpoint := startTestMinio(t)
	bucket := "datascape-state-force-unlock"
	ensureTestBucket(t, endpoint, bucket)

	store, err := New(Config{
		Endpoint: endpoint, AccessKey: testMinioUser, SecretKey: testMinioPass,
		Bucket: bucket, Prefix: "force/", LeaseTTL: 30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := store.Lock(ctx); err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if _, err := store.Lock(ctx); err == nil {
		t.Fatal("second Lock succeeded while the first is still live")
	}
	if err := store.ForceUnlock(ctx); err != nil {
		t.Fatalf("ForceUnlock: %v", err)
	}
	unlock, err := store.Lock(ctx)
	if err != nil {
		t.Fatalf("Lock after ForceUnlock: %v", err)
	}
	_ = unlock()
}
