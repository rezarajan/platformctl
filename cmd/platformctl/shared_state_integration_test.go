//go:build integration

package main

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	s3state "github.com/rezarajan/platformctl/internal/adapters/state/s3"
)

const (
	sharedStateMinioContainer = "datascape-test-shared-state-minio"
	sharedStateMinioPort      = "19122"
	sharedStateMinioUser      = "datascape-shared-state-test"
	sharedStateMinioPass      = "datascape-shared-state-test-pw"
)

func startSharedStateMinio(t *testing.T) string {
	t.Helper()
	endpoint := "127.0.0.1:" + sharedStateMinioPort
	if out, err := exec.Command("docker", "run", "-d", "--name", sharedStateMinioContainer,
		"-p", sharedStateMinioPort+":9000",
		"-e", "MINIO_ROOT_USER="+sharedStateMinioUser,
		"-e", "MINIO_ROOT_PASSWORD="+sharedStateMinioPass,
		"minio/minio:RELEASE.2025-04-22T22-12-26Z@sha256:a1ea29fa28355559ef137d71fc570e508a214ec84ff8083e39bc5428980b015e",
		"server", "/data").CombinedOutput(); err != nil {
		t.Fatalf("start minio: %v\n%s", err, out)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", "-v", sharedStateMinioContainer).Run() })

	cl, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(sharedStateMinioUser, sharedStateMinioPass, "")})
	if err != nil {
		t.Fatalf("minio client: %v", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, err := cl.ListBuckets(context.Background()); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("minio at %s did not become ready in time", endpoint)
		}
		time.Sleep(300 * time.Millisecond)
	}
	if err := cl.MakeBucket(context.Background(), "datascape-shared-state", minio.MakeBucketOptions{}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	return endpoint
}

// TestSharedStateBackendEndToEnd covers docs/planning/08 A4 / docs/adr/003:
// apply/status/destroy against a real S3 (MinIO) state backend through the
// actual CLI — not just the adapter's own conformance suite — with the
// SharedStateBackend gate off (refused) and on (works), credentials
// resolved through the env-backend SecretReference convention.
func TestSharedStateBackendEndToEnd(t *testing.T) {
	endpoint := startSharedStateMinio(t)
	t.Setenv("DATASCAPE_SECRET_MINIOSTATECREDS_ACCESSKEY", sharedStateMinioUser)
	t.Setenv("DATASCAPE_SECRET_MINIOSTATECREDS_SECRETKEY", sharedStateMinioPass)

	baseArgs := []string{
		"--state-backend", "s3",
		"--state-bucket", "datascape-shared-state",
		"--state-prefix", "e2e/",
		"--state-endpoint", endpoint,
		"--state-insecure",
		"--state-secret-ref", "miniostatecreds",
	}
	gatedArgs := append(append([]string{}, baseArgs...), "--feature-gates", "SharedStateBackend=true")

	manifests := "testdata/noop-scenario"

	// Without the gate: refused, naming the gate.
	args := append([]string{"apply", manifests, "--auto-approve"}, baseArgs...)
	_, err, _ := run(t, args...)
	if err == nil {
		t.Fatal("apply against the s3 backend succeeded without the SharedStateBackend gate")
	}
	if !strings.Contains(err.Error(), "SharedStateBackend") {
		t.Errorf("gate refusal does not name SharedStateBackend: %v", err)
	}

	// With the gate: applies for real, state persists in MinIO.
	args = append([]string{"apply", manifests, "--auto-approve"}, gatedArgs...)
	out, err, code := run(t, args...)
	if err != nil || code != 0 {
		t.Fatalf("apply against the s3 backend failed (code %d): %v\n%s", code, err, out)
	}

	args = append([]string{"status", manifests}, gatedArgs...)
	out, err, code = run(t, args...)
	if err != nil || code != 0 {
		t.Fatalf("status against the s3 backend failed (code %d): %v\n%s", code, err, out)
	}
	assertAllStatusReady(t, out, "s3-backed apply")

	args = append([]string{"destroy", manifests, "--auto-approve"}, gatedArgs...)
	out, err, code = run(t, args...)
	if err != nil || code != 0 {
		t.Fatalf("destroy against the s3 backend failed (code %d): %v\n%s", code, err, out)
	}
}

// TestSharedStateBackendConcurrentApplyOneBlocks covers docs/planning/08
// A4's own accept criterion: two operators racing to apply against the same
// S3-backed state — one proceeds, the other fails fast naming the first's
// holder identity, and no interleaved/corrupted write results. The second
// operator's lock attempt is deterministically simulated by acquiring the
// lock directly (the same s3state.Store the CLI itself constructs) before
// invoking `apply`, rather than racing two OS processes' timing — this
// proves the identical code path without depending on scheduler luck.
func TestSharedStateBackendConcurrentApplyOneBlocks(t *testing.T) {
	endpoint := startSharedStateMinio(t)
	t.Setenv("DATASCAPE_SECRET_MINIOSTATECREDS_ACCESSKEY", sharedStateMinioUser)
	t.Setenv("DATASCAPE_SECRET_MINIOSTATECREDS_SECRETKEY", sharedStateMinioPass)

	gatedArgs := []string{
		"--state-backend", "s3",
		"--state-bucket", "datascape-shared-state",
		"--state-prefix", "concurrent/",
		"--state-endpoint", endpoint,
		"--state-insecure",
		"--state-secret-ref", "miniostatecreds",
		"--feature-gates", "SharedStateBackend=true",
	}
	manifests := "testdata/noop-scenario"

	// "Operator A": holds the lock directly, simulating an apply in progress.
	operatorA, err := s3state.New(s3state.Config{
		Endpoint: endpoint, AccessKey: sharedStateMinioUser, SecretKey: sharedStateMinioPass,
		Bucket: "datascape-shared-state", Prefix: "concurrent/", LeaseTTL: 30 * time.Second, Holder: "operator-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	release, err := operatorA.Lock(context.Background())
	if err != nil {
		t.Fatalf("operator A Lock: %v", err)
	}

	// "Operator B": the CLI's own apply, blocked by operator A's lock.
	args := append([]string{"apply", manifests, "--auto-approve"}, gatedArgs...)
	_, err, code := run(t, args...)
	if err == nil {
		t.Fatal("operator B's apply succeeded while operator A holds the lock")
	}
	if code != 4 { // cliutil.ExitLockHeld
		t.Errorf("operator B's apply exit code = %d, want 4 (ExitLockHeld)", code)
	}
	if !strings.Contains(err.Error(), "operator-a") {
		t.Errorf("lock-held error does not name operator A's holder identity: %v", err)
	}

	// No interleaved write: state is untouched (empty — operator B never
	// got past Lock to write anything).
	loadArgs := append(append([]string{"state", "inspect"}, gatedArgs...), "-o", "json")
	out, _, _ := run(t, loadArgs...)
	if !strings.Contains(out, `"resources": []`) {
		t.Errorf("state was written to despite the lock being held: %s", out)
	}

	// Operator A finishes; operator B's retry now succeeds.
	if err := release(); err != nil {
		t.Fatalf("release operator A's lock: %v", err)
	}
	args = append([]string{"apply", manifests, "--auto-approve"}, gatedArgs...)
	out, err, code = run(t, args...)
	if err != nil || code != 0 {
		t.Fatalf("apply after lock release failed (code %d): %v\n%s", code, err, out)
	}
}
