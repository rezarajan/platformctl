//go:build integration

package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	dockerruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/docker"
	"github.com/rezarajan/platformctl/internal/testkit"
)

const (
	s3srcKafkaAddr = "localhost:19295"
	s3srcMinioAddr = "localhost:19102"
	s3srcOrigin    = "s3src-origin-events"
	s3srcReplayed  = "s3src-replayed-events"
	s3srcGates     = "IngestProvider=true"
)

// TestS3SourceIngestEndToEnd covers docs/planning/08 D4's accept criteria:
// objects written by the existing sink suite (the s3sink provider) are
// replayed by s3source into a fresh topic and consumed with content
// asserted, plus the standing per-task bars: idempotent re-apply and clean
// destroy.
func TestS3SourceIngestEndToEnd(t *testing.T) {
	t.Setenv("DATASCAPE_SECRET_S3SRC_MINIO_ROOT_USERNAME", "datascape_minio")
	t.Setenv("DATASCAPE_SECRET_S3SRC_MINIO_ROOT_PASSWORD", "minio-secret-pw")

	// s3sink's own required image (read-only reference — this task exercises
	// it, does not modify it) and this task's own s3source image.
	buildImage(t, "datascape-s3sink-connect:test", "testdata/s3sink-image")
	buildImage(t, "datascape-s3source-connect:test", "testdata/s3source-image")

	rt, err := dockerruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to Docker: %v", err)
	}
	ctx := context.Background()

	containers := []string{
		"datascape-s3src-source", "datascape-s3src-sink", "datascape-s3src-minio", "datascape-s3src-rp",
	}
	// docs/adr/029: janitor-owned cleanup (J2 sweep) — declared
	// objects, canonical order, silent pre-clean, loud post-clean.
	jan := testkit.Janitor{
		RT:        rt,
		Workloads: containers,
		Volumes:   []string{"datascape-s3src-rp-data", "datascape-s3src-minio-data"},
		Networks:  []string{"datascape-s3src-net"},
	}
	jan.CleanSilent(ctx)
	jan.Register(ctx, t)

	stateFile := filepath.Join(t.TempDir(), "state.json")
	manifests := "testdata/s3source-scenario"

	start := time.Now()
	out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", s3srcGates)
	if err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}
	t.Logf("apply from empty state took %s", time.Since(start).Round(time.Second))

	out, err, code = run(t, "status", manifests, "--state-file", stateFile, "--feature-gates", s3srcGates)
	if err != nil || code != 0 {
		t.Fatalf("status failed (code %d): %v\n%s", code, err, out)
	}
	assertAllStatusReady(t, out, "apply")

	// Produce directly to the origin topic (bypassing CDC entirely — an
	// EventStream's resource name IS its Kafka topic name, and there is no
	// CDC leg in this scenario at all, mirroring connect_ha_dlq_integration_
	// test.go's chdlqProduce comment). The sink Binding lands these as one
	// or more objects in MinIO.
	records := []string{
		`{"id":1,"name":"alice"}`,
		`{"id":2,"name":"bob"}`,
		`{"id":3,"name":"carol"}`,
	}
	for _, r := range records {
		s3srcProduce(t, s3srcOrigin, []byte(r))
	}

	// The sink leg: an object appears in MinIO under the Dataset's
	// bucket/prefix (reuses sink_integration_test.go's waitForObjectAt
	// verbatim).
	obj := waitForObjectAt(t, ctx, s3srcMinioAddr, "datascape_minio", "minio-secret-pw", "raw-replay", "events/", 180*time.Second)
	for _, r := range records {
		if !strings.Contains(obj, extractName(r)) {
			t.Errorf("landed object does not contain %q:\n%s", extractName(r), obj)
		}
	}

	// The ingest leg: s3source replays the object(s) into the FRESH target
	// topic — content asserted (every produced record's distinguishing
	// field value is present somewhere on the replayed topic).
	s3srcWaitForRecordsContaining(t, s3srcReplayed, []string{"alice", "bob", "carol"}, 180*time.Second)

	// Idempotent re-apply.
	out, err, code = run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", s3srcGates)
	if err != nil || code != 0 {
		t.Fatalf("re-apply failed (code %d): %v\n%s", code, err, out)
	}
	if !strings.Contains(out, "no changes") {
		t.Errorf("re-apply did not report 'no changes':\n%s", out)
	}

	// Clean destroy.
	out, err, code = run(t, "destroy", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", s3srcGates)
	if err != nil || code != 0 {
		t.Fatalf("destroy failed (code %d): %v\n%s", code, err, out)
	}
	for _, c := range containers {
		if _, found, _ := rt.Inspect(ctx, c); found {
			t.Errorf("container %s still present after destroy", c)
		}
	}
	managed, err := rt.ListManaged(ctx)
	if err != nil {
		t.Fatalf("list managed: %v", err)
	}
	for _, m := range managed {
		if strings.HasPrefix(m.Name, "datascape-s3src-") {
			t.Errorf("orphaned managed container after destroy: %s", m.Name)
		}
	}
}

// TestS3SourceValidateCapabilityErrorExact covers this task's negative-path
// requirement: an ingest-mode Binding against a provider that does NOT
// implement IngestCapableProvider fails at validate with the exact ADR 009
// error shape — no image build, no Docker containers.
func TestS3SourceValidateCapabilityErrorExact(t *testing.T) {
	dir := t.TempDir()
	manifest := `
apiVersion: datascape.io/v1alpha1
kind: SecretReference
metadata:
  name: creds
spec:
  backend: env
  keys: [username, password]
---
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: not-capable
spec:
  type: s3sink
  runtime: {type: docker, network: n}
  configuration: {image: "x:test", bootstrapServers: "broker:29092", credentialsSecretRef: creds}
  secretRefs: [creds]
---
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: store
spec:
  type: minio
  runtime: {type: docker, network: n}
  configuration: {rootSecretRef: creds}
  secretRefs: [creds]
---
apiVersion: datascape.io/v1alpha1
kind: Dataset
metadata:
  name: lake
spec:
  providerRef: {name: store}
  bucket: raw
  format: jsonl
---
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: broker
spec:
  type: redpanda
  runtime: {type: docker, network: n}
---
apiVersion: datascape.io/v1alpha1
kind: EventStream
metadata:
  name: events
spec:
  providerRef: {name: broker}
  partitions: 1
---
apiVersion: datascape.io/v1alpha1
kind: Binding
metadata:
  name: lake-to-events
spec:
  mode: ingest
  sourceRef: {name: lake}
  targetRef: {name: events}
  providerRef: {name: not-capable}
`
	if err := os.WriteFile(filepath.Join(dir, "manifests.yaml"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err, code := run(t, "validate", dir)
	if err == nil || code == 0 {
		t.Fatalf("want validate to fail against a non-ingest-capable provider, got code %d\n%s", code, out)
	}
	const want = `Binding "lake-to-events": Provider "not-capable" (type: s3sink)
does not support mode "ingest" (provider implements no ingest capability)`
	if !strings.Contains(out, want) && !strings.Contains(err.Error(), want) {
		t.Errorf("error does not match the exact ADR 009 shape.\nwant substring:\n%s\ngot stdout:\n%s\ngot err: %v", want, out, err)
	}
}

// s3srcKafkaClient is a plain single-broker kgo client, mirroring
// connect_ha_dlq_integration_test.go's chdlqKafkaClient — this scenario's
// redpanda Provider does not declare configuration.brokers, so the legacy
// single-broker advertised address is directly dialable.
func s3srcKafkaClient(t *testing.T, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(s3srcKafkaAddr)}, opts...)...)
	if err != nil {
		t.Fatalf("kafka client: %v", err)
	}
	return cl
}

func s3srcProduce(t *testing.T, topic string, value []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl := s3srcKafkaClient(t)
	defer cl.Close()
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: value}).FirstErr(); err != nil {
		t.Fatalf("produce to %q: %v", topic, err)
	}
}

// s3srcWaitForRecordsContaining polls topic from the earliest offset until a
// record has appeared for every marker in want (each marker found in at
// least one record's value), or timeout elapses.
func s3srcWaitForRecordsContaining(t *testing.T, topic string, want []string, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cl := s3srcKafkaClient(t, kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	defer cl.Close()
	remaining := make(map[string]bool, len(want))
	for _, w := range want {
		remaining[w] = true
	}
	for {
		fetches := cl.PollFetches(ctx)
		fetches.EachRecord(func(r *kgo.Record) {
			for w := range remaining {
				if strings.Contains(string(r.Value), w) {
					delete(remaining, w)
				}
			}
		})
		if len(remaining) == 0 {
			return
		}
		if ctx.Err() != nil {
			missing := make([]string, 0, len(remaining))
			for w := range remaining {
				missing = append(missing, w)
			}
			t.Fatalf("markers %v did not appear on replayed topic %q within %s", missing, topic, timeout)
		}
	}
}

// extractName pulls the "name" field's value out of one of this test's
// hand-written JSON records, for the substring assertion against the raw
// landed object body.
func extractName(record string) string {
	i := strings.Index(record, `"name":"`)
	if i < 0 {
		return record
	}
	rest := record[i+len(`"name":"`):]
	j := strings.Index(rest, `"`)
	if j < 0 {
		return rest
	}
	return rest[:j]
}
