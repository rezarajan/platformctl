//go:build integration

package main

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

const (
	chdlqKafkaAddr  = "localhost:19693"
	chdlqSinkURL    = "http://localhost:18685"
	chdlqMinioAddr  = "localhost:19501"
	chdlqTopic      = "chdlq-attendance-events"
	chdlqDLQTopic   = "chdlq-attendance-events-dlq"
	chdlqGates      = "HighAvailability=true"
	chdlqSinkBucket = "raw-events"
	chdlqSinkPrefix = "attendance/"
)

// chdlqKafkaClient is a plain single-broker kgo client — this scenario's
// redpanda Provider does not declare configuration.brokers, so the legacy
// single-broker advertised address (127.0.0.1:<kafkaPort>) is directly
// dialable with no token/dialer-redirect trick (that machinery is only
// needed for the multi-broker path — docs/adr/017 §a.4).
func chdlqKafkaClient(t *testing.T, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(chdlqKafkaAddr)}, opts...)...)
	if err != nil {
		t.Fatalf("kafka client: %v", err)
	}
	return cl
}

// chdlqProduce produces one record with a raw byte value directly to topic
// — bypassing CDC entirely. This EventStream's own bare topic name is what
// redpanda's topic reconcile creates (an EventStream's name IS its Kafka
// topic name — the same convention redpanda.reconcileTopic uses), and
// s3sink's topics.regex (^<sourceRef>(\..*)?$) matches the bare name in
// addition to any CDC-per-table-prefixed one, so a direct produce here
// exercises the sink leg without needing a live CDC pipeline for this
// particular assertion.
func chdlqProduce(t *testing.T, topic string, value []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl := chdlqKafkaClient(t)
	defer cl.Close()
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: value}).FirstErr(); err != nil {
		t.Fatalf("produce to %q: %v", topic, err)
	}
}

// chdlqWaitForRecordContaining polls topic from the earliest offset until a
// record whose value contains marker arrives, or timeout elapses.
func chdlqWaitForRecordContaining(t *testing.T, topic string, marker []byte, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cl := chdlqKafkaClient(t, kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	defer cl.Close()
	for {
		fetches := cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			t.Fatalf("no record containing %q appeared on topic %q within %s", marker, topic, timeout)
		}
		found := false
		fetches.EachRecord(func(r *kgo.Record) {
			if strings.Contains(string(r.Value), string(marker)) {
				found = true
			}
		})
		if found {
			return
		}
	}
}

func chdlqConnectorState(t *testing.T, name string) string {
	t.Helper()
	var body struct {
		Connector struct {
			State string `json:"state"`
		} `json:"connector"`
		Tasks []struct {
			State string `json:"state"`
		} `json:"tasks"`
	}
	getJSON(t, fmt.Sprintf("%s/connectors/%s/status", chdlqSinkURL, name), &body)
	state := body.Connector.State
	for _, task := range body.Tasks {
		if task.State != "RUNNING" {
			return task.State
		}
	}
	return state
}

func chdlqConnectorConfig(t *testing.T, name string) map[string]string {
	t.Helper()
	var cfg map[string]string
	getJSON(t, fmt.Sprintf("%s/connectors/%s/config", chdlqSinkURL, name), &cfg)
	return cfg
}

// chdlqObjectContains polls the MinIO bucket/prefix until some landed
// object's content contains marker, or timeout elapses — unlike
// waitForObjectAt (which returns the *first* object it happens to see and
// stops there), this scans every object each poll so a marker introduced
// by a *later* record is not missed just because an earlier object already
// satisfied a prior check.
func chdlqObjectContains(t *testing.T, ctx context.Context, marker string, timeout time.Duration) bool {
	t.Helper()
	cl, err := minio.New(chdlqMinioAddr, &minio.Options{
		Creds:  credentials.NewStaticV4("datascape_minio", "minio-secret-pw", ""),
		Secure: false,
	})
	if err != nil {
		t.Fatalf("minio client: %v", err)
	}
	deadline := time.Now().Add(timeout)
	for {
		for obj := range cl.ListObjects(ctx, chdlqSinkBucket, minio.ListObjectsOptions{Prefix: chdlqSinkPrefix, Recursive: true}) {
			if obj.Err != nil {
				continue
			}
			r, err := cl.GetObject(ctx, chdlqSinkBucket, obj.Key, minio.GetObjectOptions{})
			if err != nil {
				continue
			}
			body := make([]byte, 0, 4096)
			buf := make([]byte, 4096)
			for {
				n, rerr := r.Read(buf)
				body = append(body, buf[:n]...)
				if rerr != nil {
					break
				}
			}
			r.Close()
			if strings.Contains(string(body), marker) {
				return true
			}
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(3 * time.Second)
	}
}

// TestConnectWorkersHAAndDeadLetterQueue covers docs/planning/08 C3's and
// D6's Accept lists together (bundled: they touch the same files —
// internal/adapters/kafkaconnect, internal/adapters/providers/{debezium,
// s3sink}):
//
//   - C3: a 2-worker debezium Connect set stays live when one worker is
//     killed out-of-band — `drift` (never `apply`) reports the CDC Binding
//     still Ready=True, proving multi-address REST failover works; worker
//     count drift is detected once the killed worker is confirmed absent
//     from the runtime.
//   - D6: a sink Binding with options.deadLetter declared routes a poison
//     (non-JSON) record to the declared DLQ EventStream's topic, the sink
//     connector stays RUNNING throughout, and a subsequent valid record
//     still lands in the object store.
func TestConnectWorkersHAAndDeadLetterQueue(t *testing.T) {
	t.Setenv("DATASCAPE_SECRET_CHDLQ_PG_ADMIN_USERNAME", "datascape_admin")
	t.Setenv("DATASCAPE_SECRET_CHDLQ_PG_ADMIN_PASSWORD", "admin-secret-pw")
	t.Setenv("DATASCAPE_SECRET_CHDLQ_PG_REPL_USERNAME", "datascape_repl")
	t.Setenv("DATASCAPE_SECRET_CHDLQ_PG_REPL_PASSWORD", "repl-secret-pw")
	t.Setenv("DATASCAPE_SECRET_CHDLQ_MINIO_ROOT_USERNAME", "datascape_minio")
	t.Setenv("DATASCAPE_SECRET_CHDLQ_MINIO_ROOT_PASSWORD", "minio-secret-pw")

	buildSinkConnectImage(t) // shared with TestSinkEndToEnd (testdata/s3sink-image)

	rt := requireDocker(t)
	ctx := context.Background()

	dbzBase := "datascape-chdlq-dbz"
	containers := []string{
		"datascape-chdlq-rp", "datascape-chdlq-pg", "datascape-chdlq-minio", "datascape-chdlq-s3",
		runtime.OrdinalName(dbzBase, 0), runtime.OrdinalName(dbzBase, 1),
	}
	volumes := []string{"datascape-chdlq-pg-data", "datascape-chdlq-rp-data", "datascape-chdlq-minio-data"}
	cleanup := registerDockerCleanup(t, rt, containers, volumes, "datascape-chdlq-net")
	cleanup()

	stateFile := filepath.Join(t.TempDir(), "state.json")
	manifests := "testdata/connect-ha-dlq-scenario"

	out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", chdlqGates)
	if err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}

	// Both debezium worker ordinals must be up — the C3 replica-set shape.
	for i := 0; i < 2; i++ {
		ord := runtime.OrdinalName(dbzBase, i)
		if st, found, err := rt.Inspect(ctx, ord); err != nil || !found || !st.Running {
			t.Fatalf("debezium worker ordinal %s not running after apply: found=%v err=%v", ord, found, err)
		}
	}
	if state := chdlqConnectorState(t, "chdlq-events-to-lake"); state != "RUNNING" {
		t.Fatalf("sink connector state = %q, want RUNNING", state)
	}

	// --- D6: valid record flows through before any poison is introduced ---
	chdlqProduce(t, chdlqTopic, []byte(`{"id":1,"name":"before-poison-marker"}`))
	if !chdlqObjectContains(t, ctx, "before-poison-marker", 180*time.Second) {
		t.Fatal("pre-poison valid record did not land in the object store")
	}

	// --- C3: kill one debezium worker out-of-band, verify via `drift` ---
	// (never `apply`) that the CDC Binding stays/returns RUNNING — the
	// surviving ordinal answers Connect's distributed-mode REST API for
	// the whole group.
	killedOrdinal := runtime.OrdinalName(dbzBase, 1)
	if err := rt.Remove(ctx, killedOrdinal); err != nil {
		t.Fatalf("out-of-band worker kill: %v", err)
	}
	report, code := runDrift(t, manifests, stateFile, "--feature-gates", chdlqGates)
	bindingReport := report["Binding/chdlq-students-to-events"]
	if bindingReport.Ready != "True" {
		t.Fatalf("CDC Binding drift report after killing one of two workers = %+v, want Ready=True (drift, not apply, must observe the survivor)", bindingReport)
	}
	// Worker-count drift is detected at the Provider level, naming the
	// killed ordinal — the C3 "worker-count drift detected" Accept item.
	workerReport := report["Provider/"+dbzBase]
	if workerReport.Ready != "False" || !strings.Contains(workerReport.Reason, "ConnectWorkerMissing") || !strings.Contains(workerReport.Reason, killedOrdinal) {
		t.Fatalf("debezium worker-set drift report = %+v, want Ready=False naming ConnectWorkerMissing(%s)", workerReport, killedOrdinal)
	}
	if code == 0 {
		t.Fatal("drift reported clean with a debezium worker missing")
	}

	// Heal the worker set back (apply is allowed again from here — the
	// "without apply" requirement only covers the observation step above).
	out, err, code = run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", chdlqGates)
	if err != nil || code != 0 {
		t.Fatalf("healing apply failed (code %d): %v\n%s", code, err, out)
	}
	if st, found, err := rt.Inspect(ctx, killedOrdinal); err != nil || !found || !st.Running {
		t.Fatalf("killed worker %s not healed by re-apply: found=%v err=%v", killedOrdinal, found, err)
	}

	// --- D6: the poison record itself ---
	poison := []byte("this-is-not-json-poison-marker-xyz")
	chdlqProduce(t, chdlqTopic, poison)

	// The poison record must land in the declared DLQ topic.
	chdlqWaitForRecordContaining(t, chdlqDLQTopic, poison, 180*time.Second)

	// The sink connector must stay RUNNING throughout (errors.tolerance:
	// all absorbed the conversion failure).
	if state := chdlqConnectorState(t, "chdlq-events-to-lake"); state != "RUNNING" {
		t.Errorf("sink connector state after poison record = %q, want RUNNING", state)
	}

	// Valid records must keep flowing after the poison.
	chdlqProduce(t, chdlqTopic, []byte(`{"id":2,"name":"after-poison-marker"}`))
	if !chdlqObjectContains(t, ctx, "after-poison-marker", 180*time.Second) {
		t.Fatal("valid record after the poison did not land in the object store")
	}

	// The registered connector config carries the DLQ keys
	// (docs/planning/08 D6) — the live counterpart to s3sink_test.go's
	// TestDeadLetterConfigTranslation.
	cfg := chdlqConnectorConfig(t, "chdlq-events-to-lake")
	if got := cfg["errors.tolerance"]; got != "all" {
		t.Errorf("live connector config errors.tolerance = %q, want all", got)
	}
	if got := cfg["errors.deadletterqueue.topic.name"]; got != chdlqDLQTopic {
		t.Errorf("live connector config errors.deadletterqueue.topic.name = %q, want %q", got, chdlqDLQTopic)
	}

	// Exit: destroy tears everything down cleanly, including both worker
	// ordinals.
	out, err, code = run(t, "destroy", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", chdlqGates)
	if err != nil || code != 0 {
		t.Fatalf("destroy failed (code %d): %v\n%s", code, err, out)
	}
	for _, c := range containers {
		if _, found, _ := rt.Inspect(ctx, c); found {
			t.Errorf("container %s still present after destroy", c)
		}
	}
}
