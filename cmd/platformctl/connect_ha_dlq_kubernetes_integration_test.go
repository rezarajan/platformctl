//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/twmb/franz-go/pkg/kgo"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	k8sruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/kubernetes"
	"github.com/rezarajan/platformctl/internal/testkit"
)

const (
	chdlqk8sTopic      = "chdlqk8s-attendance-events"
	chdlqk8sDLQTopic   = "chdlqk8s-attendance-events-dlq"
	chdlqk8sGates      = "KubernetesRuntime=true,HighAvailability=true" // workers: 2 (I7)
	chdlqk8sSinkBucket = "raw-events"
	chdlqk8sSinkPrefix = "attendance/"
	chdlqk8sNS         = "datascape-chdlqk8s-test-ns"
	chdlqk8sRPName     = "datascape-chdlqk8s-rp"
	chdlqk8sPGName     = "datascape-chdlqk8s-pg"
	chdlqk8sDBZName    = "datascape-chdlqk8s-dbz"
	chdlqk8sMinioName  = "datascape-chdlqk8s-minio"
	chdlqk8sS3Name     = "datascape-chdlqk8s-s3"
	chdlqk8sConnector  = "chdlqk8s-events-to-lake"
	chdlqk8sBindingKey = "Binding/chdlqk8s-students-to-events"
)

// chdlqk8sKafkaClient opens a fresh client against the redpanda Provider's
// node-port address. Metadata-only admin calls
// (TestRedpandaKubernetesEndToEnd's describeTopicAt) get away with a plain
// seed-broker client, but produce/consume follow Kafka's own protocol to
// the broker's ADVERTISED address — which for the legacy single-broker
// shape is a deliberately undialable loopback sentinel
// (redpanda.advertisedAddr: "127.0.0.1:<kafkaPort>", never host-correct on
// Kubernetes — docs/adr/017 §a.4, doc 07's B1 note). The provider's own
// admin client solves this with a dialer redirect; this test needs the
// identical trick (proven live in I6's first runs: a plain client's
// ProduceSync hangs dialing the sentinel until context deadline): every
// dial, whatever host the broker advertised, is redirected to the resolved
// tunnel address — always correct here, since there is exactly one broker.
func chdlqk8sKafkaClient(t *testing.T, ctx context.Context, rt *k8sruntime.Runtime, opts ...kgo.Opt) (*kgo.Client, func()) {
	t.Helper()
	addr, closeAddr, err := rt.EnsureReachable(ctx, chdlqk8sRPName, 9092)
	if err != nil {
		t.Fatalf("EnsureReachable(%s): %v", chdlqk8sRPName, err)
	}
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(addr), kgo.Dialer(chdlqk8sRedirectDialer(addr))}, opts...)...)
	if err != nil {
		_ = closeAddr()
		t.Fatalf("kafka client: %v", err)
	}
	return cl, func() { _ = closeAddr() }
}

// chdlqk8sRedirectDialer dials addr regardless of the host requested — the
// single-broker counterpart of redpanda kafka.go's advertised-sentinel
// redirect and rpHAK8sClient's per-ordinal token dial map.
func chdlqk8sRedirectDialer(addr string) func(ctx context.Context, network, host string) (net.Conn, error) {
	return func(ctx context.Context, network, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, network, addr)
	}
}

// chdlqk8sProduce retries with a fresh tunnel + client on failure: a
// node-port Service's address can be assigned before kube-proxy has
// actually programmed the node's iptables/ipvs rule for it (a documented
// race, internal/adapters/runtime/kubernetes/reachability.go's
// serviceReachableAddr doc comment) — a single 30s attempt right after
// apply can lose that race even though EnsureReachable's own dial-proof
// polling already covers most of it.
func chdlqk8sProduce(t *testing.T, rt *k8sruntime.Runtime, topic string, value []byte) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	var lastErr error
	for {
		if lastErr = chdlqk8sProduceOnce(rt, topic, value); lastErr == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("produce to %q: %v", topic, lastErr)
		}
		time.Sleep(3 * time.Second)
	}
}

func chdlqk8sProduceOnce(rt *k8sruntime.Runtime, topic string, value []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	addr, closeAddr, err := rt.EnsureReachable(ctx, chdlqk8sRPName, 9092)
	if err != nil {
		return fmt.Errorf("EnsureReachable(%s): %w", chdlqk8sRPName, err)
	}
	defer closeAddr()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.Dialer(chdlqk8sRedirectDialer(addr)))
	if err != nil {
		return fmt.Errorf("kafka client: %w", err)
	}
	defer cl.Close()
	return cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: value}).FirstErr()
}

// chdlqk8sWaitForRecordContaining polls topic from the earliest offset until
// a record whose value contains marker arrives, or timeout elapses.
func chdlqk8sWaitForRecordContaining(t *testing.T, rt *k8sruntime.Runtime, topic string, marker []byte, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cl, closeCl := chdlqk8sKafkaClient(t, ctx, rt, kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	defer closeCl()
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

func chdlqk8sConnectorState(t *testing.T, ctx context.Context, rt *k8sruntime.Runtime, name string) string {
	t.Helper()
	addr, closeAddr, err := rt.EnsureReachable(ctx, chdlqk8sS3Name, 8083)
	if err != nil {
		t.Fatalf("EnsureReachable(%s): %v", chdlqk8sS3Name, err)
	}
	defer closeAddr()
	var body struct {
		Connector struct {
			State string `json:"state"`
		} `json:"connector"`
		Tasks []struct {
			State string `json:"state"`
		} `json:"tasks"`
	}
	getJSON(t, fmt.Sprintf("http://%s/connectors/%s/status", addr, name), &body)
	state := body.Connector.State
	for _, task := range body.Tasks {
		if task.State != "RUNNING" {
			return task.State
		}
	}
	return state
}

func chdlqk8sConnectorConfig(t *testing.T, ctx context.Context, rt *k8sruntime.Runtime, name string) map[string]string {
	t.Helper()
	addr, closeAddr, err := rt.EnsureReachable(ctx, chdlqk8sS3Name, 8083)
	if err != nil {
		t.Fatalf("EnsureReachable(%s): %v", chdlqk8sS3Name, err)
	}
	defer closeAddr()
	var cfg map[string]string
	getJSON(t, fmt.Sprintf("http://%s/connectors/%s/config", addr, name), &cfg)
	return cfg
}

// chdlqk8sObjectContains polls the MinIO bucket/prefix (reached through a
// fresh EnsureReachable tunnel each attempt) until some landed object's
// content contains marker, or timeout elapses — scans every object each
// poll, mirroring the Docker DLQ test's chdlqObjectContains, so a marker
// introduced by a later record is never missed just because an earlier
// object already satisfied a prior check.
func chdlqk8sObjectContains(t *testing.T, ctx context.Context, rt *k8sruntime.Runtime, marker string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if found := chdlqk8sScanObjectsOnce(t, ctx, rt, marker); found {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(3 * time.Second)
	}
}

func chdlqk8sScanObjectsOnce(t *testing.T, ctx context.Context, rt *k8sruntime.Runtime, marker string) bool {
	t.Helper()
	addr, closeAddr, err := rt.EnsureReachable(ctx, chdlqk8sMinioName, 9000)
	if err != nil {
		t.Fatalf("EnsureReachable(%s): %v", chdlqk8sMinioName, err)
	}
	defer closeAddr()
	cl, err := minio.New(addr, &minio.Options{
		Creds:  credentials.NewStaticV4("datascape_minio", "minio-secret-pw", ""),
		Secure: false,
	})
	if err != nil {
		t.Fatalf("minio client: %v", err)
	}
	for obj := range cl.ListObjects(ctx, chdlqk8sSinkBucket, minio.ListObjectsOptions{Prefix: chdlqk8sSinkPrefix, Recursive: true}) {
		if obj.Err != nil {
			continue
		}
		r, err := cl.GetObject(ctx, chdlqk8sSinkBucket, obj.Key, minio.GetObjectOptions{})
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
	return false
}

// chdlqk8sBindingReady polls `drift` (never `apply`) until the CDC Binding
// reports Ready=True, tolerating transient non-JSON/error output during the
// window a killed worker pod is being replaced — a hard decode/command
// failure must retry, not abort the test, since that window is exactly what
// this helper is polling through.
func chdlqk8sBindingReady(t *testing.T, manifests, stateFile string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastOut string
	for {
		out, _, _ := run(t, "drift", manifests, "--state-file", stateFile, "-o", "json", "--feature-gates", chdlqk8sGates)
		lastOut = out
		var payload struct {
			Resources []driftReport `json:"resources"`
		}
		if err := json.NewDecoder(strings.NewReader(out)).Decode(&payload); err == nil {
			for _, r := range payload.Resources {
				name := strings.TrimPrefix(r.Resource, "default/")
				if name == chdlqk8sBindingKey && r.Ready == "True" {
					return
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("CDC Binding did not return Ready within %s (last drift output:\n%s)", timeout, lastOut)
		}
		time.Sleep(3 * time.Second)
	}
}

// chdlqk8sWorkerSetReady polls the debezium worker set's own aggregate
// Inspect until both replicas are observed ready again (the Deployment
// controller's replacement pod has come up and passed its health check),
// or timeout elapses — the I7 counterpart of chdlqk8sBindingReady, for the
// worker set itself rather than the Binding it serves.
func chdlqk8sWorkerSetReady(t *testing.T, ctx context.Context, rt *k8sruntime.Runtime, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		st, found, err := rt.Inspect(ctx, chdlqk8sDBZName)
		if err == nil && found && st.ReadyReplicas == 2 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("debezium worker set did not return to 2/2 ready within %s (found=%v readyReplicas=%d err=%v)", timeout, found, st.ReadyReplicas, err)
		}
		time.Sleep(3 * time.Second)
	}
}

// TestKubernetesConnectDeadLetterQueueAndWorkerResilience is I6/I7's
// (docs/planning/08 §7.8) Kubernetes leg of
// TestConnectWorkersHAAndDeadLetterQueue — now C3's and D6's assertions
// together, the same bar the Docker suite holds:
//
//   - C3: a 2-worker debezium Connect set stays live when one worker pod is
//     killed out-of-band — `drift` (never `apply`, bounded-polled per
//     docs/planning/02 §4.1's settledness rule: a live run found the REST
//     API transiently reports the task UNASSIGNED for a few seconds while
//     Connect's own consumer-group protocol rebalances it) reports the CDC
//     Binding back to Ready=True on the surviving worker, and a record
//     produced right after the kill still reaches the object store — the
//     real end-to-end proof the pipeline kept flowing through the
//     rebalance. Kubernetes then self-heals the killed pod natively (the
//     Deployment controller recreates it with no `apply` needed, unlike
//     Docker).
//   - D6: a sink Binding with options.deadLetter declared routes a poison
//     (non-JSON) record to the declared DLQ EventStream's topic, the sink
//     connector stays RUNNING throughout, and a subsequent valid record
//     still lands in the object store.
//
// I6 found this Kubernetes leg didn't work at all for workers > 1:
// providerkit.ReachableURLs/ProbeConnectWorkerSet addressed a Deployment-
// shaped (StableIdentity: false, docs/adr/004) worker set via
// runtime.OrdinalName(name, i) -> EnsureReachable/Inspect, but Kubernetes
// never creates an object literally named "<name>-0"/"<name>-1" for a
// Deployment (only StatefulSet ordinals get that treatment,
// findOrdinalPod's own doc comment) — every ordinal lookup failed outright,
// `no member of "<name>" (2 ordinals) is currently reachable`, even with a
// perfectly healthy worker set. I7 (docs/adr/004's addendum) fixed this at
// the providerkit/runtime-port seam: the new runtime.MemberSetRuntime
// capability lets providerkit resolve/probe the set once, by its own bare
// Name, which Kubernetes' Service/label-selector and Inspect(name) already
// answer correctly — no Kubernetes reachability code needed to change.
func TestKubernetesConnectDeadLetterQueueAndWorkerResilience(t *testing.T) {
	requireK8s(t)
	t.Setenv("DATASCAPE_SECRET_CHDLQK8S_PG_ADMIN_USERNAME", "datascape_admin")
	t.Setenv("DATASCAPE_SECRET_CHDLQK8S_PG_ADMIN_PASSWORD", "admin-secret-pw")
	t.Setenv("DATASCAPE_SECRET_CHDLQK8S_PG_REPL_USERNAME", "datascape_repl")
	t.Setenv("DATASCAPE_SECRET_CHDLQK8S_PG_REPL_PASSWORD", "repl-secret-pw")
	t.Setenv("DATASCAPE_SECRET_CHDLQK8S_MINIO_ROOT_USERNAME", "datascape_minio")
	t.Setenv("DATASCAPE_SECRET_CHDLQK8S_MINIO_ROOT_PASSWORD", "minio-secret-pw")

	buildSinkConnectImage(t) // shared with the Docker DLQ suite + the K8s CDC example
	loadImageIntoCluster(t, "datascape-s3sink-connect:test")

	rt, err := k8sruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to kubernetes: %v", err)
	}
	ctx := context.Background()
	workloads := []string{chdlqk8sS3Name, chdlqk8sMinioName, chdlqk8sDBZName, chdlqk8sPGName, chdlqk8sRPName}
	// docs/adr/029: janitor-owned cleanup (J2 sweep) — declared
	// objects, canonical order, silent pre-clean, loud post-clean.
	jan := testkit.Janitor{
		RT:        rt,
		Workloads: workloads,
		Networks:  []string{chdlqk8sNS},
	}
	jan.CleanSilent(ctx)
	jan.Register(ctx, t)

	stateFile := filepath.Join(t.TempDir(), "state.json")
	manifests := "testdata/connect-ha-dlq-k8s-scenario"

	out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", chdlqk8sGates)
	if err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}
	if st, found, err := rt.Inspect(ctx, chdlqk8sDBZName); err != nil || !found || !st.Running || st.ReadyReplicas != 2 {
		t.Fatalf("debezium worker set not fully up after apply: found=%v running=%v readyReplicas=%d err=%v", found, st.Running, st.ReadyReplicas, err)
	}
	if state := chdlqk8sConnectorState(t, ctx, rt, chdlqk8sConnector); state != "RUNNING" {
		t.Fatalf("sink connector state = %q, want RUNNING", state)
	}

	// --- D6: valid record flows through before any poison is introduced ---
	chdlqk8sProduce(t, rt, chdlqk8sTopic, []byte(`{"id":1,"name":"before-poison-marker"}`))
	if !chdlqk8sObjectContains(t, ctx, rt, "before-poison-marker", 180*time.Second) {
		t.Fatal("pre-poison valid record did not land in the object store")
	}

	// --- C3: kill one of the two debezium worker pods out-of-band (by
	// label, since a Deployment's pod name is randomized — unlike Docker's/
	// StatefulSet's literal ordinal container names) and verify the
	// pipeline keeps flowing on the surviving worker.
	cs := ambientClientset(t)
	pods, err := cs.CoreV1().Pods(chdlqk8sNS).List(ctx, metav1.ListOptions{LabelSelector: "app=" + chdlqk8sDBZName})
	if err != nil || len(pods.Items) != 2 {
		t.Fatalf("expected 2 debezium worker pods before the kill: found=%d err=%v", len(pods.Items), err)
	}
	killedPod := pods.Items[0].Name
	if err := cs.CoreV1().Pods(chdlqk8sNS).Delete(ctx, killedPod, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("out-of-band worker pod delete: %v", err)
	}

	// C3: the CDC Binding must recover to (or stay) Ready=True on its own —
	// the surviving worker, once Kafka Connect's own consumer-group
	// protocol rebalances the task onto it, answers Connect's
	// distributed-mode REST API for the whole group again (I7's collective
	// addressing: providerkit.ReachableURLs resolves the set's bare Name
	// via the Service/label-selector, which keeps picking a live member
	// throughout). This is a bounded poll, never a single snapshot
	// (docs/planning/02 §4.1's settledness rule) — found live: the REST
	// API transiently reports the task UNASSIGNED for a few seconds right
	// after the kill, while the group notices the departure and
	// rebalances; a one-shot `drift` right after the delete call can catch
	// exactly that transient (Docker's tighter, same-host timing usually
	// outruns it in the equivalent one-shot check, which is not a contract
	// this runtime owes).
	chdlqk8sBindingReady(t, manifests, stateFile, 120*time.Second)

	// C3: the pipeline actually kept flowing — a fresh record produced now
	// (the killed pod's replacement may still be starting) must still
	// reach the object store via the rebalanced task, the real end-to-end
	// proof of "task rebalance," stronger than any REST snapshot.
	chdlqk8sProduce(t, rt, chdlqk8sTopic, []byte(`{"id":3,"name":"mid-kill-marker"}`))
	if !chdlqk8sObjectContains(t, ctx, rt, "mid-kill-marker", 180*time.Second) {
		t.Fatal("record produced after killing one of two workers did not land in the object store — the pipeline did not keep flowing through the task rebalance")
	}

	// The Deployment controller recreates the killed pod natively; the
	// worker set itself returns to 2/2 ready with no `apply` needed.
	chdlqk8sWorkerSetReady(t, ctx, rt, 3*time.Minute)

	// Heal any remaining spec-level drift (e.g. a stale providerState
	// annotation) — apply is allowed again from here.
	out, err, code = run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", chdlqk8sGates)
	if err != nil || code != 0 {
		t.Fatalf("healing apply failed (code %d): %v\n%s", code, err, out)
	}

	// --- D6: the poison record itself ---
	poison := []byte("this-is-not-json-poison-marker-xyz")
	chdlqk8sProduce(t, rt, chdlqk8sTopic, poison)

	// The poison record must land in the declared DLQ topic.
	chdlqk8sWaitForRecordContaining(t, rt, chdlqk8sDLQTopic, poison, 180*time.Second)

	// The sink connector must stay RUNNING throughout (errors.tolerance:
	// all absorbed the conversion failure).
	if state := chdlqk8sConnectorState(t, ctx, rt, chdlqk8sConnector); state != "RUNNING" {
		t.Errorf("sink connector state after poison record = %q, want RUNNING", state)
	}

	// Valid records must keep flowing after the poison.
	chdlqk8sProduce(t, rt, chdlqk8sTopic, []byte(`{"id":2,"name":"after-poison-marker"}`))
	if !chdlqk8sObjectContains(t, ctx, rt, "after-poison-marker", 180*time.Second) {
		t.Fatal("valid record after the poison did not land in the object store")
	}

	// The registered connector config carries the DLQ keys
	// (docs/planning/08 D6) — the live K8s counterpart to s3sink_test.go's
	// TestDeadLetterConfigTranslation and the Docker DLQ test's own check.
	cfg := chdlqk8sConnectorConfig(t, ctx, rt, chdlqk8sConnector)
	if got := cfg["errors.tolerance"]; got != "all" {
		t.Errorf("live connector config errors.tolerance = %q, want all", got)
	}
	if got := cfg["errors.deadletterqueue.topic.name"]; got != chdlqk8sDLQTopic {
		t.Errorf("live connector config errors.deadletterqueue.topic.name = %q, want %q", got, chdlqk8sDLQTopic)
	}

	// Exit: destroy tears everything down cleanly, including the healed
	// worker.
	out, err, code = run(t, "destroy", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", chdlqk8sGates)
	if err != nil || code != 0 {
		t.Fatalf("destroy failed (code %d): %v\n%s", code, err, out)
	}
	for _, name := range workloads {
		if _, found, _ := rt.Inspect(ctx, name); found {
			t.Errorf("deployment %q still present after destroy", name)
		}
	}
}
