//go:build integration

package main

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	k8sruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/kubernetes"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

const (
	rpHAK8sBase    = "datascape-rphak8s-test"
	rpHAK8sNS      = "datascape-rphak8s-test-ns"
	rpHAK8sTopic   = "datascape-rphak8s-events"
	rpHAK8sBrokers = 3
	rpHAK8sGates   = "KubernetesRuntime=true,HighAvailability=true"
)

// rpHAK8sClient opens per-ordinal EnsureReachable tunnels (port-forward by
// default) and builds a kgo client with the token→tunnel dialer map — the
// same redirect the provider's own admin client uses (docs/adr/017 §a.4).
// Ordinals that cannot be reached (a killed pod mid-recreate) are skipped.
// The returned closer tears the tunnels down.
func rpHAK8sClient(t *testing.T, rt *k8sruntime.Runtime, opts ...kgo.Opt) (*kgo.Client, func()) {
	t.Helper()
	ctx := context.Background()
	dialMap := map[string]string{}
	var seeds []string
	var closers []func() error
	for i := 0; i < rpHAK8sBrokers; i++ {
		ord := runtime.OrdinalName(rpHAK8sBase, i)
		addr, closeAddr, err := rt.EnsureReachable(ctx, ord, 9092)
		if err != nil {
			continue
		}
		token := fmt.Sprintf("%s:%d", ord, 9092)
		dialMap[token] = addr
		seeds = append(seeds, token)
		closers = append(closers, closeAddr)
	}
	if len(seeds) == 0 {
		t.Fatal("no live broker ordinal reachable")
	}
	closeAll := func() {
		for _, c := range closers {
			_ = c()
		}
	}
	dial := func(ctx context.Context, network, host string) (net.Conn, error) {
		if mapped, ok := dialMap[host]; ok {
			host = mapped
		}
		var d net.Dialer
		return d.DialContext(ctx, network, host)
	}
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(seeds...), kgo.Dialer(dial)}, opts...)...)
	if err != nil {
		closeAll()
		t.Fatalf("kafka client: %v", err)
	}
	return cl, closeAll
}

func rpHAK8sProduceConsume(t *testing.T, rt *k8sruntime.Runtime, marker string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	prod, closeProd := rpHAK8sClient(t, rt)
	defer closeProd()
	defer prod.Close()
	if err := prod.ProduceSync(ctx, &kgo.Record{Topic: rpHAK8sTopic, Value: []byte(marker)}).FirstErr(); err != nil {
		t.Fatalf("produce %q: %v", marker, err)
	}

	cons, closeCons := rpHAK8sClient(t, rt,
		kgo.ConsumeTopics(rpHAK8sTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	defer closeCons()
	defer cons.Close()
	for {
		fetches := cons.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			t.Fatalf("consume %q: %v", marker, err)
		}
		found := false
		fetches.EachRecord(func(r *kgo.Record) {
			if string(r.Value) == marker {
				found = true
			}
		})
		if found {
			return
		}
	}
}

// ambientClientset builds a client-go clientset from the ambient kubeconfig
// (the same one the runtime adapter uses) for the out-of-band pod kill —
// deleting one broker pod behind platformctl's back, exactly as a node
// failure or an operator's kubectl would.
func ambientClientset(t *testing.T) *kubernetes.Clientset {
	t.Helper()
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(), nil).ClientConfig()
	if err != nil {
		t.Fatalf("load ambient kubeconfig: %v", err)
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("build clientset: %v", err)
	}
	return cs
}

// TestRedpandaHAKubernetesEndToEnd is the Kubernetes leg of docs/planning/08
// C2's Accept list (docs/adr/017): a 3-broker StatefulSet cluster to Ready
// (redpanda refuses even replication factors — Raft quorum — so a
// meaningfully replicated leg needs 3 brokers) with a replication-factor-3
// topic verified over per-ordinal port-forwards;
// produce/consume keeps working while one broker pod is deleted out-of-band
// (the StatefulSet controller heals it — the documented per-runtime
// difference from Docker's re-apply heal); idempotent re-apply clean;
// destroy clean.
func TestRedpandaHAKubernetesEndToEnd(t *testing.T) {
	requireK8s(t)
	rt, err := k8sruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to kubernetes: %v", err)
	}
	ctx := context.Background()
	// Remove the workload first: RemoveNetwork refuses (by contract) while
	// the namespace still holds a StatefulSet, so a network-only cleanup
	// would leak the whole deployment into the next run.
	cleanup := func() {
		_ = rt.Remove(ctx, rpHAK8sBase)
		_ = rt.RemoveNetwork(ctx, rpHAK8sNS)
	}
	cleanup()
	t.Cleanup(cleanup)

	stateFile := filepath.Join(t.TempDir(), "state.json")
	manifests := "testdata/redpanda-ha-k8s-scenario"

	out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", rpHAK8sGates)
	if err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}
	for i := 0; i < rpHAK8sBrokers; i++ {
		ord := runtime.OrdinalName(rpHAK8sBase, i)
		st, found, err := rt.Inspect(ctx, ord)
		if err != nil || !found || !st.Running {
			t.Fatalf("broker ordinal %s not running after apply: found=%v err=%v", ord, found, err)
		}
	}

	// Replication factor verified via the live admin API.
	cl, closeCl := rpHAK8sClient(t, rt)
	adm := kadm.NewClient(cl)
	dctx, dcancel := context.WithTimeout(ctx, 30*time.Second)
	details, err := adm.ListTopics(dctx, rpHAK8sTopic)
	dcancel()
	if err != nil {
		t.Fatalf("list topics: %v", err)
	}
	if !details.Has(rpHAK8sTopic) {
		t.Fatalf("topic %q does not exist", rpHAK8sTopic)
	}
	for _, p := range details[rpHAK8sTopic].Partitions {
		if got := len(p.Replicas); got != rpHAK8sBrokers {
			t.Errorf("topic replication factor = %d, want %d", got, rpHAK8sBrokers)
		}
		break
	}
	cl.Close()
	closeCl()

	rpHAK8sProduceConsume(t, rt, "before-kill")

	// Out-of-band pod kill: the StatefulSet controller will recreate it;
	// produce/consume must keep working against the survivor meanwhile.
	killed := runtime.OrdinalName(rpHAK8sBase, 1)
	cs := ambientClientset(t)
	if err := cs.CoreV1().Pods(rpHAK8sNS).Delete(ctx, killed, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("out-of-band pod delete: %v", err)
	}
	rpHAK8sProduceConsume(t, rt, "during-kill")

	// The controller heals the pod (per-runtime difference from Docker,
	// where re-apply performs the heal); wait for it, then prove the
	// cluster is fully back.
	deadline := time.Now().Add(3 * time.Minute)
	for {
		st, found, err := rt.Inspect(ctx, killed)
		if err == nil && found && st.Running {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pod %s not recreated by the StatefulSet controller: found=%v err=%v", killed, found, err)
		}
		time.Sleep(2 * time.Second)
	}
	rpHAK8sProduceConsume(t, rt, "after-heal")

	// Idempotent re-apply, zero changes.
	out, err, code = run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", rpHAK8sGates)
	if err != nil || code != 0 {
		t.Fatalf("idempotent re-apply failed (code %d): %v\n%s", code, err, out)
	}
	if !strings.Contains(out, "no changes") {
		t.Errorf("idempotent re-apply did not report 'no changes':\n%s", out)
	}

	// Destroy tears down the StatefulSet, its Services, and the Namespace.
	out, err, code = run(t, "destroy", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", rpHAK8sGates)
	if err != nil || code != 0 {
		t.Fatalf("destroy failed (code %d): %v\n%s", code, err, out)
	}
	if _, found, _ := rt.Inspect(ctx, rpHAK8sBase); found {
		t.Errorf("broker statefulset still present after destroy")
	}
}
