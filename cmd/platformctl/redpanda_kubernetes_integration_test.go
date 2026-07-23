//go:build integration

package main

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	k8sruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/kubernetes"
	"github.com/rezarajan/platformctl/internal/testkit"
)

// TestRedpandaKubernetesEndToEnd covers docs/planning/08 B1's literal accept
// criterion against a real cluster: apply the redpanda Provider + EventStream
// from outside the cluster with access: node-port, prove the topic really
// exists via a live Kafka admin connection to the exact address `platformctl
// inventory` reports (so this doubles as B2's "inventory tells the truth"
// proof — a lying host would fail this dial, not just an assertion on the
// string shape), then destroy cleanly.
func TestRedpandaKubernetesEndToEnd(t *testing.T) {
	requireK8s(t)
	rt, err := k8sruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to kubernetes: %v", err)
	}
	ctx := context.Background()
	const ns = "datascape-rpk8s-test-ns"
	stateFile := filepath.Join(t.TempDir(), "state.json")
	manifests := "testdata/redpanda-k8s-scenario"
	const gateVal = "KubernetesRuntime=true"

	// docs/adr/029 (J2 sweep): destroy-then-janitor — see the ingress K8s
	// suite's comment for why namespace-only cleanup strands.
	jan := testkit.Janitor{RT: rt, Networks: []string{ns}}
	jan.CleanSilent(ctx)
	jan.Register(ctx, t)
	t.Cleanup(func() {
		_, _, _ = run(t, "destroy", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", gateVal)
	})

	out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", gateVal)
	if err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}

	addr := inventoryKafkaAddr(t, manifests, stateFile, gateVal)
	if addr == "" {
		t.Fatal("inventory reported no host-reachable kafka endpoint")
	}
	partitions, retention := describeTopicAt(t, addr, "datascape-rpk8s-test-events")
	if partitions != 3 {
		t.Errorf("topic partitions = %d, want 3", partitions)
	}
	if retention != "604800000" { // 7d in ms
		t.Errorf("retention.ms = %q, want 604800000", retention)
	}

	// Exit criterion: destroy tears down the broker Deployment, its
	// Service(s), volume, and Namespace cleanly.
	out, err, code = run(t, "destroy", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", gateVal)
	if err != nil || code != 0 {
		t.Fatalf("destroy failed (code %d): %v\n%s", code, err, out)
	}
	if _, found, _ := rt.Inspect(ctx, "datascape-rpk8s-test"); found {
		t.Errorf("broker deployment still present after destroy")
	}
}

// invRow mirrors newInventoryCmd's unexported row shape (JSON tags only —
// that's the only part of the contract this test needs).
type invRow struct {
	Component string `json:"component"`
	Endpoint  string `json:"endpoint"`
	Scheme    string `json:"scheme"`
	Host      string `json:"host"`
}

func inventoryKafkaAddr(t *testing.T, manifests, stateFile, gateVal string) string {
	t.Helper()
	out, err, code := run(t, "inventory", manifests, "--state-file", stateFile, "-o", "json", "--feature-gates", gateVal)
	if err != nil || code != 0 {
		t.Fatalf("inventory failed (code %d): %v\n%s", code, err, out)
	}
	var parsed struct {
		Endpoints []invRow `json:"endpoints"`
	}
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("parse inventory JSON: %v\n%s", err, out)
	}
	for _, ep := range parsed.Endpoints {
		if ep.Scheme == "kafka" && ep.Host != "(in-network only)" {
			return ep.Host
		}
	}
	return ""
}

func describeTopicAt(t *testing.T, addr, topic string) (int, string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr))
	if err != nil {
		t.Fatalf("kafka client: %v", err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	details, err := adm.ListTopics(ctx, topic)
	if err != nil {
		t.Fatalf("list topics on %s: %v", addr, err)
	}
	if !details.Has(topic) {
		t.Fatalf("topic %q does not exist at %s", topic, addr)
	}

	rc, err := adm.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		t.Fatalf("describe configs: %v", err)
	}
	cfg, err := rc.On(topic, nil)
	if err != nil {
		t.Fatalf("configs for %q: %v", topic, err)
	}
	retention := ""
	for _, c := range cfg.Configs {
		if c.Key == "retention.ms" && c.Value != nil {
			retention = *c.Value
		}
	}
	return len(details[topic].Partitions), retention
}
