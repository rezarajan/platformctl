//go:build integration

package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const rpKafkaAddr = "localhost:19192"

// TestRedpandaEndToEnd covers the Phase 2 exit criteria.
func TestRedpandaEndToEnd(t *testing.T) {
	rt := requireDocker(t)
	ctx := context.Background()

	cleanup := registerDockerCleanup(t, rt, []string{"datascape-rp-test"}, []string{"datascape-rp-test-data"}, "datascape-rp-test-net")
	cleanup()

	stateFile := filepath.Join(t.TempDir(), "state.json")
	manifests := "testdata/redpanda-scenario"

	// Exit criterion: apply produces a healthy broker with the declared topic
	// and retention.
	out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}

	brokerBefore, found, err := rt.Inspect(ctx, "datascape-rp-test")
	if err != nil || !found {
		t.Fatalf("broker not found after apply: %v", err)
	}

	partitions, retention := describeTopic(t, "datascape-test-events")
	if partitions != 3 {
		t.Errorf("topic partitions = %d, want 3", partitions)
	}
	if retention != "604800000" { // 7d in ms
		t.Errorf("retention.ms = %q, want 604800000", retention)
	}

	// Exit criterion: idempotent re-apply — zero mutations, same container.
	out, err, code = run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("re-apply failed (code %d): %v\n%s", code, err, out)
	}
	if !strings.Contains(out, "no changes") {
		t.Errorf("re-apply did not report 'no changes':\n%s", out)
	}

	// Exit criterion: changing partitions updates the topic without
	// recreating the broker.
	changed := filepath.Join(t.TempDir(), "changed")
	if err := os.MkdirAll(changed, 0o755); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(manifests, "manifests.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	bumped := strings.Replace(string(data), "partitions: 3", "partitions: 6", 1)
	if err := os.WriteFile(filepath.Join(changed, "manifests.yaml"), []byte(bumped), 0o644); err != nil {
		t.Fatal(err)
	}

	out, err, code = run(t, "apply", changed, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("partition-change apply failed (code %d): %v\n%s", code, err, out)
	}
	partitions, _ = describeTopic(t, "datascape-test-events")
	if partitions != 6 {
		t.Errorf("topic partitions after update = %d, want 6", partitions)
	}
	brokerAfter, found, err := rt.Inspect(ctx, "datascape-rp-test")
	if err != nil || !found {
		t.Fatalf("broker missing after partition update: %v", err)
	}
	if brokerAfter.ID != brokerBefore.ID {
		t.Errorf("broker container was recreated (ID %s -> %s); partition update must not touch the broker", brokerBefore.ID, brokerAfter.ID)
	}

	// Exit criterion: destroy tears down broker, network, and volume cleanly.
	out, err, code = run(t, "destroy", changed, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("destroy failed (code %d): %v\n%s", code, err, out)
	}
	if _, found, _ := rt.Inspect(ctx, "datascape-rp-test"); found {
		t.Errorf("broker container still present after destroy")
	}
}

func describeTopic(t *testing.T, topic string) (int, string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(rpKafkaAddr))
	if err != nil {
		t.Fatalf("kafka client: %v", err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	details, err := adm.ListTopics(ctx, topic)
	if err != nil {
		t.Fatalf("list topics: %v", err)
	}
	if !details.Has(topic) {
		t.Fatalf("topic %q does not exist", topic)
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
