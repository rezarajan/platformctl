package redpanda

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// adminClient connects using dialMap — advertised-address → an address
// genuinely dialable right now (each value from EnsureReachable) — and tells
// kgo to seed and identify brokers by their advertised addresses, the
// (possibly meaningful-only-as-a-token) addresses baked into each broker's
// own --advertise-kafka-addr. Kafka's client/broker protocol has a broker
// tell connected clients which address to use for follow-up requests
// (metadata, leader redirects, ...); on Kubernetes that address can't be
// made correct at broker-start time, and on Docker a multi-broker set's
// auto-assigned host ports can't either (see redpanda.go's advertisedAddr
// and docs/adr/017 §a.4). The custom Dialer below intercepts every dial the
// client attempts — including follow-up redials to any advertised address —
// and transparently redirects it to that broker's resolved address, so a
// broker's own advertised value never needs to be true, only stable and
// per-broker-unique for the lifetime of one admin call. B1 introduced this
// for one broker; docs/adr/017 generalizes the single pair to a map. A
// broker absent from dialMap (killed, mid-heal) fails its dial and kgo
// retries against the mapped survivors.
func adminClient(dialMap map[string]string, seeds []string) (*kadm.Client, *kgo.Client, error) {
	dial := func(ctx context.Context, network, host string) (net.Conn, error) {
		if mapped, ok := dialMap[host]; ok {
			host = mapped
		}
		var d net.Dialer
		return d.DialContext(ctx, network, host)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(seeds...), kgo.Dialer(dial))
	if err != nil {
		return nil, nil, fmt.Errorf("connect to broker(s) %s: %w", strings.Join(seeds, ","), err)
	}
	return kadm.NewClient(cl), cl, nil
}

// topicReplicationFactor reads the observed replication factor of a topic's
// lowest-numbered partition (every partition of a platformctl-created topic
// shares one factor; Kafka's topic creation enforces it uniformly).
func topicReplicationFactor(detail kadm.TopicDetail) int {
	ids := make([]int32, 0, len(detail.Partitions))
	for id := range detail.Partitions {
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return 0
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return len(detail.Partitions[ids[0]].Replicas)
}

// ensureTopic is idempotent: creates the topic if absent (with the declared
// replication factor — docs/adr/017 §a.7), grows partitions if the desired
// count is higher, and aligns retention.ms — issuing zero calls when actual
// state already matches. A replication-factor change on an existing topic is
// refused: Kafka cannot alter a topic's RF short of a partition
// reassignment, mirroring the partition-shrink refusal below.
func ensureTopic(ctx context.Context, dialMap map[string]string, seeds []string, topic string, partitions, replication int, retentionMS int64) error {
	adm, cl, err := adminClient(dialMap, seeds)
	if err != nil {
		return err
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()

	details, err := adm.ListTopics(ctx, topic)
	if err != nil {
		return fmt.Errorf("list topics on %s: %w", strings.Join(seeds, ","), err)
	}

	configs := map[string]*string{}
	if retentionMS >= 0 {
		v := strconv.FormatInt(retentionMS, 10)
		configs["retention.ms"] = &v
	}

	if !details.Has(topic) {
		// INVALID_REPLICATION_FACTOR is retried briefly: a broker that just
		// joined (waitClusterFormed sees it in the metadata broker list)
		// can lag the controller's health/allocation snapshot by a few
		// seconds, during which Redpanda refuses to place replicas on it —
		// caught live on the C2 Kubernetes leg, where formation is fast
		// enough to hit the window (docs/adr/017 §a.6).
		deadline := time.Now().Add(60 * time.Second)
		for {
			_, err := adm.CreateTopic(ctx, int32(partitions), int16(replication), configs, topic)
			if err == nil {
				return nil
			}
			if !errors.Is(err, kerr.InvalidReplicationFactor) || time.Now().After(deadline) {
				return fmt.Errorf("create topic %q (replication %d): %w", topic, replication, err)
			}
			select {
			case <-ctx.Done():
				return fmt.Errorf("create topic %q (replication %d): %w", topic, replication, ctx.Err())
			case <-time.After(2 * time.Second):
			}
		}
	}

	current := details[topic]
	if got := topicReplicationFactor(current); got != 0 && got != replication {
		return fmt.Errorf("topic %q has replication factor %d; Kafka cannot change it to %d in place (recreate the EventStream instead)", topic, got, replication)
	}
	currentPartitions := len(current.Partitions)
	if partitions > currentPartitions {
		// UpdatePartitions sets the absolute count (CreatePartitions would add).
		if _, err := adm.UpdatePartitions(ctx, partitions, topic); err != nil {
			return fmt.Errorf("grow topic %q to %d partitions: %w", topic, partitions, err)
		}
	} else if partitions < currentPartitions {
		return fmt.Errorf("topic %q has %d partitions; Kafka cannot shrink to %d (recreate the EventStream instead)", topic, currentPartitions, partitions)
	}

	if retentionMS >= 0 {
		rc, err := adm.DescribeTopicConfigs(ctx, topic)
		if err != nil {
			return fmt.Errorf("describe configs for %q: %w", topic, err)
		}
		cfg, err := rc.On(topic, nil)
		if err != nil {
			return fmt.Errorf("describe configs for %q: %w", topic, err)
		}
		currentRetention := ""
		for _, c := range cfg.Configs {
			if c.Key == "retention.ms" && c.Value != nil {
				currentRetention = *c.Value
			}
		}
		want := strconv.FormatInt(retentionMS, 10)
		if currentRetention != want {
			alter := []kadm.AlterConfig{{Op: kadm.SetConfig, Name: "retention.ms", Value: &want}}
			if _, err := adm.AlterTopicConfigs(ctx, alter, topic); err != nil {
				return fmt.Errorf("alter retention.ms for %q: %w", topic, err)
			}
		}
	}
	return nil
}

func deleteTopic(ctx context.Context, dialMap map[string]string, seeds []string, topic string) error {
	adm, cl, err := adminClient(dialMap, seeds)
	if err != nil {
		return err
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if _, err := adm.DeleteTopics(ctx, topic); err != nil {
		return fmt.Errorf("delete topic %q: %w", topic, err)
	}
	return nil
}

// countJoinedBrokersMinView asks EVERY seed individually for its broker
// list and returns the minimum view. ListBrokers is answered by whichever
// one broker the client picks, and Kafka metadata propagation between
// brokers is eventually consistent — observed live (doc 11, wave-3 gate):
// after a heal, reconcile's client hit a broker already seeing 3/3 while
// the immediately-following drift probe's fresh client hit one still
// seeing 2/3. "Settled" for a broker SET therefore means every member's
// own view agrees — a bar no subsequent same-instant probe can disagree
// with, from any vantage. A seed that errors counts as view 0 (not
// settled), which is exactly right mid-rejoin.
func countJoinedBrokersMinView(ctx context.Context, dialMap map[string]string, seeds []string) int {
	minView := -1
	for _, seed := range seeds {
		v, err := countJoinedBrokers(ctx, dialMap, []string{seed})
		if err != nil {
			return 0
		}
		if minView < 0 || v < minView {
			minView = v
		}
	}
	if minView < 0 {
		return 0
	}
	return minView
}

// countJoinedBrokers reports how many brokers are currently members of the
// cluster per the admin metadata — the "all brokers joined" half of the C2
// probe (docs/adr/017 §a.6); per-ordinal container presence is the other
// half, checked runtime-side by Probe before this is called.
func countJoinedBrokers(ctx context.Context, dialMap map[string]string, seeds []string) (int, error) {
	adm, cl, err := adminClient(dialMap, seeds)
	if err != nil {
		return 0, err
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	brokers, err := adm.ListBrokers(ctx)
	if err != nil {
		return 0, fmt.Errorf("list brokers on %s: %w", strings.Join(seeds, ","), err)
	}
	return len(brokers), nil
}

// probeTopic reports drift: (drifted, reason, error).
// waitTopicSettled re-runs probeTopic until it reports clean, bounded by
// topicSettleTimeout — the reconcile-side half of "ready means serving"
// (docs/planning/09 F3) for topics. ensureTopic's success only means the
// admin API accepted the desired state; after a broker rejoin, partition
// leadership and cluster metadata settle asynchronously, during which
// ListTopics/DescribeConfigs can transiently error and a same-instant
// drift snapshot would report ProbeFailed against a genuinely-healing
// cluster. A healthy cluster passes on the first attempt.
const topicSettleTimeout = 45 * time.Second

// topicProbeRetryWindow bounds retryTransientProbe below: the PROBE-side
// half of the same discipline. Found live twice (the second time on CI,
// doc 11): right after a healing apply, a just-restarted broker accepts
// TCP but closes connections during ApiVersions negotiation for a few
// seconds; a DescribeConfigs shard routed to it errors, and a
// single-shot probe reported ProbeFailed (drift dirty) against a topic
// that was serving fine on the survivors. Errors mean "state
// undetermined" — retried within this window; verdicts (clean OR
// drifted) are determined and return immediately, so real drift is
// never masked and an unreachable cluster still reports, 15s later,
// with the honest last error. Vars, not consts, so tests shrink them.
var (
	topicProbeRetryWindow   = 15 * time.Second
	topicProbeRetryInterval = 2 * time.Second
)

// retryTransientProbe re-runs probe while it returns an error, bounded by
// topicProbeRetryWindow; the first determined verdict returns immediately.
func retryTransientProbe(ctx context.Context, probe func() (bool, string, error)) (bool, string, error) {
	deadline := time.Now().Add(runtime.ScaledWait(topicProbeRetryWindow))
	for {
		drifted, reason, err := probe()
		if err == nil {
			return drifted, reason, nil
		}
		if time.Now().After(deadline) {
			return false, "", err
		}
		select {
		case <-ctx.Done():
			return false, "", ctx.Err()
		case <-time.After(topicProbeRetryInterval):
		}
	}
}

func waitTopicSettled(ctx context.Context, dialMap map[string]string, seeds []string, topic string, wantPartitions, wantReplication int, wantRetentionMS int64) error {
	deadline := time.Now().Add(runtime.ScaledWait(topicSettleTimeout))
	var lastErr error
	var lastReason string
	for {
		drifted, reason, err := probeTopic(ctx, dialMap, seeds, topic, wantPartitions, wantReplication, wantRetentionMS)
		if err == nil && !drifted {
			return nil
		}
		lastErr, lastReason = err, reason
		if time.Now().After(deadline) {
			if lastErr != nil {
				return fmt.Errorf("topic %q did not settle to a clean probe within %s: %w", topic, topicSettleTimeout, lastErr)
			}
			return fmt.Errorf("topic %q did not settle to a clean probe within %s (last probe: %s)", topic, topicSettleTimeout, lastReason)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func probeTopic(ctx context.Context, dialMap map[string]string, seeds []string, topic string, wantPartitions, wantReplication int, wantRetentionMS int64) (bool, string, error) {
	adm, cl, err := adminClient(dialMap, seeds)
	if err != nil {
		return false, "", err
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	details, err := adm.ListTopics(ctx, topic)
	if err != nil {
		return false, "", fmt.Errorf("list topics on %s: %w", strings.Join(seeds, ","), err)
	}
	if !details.Has(topic) {
		return true, status.ReasonTopicMissing, nil
	}
	if got := len(details[topic].Partitions); got != wantPartitions {
		// The reason carries the observed/wanted counts inline (no separate
		// Message field on this path) — the constant names the stable,
		// greppable prefix; the suffix is intentionally dynamic
		// (docs/planning/08 G4).
		return true, fmt.Sprintf("%s(%d!=%d)", status.ReasonPartitionCountMismatch, got, wantPartitions), nil
	}
	// Per-topic replication factor (docs/adr/017 §a.6): the declared
	// spec.replication must hold on the real topic — an out-of-band
	// recreation with a different factor is drift. Same constant-prefix +
	// dynamic-detail convention as PartitionCountMismatch.
	if got := topicReplicationFactor(details[topic]); got != 0 && got != wantReplication {
		return true, fmt.Sprintf("%s(%d!=%d)", status.ReasonReplicationFactorMismatch, got, wantReplication), nil
	}
	// Full desired configuration, not just liveness (docs/planning/07
	// §2.1): declared retention must still hold against out-of-band
	// alteration. A manifest that declares none (-1) leaves retention
	// deliberately not drift-managed.
	if wantRetentionMS >= 0 {
		rc, err := adm.DescribeTopicConfigs(ctx, topic)
		if err != nil {
			return false, "", fmt.Errorf("describe configs for %q: %w", topic, err)
		}
		cfg, err := rc.On(topic, nil)
		if err != nil {
			return false, "", fmt.Errorf("describe configs for %q: %w", topic, err)
		}
		currentRetention := ""
		for _, c := range cfg.Configs {
			if c.Key == "retention.ms" && c.Value != nil {
				currentRetention = *c.Value
			}
		}
		if want := strconv.FormatInt(wantRetentionMS, 10); currentRetention != want {
			// Same pattern as PartitionCountMismatch above: constant prefix,
			// dynamic detail suffix (docs/planning/08 G4).
			return true, fmt.Sprintf("%s(%s!=%s)", status.ReasonRetentionMismatch, currentRetention, want), nil
		}
	}
	return false, "", nil
}
