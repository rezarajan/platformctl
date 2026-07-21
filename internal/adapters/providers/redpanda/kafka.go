package redpanda

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/rezarajan/platformctl/internal/domain/status"
)

// adminClient connects using dialAddr — an address genuinely dialable right
// now (from redpanda.go's reachableAddr) — but tells kgo to seed and
// identify the broker by advertisedAddr, the (possibly Kubernetes-only-
// meaningful-as-a-token) address baked into the broker's own
// --advertise-kafka-addr. Kafka's client/broker protocol has the broker
// tell connected clients which address to use for follow-up requests
// (metadata, leader redirects, ...); on Kubernetes that address can't be
// made correct at broker-start time (see redpanda.go's advertisedAddr doc).
// The custom Dialer below intercepts every dial the client attempts —
// including that follow-up redial to advertisedAddr — and transparently
// redirects it to dialAddr, so the broker's own advertised value never
// needs to be true, only stable for the lifetime of one admin call.
func adminClient(dialAddr, advertisedAddr string) (*kadm.Client, *kgo.Client, error) {
	dial := func(ctx context.Context, network, host string) (net.Conn, error) {
		if host == advertisedAddr {
			host = dialAddr
		}
		var d net.Dialer
		return d.DialContext(ctx, network, host)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(advertisedAddr), kgo.Dialer(dial))
	if err != nil {
		return nil, nil, fmt.Errorf("connect to broker %s: %w", dialAddr, err)
	}
	return kadm.NewClient(cl), cl, nil
}

// ensureTopic is idempotent: creates the topic if absent, grows partitions if
// the desired count is higher, and aligns retention.ms — issuing zero calls
// when actual state already matches.
func ensureTopic(ctx context.Context, addr, advertisedAddr, topic string, partitions int, retentionMS int64) error {
	adm, cl, err := adminClient(addr, advertisedAddr)
	if err != nil {
		return err
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	details, err := adm.ListTopics(ctx, topic)
	if err != nil {
		return fmt.Errorf("list topics on %s: %w", addr, err)
	}

	configs := map[string]*string{}
	if retentionMS >= 0 {
		v := strconv.FormatInt(retentionMS, 10)
		configs["retention.ms"] = &v
	}

	if !details.Has(topic) {
		if _, err := adm.CreateTopic(ctx, int32(partitions), 1, configs, topic); err != nil {
			return fmt.Errorf("create topic %q: %w", topic, err)
		}
		return nil
	}

	current := details[topic]
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

func deleteTopic(ctx context.Context, addr, advertisedAddr, topic string) error {
	adm, cl, err := adminClient(addr, advertisedAddr)
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

// probeTopic reports drift: (drifted, reason, error).
func probeTopic(ctx context.Context, addr, advertisedAddr, topic string, wantPartitions int, wantRetentionMS int64) (bool, string, error) {
	adm, cl, err := adminClient(addr, advertisedAddr)
	if err != nil {
		return false, "", err
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	details, err := adm.ListTopics(ctx, topic)
	if err != nil {
		return false, "", fmt.Errorf("list topics on %s: %w", addr, err)
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
