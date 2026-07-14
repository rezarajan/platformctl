package redpanda

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func adminClient(addr string) (*kadm.Client, *kgo.Client, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr))
	if err != nil {
		return nil, nil, fmt.Errorf("connect to broker %s: %w", addr, err)
	}
	return kadm.NewClient(cl), cl, nil
}

// ensureTopic is idempotent: creates the topic if absent, grows partitions if
// the desired count is higher, and aligns retention.ms — issuing zero calls
// when actual state already matches.
func ensureTopic(ctx context.Context, addr, topic string, partitions int, retentionMS int64) error {
	adm, cl, err := adminClient(addr)
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

func deleteTopic(ctx context.Context, addr, topic string) error {
	adm, cl, err := adminClient(addr)
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
func probeTopic(ctx context.Context, addr, topic string, wantPartitions int) (bool, string, error) {
	adm, cl, err := adminClient(addr)
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
		return true, "TopicMissing", nil
	}
	if got := len(details[topic].Partitions); got != wantPartitions {
		return true, fmt.Sprintf("PartitionCountMismatch(%d!=%d)", got, wantPartitions), nil
	}
	return false, "", nil
}
