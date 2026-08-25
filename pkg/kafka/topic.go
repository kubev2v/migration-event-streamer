package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

// NewAdminClient builds a kadm admin client against the given brokers. The
// caller owns the returned client and must Close it, which also closes the
// underlying kgo client.
func NewAdminClient(brokers []string, opts []kgo.Opt) (*kadm.Client, error) {
	clientOpts := append([]kgo.Opt{kgo.SeedBrokers(brokers...)}, opts...)
	adm, err := kadm.NewOptClient(clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka admin client: %w", err)
	}
	return adm, nil
}

// EnsureTopic creates the topic if it does not already exist. It is idempotent:
// an existing topic is treated as success.
func EnsureTopic(ctx context.Context, adm *kadm.Client, topic string, partitions int32, replicationFactor int16) error {
	resp, err := adm.CreateTopic(ctx, partitions, replicationFactor, nil, topic)
	if err != nil {
		if errors.Is(err, kerr.TopicAlreadyExists) {
			zap.S().Infow("topic already exists", "topic", topic)
			return nil
		}
		return fmt.Errorf("failed to create topic %s: %w", topic, err)
	}

	if resp.Err != nil {
		return fmt.Errorf("failed to create topic %s: %w", topic, resp.Err)
	}

	zap.S().Infow("topic created", "topic", topic, "partitions", partitions, "replication_factor", replicationFactor)
	return nil
}
