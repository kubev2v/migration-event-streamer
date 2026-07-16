package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

func EnsureTopic(brokers []string, opts []kgo.Opt, topic string, partitions int32, replicationFactor int16) error {
	clientOpts := append([]kgo.Opt{kgo.SeedBrokers(brokers...)}, opts...)
	cl, err := kgo.NewClient(clientOpts...)
	if err != nil {
		return fmt.Errorf("failed to create kafka admin client: %w", err)
	}
	defer cl.Close()

	adm := kadm.NewClient(cl)
	defer adm.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	resp, err := adm.CreateTopic(ctx, partitions, replicationFactor, nil, topic)
	if err != nil {
		return fmt.Errorf("failed to create topic %s: %w", topic, err)
	}

	if resp.Err != nil {
		if errors.Is(resp.Err, kerr.TopicAlreadyExists) {
			zap.S().Infow("topic already exists", "topic", topic)
			return nil
		}
		return fmt.Errorf("failed to create topic %s: %w", topic, resp.Err)
	}

	zap.S().Infow("topic created", "topic", topic, "partitions", partitions, "replication_factor", replicationFactor)
	return nil
}
