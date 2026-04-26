package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/kubev2v/migration-event-streamer/internal/config"
	"github.com/kubev2v/migration-event-streamer/internal/entity"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kprom"
	"go.uber.org/zap"
)

type Consumer struct {
	cl     *kgo.Client
	cancel context.CancelFunc
	done   chan struct{}
}

func NewConsumer(cfg config.Kafka, topic, consumerGroupID string) (*Consumer, error) {
	clientID := cfg.ClientID
	if clientID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			hostname = fmt.Sprintf("consumer-%s", consumerGroupID)
		}
		clientID = hostname
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(clientID),
		kgo.ConsumerGroup(consumerGroupID),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.WithHooks(kprom.NewMetrics("kafka_consumer")),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka consumer: %w", err)
	}

	return &Consumer{cl: cl}, nil
}

func (c *Consumer) Consume(ctx context.Context, messages chan entity.Message) error {
	consumerCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.done = make(chan struct{})

	go func() {
		defer close(messages)
		defer close(c.done)

		for {
			fetches := c.cl.PollRecords(consumerCtx, 1)
			if consumerCtx.Err() != nil {
				return
			}

			if errs := fetches.Errors(); len(errs) > 0 {
				for _, e := range errs {
					zap.S().Errorw("fetch error", "topic", e.Topic, "partition", e.Partition, "error", e.Err)
				}
			}

			records := fetches.Records()
			if len(records) == 0 {
				c.cl.AllowRebalance()
				continue
			}

			r := records[0]

			var event cloudevents.Event
			if err := json.Unmarshal(r.Value, &event); err != nil {
				zap.S().Warnw("failed to unmarshal cloudevent", "error", err, "topic", r.Topic, "offset", r.Offset)
				c.cl.MarkCommitRecords(r)
				c.cl.CommitMarkedOffsets(consumerCtx)
				c.cl.AllowRebalance()
				continue
			}

			msg := entity.NewMessage(event)
			messages <- msg

			select {
			case <-msg.CommitCh:
			case <-consumerCtx.Done():
				c.cl.AllowRebalance()
				return
			}

			c.cl.MarkCommitRecords(r)
			c.cl.CommitMarkedOffsets(consumerCtx)
			c.cl.AllowRebalance()
		}
	}()

	return nil
}

func (c *Consumer) Close(_ context.Context) error {
	if c.cancel != nil {
		c.cancel()
	}
	if c.done != nil {
		<-c.done
	}
	c.cl.Close()
	return nil
}
