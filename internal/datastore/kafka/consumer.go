package kafka

import (
	"context"
	"fmt"
	"os"

	"github.com/IBM/sarama"
	"github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	logCtx "github.com/cloudevents/sdk-go/v2/context"
	"github.com/tupyy/migration-event-streamer/internal/config"
	"go.uber.org/zap"
)

type Consumer struct {
	client   cloudevents.Client
	protocol *kafka_sarama.Protocol
}

func NewConsumer(config config.KafkaConfig, topic, consumerGroupID string) (*Consumer, error) {
	saramaConfig := sarama.NewConfig()
	if config.SaramaConfig != nil {
		saramaConfig = config.SaramaConfig
	}

	// mandatory configuration
	saramaConfig.Consumer.Offsets.AutoCommit.Enable = true
	saramaConfig.Consumer.Return.Errors = true

	// make it configurable ?
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	// clientID
	if saramaConfig.ClientID == "" {
		clientID := config.ClientID
		if clientID == "" {
			hostname, err := os.Hostname()
			if err != nil {
				hostname = fmt.Sprintf("clientid-%v", consumerGroupID)
			}

			clientID = hostname
		}

		saramaConfig.ClientID = clientID
	}

	// kafka version
	saramaConfig.Version = sarama.V3_6_0_0

	protocol, err := kafka_sarama.NewProtocol(config.Brokers,
		saramaConfig,
		topic,
		topic,
		kafka_sarama.WithReceiverGroupId(consumerGroupID))
	if err != nil {
		return nil, fmt.Errorf("failed to create protocol: %w", err)
	}

	c, err := cloudevents.NewClient(protocol, cloudevents.WithTimeNow(), cloudevents.WithUUIDs())
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}

	return &Consumer{
		client:   c,
		protocol: protocol,
	}, nil
}

func (kc *Consumer) Consume(ctx context.Context, messages chan cloudevents.Event) error {
	var err error
	lCtx := logCtx.WithLogger(ctx, zap.S())
	go func() {
		err = kc.client.StartReceiver(lCtx, func(event cloudevents.Event) {
			messages <- event
		})
	}()
	return err
}

func (kc *Consumer) Close(ctx context.Context) error {
	return kc.protocol.Close(ctx)
}
