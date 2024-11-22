package kafka

import (
	"context"
	"fmt"
	"os"

	"github.com/IBM/sarama"
	"github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/client"
	logCtx "github.com/cloudevents/sdk-go/v2/context"
	"github.com/tupyy/migration-event-streamer/internal/config"
	"github.com/tupyy/migration-event-streamer/internal/entity"
	"go.uber.org/zap"
)

type Consumer struct {
	client           cloudevents.Client
	protocol         *kafka_sarama.Protocol
	done             chan chan any
	receiverCancelFn context.CancelFunc
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

	c, err := cloudevents.NewClient(protocol, cloudevents.WithTimeNow(), cloudevents.WithUUIDs(), client.WithBlockingCallback(), client.WithPollGoroutines(1))
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}

	return &Consumer{
		client:   c,
		protocol: protocol,
	}, nil
}

func (kc *Consumer) Consume(ctx context.Context, messages chan entity.Message) error {
	var err error
	rcvCtx, cancel := context.WithCancel(logCtx.WithLogger(ctx, zap.S()))
	kc.receiverCancelFn = cancel
	kc.done = make(chan chan any)

	go func() {
		err = kc.client.StartReceiver(rcvCtx, func(event cloudevents.Event) {
			msg := entity.NewMessage(event)
			messages <- msg

			fmt.Println("2")
			select {
			case <-msg.CommitCh:
			case <-rcvCtx.Done():
			}
		})
		close(messages)
		close(<-kc.done)
		zap.S().Info("consumer closed")
	}()

	return err
}

func (kc *Consumer) Close(ctx context.Context) error {
	err := kc.protocol.Close(ctx)
	if kc.done == nil {
		return nil
	}

	kc.receiverCancelFn()

	// wait for receiver to exit the loop
	c := make(chan any)
	kc.done <- c
	<-c
	return err
}
