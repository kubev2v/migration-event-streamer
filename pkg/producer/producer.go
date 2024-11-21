package producer

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2"
)

type KafkaProducer struct {
	client   cloudevents.Client
	protocol *kafka_sarama.Sender
}

func NewKafkaProducer(brokers []string, sConfig *sarama.Config, topic string) (*KafkaProducer, error) {
	sender, err := kafka_sarama.NewSender(brokers, sConfig, topic)
	if err != nil {
		return nil, fmt.Errorf("failed to create protocol: %s", err.Error())
	}

	c, err := cloudevents.NewClient(sender, cloudevents.WithTimeNow(), cloudevents.WithUUIDs())
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %v", err)
	}

	return &KafkaProducer{client: c, protocol: sender}, nil
}

func (kp *KafkaProducer) Write(ctx context.Context, e cloudevents.Event) error {
	err := kp.client.Send(
		kafka_sarama.WithMessageKey(ctx, sarama.StringEncoder(e.ID())),
		e,
	)
	if cloudevents.IsUndelivered(err) {
		return fmt.Errorf("failed to send: %v", err)
	}
	return nil
}

func (kp *KafkaProducer) Close(ctx context.Context) error {
	return kp.protocol.Close(ctx)
}
