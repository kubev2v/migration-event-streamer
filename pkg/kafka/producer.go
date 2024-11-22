package kafka

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/IBM/sarama"
	"github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type info struct {
	Topic     string
	Partition int32
	Offset    int64
}

type KafkaProducer struct {
	kp sarama.SyncProducer
}

func NewKafkaProducer(brokers []string, sConfig *sarama.Config) (*KafkaProducer, error) {
	additionalConfig := sarama.NewConfig()
	if sConfig != nil {
		additionalConfig = sConfig
	}

	// mandatory configuration
	additionalConfig.Producer.Return.Errors = true
	additionalConfig.Producer.Return.Successes = true

	// make it configurable ?
	additionalConfig.Producer.RequiredAcks = sarama.WaitForAll

	// clientID
	if additionalConfig.ClientID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			hostname = fmt.Sprintf("clientid-producer-%v", uuid.NewString())
		}

		additionalConfig.ClientID = fmt.Sprintf("%v-%v", hostname, time.Now().Unix())
	}

	kp, err := sarama.NewSyncProducer(brokers, additionalConfig)
	if err != nil {
		return nil, err
	}

	return &KafkaProducer{kp: kp}, err
}

func (m *KafkaProducer) Write(ctx context.Context, topic string, e cloudevents.Event) error {
	msg, err := m.buildKafkaMessage(ctx, e)
	if err != nil {
		return err
	}

	msg.Topic = topic

	info, err := m.pushMessage(ctx, msg)
	if err != nil {
		return err
	}

	zap.S().Infow("message pushed to kafka", "topic", info.Topic, "offset", info.Offset, "partition", info.Partition)
	return nil
}

func (m *KafkaProducer) pushMessage(ctx context.Context, msg *sarama.ProducerMessage) (*info, error) {
	if msg == nil {
		return nil, nil
	}

	partition, offset, err := m.kp.SendMessage(msg)
	if err != nil {
		return nil, err
	}

	info := &info{
		Topic:     msg.Topic,
		Partition: partition,
		Offset:    offset,
	}

	return info, nil
}

func (m *KafkaProducer) Close(ctx context.Context) error {
	return m.kp.Close()
}

func (m *KafkaProducer) buildKafkaMessage(ctx context.Context, event cloudevents.Event) (*sarama.ProducerMessage, error) {
	err := event.Validate()
	if err != nil {
		return nil, err
	}

	ret := &sarama.ProducerMessage{}
	err = kafka_sarama.WriteProducerMessage(ctx, binding.ToMessage(&event), ret)

	return ret, err
}
