package kafka

import (
	"context"
	"fmt"
	"os"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

const maxProducerBatchBytes = 10 * 1024 * 1024 // 10 MiB

type KafkaProducer struct {
	cl *kgo.Client
}

func NewKafkaProducer(brokers []string, opts ...kgo.Opt) (*KafkaProducer, error) {
	clientID, err := os.Hostname()
	if err != nil {
		clientID = fmt.Sprintf("producer-%s", uuid.NewString())
	}

	defaults := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ClientID(clientID),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchMaxBytes(maxProducerBatchBytes),
	}

	cl, err := kgo.NewClient(append(defaults, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer: %w", err)
	}

	return &KafkaProducer{cl: cl}, nil
}

func (p *KafkaProducer) Write(ctx context.Context, topic string, data []byte) error {
	record := &kgo.Record{
		Topic: topic,
		Key:   []byte(uuid.New().String()),
		Value: data,
	}

	results := p.cl.ProduceSync(ctx, record)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}

	return nil
}

func (p *KafkaProducer) Ping(ctx context.Context) error {
	return p.cl.Ping(ctx)
}

func (p *KafkaProducer) Close() {
	p.cl.Close()
}
