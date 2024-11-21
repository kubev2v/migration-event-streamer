package datastore

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/tupyy/migration-event-streamer/internal/config"
	"github.com/tupyy/migration-event-streamer/internal/datastore/elastic"
	"github.com/tupyy/migration-event-streamer/internal/datastore/kafka"
	"github.com/tupyy/migration-event-streamer/pkg/producer"
	"go.uber.org/zap"
)

type Datastore struct {
	kConfig        config.KafkaConfig
	elasticRepo    *elastic.ElasticRepository
	kafkaConsumers []*kafka.Consumer
	kafkaProducers []*producer.KafkaProducer
}

func NewDatastore(esConfig config.ElasticSearchEnvConfig, kConfig config.KafkaConfig) (*Datastore, error) {
	es, err := elastic.NewElasticRepository(esConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create elastic client: %w", err)
	}
	return &Datastore{
		kConfig:        kConfig,
		elasticRepo:    es,
		kafkaConsumers: []*kafka.Consumer{},
	}, nil
}

func (d *Datastore) ElasticRepository() *elastic.ElasticRepository {
	return d.elasticRepo
}

func (d *Datastore) CreateKafkaConsumer(topic, consumerGroupID string) (*kafka.Consumer, error) {
	kc, err := kafka.NewConsumer(d.kConfig, topic, consumerGroupID)
	if err != nil {
		return nil, err
	}

	d.kafkaConsumers = append(d.kafkaConsumers, kc)
	zap.S().Infow("kafka consumer created", "topic", topic, "consumerGroupID", consumerGroupID)

	return kc, nil
}

func (d *Datastore) CreateKafkaProducer(topic string) (*producer.KafkaProducer, error) {
	saramaConfig := sarama.NewConfig()
	if d.kConfig.SaramaConfig != nil {
		saramaConfig = d.kConfig.SaramaConfig
	}
	saramaConfig.Version = sarama.V3_6_0_0

	kp, err := producer.NewKafkaProducer(d.kConfig.Brokers, saramaConfig, topic)
	if err != nil {
		return nil, err
	}

	d.kafkaProducers = append(d.kafkaProducers, kp)
	zap.S().Infow("kafka producer created", "topic", topic)

	return kp, nil
}

func (d *Datastore) CreateIndexes(prefix string, indexes []string) error {
	for _, idx := range indexes {
		if err := d.elasticRepo.CreateIndex(fmt.Sprintf("%s_%s", prefix, idx)); err != nil {
			return err
		}
	}
	return nil
}

func (d *Datastore) Close(ctx context.Context) error {
	var err error
	for _, c := range d.kafkaConsumers {
		if cerr := c.Close(ctx); cerr != nil {
			err = cerr
			zap.S().Errorf("consumer closed with error: %s", err)
		}
	}
	for _, p := range d.kafkaProducers {
		p.Close(ctx)
	}
	return err
}
