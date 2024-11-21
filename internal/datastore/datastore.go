package datastore

import (
	"context"
	"fmt"

	"github.com/tupyy/migration-event-streamer/internal/config"
	"github.com/tupyy/migration-event-streamer/internal/datastore/elastic"
	"github.com/tupyy/migration-event-streamer/internal/datastore/kafka"
	"go.uber.org/zap"
)

type Datastore struct {
	kConfig        config.KafkaConfig
	elasticRepo    *elastic.ElasticRepository
	kafkaConsumers map[string]*kafka.Consumer
}

func NewDatastore(esConfig config.ElasticSearchEnvConfig, kConfig config.KafkaConfig) (*Datastore, error) {
	es, err := elastic.NewElasticRepository(esConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create elastic client: %w", err)
	}
	return &Datastore{
		kConfig:        kConfig,
		elasticRepo:    es,
		kafkaConsumers: make(map[string]*kafka.Consumer),
	}, nil
}

func (d *Datastore) ElasticRepository() *elastic.ElasticRepository {
	return d.elasticRepo
}

func (d *Datastore) CreateKafkaConsumer(topic, consumerGroupID string) (*kafka.Consumer, error) {
	consumer, ok := d.kafkaConsumers[topic]
	if !ok {
		kc, err := kafka.NewConsumer(d.kConfig, topic, consumerGroupID)
		if err != nil {
			return nil, err
		}
		d.kafkaConsumers[topic] = kc
		consumer = kc
	}
	return consumer, nil
}

func (d *Datastore) CreateKafkaProducer(topic string) error {
	return nil
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
	return err
}
