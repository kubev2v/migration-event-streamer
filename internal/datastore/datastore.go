package datastore

import (
	"context"
	"fmt"

	"github.com/kubev2v/migration-event-streamer/internal/config"
	"github.com/kubev2v/migration-event-streamer/internal/datastore/elastic"
	"github.com/kubev2v/migration-event-streamer/internal/datastore/kafka"
	pkgKafka "github.com/kubev2v/migration-event-streamer/pkg/kafka"
	"go.uber.org/zap"
)

type buildFn func() error

type Datastore struct {
	buildFns       []buildFn
	elasticRepo    *elastic.ElasticRepository
	kafkaConsumers map[string]*kafka.Consumer
	kafkaProducers map[string]*pkgKafka.KafkaProducer
}

func NewDatastore() *Datastore {
	return &Datastore{
		kafkaConsumers: make(map[string]*kafka.Consumer),
		kafkaProducers: make(map[string]*pkgKafka.KafkaProducer),
	}
}

func (d *Datastore) WithElasticRepository(esConfig config.ElasticSearch) *Datastore {
	d.buildFns = append(d.buildFns, func() error {
		if d.elasticRepo != nil {
			return nil
		}
		elasticRepo, err := elastic.NewElasticRepository(esConfig)
		if err != nil {
			return err
		}
		if err := d.createIndexes(elasticRepo, esConfig.Indexes); err != nil {
			return fmt.Errorf("failed to create indexes: %v", err)
		}
		d.elasticRepo = elasticRepo
		return nil
	})
	return d
}

func (d *Datastore) WithKafkaConsumer(name string, kConfig config.Kafka, topic, consumerGroupID string) *Datastore {
	d.buildFns = append(d.buildFns, func() error {
		if _, ok := d.kafkaConsumers[name]; ok {
			return fmt.Errorf("failed to create kafka consumer with name %s. consumer already exists", name)
		}
		kc, err := kafka.NewConsumer(kConfig, topic, consumerGroupID)
		if err != nil {
			return err
		}
		d.kafkaConsumers[name] = kc
		zap.S().Infow("kafka consumer created", "topic", topic, "consumerGroupID", consumerGroupID)
		return nil
	})
	return d
}

func (d *Datastore) WithKafkaProducer(name string, kConfig config.Kafka) *Datastore {
	d.buildFns = append(d.buildFns, func() error {
		if _, ok := d.kafkaProducers[name]; ok {
			return fmt.Errorf("failed to create kafka producer with name %s. producer already exists", name)
		}
		kp, err := pkgKafka.NewKafkaProducer(kConfig.Brokers)
		if err != nil {
			return err
		}
		d.kafkaProducers[name] = kp
		zap.S().Infow("kafka producer created", "name", name)
		return nil
	})
	return d
}

func (d *Datastore) Build() error {
	for _, buildFn := range d.buildFns {
		if err := buildFn(); err != nil {
			return fmt.Errorf("failed to create datastore: %s", err)
		}
	}
	return nil
}

func (d *Datastore) ElasticRepository() *elastic.ElasticRepository {
	return d.elasticRepo
}

func (d *Datastore) GetConsumer(name string) (*kafka.Consumer, error) {
	if c, ok := d.kafkaConsumers[name]; ok {
		return c, nil
	}
	return nil, fmt.Errorf("consumer %s not found", name)
}

func (d *Datastore) GetProducer(name string) (*pkgKafka.KafkaProducer, error) {
	if p, ok := d.kafkaProducers[name]; ok {
		return p, nil
	}
	return nil, fmt.Errorf("producer %s not found", name)
}

func (d *Datastore) MustHaveConsumer(name string) *kafka.Consumer {
	c, err := d.GetConsumer(name)
	if err != nil {
		panic(err)
	}
	return c
}

func (d *Datastore) MustHaveProducer(name string) *pkgKafka.KafkaProducer {
	p, err := d.GetProducer(name)
	if err != nil {
		panic(err)
	}
	return p
}

func (d *Datastore) createIndexes(es *elastic.ElasticRepository, indexes []string) error {
	for _, idx := range indexes {
		if err := es.CreateIndex(idx); err != nil {
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
