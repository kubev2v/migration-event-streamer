package datastore

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/tupyy/migration-event-streamer/internal/config"
	"github.com/tupyy/migration-event-streamer/internal/datastore/elastic"
	"github.com/tupyy/migration-event-streamer/internal/datastore/kafka"
	pkgKafka "github.com/tupyy/migration-event-streamer/pkg/kafka"
	"go.uber.org/zap"
)

type buildFn func() error

type Datastore struct {
	buildFns       []buildFn
	kConfig        config.KafkaConfig
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

func (d *Datastore) WithElasticRepository(esConfig config.ElasticSearchEnvConfig, indexes []string) *Datastore {
	d.buildFns = append(d.buildFns, func() error {
		if d.elasticRepo != nil {
			return nil
		}
		elasticRepo, err := elastic.NewElasticRepository(esConfig)
		if err != nil {
			return err
		}
		// create indexes
		if err := d.createIndexes(elasticRepo, esConfig.Index, indexes); err != nil {
			return fmt.Errorf("failed to create indexes: %v", err)
		}
		d.elasticRepo = elasticRepo
		return nil
	})
	return d
}

func (d *Datastore) WithKafkaConsumer(name string, kConfig config.KafkaConfig, topic, consumerGroupID string) *Datastore {
	d.buildFns = append(d.buildFns, func() error {
		if _, ok := d.kafkaConsumers[name]; ok {
			return fmt.Errorf("failed to create kafka consumer with name %s. consumer already exists", name)
		}
		kc, err := d.createKafkaConsumer(kConfig, topic, consumerGroupID)
		if err != nil {
			return err
		}
		d.kafkaConsumers[name] = kc
		zap.S().Infow("kafka consumer created", "topic", topic, "consumerGroupID", consumerGroupID)
		return nil
	})
	return d
}

func (d *Datastore) WithKafkaProducer(name string, kConfig config.KafkaConfig) *Datastore {
	d.buildFns = append(d.buildFns, func() error {
		if _, ok := d.kafkaProducers[name]; ok {
			return fmt.Errorf("failed to create kafka producer with name %s. consumer already exists", name)
		}
		kp, err := d.createKafkaProducer(kConfig.Brokers)
		if err != nil {
			return err
		}
		d.kafkaProducers[name] = kp
		zap.S().Infow("kafka producer created", name)
		return nil
	})
	return d
}

func (d *Datastore) Build() error {
	for _, claim := range d.buildFns {
		if err := claim(); err != nil {
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

func (d *Datastore) createKafkaConsumer(config config.KafkaConfig, topic, consumerGroupID string) (*kafka.Consumer, error) {
	kc, err := kafka.NewConsumer(config, topic, consumerGroupID)
	if err != nil {
		return nil, err
	}

	return kc, nil
}

func (d *Datastore) createKafkaProducer(brokers []string) (*pkgKafka.KafkaProducer, error) {
	saramaConfig := sarama.NewConfig()
	if d.kConfig.SaramaConfig != nil {
		saramaConfig = d.kConfig.SaramaConfig
	}
	saramaConfig.Version = sarama.V3_6_0_0

	kp, err := pkgKafka.NewKafkaProducer(brokers, saramaConfig)
	if err != nil {
		return nil, err
	}

	return kp, nil
}

func (d *Datastore) createIndexes(es *elastic.ElasticRepository, prefix string, indexes []string) error {
	for _, idx := range indexes {
		if err := es.CreateIndex(fmt.Sprintf("%s_%s", prefix, idx)); err != nil {
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
