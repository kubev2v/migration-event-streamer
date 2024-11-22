package pipeline

import (
	"context"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/tupyy/migration-event-streamer/internal/entity"
)

type Consumer interface {
	Consume(context.Context, chan entity.Message) error
}

type Manager struct {
	elasticPipelines []*Pipeline[entity.Event]
	kafkaPipelines   []*Pipeline[cloudevents.Event]
}

func NewManager() *Manager {
	return &Manager{
		elasticPipelines: []*Pipeline[entity.Event]{},
	}
}

// CreateElasticPipeline creates a pipeline which reads from kafka and writes to elastic.
func (m *Manager) CreateElasticPipeline(ctx context.Context, name string, consumer Consumer, writer Writer[entity.Event], worker Worker[entity.Event]) error {
	messages := make(chan entity.Message)
	if err := consumer.Consume(ctx, messages); err != nil {
		return err
	}

	pipeline := NewPipeline[entity.Event](name, messages, writer, worker)
	go pipeline.Start(ctx)

	m.elasticPipelines = append(m.elasticPipelines, pipeline)

	return nil
}

// CreateKafkaPipeline creates a pipeline which reads from kafka and writes to kafka.
func (m *Manager) CreateKafkaPipeline(ctx context.Context, name string, consumer Consumer, writer Writer[cloudevents.Event], worker Worker[cloudevents.Event]) error {
	messages := make(chan entity.Message)
	if err := consumer.Consume(ctx, messages); err != nil {
		return err
	}

	pipeline := NewPipeline[cloudevents.Event](name, messages, writer, worker)
	go pipeline.Start(ctx)

	m.kafkaPipelines = append(m.kafkaPipelines, pipeline)

	return nil
}

func (m *Manager) Close(ctx context.Context) error {
	return nil
}
