package pipeline

import (
	"context"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/tupyy/migration-event-streamer/internal/entity"
)

type Consumer interface {
	Consume(context.Context, chan cloudevents.Event) error
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
func (m *Manager) CreateElasticPipeline(ctx context.Context, consumer Consumer, writer Writer[entity.Event], worker Worker[entity.Event]) error {
	messages := make(chan cloudevents.Event)
	if err := consumer.Consume(ctx, messages); err != nil {
		return err
	}

	pipeline := NewPipeline[entity.Event](messages, writer, worker)
	go pipeline.Start(ctx)

	m.elasticPipelines = append(m.elasticPipelines, pipeline)

	return nil
}

// CreateKafkaPipeline creates a pipeline which reads from kafka and writes to kafka.
func (m *Manager) CreateKafkaPipeline(ctx context.Context, consumer Consumer, writer Writer[cloudevents.Event], worker Worker[cloudevents.Event]) error {
	messages := make(chan cloudevents.Event)
	if err := consumer.Consume(ctx, messages); err != nil {
		return err
	}

	pipeline := NewPipeline[cloudevents.Event](messages, writer, worker)
	go pipeline.Start(ctx)

	m.kafkaPipelines = append(m.kafkaPipelines, pipeline)

	return nil
}

func (m *Manager) Close(ctx context.Context) error {
	var err error
	for _, p := range m.kafkaPipelines {
		err = p.Close(ctx)
	}
	for _, p := range m.elasticPipelines {
		err = p.Close(ctx)
	}
	return err
}
