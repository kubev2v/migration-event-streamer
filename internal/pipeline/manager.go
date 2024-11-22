package pipeline

import (
	"context"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/kubev2v/migration-event-streamer/internal/entity"
	"go.uber.org/zap"
)

type Consumer interface {
	Consume(context.Context, chan entity.Message) error
}

type buildFn func() error

type Manager struct {
	buildFns         []buildFn
	elasticPipelines []*Pipeline[entity.Event]
	kafkaPipelines   []*Pipeline[cloudevents.Event]
	router           *Router
}

func NewManager() *Manager {
	return &Manager{
		buildFns:         []buildFn{},
		elasticPipelines: []*Pipeline[entity.Event]{},
	}
}

// CreateElasticPipeline creates a pipeline which reads from kafka and writes to elastic.
func (m *Manager) ElasticPipeline(ctx context.Context, name string, consumer Consumer, writer Writer[entity.Event], worker Worker[entity.Event]) *Manager {
	m.buildFns = append(m.buildFns, func() error {
		messages := make(chan entity.Message)
		if err := consumer.Consume(ctx, messages); err != nil {
			return err
		}

		pipeline := NewPipeline[entity.Event](name, messages, writer, worker)
		m.elasticPipelines = append(m.elasticPipelines, pipeline)

		zap.S().Infof("elastic %q pipeline created", name)
		return nil
	})
	return m
}

// CreateKafkaPipeline creates a pipeline which reads from kafka and writes to kafka.
func (m *Manager) KafkaPipeline(ctx context.Context, name string, consumer Consumer, writer Writer[cloudevents.Event], worker Worker[cloudevents.Event]) *Manager {
	m.buildFns = append(m.buildFns, func() error {
		messages := make(chan entity.Message)
		if err := consumer.Consume(ctx, messages); err != nil {
			return err
		}

		pipeline := NewPipeline[cloudevents.Event](name, messages, writer, worker)
		m.kafkaPipelines = append(m.kafkaPipelines, pipeline)

		zap.S().Infof("kafka %q pipeline created", name)

		return nil
	})
	return m
}

func (m *Manager) Router(ctx context.Context, consumer Consumer, writer RouteWriter, topicMap map[string]string) *Manager {
	m.buildFns = append(m.buildFns, func() error {
		messages := make(chan entity.Message)
		if err := consumer.Consume(ctx, messages); err != nil {
			return err
		}

		m.router = NewRouter(messages, writer, topicMap)

		zap.S().Info("router created")

		return nil
	})
	return m
}

func (m *Manager) Build(ctx context.Context) error {
	for _, buildFn := range m.buildFns {
		if err := buildFn(); err != nil {
			return err
		}
	}
	// start pipelines
	for _, p := range m.elasticPipelines {
		go p.Start(ctx)
	}

	for _, k := range m.kafkaPipelines {
		go k.Start(ctx)
	}

	go m.router.Start(ctx)
	return nil
}
