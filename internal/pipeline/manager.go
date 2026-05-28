package pipeline

import (
	"context"
	"github.com/kubev2v/migration-event-streamer/internal/entity"
	"go.uber.org/zap"
)

type Consumer interface {
	Consume(context.Context, chan entity.Message) error
}

type buildFn func() error

type Manager struct {
	buildFns         []buildFn
	elasticPipelines []*Pipeline
	router           *Router
}

func NewManager() *Manager {
	return &Manager{
		buildFns:         []buildFn{},
		elasticPipelines: []*Pipeline{},
	}
}

// ElasticPipeline creates a pipeline which reads from kafka and writes to elastic.
func (m *Manager) ElasticPipeline(ctx context.Context, name string, consumer Consumer, writer ElasticWriter, worker Worker) *Manager {
	m.buildFns = append(m.buildFns, func() error {
		messages := make(chan entity.Message)
		if err := consumer.Consume(ctx, messages); err != nil {
			return err
		}

		pipeline := NewPipeline(name, messages, writer, worker).WithRetry().WithObservability()
		m.elasticPipelines = append(m.elasticPipelines, pipeline)

		zap.S().Infof("elastic %q pipeline created", name)
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

	go m.router.Start(ctx)
	return nil
}
