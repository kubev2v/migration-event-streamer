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
	buildFns  []buildFn
	pipelines []Starter
	router    *Router
}

func NewManager() *Manager {
	return &Manager{
		buildFns:  []buildFn{},
		pipelines: []Starter{},
	}
}

func AddPipeline[T any](m *Manager, ctx context.Context, name string, consumer Consumer, processor Processor[T], writeFn WriteFn[T]) {
	m.buildFns = append(m.buildFns, func() error {

		messages := make(chan entity.Message)
		if err := consumer.Consume(ctx, messages); err != nil {
			return err
		}

		p := NewPipeline(name, messages, processor, writeFn).WithRetry().WithObservability()
		m.pipelines = append(m.pipelines, p)

		zap.S().Infof("%q pipeline created", name)
		return nil
	})
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

	for _, p := range m.pipelines {
		go p.Start(ctx)
	}

	go m.router.Start(ctx)
	return nil
}
