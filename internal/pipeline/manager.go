package pipeline

import (
	"context"

	"github.com/kubev2v/migration-event-streamer/internal/datastore/elastic"
	"github.com/kubev2v/migration-event-streamer/internal/entity"
	"go.uber.org/zap"
)

type Consumer interface {
	Consume(context.Context, chan entity.Message) error
}

type Manager struct {
	router       *Router
	dispatcher   *Dispatcher
	errorHandler ErrorHandler
	errorCh      chan PipelineError
}

func NewManager(ctx context.Context, routerConsumer, dispatcherConsumer Consumer, writer RouteWriter) (*Manager, error) {
	routerCh := make(chan entity.Message)
	dispatcherCh := make(chan entity.Message)
	errorCh := make(chan PipelineError, 100)

	router := NewRouter(routerCh, writer)
	dispatcher := NewDispatcher(dispatcherCh, errorCh)
	errorHandler := NewLogErrorHandler(errorCh)

	m := &Manager{
		router:       router,
		dispatcher:   dispatcher,
		errorHandler: errorHandler,
		errorCh:      errorCh,
	}

	if err := routerConsumer.Consume(ctx, routerCh); err != nil {
		return nil, err
	}
	if err := dispatcherConsumer.Consume(ctx, dispatcherCh); err != nil {
		return nil, err
	}

	zap.S().Info("manager created with router and dispatcher")
	return m, nil
}

func (m *Manager) Start(ctx context.Context) {
	m.errorHandler.Start(ctx)
	m.router.Start(ctx)
	m.dispatcher.Start(ctx)

	go func() {
		<-m.dispatcher.Done()
		close(m.errorCh)
	}()
}

func (m *Manager) WithAssessmentCreatedPipeline(w elastic.Writer) *Manager {
	m.dispatcher.WithAssessmentCreatedPipeline(w)
	return m
}

func (m *Manager) WithAssessmentDeletedPipeline(w elastic.Writer) *Manager {
	m.dispatcher.WithAssessmentDeletedPipeline(w)
	return m
}

func (m *Manager) WithVisitedPipeline(w elastic.Writer) *Manager {
	m.dispatcher.WithVisitedPipeline(w)
	return m
}

func (m *Manager) WithPartnerCustomerUpdatedPipeline(w elastic.Writer) *Manager {
	m.dispatcher.WithPartnerCustomerUpdatedPipeline(w)
	return m
}

func (m *Manager) WithShareAssessmentPipeline(w elastic.Writer) *Manager {
	m.dispatcher.WithShareAssessmentPipeline(w)
	return m
}

func (m *Manager) WithUnshareAssessmentPipeline(w elastic.Writer) *Manager {
	m.dispatcher.WithUnshareAssessmentPipeline(w)
	return m
}

func (m *Manager) WithSizingRequestedPipeline(w elastic.Writer) *Manager {
	m.dispatcher.WithSizingRequestedPipeline(w)
	return m
}

func (m *Manager) WithComplexityEstimatedPipeline(w elastic.Writer) *Manager {
	m.dispatcher.WithComplexityEstimatedPipeline(w)
	return m
}

func (m *Manager) WithOVADownloadedPipeline(w elastic.Writer) *Manager {
	m.dispatcher.WithOVADownloadedPipeline(w)
	return m
}
