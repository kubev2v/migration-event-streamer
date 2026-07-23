package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/kubev2v/migration-event-streamer/internal/datastore/elastic"
	"github.com/kubev2v/migration-event-streamer/internal/entity"
	"github.com/kubev2v/migration-event-streamer/internal/processors"
	"go.uber.org/zap"
)

const (
	defaultMaxRetries = 3
	defaultRetryDelay = 1000 * time.Millisecond

	AssessmentCreated         = "assessment.created"
	AssessmentDeleted         = "assessment.deleted"
	PartnerCustomerUpdated    = "partner_customer.updated"
	UserActionShared          = "user_action.assessment_shared"
	UserActionUnshared        = "user_action.assessment_unshared"
	UserActionSizingRequested = "user_action.sizing_requested"
	UserActionComplexity      = "user_action.complexity_estimated"
	UserActionTimeEstimated   = "user_action.time_estimated"
	UserActionOVADownloaded   = "user_action.ova_downloaded"
	UserActionVisited         = "user_action.visited"
)

type Consumer interface {
	Consume(context.Context, chan entity.Message) error
}

type Manager struct {
	router       *Router
	dispatcher   *Dispatcher
	errorHandler ErrorHandler
	errorCh      chan entity.PipelineError
	wg           sync.WaitGroup
}

func NewManager(ctx context.Context, routerConsumer, dispatcherConsumer Consumer, writer RouteWriter) (*Manager, error) {
	routerCh := make(chan entity.Message)
	dispatcherCh := make(chan entity.Message)
	errorCh := make(chan entity.PipelineError, 100)

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

func (m *Manager) Start(ctx context.Context, cancel context.CancelFunc) {
	errorsDone := m.errorHandler.Start(ctx)
	routerDone := m.router.Start(ctx)
	dispatcherDone := m.dispatcher.Start(ctx)

	m.wg.Add(3)
	go func() { <-routerDone; cancel(); m.wg.Done() }()
	go func() { <-dispatcherDone; close(m.errorCh); cancel(); m.wg.Done() }()
	go func() { <-errorsDone; cancel(); m.wg.Done() }()
}

func (m *Manager) Wait() {
	m.wg.Wait()
}

func (m *Manager) InitAllPipelines(w elastic.Writer) {
	registerPipeline(m.dispatcher, m.errorCh, AssessmentCreated, processors.AssessmentCreatedProcessor, w.Assessment().WriteCreated)
	registerPipeline(m.dispatcher, m.errorCh, AssessmentDeleted, processors.AssessmentDeletedProcessor, w.Assessment().WriteCascadeDelete)
	registerPipeline(m.dispatcher, m.errorCh, UserActionVisited, processors.VisitedProcessor, w.UserAction().WriteVisited)
	registerPipeline(m.dispatcher, m.errorCh, PartnerCustomerUpdated, processors.PartnerCustomerProcessor, w.PartnerCustomer().Write)
	registerPipeline(m.dispatcher, m.errorCh, UserActionShared, processors.ShareAssessmentProcessor, w.UserAction().WriteShareAssessment)
	registerPipeline(m.dispatcher, m.errorCh, UserActionUnshared, processors.UnshareAssessmentProcessor, w.UserAction().WriteUnshareAssessment)
	registerPipeline(m.dispatcher, m.errorCh, UserActionSizingRequested, processors.SizingRequestedProcessor, w.UserAction().WriteSizingRequested)
	registerPipeline(m.dispatcher, m.errorCh, UserActionComplexity, processors.ComplexityEstimatedProcessor, w.UserAction().WriteComplexityEstimated)
	registerPipeline(m.dispatcher, m.errorCh, UserActionTimeEstimated, processors.TimeEstimatedProcessor, w.UserAction().WriteTimeEstimated)
	registerPipeline(m.dispatcher, m.errorCh, UserActionOVADownloaded, processors.OVADownloadedProcessor, w.UserAction().WriteOVADownloaded)
}

func registerPipeline[T any, S any](d *Dispatcher, errors chan<- entity.PipelineError, name string, process Processor[T, S], write WriteFn[S]) {
	input := make(chan entity.PipelineJob)
	d.Register(name, input, NewPipeline(name, process, write, input, errors).WithRetry(defaultMaxRetries, defaultRetryDelay).WithObservability().WithRecovery())
}
