package pipeline

import (
	"context"

	"github.com/kubev2v/migration-event-streamer/internal/datastore/elastic"
	"github.com/kubev2v/migration-event-streamer/internal/entity"
	"github.com/kubev2v/migration-event-streamer/internal/processors"
	"go.uber.org/zap"
)

const (
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

type PipelineStarter interface {
	Start(ctx context.Context) <-chan struct{}
}

type pipelineEntry struct {
	input    chan entity.PipelineJob
	pipeline PipelineStarter
}

type Dispatcher struct {
	input     chan entity.Message
	errors    chan entity.PipelineError
	pipelines map[string]*pipelineEntry
	done      chan struct{}
}

func NewDispatcher(input chan entity.Message, errors chan entity.PipelineError) *Dispatcher {
	return &Dispatcher{
		input:     input,
		errors:    errors,
		pipelines: make(map[string]*pipelineEntry),
		done:      make(chan struct{}),
	}
}

func (d *Dispatcher) Done() <-chan struct{} {
	return d.done
}

func (d *Dispatcher) Start(ctx context.Context) {
	var doneChans []<-chan struct{}
	for _, p := range d.pipelines {
		doneChans = append(doneChans, p.pipeline.Start(ctx))
	}

	go func() {
		zap.S().Infow("dispatcher started", "pipelines", len(d.pipelines))
		defer func() {
			for _, p := range d.pipelines {
				close(p.input)
			}
			for _, done := range doneChans {
				<-done
			}
			close(d.done)
			zap.S().Info("dispatcher stopped")
		}()

		for msg := range d.input {
			ceType := msg.Event.Context.GetType()
			key := ExtractEntityAction(ceType)

			entry, ok := d.pipelines[key]
			if !ok {
				zap.S().Warnw("no pipeline registered", "key", key, "event_type", ceType)
				close(msg.CommitCh)
				continue
			}

			job := entity.NewPipelineJob(msg.Event.Data())
			entry.input <- job
			<-job.Done
			close(msg.CommitCh)

			zap.S().Infow("message dispatched", "key", key, "id", msg.Event.Context.GetID(), "type", ceType)
		}
	}()
}

func (d *Dispatcher) InitAllPipelines(w elastic.Writer) {
	registerPipeline(d, AssessmentCreated, processors.AssessmentCreatedProcessor, w.Assessment().WriteCreated)
	registerPipeline(d, AssessmentDeleted, processors.AssessmentDeletedProcessor, w.Assessment().WriteCascadeDelete)
	registerPipeline(d, UserActionVisited, processors.VisitedProcessor, w.UserAction().WriteVisited)
	registerPipeline(d, PartnerCustomerUpdated, processors.PartnerCustomerProcessor, w.PartnerCustomer().Write)
	registerPipeline(d, UserActionShared, processors.ShareAssessmentProcessor, w.UserAction().WriteShareAssessment)
	registerPipeline(d, UserActionUnshared, processors.UnshareAssessmentProcessor, w.UserAction().WriteUnshareAssessment)
	registerPipeline(d, UserActionSizingRequested, processors.SizingRequestedProcessor, w.UserAction().WriteSizingRequested)
	registerPipeline(d, UserActionComplexity, processors.ComplexityEstimatedProcessor, w.UserAction().WriteComplexityEstimated)
	registerPipeline(d, UserActionTimeEstimated, processors.TimeEstimatedProcessor, w.UserAction().WriteTimeEstimated)
	registerPipeline(d, UserActionOVADownloaded, processors.OVADownloadedProcessor, w.UserAction().WriteOVADownloaded)
}

func registerPipeline[T any, S any](d *Dispatcher, name string, process Processor[T, S], write WriteFn[S]) {
	input := make(chan entity.PipelineJob)
	d.pipelines[name] = &pipelineEntry{
		input:    input,
		pipeline: NewPipeline(name, process, write, input, d.errors).WithRetry().WithObservability(),
	}
}
