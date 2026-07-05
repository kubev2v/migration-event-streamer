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
}

func NewDispatcher(input chan entity.Message, errors chan entity.PipelineError) *Dispatcher {
	return &Dispatcher{
		input:     input,
		errors:    errors,
		pipelines: make(map[string]*pipelineEntry),
	}
}

func (d *Dispatcher) Start(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})

	var pipelineDoneChans []<-chan struct{}
	for _, p := range d.pipelines {
		pipelineDoneChans = append(pipelineDoneChans, p.pipeline.Start(ctx))
	}

	go func() {
		defer close(done)
		zap.S().Infow("dispatcher started", "pipelines", len(d.pipelines))
		defer func() {
			for _, p := range d.pipelines {
				close(p.input)
			}
			for _, ch := range pipelineDoneChans {
				<-ch
			}
			zap.S().Info("dispatcher stopped")
		}()

		for {
			select {
			case msg, ok := <-d.input:
				if !ok {
					return
				}
				ceType := msg.Event.Context.GetType()
				key := ExtractEntityAction(ceType)

				entry, ok := d.pipelines[key]
				if !ok {
					zap.S().Warnw("no pipeline registered", "key", key, "event_type", ceType)
					close(msg.CommitCh)
					continue
				}

				job := entity.NewPipelineJob(msg.Event.Data())
				select {
				case entry.input <- job:
				case <-ctx.Done():
					return
				}

				select {
				case <-job.Done:
				case <-ctx.Done():
					return
				}
				close(msg.CommitCh)

				zap.S().Infow("message dispatched", "key", key, "id", msg.Event.Context.GetID(), "type", ceType)
			case <-ctx.Done():
				return
			}
		}
	}()
	return done
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
		pipeline: NewPipeline(name, process, write, input, d.errors).WithRetry().WithObservability().WithRecovery(),
	}
}
