package pipeline

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/kubev2v/migration-event-streamer/internal/datastore/elastic"
	"github.com/kubev2v/migration-event-streamer/internal/entity"
	"github.com/kubev2v/migration-event-streamer/internal/processors"
	plannerEvents "github.com/kubev2v/migration-planner/pkg/events"
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
	input    chan any
	pipeline PipelineStarter
}

type Dispatcher struct {
	input     chan entity.Message
	errors    chan PipelineError
	pipelines map[string]*pipelineEntry
	done      chan struct{}
}

func NewDispatcher(input chan entity.Message, errors chan PipelineError) *Dispatcher {
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

			event, err := unmarshalEvent(key, msg.Event.Data())
			if err != nil {
				zap.S().Warnw("failed to unmarshal event", "key", key, "error", err)
				close(msg.CommitCh)
				continue
			}

			entry.input <- event
			close(msg.CommitCh)

			zap.S().Infow("message dispatched", "key", key, "id", msg.Event.Context.GetID(), "type", ceType)
		}
	}()
}

func (d *Dispatcher) WithAssessmentCreatedPipeline(w elastic.Writer) *Dispatcher {
	input := make(chan any)
	d.pipelines[AssessmentCreated] = &pipelineEntry{
		input:    input,
		pipeline: NewPipeline(AssessmentCreated, processors.AssessmentCreatedProcessor, w.Assessment().WriteCreated, input, d.errors).WithRetry().WithObservability(),
	}
	return d
}

func (d *Dispatcher) WithAssessmentDeletedPipeline(w elastic.Writer) *Dispatcher {
	input := make(chan any)
	d.pipelines[AssessmentDeleted] = &pipelineEntry{
		input:    input,
		pipeline: NewPipeline(AssessmentDeleted, processors.AssessmentDeletedProcessor, w.Assessment().WriteCascadeDelete, input, d.errors).WithRetry().WithObservability(),
	}
	return d
}

func (d *Dispatcher) WithVisitedPipeline(w elastic.Writer) *Dispatcher {
	input := make(chan any)
	d.pipelines[UserActionVisited] = &pipelineEntry{
		input:    input,
		pipeline: NewPipeline(UserActionVisited, processors.VisitedProcessor, w.UserAction().WriteVisited, input, d.errors).WithRetry().WithObservability(),
	}
	return d
}

func (d *Dispatcher) WithPartnerCustomerUpdatedPipeline(w elastic.Writer) *Dispatcher {
	input := make(chan any)
	d.pipelines[PartnerCustomerUpdated] = &pipelineEntry{
		input:    input,
		pipeline: NewPipeline(PartnerCustomerUpdated, processors.PartnerCustomerProcessor, w.PartnerCustomer().Write, input, d.errors).WithRetry().WithObservability(),
	}
	return d
}

func (d *Dispatcher) WithShareAssessmentPipeline(w elastic.Writer) *Dispatcher {
	input := make(chan any)
	d.pipelines[UserActionShared] = &pipelineEntry{
		input:    input,
		pipeline: NewPipeline(UserActionShared, processors.ShareAssessmentProcessor, w.UserAction().WriteShareAssessment, input, d.errors).WithRetry().WithObservability(),
	}
	return d
}

func (d *Dispatcher) WithUnshareAssessmentPipeline(w elastic.Writer) *Dispatcher {
	input := make(chan any)
	d.pipelines[UserActionUnshared] = &pipelineEntry{
		input:    input,
		pipeline: NewPipeline(UserActionUnshared, processors.UnshareAssessmentProcessor, w.UserAction().WriteUnshareAssessment, input, d.errors).WithRetry().WithObservability(),
	}
	return d
}

func (d *Dispatcher) WithSizingRequestedPipeline(w elastic.Writer) *Dispatcher {
	input := make(chan any)
	d.pipelines[UserActionSizingRequested] = &pipelineEntry{
		input:    input,
		pipeline: NewPipeline(UserActionSizingRequested, processors.SizingRequestedProcessor, w.UserAction().WriteSizingRequested, input, d.errors).WithRetry().WithObservability(),
	}
	return d
}

func (d *Dispatcher) WithComplexityEstimatedPipeline(w elastic.Writer) *Dispatcher {
	input := make(chan any)
	d.pipelines[UserActionComplexity] = &pipelineEntry{
		input:    input,
		pipeline: NewPipeline(UserActionComplexity, processors.ComplexityEstimatedProcessor, w.UserAction().WriteComplexityEstimated, input, d.errors).WithRetry().WithObservability(),
	}
	return d
}

func (d *Dispatcher) WithTimeEstimatedPipeline(w elastic.Writer) *Dispatcher {
	input := make(chan any)
	d.pipelines[UserActionTimeEstimated] = &pipelineEntry{
		input:    input,
		pipeline: NewPipeline(UserActionTimeEstimated, processors.TimeEstimatedProcessor, w.UserAction().WriteTimeEstimated, input, d.errors).WithRetry().WithObservability(),
	}
	return d
}

func (d *Dispatcher) WithOVADownloadedPipeline(w elastic.Writer) *Dispatcher {
	input := make(chan any)
	d.pipelines[UserActionOVADownloaded] = &pipelineEntry{
		input:    input,
		pipeline: NewPipeline(UserActionOVADownloaded, processors.OVADownloadedProcessor, w.UserAction().WriteOVADownloaded, input, d.errors).WithRetry().WithObservability(),
	}
	return d
}

func unmarshalEvent(key string, data []byte) (any, error) {
	switch key {
	case AssessmentCreated, AssessmentDeleted:
		var payload plannerEvents.AssessmentEventPayload
		if err := json.Unmarshal(data, &payload); err != nil {
			return nil, err
		}
		return entity.Event[plannerEvents.AssessmentEventPayload]{Payload: payload}, nil
	case PartnerCustomerUpdated:
		var payload plannerEvents.PartnerCustomerEventPayload
		if err := json.Unmarshal(data, &payload); err != nil {
			return nil, err
		}
		return entity.Event[plannerEvents.PartnerCustomerEventPayload]{Payload: payload}, nil
	case UserActionShared, UserActionUnshared, UserActionSizingRequested, UserActionComplexity, UserActionTimeEstimated, UserActionOVADownloaded, UserActionVisited:
		var payload plannerEvents.UserActionEventPayload
		if err := json.Unmarshal(data, &payload); err != nil {
			return nil, err
		}
		return entity.Event[plannerEvents.UserActionEventPayload]{Payload: payload}, nil
	default:
		return nil, fmt.Errorf("unknown pipeline %q", key)
	}
}
