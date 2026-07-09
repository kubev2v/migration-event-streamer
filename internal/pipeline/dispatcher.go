package pipeline

import (
	"context"
	"github.com/kubev2v/migration-event-streamer/internal/entity"
	"go.uber.org/zap"
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

	pipelineCtx, pipelineCancel := context.WithCancel(ctx)

	var pipelineDoneChans []<-chan struct{}
	for _, p := range d.pipelines {
		pipelineDoneChans = append(pipelineDoneChans, p.pipeline.Start(pipelineCtx))
	}

	go func() {
		defer close(done)
		zap.S().Infow("dispatcher started", "pipelines", len(d.pipelines))
		defer func() {
			pipelineCancel()
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

func (d *Dispatcher) Register(name string, input chan entity.PipelineJob, pipeline PipelineStarter) {
	d.pipelines[name] = &pipelineEntry{input: input, pipeline: pipeline}
}
