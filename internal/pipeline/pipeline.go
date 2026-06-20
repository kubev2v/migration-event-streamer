package pipeline

import (
	"context"
	"encoding/json"

	"github.com/kubev2v/migration-event-streamer/internal/entity"
	"go.uber.org/zap"
)

type Processor[T any, S any] func(context.Context, T) (S, error)
type WriteFn[S any] func(context.Context, S) error

type Pipeline[T any, S any] struct {
	name   string
	handle func(context.Context, T) error
	input  <-chan entity.PipelineJob
	errors chan<- entity.PipelineError
}

func NewPipeline[T any, S any](name string, process Processor[T, S], write WriteFn[S], input <-chan entity.PipelineJob, errors chan<- entity.PipelineError) *Pipeline[T, S] {
	p := &Pipeline[T, S]{
		name:   name,
		input:  input,
		errors: errors,
	}
	p.handle = func(ctx context.Context, payload T) error {
		r, err := process(ctx, payload)
		if err != nil {
			return err
		}
		return write(ctx, r)
	}
	return p
}

func (p *Pipeline[T, S]) Start(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer func() {
			close(done)
			zap.S().Infow("pipeline stopped", "name", p.name)
		}()
		for {
			select {
			case job, ok := <-p.input:
				if !ok {
					return
				}
				var payload T
				if err := json.Unmarshal(job.Data, &payload); err != nil {
					p.sendError(err)
					close(job.Done)
					continue
				}
				if err := p.handle(ctx, payload); err != nil {
					p.sendError(err)
				}
				close(job.Done)
			case <-ctx.Done():
				return
			}
		}
	}()
	return done
}

func (p *Pipeline[T, S]) sendError(err error) {
	pe := entity.NewPipelineError(p.name, err)
	p.errors <- pe
	<-pe.Ack
}

func (p *Pipeline[T, S]) WithRetry() *Pipeline[T, S] {
	previous := p.handle
	p.handle = func(ctx context.Context, input T) error {
		// TODO add retry function
		return previous(ctx, input)
	}
	return p
}

func (p *Pipeline[T, S]) WithObservability() *Pipeline[T, S] {
	previous := p.handle
	p.handle = func(ctx context.Context, input T) error {
		// TODO add observability function
		return previous(ctx, input)
	}
	return p
}
