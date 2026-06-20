package pipeline

import (
	"context"

	"github.com/kubev2v/migration-event-streamer/internal/entity"
	"go.uber.org/zap"
)

type Processor[T any, S any] func(context.Context, entity.Event[T]) (S, error)
type WriteFn[S any] func(context.Context, S) error

type Pipeline[T any, S any] struct {
	name   string
	handle func(context.Context, entity.Event[T]) error
	input  <-chan any
	errors chan<- PipelineError
}

func NewPipeline[T any, S any](name string, process Processor[T, S], write WriteFn[S], input <-chan any, errors chan<- PipelineError) *Pipeline[T, S] {
	p := &Pipeline[T, S]{
		name:   name,
		input:  input,
		errors: errors,
	}
	p.handle = func(ctx context.Context, ev entity.Event[T]) error {
		r, err := process(ctx, ev)
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
			case raw, ok := <-p.input:
				if !ok {
					return
				}
				event := raw.(entity.Event[T])
				if err := p.handle(ctx, event); err != nil {
					p.errors <- PipelineError{Pipeline: p.name, Err: err}
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return done
}

func (p *Pipeline[T, S]) WithRetry() *Pipeline[T, S] {
	previous := p.handle
	p.handle = func(ctx context.Context, input entity.Event[T]) error {
		// TODO add retry function
		return previous(ctx, input)
	}
	return p
}

func (p *Pipeline[T, S]) WithObservability() *Pipeline[T, S] {
	previous := p.handle
	p.handle = func(ctx context.Context, input entity.Event[T]) error {
		// TODO add observability function
		return previous(ctx, input)
	}
	return p
}
