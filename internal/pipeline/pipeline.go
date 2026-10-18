package pipeline

import (
	"context"
	"encoding/json"
	"math"
	"runtime/debug"
	"time"

	"github.com/kubev2v/migration-event-streamer/internal/entity"
	"github.com/kubev2v/migration-event-streamer/internal/metrics"
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
	metrics.RegisterEventType(name)

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
				p.processJob(ctx, job)
			case <-ctx.Done():
				return
			}
		}
	}()
	return done
}

func (p *Pipeline[T, S]) WithRetry(maxRetries int, baseDelay time.Duration) *Pipeline[T, S] {
	previous := p.handle
	p.handle = func(ctx context.Context, input T) error {
		var err error
		delay := baseDelay
		for attempt := range maxRetries + 1 {
			if attempt > 0 {
				metrics.Retry(metrics.StagePipeline, p.name)
			}
			if err = previous(ctx, input); err == nil {
				return nil
			}

			if attempt == maxRetries {
				break
			}

			zap.S().Warnw("pipeline handler failed, retrying",
				"pipeline", p.name,
				"attempt", attempt+1,
				"maxRetries", maxRetries,
				"error", err,
			)

			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}

			if delay > time.Duration(math.MaxInt64/2) {
				delay = time.Duration(math.MaxInt64)
				continue
			}
			delay *= 2
		}

		return err
	}

	return p
}

func (p *Pipeline[T, S]) WithRecovery() *Pipeline[T, S] {
	previous := p.handle
	p.handle = func(ctx context.Context, input T) (err error) {
		defer func() {
			if r := recover(); r != nil {
				zap.S().Errorw("pipeline panic recovered",
					"pipeline", p.name,
					"panic", r,
					"stack", string(debug.Stack()),
				)
			}
		}()
		return previous(ctx, input)
	}
	return p
}

func (p *Pipeline[T, S]) processJob(ctx context.Context, job entity.PipelineJob) {
	defer close(job.Done)
	outcome := metrics.Failed
	finish := metrics.Begin(metrics.StagePipeline, p.name)
	defer func() { finish(outcome) }()
	var payload T
	if err := json.Unmarshal(job.Data, &payload); err != nil {
		outcome = metrics.Invalid
		p.sendError(err)
		return
	}
	if err := p.handle(ctx, payload); err != nil {
		if ctx.Err() != nil {
			outcome = metrics.Canceled
		}
		p.sendError(err)
		return
	}
	outcome = metrics.Success
}

func (p *Pipeline[T, S]) sendError(err error) {
	pe := entity.NewPipelineError(p.name, err)
	p.errors <- pe
	<-pe.Ack
}
