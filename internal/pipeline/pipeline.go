package pipeline

import (
	"context"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/kubev2v/migration-event-streamer/internal/entity"
	"github.com/kubev2v/migration-event-streamer/internal/metrics"
	"go.uber.org/zap"
)

type HandleFn func(context.Context, cloudevents.Event) error
type Processor[T any] func(context.Context, cloudevents.Event) (T, error)
type WriteFn[T any] func(context.Context, T) error

type Starter interface {
	Start(context.Context)
}

type Pipeline[T any] struct {
	name      string
	processor Processor[T]
	write     WriteFn[T]
	handle    HandleFn
	messages  chan entity.Message
}

func NewPipeline[T any](name string, messages chan entity.Message, processor Processor[T], write WriteFn[T]) *Pipeline[T] {
	p := &Pipeline[T]{
		name:      name,
		processor: processor,
		write:     write,
		messages:  messages,
	}
	p.handle = func(ctx context.Context, event cloudevents.Event) error {
		result, err := p.processor(ctx, event)
		if err != nil {
			return err
		}
		return p.write(ctx, result)
	}
	return p
}

func (d *Pipeline[T]) WithRetry() *Pipeline[T] {
	previous := d.handle
	d.handle = func(ctx context.Context, event cloudevents.Event) error {
		// TODO add retry function
		return previous(ctx, event)
	}
	return d
}

func (d *Pipeline[T]) WithObservability() *Pipeline[T] {
	previous := d.handle
	d.handle = func(ctx context.Context, event cloudevents.Event) error {
		startTimestamp := event.Context.GetTime()
		metrics.IncreaseMessagesCount(event.Context.GetType())
		err := previous(ctx, event)
		if err != nil {
			metrics.IncreaseErrorProcessingCount(event.Context.GetType())
			return err
		}
		metrics.IncreaseProcessedMessagesCount(event.Context.GetType())
		metrics.UpdateProcessingMetric(event.Context.GetType(), time.Since(startTimestamp))
		return nil
	}
	return d
}

func (d *Pipeline[T]) Start(ctx context.Context) {
	zap.S().Infof("%s pipeline started", d.name)
	defer func() { zap.S().Infof("%s pipeline closed", d.name) }()

	for msg := range d.messages {
		if err := d.handle(ctx, msg.Event); err != nil {
			zap.S().Warnw("failed to process message", "message", msg, "error", err)
		}

		close(msg.CommitCh)

		zap.S().Infow("message consumed", "pipeline", d.name, "id", msg.Event.Context.GetID(), "type", msg.Event.Context.GetType(), "topic", msg.Event.Extensions()["kafkatopic"])
	}
}
