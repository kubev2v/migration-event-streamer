package pipeline

import (
	"context"
	"errors"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"go.uber.org/zap"
)

type Worker[T any] func(ctx context.Context, message cloudevents.Event, writer Writer[T]) error

type Writer[T any] interface {
	Write(context.Context, T) error
}

type Pipeline[T any] struct {
	worker   Worker[T]
	writer   Writer[T]
	messages chan cloudevents.Event
	done     chan chan any
}

func NewPipeline[T any](messages chan cloudevents.Event, writer Writer[T], worker Worker[T]) *Pipeline[T] {
	return &Pipeline[T]{
		writer:   writer,
		messages: messages,
		worker:   worker,
		done:     make(chan chan any),
	}
}

func (d *Pipeline[T]) Start(ctx context.Context) {
	for msg := range d.messages {
		// TODO add retry and metrics
		// process the message
		if err := d.worker(ctx, msg, d.writer); err != nil {
			zap.S().Warnw("failed to process message", "message", msg, "error", err)
		}

		select {
		case c := <-d.done:
			close(c)
			return
		default:
		}
	}
}

func (d *Pipeline[T]) Close(ctx context.Context) error {
	c := make(chan any)
	go func() { d.done <- c }()
	select {
	case <-c:
		return nil
	case <-ctx.Done():
		return errors.New("pipeline closed with error: context expired")
	}
}
