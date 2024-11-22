package pipeline

import (
	"context"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/tupyy/migration-event-streamer/internal/entity"
	"go.uber.org/zap"
)

type Worker[T any] func(ctx context.Context, message cloudevents.Event, writer Writer[T]) error

type Writer[T any] interface {
	Write(context.Context, T) error
}

type Pipeline[T any] struct {
	name     string
	worker   Worker[T]
	writer   Writer[T]
	messages chan entity.Message
}

func NewPipeline[T any](name string, messages chan entity.Message, writer Writer[T], worker Worker[T]) *Pipeline[T] {
	return &Pipeline[T]{
		name:     name,
		writer:   writer,
		messages: messages,
		worker:   worker,
	}
}

func (d *Pipeline[T]) Start(ctx context.Context) {
	zap.S().Infof("%s pipeline started", d.name)
	defer func() { zap.S().Infof("%s pipeline closed", d.name) }()

	for msg := range d.messages {
		// TODO add retry and metrics
		if err := d.worker(ctx, msg.Event, d.writer); err != nil {
			zap.S().Warnw("failed to process message", "message", msg, "error", err)
		}

		// commit message
		close(msg.CommitCh)

		zap.S().Infow("message consumed", "pipeline", d.name, "id", msg.Event.Context.GetID(), "topic", msg.Event.Extensions()["kafkatopic"])
	}
}
