package pipeline

import (
	"context"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/kubev2v/migration-event-streamer/internal/datastore/elastic"
	"github.com/kubev2v/migration-event-streamer/internal/entity"
	"github.com/kubev2v/migration-event-streamer/internal/metrics"
	"go.uber.org/zap"
)

type Worker func(ctx context.Context, message cloudevents.Event, writer ElasticWriter) error

type ElasticWriter interface {
	Overwrite(context.Context, entity.Event) error
	Upsert(context.Context, entity.Event) error
	UpdateByQuery(context.Context, elastic.UpdateByQueryRequest) (*elastic.UpdateByQueryResult, error)
}

type Pipeline struct {
	name     string
	worker   Worker
	writer   ElasticWriter
	messages chan entity.Message
}

func NewPipeline(name string, messages chan entity.Message, writer ElasticWriter, worker Worker) *Pipeline {
	return &Pipeline{
		name:     name,
		writer:   writer,
		messages: messages,
		worker:   worker,
	}
}

func (d *Pipeline) WithRetry() *Pipeline {
	// wrap the original worker with a retry function
	previous := d.worker
	d.worker = func(ctx context.Context, message cloudevents.Event, writer ElasticWriter) error {
		// TODO add retry function
		return previous(ctx, message, writer)
	}
	return d
}

func (d *Pipeline) WithObservability() *Pipeline {
	previousWorker := d.worker
	d.worker = func(ctx context.Context, message cloudevents.Event, writer ElasticWriter) error {
		// get timestamp
		startTimestamp := message.Context.GetTime()
		metrics.IncreaseMessagesCount(message.Context.GetType())
		err := previousWorker(ctx, message, writer)
		if err != nil {
			metrics.IncreaseErrorProcessingCount(message.Context.GetType())
			return err
		}
		metrics.IncreaseProcessedMessagesCount(message.Context.GetType())
		metrics.UpdateProcessingMetric(message.Context.GetType(), time.Since(startTimestamp))
		return nil
	}
	return d
}

func (d *Pipeline) Start(ctx context.Context) {
	zap.S().Infof("%s pipeline started", d.name)
	defer func() { zap.S().Infof("%s pipeline closed", d.name) }()

	for msg := range d.messages {
		// TODO add retry and metrics
		if err := d.worker(ctx, msg.Event, d.writer); err != nil {
			zap.S().Warnw("failed to process message", "message", msg, "error", err)
		}

		// commit message
		close(msg.CommitCh)

		zap.S().Infow("message consumed", "pipeline", d.name, "id", msg.Event.Context.GetID(), "type", msg.Event.Context.GetType(), "topic", msg.Event.Extensions()["kafkatopic"])
	}
}
