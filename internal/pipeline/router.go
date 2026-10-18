package pipeline

import (
	"context"
	"encoding/json"

	"github.com/kubev2v/migration-event-streamer/internal/entity"
	"github.com/kubev2v/migration-event-streamer/internal/metrics"
	"go.uber.org/zap"
)

type RouteWriter interface {
	Write(ctx context.Context, topic string, data []byte) error
}

type Router struct {
	writer RouteWriter
	input  chan entity.Message
}

func NewRouter(input chan entity.Message, writer RouteWriter) *Router {
	return &Router{
		writer: writer,
		input:  input,
	}
}

func (r *Router) Start(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		zap.S().Info("router started")
		defer func() { zap.S().Info("router stopped") }()

		for {
			select {
			case msg, ok := <-r.input:
				if !ok {
					return
				}
				r.route(ctx, msg)
			case <-ctx.Done():
				return
			}
		}
	}()
	return done
}

func (r *Router) route(ctx context.Context, msg entity.Message) {
	defer close(msg.CommitCh)
	ceType := msg.Event.Context.GetType()
	finish := metrics.Begin(metrics.StageRouter, ExtractEntityAction(ceType))
	outcome := metrics.Failed
	defer func() { finish(outcome) }()

	namespace := msg.Event.Context.GetSource()
	topic := namespace + ".events"
	data, err := json.Marshal(msg.Event)
	if err != nil {
		outcome = metrics.Invalid
		zap.S().Warnw("failed to marshal event", "event_type", ceType, "error", err)
		return
	}
	if err := r.writer.Write(ctx, topic, data); err != nil {
		if ctx.Err() != nil {
			outcome = metrics.Canceled
		}
		zap.S().Warnw("failed to write message", "event_type", ceType, "topic", topic, "error", err)
		return
	}
	outcome = metrics.Success
	zap.S().Infow("message routed", "type", ceType, "namespace", namespace, "topic", topic)
}
