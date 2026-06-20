package pipeline

import (
	"context"
	"encoding/json"

	"github.com/kubev2v/migration-event-streamer/internal/entity"
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

func (r *Router) Start(ctx context.Context) {
	go func() {
		zap.S().Info("router started")
		defer func() { zap.S().Info("router stopped") }()

		for msg := range r.input {
			namespace := msg.Event.Context.GetSource()
			ceType := msg.Event.Context.GetType()

			topic := namespace + ".events"

			data, err := json.Marshal(msg.Event)
			if err != nil {
				zap.S().Warnw("failed to marshal event", "event_type", ceType, "error", err)
				close(msg.CommitCh)
				continue
			}

			if err := r.writer.Write(ctx, topic, data); err != nil {
				zap.S().Warnw("failed to write message", "event_type", ceType, "topic", topic, "error", err)
				close(msg.CommitCh)
				continue
			}

			zap.S().Infow("message routed", "type", ceType, "namespace", namespace, "topic", topic)
			close(msg.CommitCh)
		}
	}()
}
