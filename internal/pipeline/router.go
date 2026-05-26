package pipeline

import (
	"context"
	"encoding/json"

	"github.com/kubev2v/migration-event-streamer/internal/entity"
	"github.com/kubev2v/migration-event-streamer/internal/metrics"
	"go.uber.org/zap"
)

const plannerTopic = "assisted.migrations.events"

type RouteWriter interface {
	Write(ctx context.Context, topic string, data []byte) error
}

type Router struct {
	writer RouteWriter
	input  chan entity.Message
	// routes holds the routing information for each ce type.
	// key is the ce_type and value is the topic.
	routes map[string]string
}

func NewRouter(input chan entity.Message, writer RouteWriter, routes map[string]string) *Router {
	return &Router{
		input:  input,
		writer: writer,
		routes: routes,
	}
}

func (r *Router) Start(ctx context.Context) {
	zap.S().Infow("router started", "routes", r.routes)
	defer func() { zap.S().Info("router stopped") }()

	for msg := range r.input {
		ceType := msg.Event.Context.GetType()
		metrics.IncreaseMessagesCount(plannerTopic)

		topic, ok := r.routes[ceType]
		if !ok {
			zap.S().Warnw("failed to find output topic", "event_type", ceType)
			close(msg.CommitCh)
			continue
		}

		data, err := json.Marshal(msg.Event)
		if err != nil {
			zap.S().Warnw("failed to marshal event", "event_type", ceType, "error", err)
			close(msg.CommitCh)
			continue
		}

		if err := r.writer.Write(ctx, topic, data); err != nil {
			zap.S().Warnw("failed to write message", "event_type", ceType, "error", err)
			close(msg.CommitCh)
			continue
		}

		zap.S().Infow("message routed", "type", ceType, "topic", topic)
		close(msg.CommitCh)
	}
}
