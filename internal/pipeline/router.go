package pipeline

import (
	"context"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/kubev2v/migration-event-streamer/internal/entity"
	"github.com/kubev2v/migration-event-streamer/internal/metrics"
	"go.uber.org/zap"
)

type RouteWriter interface {
	Write(context.Context, string, cloudevents.Event) error
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
		// count the messages received on the input topic
		metrics.IncreaseMessagesCount("assisted.migrations.events")

		topic, ok := r.routes[msg.Event.Context.GetType()]
		if !ok {
			zap.S().Warnw("failed to find output topic", "event_type", msg.Event.Context.GetType())
			close(msg.CommitCh)
			continue
		}

		if err := r.writer.Write(ctx, topic, msg.Event); err != nil {
			zap.S().Warnw("failed to write message", "message", msg, "error", err)
			close(msg.CommitCh)
			continue
		}

		zap.S().Infow("message routed", "type", msg.Event.Context.GetType(), "topic", topic)
		close(msg.CommitCh)
	}
}
