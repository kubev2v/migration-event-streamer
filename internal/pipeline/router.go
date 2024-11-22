package pipeline

import (
	"context"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/tupyy/migration-event-streamer/internal/entity"
	"go.uber.org/zap"
)

type RouteWriter interface {
	Write(context.Context, string, cloudevents.Event) error
}

type Router struct {
	writer RouteWriter
	input  chan entity.Message
	// topicMap holds the routing information for each ce type.
	// key is the ce_type and value is the output topic.
	topicMap map[string]string
}

func NewRouter(input chan entity.Message, writer RouteWriter, topicMap map[string]string) *Router {
	return &Router{
		input:    input,
		writer:   writer,
		topicMap: topicMap,
	}
}

func (r *Router) Start(ctx context.Context) {
	zap.S().Infof("router started")
	defer func() { zap.S().Info("router stopped") }()

	for msg := range r.input {
		outputTopic, ok := r.topicMap[msg.Event.Context.GetType()]
		if !ok {
			zap.S().Warnw("failed to find output topic", "ce_type", msg.Event.Context.GetType())
			close(msg.CommitCh)
			continue
		}

		if err := r.writer.Write(ctx, outputTopic, msg.Event); err != nil {
			zap.S().Warnw("failed to write message", "message", msg, "error", err)
			close(msg.CommitCh)
			continue
		}

		zap.S().Infow("message routed", "type", msg.Event.Context.GetType(), "topic", outputTopic)
		close(msg.CommitCh)
	}
}
