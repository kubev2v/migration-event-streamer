package pipeline

import (
	"context"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"go.uber.org/zap"
)

type RouteWriter interface {
	Write(context.Context, string, cloudevents.Event) error
}

type Router struct {
	writer RouteWriter
	input  chan chan cloudevents.Event
	// topicMap holds the routing information for each ce type.
	// key is the ce_type and value is the output topic.
	topicMap map[string]string
}

func NewRouter(input chan chan cloudevents.Event, writer RouteWriter, topicMap map[string]string) *Router {
	return &Router{
		input:    input,
		writer:   writer,
		topicMap: topicMap,
	}
}

func (r *Router) Start(ctx context.Context) {
	zap.S().Infof("router started")
	defer func() { zap.S().Info("router stopped") }()

	for c := range r.input {
		msg := <-c

		// this will commit the message.
		// For not it's fine to commit even if we don't know that we'll succeed write it.
		close(c)

		outputTopic, ok := r.topicMap[msg.Context.GetType()]
		if !ok {
			zap.S().Warnw("failed to find output topic", "ce_type", msg.Context.GetType())
			continue
		}
		if err := r.writer.Write(ctx, outputTopic, msg); err != nil {
			zap.S().Warnw("failed to write message", "message", msg, "error", err)
			continue
		}
		zap.S().Debugw("message routed", "type", msg.Context.GetType(), "output_topic", outputTopic)
	}
}
