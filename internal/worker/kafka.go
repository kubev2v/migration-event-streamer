package worker

import (
	"context"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/tupyy/migration-event-streamer/internal/pipeline"
	"go.uber.org/zap"
)

func KafkaWorker(ctx context.Context, e cloudevents.Event, w pipeline.Writer[cloudevents.Event]) error {
	zap.S().Debugw("event write to topic-output", "event", e)
	return w.Write(ctx, e)
}
