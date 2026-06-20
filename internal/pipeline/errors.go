package pipeline

import (
	"context"

	"github.com/kubev2v/migration-event-streamer/internal/entity"
	"go.uber.org/zap"
)

type ErrorHandler interface {
	Start(ctx context.Context)
}

// TODO: implement a persistent error handler that writes failed events to S3
// as CloudEvent JSON bytes for later replay or investigation.
type LogErrorHandler struct {
	errors <-chan entity.PipelineError
}

func NewLogErrorHandler(errors <-chan entity.PipelineError) *LogErrorHandler {
	return &LogErrorHandler{errors: errors}
}

func (h *LogErrorHandler) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case pe, ok := <-h.errors:
				if !ok {
					return
				}
				zap.S().Errorw("pipeline error", "pipeline", pe.Pipeline, "error", pe.Err)
				close(pe.Ack)
			case <-ctx.Done():
				return
			}
		}
	}()
}
