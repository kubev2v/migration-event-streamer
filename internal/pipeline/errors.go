package pipeline

import (
	"context"

	"go.uber.org/zap"
)

type PipelineError struct {
	Pipeline string
	Err      error
}

type ErrorHandler interface {
	Start(ctx context.Context)
}

// TODO: implement a persistent error handler that writes failed events to S3
// as CloudEvent JSON bytes for later replay or investigation.
type LogErrorHandler struct {
	errors <-chan PipelineError
}

func NewLogErrorHandler(errors <-chan PipelineError) *LogErrorHandler {
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
			case <-ctx.Done():
				return
			}
		}
	}()
}
