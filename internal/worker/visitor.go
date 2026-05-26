package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/kubev2v/migration-event-streamer/internal/entity"
	"github.com/kubev2v/migration-event-streamer/internal/pipeline"
	plannerEvents "github.com/kubev2v/migration-planner/pkg/events"
	"go.uber.org/zap"
)

// VisitorWorker processes visitor CloudEvents and writes to Elasticsearch
func VisitorWorker(ctx context.Context, e cloudevents.Event, w pipeline.ElasticWriter) error {
	var payload plannerEvents.VisitorEventPayload
	if err := json.Unmarshal(e.Data(), &payload); err != nil {
		zap.S().Errorw("failed to unmarshal visitor event", "error", err)
		return err
	}

	visitor := payload.Visitor

	zap.S().Infow("processing visitor event",
		"username", visitor.Username,
		"org_id", visitor.OrgID)

	doc := entity.NewVisitor(
		visitor.Username,
		visitor.OrgID,
		visitor.Timestamp,
	)

	data, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("failed to marshal visitor document: %w", err)
	}

	return w.Overwrite(ctx, entity.Event{
		Index: entity.VisitorIndex,
		ID:    doc.ID,
		Body:  bytes.NewReader(data),
	})
}
