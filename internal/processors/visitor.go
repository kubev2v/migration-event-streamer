package processors

import (
	"context"
	"encoding/json"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/kubev2v/migration-event-streamer/internal/entity"
	plannerEvents "github.com/kubev2v/migration-planner/pkg/events"
	"go.uber.org/zap"
)

func VisitorProcessor(_ context.Context, e cloudevents.Event) (*entity.Visitor, error) {
	var payload plannerEvents.VisitorEventPayload
	if err := json.Unmarshal(e.Data(), &payload); err != nil {
		zap.S().Errorw("failed to unmarshal visitor event", "error", err)
		return nil, err
	}

	visitor := payload.Visitor

	zap.S().Infow("processing visitor event",
		"username", visitor.Username,
		"org_id", visitor.OrgID)

	return entity.NewVisitor(
		visitor.Username,
		visitor.OrgID,
		visitor.Timestamp,
		e.Context.GetSource(),
	), nil
}
