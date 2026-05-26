package processors

import (
	"context"
	"encoding/json"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/kubev2v/migration-event-streamer/internal/entity"
	"github.com/kubev2v/migration-event-streamer/internal/pipeline"
	plannerEvents "github.com/kubev2v/migration-planner/pkg/events"
	"go.uber.org/zap"
)

func UserActionProcessor(_ context.Context, e cloudevents.Event) (*entity.UserAction, error) {
	var payload plannerEvents.UserActionEventPayload
	if err := json.Unmarshal(e.Data(), &payload); err != nil {
		zap.S().Errorw("failed to unmarshal user_action event", "error", err)
		return nil, err
	}

	action := payload.UserAction
	actionType := pipeline.ExtractAction(e.Context.GetType())

	zap.S().Infow("processing user_action event",
		"username", action.Username,
		"action_type", actionType,
		"assessment_id", action.AssessmentID)

	return entity.NewUserAction(
		action.Username,
		action.AssessmentID,
		action.SourceID,
		action.PartnerID,
		actionType,
		action.Timestamp,
		e.Context.GetSource(),
	), nil
}
