package processors

import (
	"context"
	"encoding/json"
	"fmt"

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

	dataBytes, err := json.Marshal(action.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal action data: %w", err)
	}

	var assessmentID, sourceID, partnerID *string

	switch actionType {
	case entity.UserActionShareAssessment:
		var data plannerEvents.ShareAssessmentActionData
		if err := json.Unmarshal(dataBytes, &data); err != nil {
			return nil, fmt.Errorf("failed to unmarshal share assessment data: %w", err)
		}
		assessmentID = &data.AssessmentID
		partnerID = &data.PartnerID

	case entity.UserActionUnshareAssessment:
		var data plannerEvents.UnshareAssessmentActionData
		if err := json.Unmarshal(dataBytes, &data); err != nil {
			return nil, fmt.Errorf("failed to unmarshal unshare assessment data: %w", err)
		}
		assessmentID = &data.AssessmentID

	case entity.UserActionSizingRequested:
		var data plannerEvents.SizingActionData
		if err := json.Unmarshal(dataBytes, &data); err != nil {
			return nil, fmt.Errorf("failed to unmarshal sizing data: %w", err)
		}
		assessmentID = &data.AssessmentID

	case entity.UserActionComplexityEstimated:
		var data plannerEvents.ComplexityActionData
		if err := json.Unmarshal(dataBytes, &data); err != nil {
			return nil, fmt.Errorf("failed to unmarshal complexity data: %w", err)
		}
		assessmentID = &data.AssessmentID

	case entity.UserActionOVADownloaded:
		var data plannerEvents.OVADownloadActionData
		if err := json.Unmarshal(dataBytes, &data); err != nil {
			return nil, fmt.Errorf("failed to unmarshal ova download data: %w", err)
		}
		sourceID = &data.SourceID

	default:
		return nil, fmt.Errorf("unknown user_action type: %s", actionType)
	}

	zap.S().Infow("processing user_action event",
		"username", action.Username,
		"action_type", actionType)

	return entity.NewUserAction(
		action.Username,
		assessmentID,
		sourceID,
		partnerID,
		actionType,
		action.Timestamp,
		e.Context.GetSource(),
	), nil
}
