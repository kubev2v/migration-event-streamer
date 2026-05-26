package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/kubev2v/migration-event-streamer/internal/datastore/elastic"
	"github.com/kubev2v/migration-event-streamer/internal/entity"
	"github.com/kubev2v/migration-event-streamer/internal/pipeline"
	plannerEvents "github.com/kubev2v/migration-planner/pkg/events"
	"go.uber.org/zap"
)

// UserActionWorker processes user_action CloudEvents and writes to Elasticsearch
func UserActionWorker(ctx context.Context, e cloudevents.Event, w pipeline.ElasticWriter) error {
	var payload plannerEvents.UserActionEventPayload
	if err := json.Unmarshal(e.Data(), &payload); err != nil {
		zap.S().Errorw("failed to unmarshal user_action event", "error", err)
		return err
	}

	action := payload.UserAction

	zap.S().Infow("processing user_action event",
		"username", action.Username,
		"action_type", action.ActionType,
		"assessment_id", action.AssessmentID)

	// update assessments for share/unshare events
	if action.ActionType == plannerEvents.ActionTypeShareAssessment || action.ActionType == plannerEvents.ActionTypeUnshareAssessment {
		if err := handleAssessmentPartnerUpdate(ctx, w, *action.AssessmentID, action.PartnerID, action.ActionType); err != nil {
			return fmt.Errorf("failed to handle %s action: %w", action.ActionType, err)
		}
	}

	// Write the user action document
	doc := entity.NewUserAction(
		action.Username,
		action.AssessmentID,
		action.SourceID,
		action.PartnerID,
		action.ActionType,
		action.Timestamp,
	)

	data, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("failed to marshal user_action document: %w", err)
	}

	return w.Overwrite(ctx, entity.Event{
		Index: entity.UserActionIndex,
		ID:    doc.ID,
		Body:  bytes.NewReader(data),
	})
}

// handleAssessmentPartnerUpdate updates partner_id on assessment, OS, and datastore documents
func handleAssessmentPartnerUpdate(ctx context.Context, w pipeline.ElasticWriter, assessmentID string, partnerID *string, actionType string) error {
	var updates map[string]interface{}

	if actionType == plannerEvents.ActionTypeShareAssessment {
		updates = map[string]interface{}{
			"partner_id": *partnerID,
		}
	} else { // unshare
		updates = map[string]interface{}{
			"partner_id": nil,
		}
	}

	// Update assessment document
	assessmentReq := elastic.UpdateByQueryRequest{
		Index:      entity.AssessmentIndex,
		MatchField: "id.keyword",
		MatchValue: assessmentID,
		Updates:    updates,
	}

	assessmentResult, err := w.UpdateByQuery(ctx, assessmentReq)
	if err != nil {
		return fmt.Errorf("failed to update assessment partner_id: %w", err)
	}
	zap.S().Infow("updated assessment partner_id",
		"action", actionType,
		"assessment_id", assessmentID,
		"updated", assessmentResult.Updated)

	// Update all OS documents for this assessment
	osReq := elastic.UpdateByQueryRequest{
		Index:      entity.OSIndex,
		MatchField: "assessment_id.keyword",
		MatchValue: assessmentID,
		Updates:    updates,
	}

	osResult, err := w.UpdateByQuery(ctx, osReq)
	if err != nil {
		return fmt.Errorf("failed to update OS documents partner_id: %w", err)
	}
	zap.S().Infow("updated OS documents partner_id",
		"action", actionType,
		"assessment_id", assessmentID,
		"updated", osResult.Updated)

	// Update all Datastore documents for this assessment
	datastoreReq := elastic.UpdateByQueryRequest{
		Index:      entity.DatastoreIndex,
		MatchField: "assessment_id.keyword",
		MatchValue: assessmentID,
		Updates:    updates,
	}

	datastoreResult, err := w.UpdateByQuery(ctx, datastoreReq)
	if err != nil {
		return fmt.Errorf("failed to update datastore documents partner_id: %w", err)
	}
	zap.S().Infow("updated datastore documents partner_id",
		"action", actionType,
		"assessment_id", assessmentID,
		"updated", datastoreResult.Updated)

	return nil
}
