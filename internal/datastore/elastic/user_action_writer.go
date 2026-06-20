package elastic

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/kubev2v/migration-event-streamer/internal/entity"
	"go.uber.org/zap"
)

type userActionWriter struct {
	base *baseWriter
}

func (w *userActionWriter) writeAction(ctx context.Context, id string, data []byte) error {
	return w.base.write(ctx, entity.UserActionIndex, id, data)
}

func (w *userActionWriter) WriteShareAssessment(ctx context.Context, result entity.ShareAssessmentResult) error {
	updates := map[string]any{"partner_id": result.PartnerID}
	for _, index := range []string{entity.AssessmentIndex, entity.OSIndex, entity.DatastoreIndex} {
		r, err := w.base.updateByQuery(ctx, UpdateByQueryRequest{
			Index:      index,
			MatchField: "assessment_id.keyword",
			MatchValue: result.AssessmentID,
			Updates:    updates,
		})
		if err != nil {
			return fmt.Errorf("failed to update %s partner_id: %w", index, err)
		}
		zap.S().Infow("updated partner_id", "action", "share_assessment",
			"index", index, "assessment_id", result.AssessmentID, "updated", r.Updated)
	}

	data, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("failed to marshal share assessment: %w", err)
	}
	return w.writeAction(ctx, result.ID, data)
}

func (w *userActionWriter) WriteUnshareAssessment(ctx context.Context, result entity.UnshareAssessmentResult) error {
	updates := map[string]any{"partner_id": nil}
	for _, index := range []string{entity.AssessmentIndex, entity.OSIndex, entity.DatastoreIndex} {
		r, err := w.base.updateByQuery(ctx, UpdateByQueryRequest{
			Index:      index,
			MatchField: "assessment_id.keyword",
			MatchValue: result.AssessmentID,
			Updates:    updates,
		})
		if err != nil {
			return fmt.Errorf("failed to update %s partner_id: %w", index, err)
		}
		zap.S().Infow("updated partner_id", "action", "unshare_assessment",
			"index", index, "assessment_id", result.AssessmentID, "updated", r.Updated)
	}

	data, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("failed to marshal unshare assessment: %w", err)
	}
	return w.writeAction(ctx, result.ID, data)
}

func (w *userActionWriter) WriteSizingRequested(ctx context.Context, result entity.SizingRequestedResult) error {
	data, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("failed to marshal sizing requested: %w", err)
	}
	return w.writeAction(ctx, result.ID, data)
}

func (w *userActionWriter) WriteComplexityEstimated(ctx context.Context, result entity.ComplexityEstimatedResult) error {
	data, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("failed to marshal complexity estimated: %w", err)
	}
	return w.writeAction(ctx, result.ID, data)
}

func (w *userActionWriter) WriteOVADownloaded(ctx context.Context, result entity.OVADownloadedResult) error {
	data, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("failed to marshal ova downloaded: %w", err)
	}
	return w.writeAction(ctx, result.ID, data)
}

func (w *userActionWriter) WriteVisited(ctx context.Context, result entity.VisitedResult) error {
	data, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("failed to marshal visited: %w", err)
	}
	return w.writeAction(ctx, result.ID, data)
}
