package elastic

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/kubev2v/migration-event-streamer/internal/entity"
	"go.uber.org/zap"
)

type assessmentWriter struct {
	base *baseWriter
}

func (w *assessmentWriter) WriteCreated(ctx context.Context, result entity.AssessmentCreatedResult) error {
	data, err := json.Marshal(result.Assessment)
	if err != nil {
		return fmt.Errorf("failed to marshal assessment: %w", err)
	}
	if err := w.base.write(ctx, result.Assessment.Index, result.Assessment.AssessmentID, data); err != nil {
		return fmt.Errorf("failed to write assessment document: %w", err)
	}

	for _, os := range result.OSEntries {
		d, err := json.Marshal(os)
		if err != nil {
			zap.S().Warnw("failed to marshal OS document", "os_type", os.OSType, "error", err)
			continue
		}
		if err := w.base.write(ctx, os.Index, os.ID, d); err != nil {
			zap.S().Warnw("failed to write OS document", "os_type", os.OSType, "error", err)
		}
	}

	for _, ds := range result.Datastores {
		d, err := json.Marshal(ds)
		if err != nil {
			zap.S().Warnw("failed to marshal datastore document", "datastore_index", ds.DatastoreIndex, "error", err)
			continue
		}
		if err := w.base.write(ctx, ds.Index, ds.ID, d); err != nil {
			zap.S().Warnw("failed to write datastore document", "datastore_index", ds.DatastoreIndex, "error", err)
		}
	}

	return nil
}

func (w *assessmentWriter) WriteCascadeDelete(ctx context.Context, result entity.AssessmentDeletedResult) error {
	assessmentID := result.DeletedID
	deletedAt := result.DeletedAt

	updates := map[string]any{
		"status":     entity.DeletedStatus,
		"deleted_at": deletedAt,
	}

	data, err := json.Marshal(updates)
	if err != nil {
		return err
	}
	if err := w.base.upsert(ctx, entity.AssessmentIndex, assessmentID, data); err != nil {
		return err
	}

	osResult, err := w.base.updateByQuery(ctx, UpdateByQueryRequest{
		Index:      entity.OSIndex,
		MatchField: "assessment_id.keyword",
		MatchValue: assessmentID,
		Updates:    updates,
	})
	if err != nil {
		return fmt.Errorf("failed to update OS documents: %w", err)
	}
	zap.S().Infow("cascade delete OS documents", "assessment_id", assessmentID,
		"updated", osResult.Updated, "failed", osResult.Failed)

	datastoreResult, err := w.base.updateByQuery(ctx, UpdateByQueryRequest{
		Index:      entity.DatastoreIndex,
		MatchField: "assessment_id.keyword",
		MatchValue: assessmentID,
		Updates:    updates,
	})
	if err != nil {
		return fmt.Errorf("failed to update datastore documents: %w", err)
	}
	zap.S().Infow("cascade delete datastore documents", "assessment_id", assessmentID,
		"updated", datastoreResult.Updated, "failed", datastoreResult.Failed)

	return nil
}
