package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/kubev2v/migration-event-streamer/internal/datastore/elastic"
	"github.com/kubev2v/migration-event-streamer/internal/entity"
	"github.com/kubev2v/migration-event-streamer/internal/pipeline"
	"github.com/kubev2v/migration-planner/api/v1alpha1"
	plannerEvents "github.com/kubev2v/migration-planner/pkg/events"
	"go.uber.org/zap"
)

// AssessmentWorker processes assessment CloudEvents and writes to Elasticsearch
func AssessmentWorker(ctx context.Context, e cloudevents.Event, w pipeline.ElasticWriter) error {
	var payload plannerEvents.AssessmentEventPayload
	if err := json.Unmarshal(e.Data(), &payload); err != nil {
		zap.S().Errorw("failed to unmarshal assessment event", "error", err)
		return err
	}

	zap.S().Infow("processing assessment event",
		"assessment_id", payload.Assessment.ID,
		"action", payload.Action)

	switch payload.Action {
	case plannerEvents.ActionAssessmentDeleted:
		if err := cascadeDeleteAssessment(ctx, w, payload.Assessment.ID, *payload.Assessment.DeletedAt); err != nil {
			return fmt.Errorf("failed to cascade delete assessment: %w", err)
		}
	case plannerEvents.ActionAssessmentCreated:
		if err := writeAssessmentDocument(ctx, w, payload); err != nil {
			return fmt.Errorf("failed to write assessment document: %w", err)
		}

		// process inventory to update OS/Datastore documents
		if err := processInventory(ctx, w, payload); err != nil {
			return fmt.Errorf("failed to process inventory: %w", err)
		}
	default:
		zap.S().Infow("ignoring unknown action", "action", payload.Action)
	}

	return nil
}

// writeAssessmentDocument creates and writes the assessment document to Elasticsearch
func writeAssessmentDocument(ctx context.Context, w pipeline.ElasticWriter, payload plannerEvents.AssessmentEventPayload) error {
	assessment := payload.Assessment

	doc := entity.NewAssessment(
		assessment.ID,
		assessment.Name,
		assessment.OrgID,
		assessment.Username,
		assessment.SourceType,
		entity.ActiveStatus,
		assessment.CreatedAt,
	)

	doc.PartnerID = assessment.PartnerID
	doc.Location = assessment.Location

	if assessment.UpdatedAt != nil {
		updatedAt := assessment.UpdatedAt.Format(time.RFC3339)
		doc.UpdatedAt = &updatedAt
	}

	if assessment.DeletedAt != nil {
		deletedAt := assessment.DeletedAt.Format(time.RFC3339)
		doc.DeletedAt = &deletedAt
	}

	data, err := json.Marshal(doc)
	if err != nil {
		return err
	}

	return w.Overwrite(ctx, entity.Event{
		Index: entity.AssessmentIndex,
		ID:    doc.ID,
		Body:  bytes.NewReader(data),
	})
}

// processInventory parses the inventory JSON and creates OS and Datastore documents
func processInventory(ctx context.Context, w pipeline.ElasticWriter, payload plannerEvents.AssessmentEventPayload) error {
	assessment := payload.Assessment

	if len(assessment.Inventory) == 0 {
		zap.S().Warnw("no inventory provided for assessment", "assessment_id", assessment.ID)
		return nil
	}

	var inventory v1alpha1.Inventory
	if err := json.Unmarshal(assessment.Inventory, &inventory); err != nil {
		return fmt.Errorf("failed to unmarshal inventory: %w", err)
	}

	// Process OS distribution
	if err := processOSDistribution(ctx, w, assessment, entity.ActiveStatus, inventory); err != nil {
		zap.S().Warnw("failed to process OS distribution", "assessment_id", assessment.ID, "error", err)
	}

	// Process Datastores
	if err := processDatastores(ctx, w, assessment, entity.ActiveStatus, inventory); err != nil {
		zap.S().Warnw("failed to process datastores", "assessment_id", assessment.ID, "error", err)
	}

	return nil
}

// processOSDistribution extracts OS data from inventory and writes to Elasticsearch
func processOSDistribution(ctx context.Context, w pipeline.ElasticWriter, assessment plannerEvents.AssessmentData, status string, inventory v1alpha1.Inventory) error {

	osCounts := make(map[string]int)

	if inventory.Vcenter.Vms.OsInfo != nil {
		for osType, info := range *inventory.Vcenter.Vms.OsInfo {
			osCounts[osType] += info.Count
		}
	}

	// Create a document for each OS type
	for osType, count := range osCounts {
		doc := entity.NewAssessmentOS(
			assessment.ID,
			fmt.Sprintf("%d", assessment.SnapshotID),
			osType,
			count,
			assessment.Username,
			assessment.OrgID,
			status,
			assessment.CreatedAt,
		)

		doc.PartnerID = assessment.PartnerID
		doc.Location = assessment.Location

		if assessment.DeletedAt != nil {
			deletedAt := assessment.DeletedAt.Format(time.RFC3339)
			doc.DeletedAt = &deletedAt
		}

		data, err := json.Marshal(doc)
		if err != nil {
			zap.S().Warnw("failed to marshal OS document", "os_type", osType, "error", err)
			continue
		}

		if err := w.Overwrite(ctx, entity.Event{
			Index: entity.OSIndex,
			ID:    doc.ID,
			Body:  bytes.NewReader(data),
		}); err != nil {
			zap.S().Warnw("failed to write OS document", "os_type", osType, "error", err)
		}
	}

	return nil
}

// processDatastores extracts datastore data from inventory and writes to Elasticsearch
func processDatastores(ctx context.Context, w pipeline.ElasticWriter, assessment plannerEvents.AssessmentData, status string, inventory v1alpha1.Inventory) error {
	datastoreIndex := 0

	// Extract datastores from all clusters
	for _, cluster := range inventory.Clusters {
		for _, datastore := range cluster.Infra.Datastores {
			doc := entity.NewAssessmentDatastore(
				assessment.ID,
				fmt.Sprintf("%d", assessment.SnapshotID),
				datastoreIndex,
				datastore.Type,
				datastore.TotalCapacityGB,
				datastore.FreeCapacityGB,
				assessment.Username,
				assessment.OrgID,
				status,
				assessment.CreatedAt,
			)

			doc.PartnerID = assessment.PartnerID
			doc.Location = assessment.Location

			if assessment.DeletedAt != nil {
				deletedAt := assessment.DeletedAt.Format(time.RFC3339)
				doc.DeletedAt = &deletedAt
			}

			data, err := json.Marshal(doc)
			if err != nil {
				zap.S().Warnw("failed to marshal datastore document", "datastore_index", datastoreIndex, "error", err)
				continue
			}

			if err := w.Overwrite(ctx, entity.Event{
				Index: entity.DatastoreIndex,
				ID:    doc.ID,
				Body:  bytes.NewReader(data),
			}); err != nil {
				zap.S().Warnw("failed to write datastore document", "datastore_index", datastoreIndex, "error", err)
			}

			datastoreIndex++
		}
	}

	return nil
}

// cascadeDeleteAssessment marks assessment, OS, and Datastore documents as deleted
func cascadeDeleteAssessment(ctx context.Context, w pipeline.ElasticWriter, assessmentID string, deletedAt time.Time) error {
	updates := map[string]interface{}{
		"status":     entity.DeletedStatus,
		"deleted_at": deletedAt.Format(time.RFC3339),
	}

	// Mark assessment as deleted
	data, err := json.Marshal(updates)
	if err != nil {
		return err
	}

	if err := w.Upsert(ctx, entity.Event{
		Index: entity.AssessmentIndex,
		ID:    assessmentID,
		Body:  bytes.NewReader(data),
	}); err != nil {
		return err
	}

	// Update all OS documents for this assessment
	osReq := elastic.UpdateByQueryRequest{
		Index:      entity.OSIndex,
		MatchField: "assessment_id.keyword",
		MatchValue: assessmentID,
		Updates:    updates,
	}

	osResult, err := w.UpdateByQuery(ctx, osReq)
	if err != nil {
		return fmt.Errorf("failed to update OS documents: %w", err)
	}
	zap.S().Warnw("cascade delete OS documents", "assessment_id", assessmentID,
		"updated", osResult.Updated,
		"failed", osResult.Failed)

	// Update all Datastore documents for this assessment
	datastoreReq := elastic.UpdateByQueryRequest{
		Index:      entity.DatastoreIndex,
		MatchField: "assessment_id.keyword",
		MatchValue: assessmentID,
		Updates:    updates,
	}

	datastoreResult, err := w.UpdateByQuery(ctx, datastoreReq)
	if err != nil {
		return fmt.Errorf("failed to update datastore documents: %w", err)
	}
	zap.S().Warnw("cascade delete datastore documents", "assessment_id", assessmentID,
		"updated", datastoreResult.Updated,
		"failed", datastoreResult.Failed)

	return nil
}
