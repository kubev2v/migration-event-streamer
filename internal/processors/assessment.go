package processors

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/kubev2v/migration-event-streamer/internal/entity"
	"github.com/kubev2v/migration-event-streamer/internal/pipeline"
	"github.com/kubev2v/migration-planner/api/v1alpha1"
	plannerEvents "github.com/kubev2v/migration-planner/pkg/events"
	"go.uber.org/zap"
)

func AssessmentProcessor(_ context.Context, e cloudevents.Event) (entity.AssessmentResult, error) {
	var payload plannerEvents.AssessmentEventPayload
	if err := json.Unmarshal(e.Data(), &payload); err != nil {
		zap.S().Errorw("failed to unmarshal assessment event", "error", err)
		return entity.AssessmentResult{}, err
	}

	action := pipeline.ExtractAction(e.Context.GetType())
	eventSource := e.Context.GetSource()

	zap.S().Infow("processing assessment event",
		"assessment_id", payload.Assessment.ID,
		"action", action)

	switch action {
	case entity.AssessmentActionDeleted:
		return buildDeleteResult(action, payload), nil
	case entity.AssessmentActionCreated:
		return buildCreateResult(action, payload, eventSource)
	default:
		zap.S().Infow("ignoring unknown action", "action", action)
		return entity.AssessmentResult{Action: action}, nil
	}
}

func buildDeleteResult(action string, payload plannerEvents.AssessmentEventPayload) entity.AssessmentResult {
	return entity.AssessmentResult{
		Action:    action,
		DeletedID: payload.Assessment.ID,
		DeletedAt: payload.Assessment.DeletedAt.Format(time.RFC3339),
	}
}

func buildCreateResult(action string, payload plannerEvents.AssessmentEventPayload, eventSource string) (entity.AssessmentResult, error) {
	assessment := payload.Assessment

	var inventory v1alpha1.Inventory
	if err := json.Unmarshal(assessment.Inventory, &inventory); err != nil {
		return entity.AssessmentResult{}, fmt.Errorf("failed to unmarshal inventory: %w", err)
	}

	snapshotID := fmt.Sprintf("%d", assessment.SnapshotID)
	doc := entity.NewAssessment(
		assessment.ID,
		assessment.Name,
		assessment.OrgID,
		assessment.Username,
		assessment.SourceType,
		entity.ActiveStatus,
		assessment.CreatedAt,
		snapshotID,
		inventory.VcenterId,
		eventSource,
	)
	doc.PartnerID = assessment.PartnerID

	TotalClusters := len(inventory.Clusters)
	if inventory.Vcenter != nil {
		doc.TotalClusters = &TotalClusters
		doc.TotalDatacenters = inventory.Vcenter.Infra.TotalDatacenters
		doc.TotalHosts = inventory.Vcenter.Infra.TotalHosts
		doc.TotalVMs = inventory.Vcenter.Vms.Total
		doc.TotalMigratable = inventory.Vcenter.Vms.TotalMigratable
		doc.TotalMigratableWithWarnings = inventory.Vcenter.Vms.TotalMigratableWithWarnings
		doc.TotalWithSharedDisks = inventory.Vcenter.Vms.TotalWithSharedDisks
	}

	return entity.AssessmentResult{
		Action:     action,
		Assessment: doc,
		OSEntries:  buildOSEntries(assessment, inventory, eventSource),
		Datastores: buildDatastoreEntries(assessment, inventory, eventSource),
	}, nil
}

func buildOSEntries(assessment plannerEvents.AssessmentData, inventory v1alpha1.Inventory, eventSource string) []*entity.AssessmentOS {
	osCounts := make(map[string]int)
	if inventory.Vcenter != nil && inventory.Vcenter.Vms.OsInfo != nil {
		for osType, info := range *inventory.Vcenter.Vms.OsInfo {
			osCounts[osType] += info.Count
		}
	}

	entries := make([]*entity.AssessmentOS, 0, len(osCounts))
	for osType, count := range osCounts {
		doc := entity.NewAssessmentOS(
			assessment.ID,
			fmt.Sprintf("%d", assessment.SnapshotID),
			osType,
			count,
			assessment.Username,
			assessment.OrgID,
			entity.ActiveStatus,
			assessment.CreatedAt,
			eventSource,
		)

		doc.PartnerID = assessment.PartnerID

		entries = append(entries, doc)
	}

	return entries
}

func buildDatastoreEntries(assessment plannerEvents.AssessmentData, inventory v1alpha1.Inventory, eventSource string) []*entity.AssessmentDatastore {
	var entries []*entity.AssessmentDatastore
	datastoreIndex := 0

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
				entity.ActiveStatus,
				assessment.CreatedAt,
				eventSource,
			)

			doc.PartnerID = assessment.PartnerID

			entries = append(entries, doc)
			datastoreIndex++
		}
	}

	return entries
}
