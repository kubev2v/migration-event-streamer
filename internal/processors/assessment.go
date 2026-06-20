package processors

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kubev2v/migration-event-streamer/internal/entity"
	"github.com/kubev2v/migration-planner/api/v1alpha1"
	plannerEvents "github.com/kubev2v/migration-planner/pkg/events"
	"go.uber.org/zap"
)

func AssessmentCreatedProcessor(_ context.Context, event plannerEvents.AssessmentEventPayload) (entity.AssessmentCreatedResult, error) {
	assessment := event.Assessment

	zap.S().Infow("processing assessment created event",
		"assessment_id", assessment.ID)
	var inventory v1alpha1.Inventory
	if err := json.Unmarshal(assessment.Inventory, &inventory); err != nil {
		return entity.AssessmentCreatedResult{}, fmt.Errorf("failed to unmarshal inventory: %w", err)
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

	return entity.AssessmentCreatedResult{
		Assessment: *doc,
		OSEntries:  buildOSEntries(assessment, inventory),
		Datastores: buildDatastoreEntries(assessment, inventory),
	}, nil
}

func AssessmentDeletedProcessor(_ context.Context, event plannerEvents.AssessmentEventPayload) (entity.AssessmentDeletedResult, error) {
	zap.S().Infow("processing assessment deleted event",
		"assessment_id", event.Assessment.ID)

	return entity.AssessmentDeletedResult{
		DeletedID: event.Assessment.ID,
		DeletedAt: event.Assessment.DeletedAt.Format(time.RFC3339),
	}, nil
}

func buildOSEntries(assessment plannerEvents.AssessmentData, inventory v1alpha1.Inventory) []entity.AssessmentOS {
	osCounts := make(map[string]int)
	if inventory.Vcenter != nil && inventory.Vcenter.Vms.OsInfo != nil {
		for osType, info := range *inventory.Vcenter.Vms.OsInfo {
			osCounts[osType] += info.Count
		}
	}

	entries := make([]entity.AssessmentOS, 0, len(osCounts))
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
		)

		doc.PartnerID = assessment.PartnerID

		entries = append(entries, *doc)
	}

	return entries
}

func buildDatastoreEntries(assessment plannerEvents.AssessmentData, inventory v1alpha1.Inventory) []entity.AssessmentDatastore {
	var entries []entity.AssessmentDatastore
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
			)

			doc.PartnerID = assessment.PartnerID

			entries = append(entries, *doc)
			datastoreIndex++
		}
	}

	return entries
}
