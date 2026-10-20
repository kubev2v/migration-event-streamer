package processors

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/kubev2v/migration-event-streamer/internal/entity"
	"github.com/kubev2v/migration-planner/api/v1alpha1"
	plannerEvents "github.com/kubev2v/migration-planner/pkg/events/kafka"
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

	var osEntities []entity.AssessmentOS
	var datastoreEntities []entity.AssessmentDatastore

	doc := entity.NewAssessment(
		assessment.ID,
		assessment.Name,
		assessment.OrgID,
		assessment.Username,
		assessment.SourceType,
		entity.ActiveStatus,
		assessment.CreatedAt,
		inventory.VcenterId,
	)
	doc.PartnerID = assessment.PartnerID
	doc.PartnerName = assessment.PartnerName
	orgName := normalizeAssessmentOrgName(assessment.OrgName)
	doc.OrgName = orgName
	doc.OrgType = assessment.OrgType

	TotalClusters := len(inventory.Clusters)
	if inventory.Vcenter != nil {
		doc.TotalClusters = &TotalClusters
		doc.TotalDatacenters = inventory.Vcenter.Infra.TotalDatacenters
		doc.TotalHosts = inventory.Vcenter.Infra.TotalHosts
		doc.TotalVMs = inventory.Vcenter.Vms.Total
		doc.TotalMigratable = inventory.Vcenter.Vms.TotalMigratable
		doc.TotalMigratableWithWarnings = inventory.Vcenter.Vms.TotalMigratableWithWarnings
		doc.TotalWithSharedDisks = inventory.Vcenter.Vms.TotalWithSharedDisks
		datastoreEntities = buildDatastoreEntries(doc, inventory.Vcenter.Infra.Datastores)
	}

	if inventory.Vcenter != nil && inventory.Vcenter.Vms.OsInfo != nil {
		osEntities = buildOSEntries(doc, *inventory.Vcenter.Vms.OsInfo)
	}

	return entity.AssessmentCreatedResult{
		Assessment: *doc,
		OSEntries:  osEntities,
		Datastores: datastoreEntities,
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

func buildOSEntries(assessment *entity.Assessment, osInfo map[string]v1alpha1.OsInfo) []entity.AssessmentOS {
	entries := make([]entity.AssessmentOS, 0, len(osInfo))

	for osType, info := range osInfo {
		doc := entity.NewAssessmentOS(
			assessment.AssessmentID,
			assessment.VCenterID,
			osType,
			info.Count,
			assessment.Username,
			assessment.OrgID,
			entity.ActiveStatus,
			assessment.CreatedAt,
		)

		doc.PartnerID = assessment.PartnerID
		doc.OrgName = assessment.OrgName
		doc.OrgType = assessment.OrgType

		entries = append(entries, *doc)
	}

	return entries
}

func buildDatastoreEntries(assessment *entity.Assessment, datastores []v1alpha1.Datastore) []entity.AssessmentDatastore {
	var entries []entity.AssessmentDatastore
	datastoreIndex := 0

	for _, datastore := range datastores {
		doc := entity.NewAssessmentDatastore(
			assessment.AssessmentID,
			assessment.VCenterID,
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
		doc.OrgName = assessment.OrgName
		doc.OrgType = assessment.OrgType

		entries = append(entries, *doc)
		datastoreIndex++
	}

	return entries
}

func normalizeAssessmentOrgName(orgName string) string {
	lowerName := strings.ToLower(orgName)
	if strings.Contains(lowerName, "redhat") || strings.Contains(lowerName, "red hat") {
		return "Red Hat"
	}
	return orgName
}
