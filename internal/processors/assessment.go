package processors

import (
	"context"
	"strings"
	"time"

	"github.com/kubev2v/migration-event-streamer/internal/entity"
	"github.com/kubev2v/migration-event-streamer/pkg/events"
	"go.uber.org/zap"
)

func AssessmentCreatedProcessor(_ context.Context, event events.AssessmentEventPayload) (entity.AssessmentCreatedResult, error) {
	assessment := event.Assessment

	zap.S().Infow("processing assessment created event",
		"assessment_id", assessment.ID)

	var osEntities []entity.AssessmentOS
	var datastoreEntities []entity.AssessmentDatastore

	inventory := assessment.MainInventory

	doc := entity.NewAssessment(
		assessment.ID,
		assessment.Name,
		assessment.OrgID,
		assessment.Username,
		assessment.SourceType,
		entity.ActiveStatus,
		assessment.CreatedAt,
		assessment.MainInventory.Id,
	)
	doc.PartnerID = assessment.PartnerID
	doc.PartnerName = assessment.PartnerName
	orgName := normalizeAssessmentOrgName(assessment.OrgName)
	doc.OrgName = orgName
	doc.OrgType = assessment.OrgType

	doc.TotalClusters = &inventory.Data.Infra.TotalClusters
	doc.TotalDatacenters = inventory.Data.Infra.TotalDatacenters
	doc.TotalHosts = inventory.Data.Infra.TotalHosts
	doc.TotalVMs = inventory.Data.Vms.Total
	doc.TotalMigratable = inventory.Data.Vms.TotalMigratable
	doc.TotalMigratableWithWarnings = inventory.Data.Vms.TotalMigratableWithWarnings
	doc.TotalWithSharedDisks = inventory.Data.Vms.TotalWithSharedDisks
	datastoreEntities = buildDatastoreEntries(doc, inventory.Data.Infra.Datastores)

	if inventory.Data.Vms.OsInfo != nil {
		osEntities = buildOSEntries(doc, inventory.Data.Vms.OsInfo)
	}

	return entity.AssessmentCreatedResult{
		Assessment: *doc,
		OSEntries:  osEntities,
		Datastores: datastoreEntities,
	}, nil
}

func AssessmentDeletedProcessor(_ context.Context, event events.AssessmentEventPayload) (entity.AssessmentDeletedResult, error) {
	zap.S().Infow("processing assessment deleted event",
		"assessment_id", event.Assessment.ID)

	return entity.AssessmentDeletedResult{
		DeletedID: event.Assessment.ID,
		DeletedAt: event.Assessment.DeletedAt.Format(time.RFC3339),
	}, nil
}

func buildOSEntries(assessment *entity.Assessment, osInfo map[string]events.OsInfo) []entity.AssessmentOS {
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

func buildDatastoreEntries(assessment *entity.Assessment, datastores []events.Datastore) []entity.AssessmentDatastore {
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
