package entity

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

// Status represent the document status. We use soft deletion.
const (
	ActiveStatus  = "active"
	DeletedStatus = "deleted"
)

type AssessmentCreatedResult struct {
	Assessment Assessment
	OSEntries  []AssessmentOS
	Datastores []AssessmentDatastore
}

type AssessmentDeletedResult struct {
	DeletedID string
	DeletedAt string
}

// Assessment represents an assessment document in Elasticsearch
type Assessment struct {
	Index                       string  `json:"-"`
	AssessmentID                string  `json:"assessment_id"`
	Name                        string  `json:"name"`
	OrgID                       string  `json:"org_id"`
	Username                    string  `json:"username"`
	SourceType                  string  `json:"source_type"`
	PartnerID                   *string `json:"partner_id,omitempty"`
	Status                      string  `json:"status"`
	CreatedAt                   string  `json:"created_at"`
	UpdatedAt                   *string `json:"updated_at,omitempty"`
	DeletedAt                   *string `json:"deleted_at,omitempty"`
	SnapshotID                  string  `json:"snapshot_id"`
	VCenterID                   string  `json:"vcenter_id"`
	TotalClusters               *int    `json:"total_clusters,omitempty"`
	TotalDatacenters            *int    `json:"total_datacenters,omitempty"`
	TotalHosts                  int     `json:"total_hosts"`
	TotalVMs                    int     `json:"total_vms"`
	TotalMigratable             int     `json:"total_migratable"`
	TotalMigratableWithWarnings *int    `json:"total_migratable_with_warnings,omitempty"`
	TotalWithSharedDisks        *int    `json:"total_with_shared_disks,omitempty"`
}

// AssessmentOS represents an OS distribution document for an assessment
type AssessmentOS struct {
	Index        string  `json:"-"`
	ID           string  `json:"-"`
	AssessmentID string  `json:"assessment_id"`
	SnapshotID   string  `json:"snapshot_id"`
	OSType       string  `json:"os_type"`
	VMCount      int     `json:"vm_count"`
	Username     string  `json:"username"`
	OrgID        string  `json:"org_id"`
	PartnerID    *string `json:"partner_id,omitempty"`
	Status       string  `json:"status"`
	CreatedAt    string  `json:"created_at"`
	DeletedAt    *string `json:"deleted_at,omitempty"`
}

// AssessmentDatastore represents a datastore document for an assessment
type AssessmentDatastore struct {
	Index           string  `json:"-"`
	ID              string  `json:"-"`
	AssessmentID    string  `json:"assessment_id"`
	SnapshotID      string  `json:"snapshot_id"`
	DatastoreIndex  int     `json:"datastore_index"`
	DatastoreType   string  `json:"datastore_type"`
	TotalCapacityGB int     `json:"total_capacity_gb"`
	FreeCapacityGB  int     `json:"free_capacity_gb"`
	Username        string  `json:"username"`
	OrgID           string  `json:"org_id"`
	PartnerID       *string `json:"partner_id,omitempty"`
	Status          string  `json:"status"`
	CreatedAt       string  `json:"created_at"`
	DeletedAt       *string `json:"deleted_at,omitempty"`
}

// NewAssessment creates a new Assessment entity
func NewAssessment(id, name, orgID, username, sourceType, status string, createdAt time.Time, snapshotID, vCenterID string) *Assessment {
	return &Assessment{
		Index:        AssessmentIndex,
		AssessmentID: id,
		Name:         name,
		OrgID:        orgID,
		Username:     username,
		SourceType:   sourceType,
		Status:       status,
		CreatedAt:    createdAt.Format(time.RFC3339),
		SnapshotID:   snapshotID,
		VCenterID:    vCenterID,
	}
}

// NewAssessmentOS creates a new AssessmentOS entity
func NewAssessmentOS(assessmentID, snapshotID, osType string, vmCount int, username, orgID, status string, createdAt time.Time) *AssessmentOS {
	hash := sha256.Sum256([]byte(osType))

	return &AssessmentOS{
		Index:        OSIndex,
		ID:           fmt.Sprintf("%s_%s", snapshotID, hex.EncodeToString(hash[:8])),
		AssessmentID: assessmentID,
		SnapshotID:   snapshotID,
		OSType:       osType,
		VMCount:      vmCount,
		Username:     username,
		OrgID:        orgID,
		Status:       status,
		CreatedAt:    createdAt.Format(time.RFC3339),
	}
}

// NewAssessmentDatastore creates a new AssessmentDatastore entity
func NewAssessmentDatastore(assessmentID, snapshotID string, datastoreIndex int, datastoreType string, totalCapGB, freeCapGB int, username, orgID, status string, createdAt time.Time) *AssessmentDatastore {
	return &AssessmentDatastore{
		Index:           DatastoreIndex,
		ID:              fmt.Sprintf("%s_%d", snapshotID, datastoreIndex),
		AssessmentID:    assessmentID,
		SnapshotID:      snapshotID,
		DatastoreIndex:  datastoreIndex,
		DatastoreType:   datastoreType,
		TotalCapacityGB: totalCapGB,
		FreeCapacityGB:  freeCapGB,
		Username:        username,
		OrgID:           orgID,
		Status:          status,
		CreatedAt:       createdAt.Format(time.RFC3339),
	}
}
