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

type AssessmentResult struct {
	Action     string
	Assessment *Assessment
	OSEntries  []*AssessmentOS
	Datastores []*AssessmentDatastore
	DeletedID  string
	DeletedAt  string
}

// Assessment represents an assessment document in Elasticsearch
type Assessment struct {
	Index                       string  `json:"-"`
	ID                          string  `json:"-"`
	EventSource                 string  `json:"event_source"`
	Name                        string  `json:"name"`
	OrgID                       string  `json:"org_id"`
	Username                    string  `json:"username"`
	SourceType                  string  `json:"source_type"`
	PartnerID                   *string `json:"partner_id,omitempty"`
	Location                    *string `json:"location,omitempty"`
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
	EventSource  string  `json:"event_source"`
	AssessmentID string  `json:"assessment_id"`
	SnapshotID   string  `json:"snapshot_id"`
	OSType       string  `json:"os_type"`
	VMCount      int     `json:"vm_count"`
	Username     string  `json:"username"`
	OrgID        string  `json:"org_id"`
	PartnerID    *string `json:"partner_id,omitempty"`
	Location     *string `json:"location,omitempty"`
	Status       string  `json:"status"`
	CreatedAt    string  `json:"created_at"`
	DeletedAt    *string `json:"deleted_at,omitempty"`
}

// AssessmentDatastore represents a datastore document for an assessment
type AssessmentDatastore struct {
	Index           string  `json:"-"`
	ID              string  `json:"-"`
	EventSource     string  `json:"event_source"`
	AssessmentID    string  `json:"assessment_id"`
	SnapshotID      string  `json:"snapshot_id"`
	DatastoreIndex  int     `json:"datastore_index"`
	DatastoreType   string  `json:"datastore_type"`
	TotalCapacityGB int     `json:"total_capacity_gb"`
	FreeCapacityGB  int     `json:"free_capacity_gb"`
	Username        string  `json:"username"`
	OrgID           string  `json:"org_id"`
	PartnerID       *string `json:"partner_id,omitempty"`
	Location        *string `json:"location,omitempty"`
	Status          string  `json:"status"`
	CreatedAt       string  `json:"created_at"`
	DeletedAt       *string `json:"deleted_at,omitempty"`
}

// NewAssessment creates a new Assessment entity
func NewAssessment(id, name, orgID, username, sourceType, status string, createdAt time.Time, snapshotID, vCenterID, eventSource string) *Assessment {
	return &Assessment{
		Index:       AssessmentIndex,
		ID:          id,
		EventSource: eventSource,
		Name:        name,
		OrgID:       orgID,
		Username:    username,
		SourceType:  sourceType,
		Status:      status,
		CreatedAt:   createdAt.Format(time.RFC3339),
		SnapshotID:  snapshotID,
		VCenterID:   vCenterID,
	}
}

// NewAssessmentOS creates a new AssessmentOS entity
func NewAssessmentOS(assessmentID, snapshotID, osType string, vmCount int, username, orgID, status string, createdAt time.Time, eventSource string) *AssessmentOS {
	// Hash OS type to create a valid Elasticsearch document ID
	hash := sha256.Sum256([]byte(osType))

	return &AssessmentOS{
		Index:        OSIndex,
		ID:           fmt.Sprintf("%s_%s", snapshotID, hex.EncodeToString(hash[:8])), // Use first 8 bytes (16 hex chars)
		EventSource:  eventSource,
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
func NewAssessmentDatastore(assessmentID, snapshotID string, datastoreIndex int, datastoreType string, totalCapGB, freeCapGB int, username, orgID, status string, createdAt time.Time, eventSource string) *AssessmentDatastore {
	return &AssessmentDatastore{
		Index:           DatastoreIndex,
		ID:              fmt.Sprintf("%s_%d", snapshotID, datastoreIndex),
		EventSource:     eventSource,
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
