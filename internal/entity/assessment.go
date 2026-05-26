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

// Assessment represents an assessment document in Elasticsearch
type Assessment struct {
	Index      string  `json:"-"`
	ID         string  `json:"-"`
	Name       string  `json:"name"`
	OrgID      string  `json:"org_id"`
	Username   string  `json:"username"`
	SourceType string  `json:"source_type"`
	PartnerID  *string `json:"partner_id,omitempty"`
	Location   *string `json:"location,omitempty"`
	Status     string  `json:"status"`
	CreatedAt  string  `json:"created_at"`
	UpdatedAt  *string `json:"updated_at,omitempty"`
	DeletedAt  *string `json:"deleted_at,omitempty"`
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
	Location     *string `json:"location,omitempty"`
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
	Location        *string `json:"location,omitempty"`
	Status          string  `json:"status"`
	CreatedAt       string  `json:"created_at"`
	DeletedAt       *string `json:"deleted_at,omitempty"`
}

// NewAssessment creates a new Assessment entity
func NewAssessment(id, name, orgID, username, sourceType, status string, createdAt time.Time) *Assessment {
	return &Assessment{
		Index:      AssessmentIndex,
		ID:         id,
		Name:       name,
		OrgID:      orgID,
		Username:   username,
		SourceType: sourceType,
		Status:     status,
		CreatedAt:  createdAt.Format(time.RFC3339),
	}
}

// NewAssessmentOS creates a new AssessmentOS entity
func NewAssessmentOS(assessmentID, snapshotID, osType string, vmCount int, username, orgID, status string, createdAt time.Time) *AssessmentOS {
	// Hash OS type to create a valid Elasticsearch document ID
	hash := sha256.Sum256([]byte(osType))

	return &AssessmentOS{
		Index:        OSIndex,
		ID:           fmt.Sprintf("%s_%s", snapshotID, hex.EncodeToString(hash[:8])), // Use first 8 bytes (16 hex chars)
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
