package entity

import (
	"time"

	"github.com/google/uuid"
)

// UserAction represents a user action tracking document in Elasticsearch
type UserAction struct {
	ID           string  `json:"id"`
	Username     string  `json:"username"`
	AssessmentID *string `json:"assessment_id,omitempty"`
	SourceID     *string `json:"source_id,omitempty"`
	PartnerID    *string `json:"partner_id,omitempty"`
	ActionType   string  `json:"action_type"`
	Timestamp    string  `json:"timestamp"`
}

// NewUserAction creates a new UserAction entity with auto-generated ID
func NewUserAction(username string, assessmentID, sourceID, partnerID *string, actionType string, timestamp time.Time) *UserAction {
	return &UserAction{
		ID:           uuid.New().String(),
		Username:     username,
		AssessmentID: assessmentID,
		SourceID:     sourceID,
		PartnerID:    partnerID,
		ActionType:   actionType,
		Timestamp:    timestamp.Format(time.RFC3339),
	}
}
