package entity

import (
	"time"

	"github.com/google/uuid"
)

type ShareAssessmentResult struct {
	ID           string `json:"-"`
	Username     string `json:"username"`
	AssessmentID string `json:"assessment_id"`
	PartnerID    string `json:"partner_id"`
	Timestamp    string `json:"timestamp"`
}

type UnshareAssessmentResult struct {
	ID           string `json:"-"`
	Username     string `json:"username"`
	AssessmentID string `json:"assessment_id"`
	Timestamp    string `json:"timestamp"`
}

type SizingRequestedResult struct {
	ID           string `json:"-"`
	Username     string `json:"username"`
	AssessmentID string `json:"assessment_id"`
	Timestamp    string `json:"timestamp"`
}

type ComplexityEstimatedResult struct {
	ID           string `json:"-"`
	Username     string `json:"username"`
	AssessmentID string `json:"assessment_id"`
	Timestamp    string `json:"timestamp"`
}

type TimeEstimatedResult struct {
	ID           string `json:"-"`
	Username     string `json:"username"`
	AssessmentID string `json:"assessment_id"`
	Timestamp    string `json:"timestamp"`
}

type OVADownloadedResult struct {
	ID        string `json:"-"`
	Username  string `json:"username"`
	SourceID  string `json:"source_id"`
	Timestamp string `json:"timestamp"`
}

type VisitedResult struct {
	ID        string `json:"-"`
	Username  string `json:"username"`
	OrgID     string `json:"org_id"`
	Timestamp string `json:"timestamp"`
}

func NewShareAssessmentResult(username, assessmentID, partnerID string, timestamp time.Time) ShareAssessmentResult {
	return ShareAssessmentResult{
		ID:           uuid.New().String(),
		Username:     username,
		AssessmentID: assessmentID,
		PartnerID:    partnerID,
		Timestamp:    timestamp.Format(time.RFC3339),
	}
}

func NewUnshareAssessmentResult(username, assessmentID string, timestamp time.Time) UnshareAssessmentResult {
	return UnshareAssessmentResult{
		ID:           uuid.New().String(),
		Username:     username,
		AssessmentID: assessmentID,
		Timestamp:    timestamp.Format(time.RFC3339),
	}
}

func NewSizingRequestedResult(username, assessmentID string, timestamp time.Time) SizingRequestedResult {
	return SizingRequestedResult{
		ID:           uuid.New().String(),
		Username:     username,
		AssessmentID: assessmentID,
		Timestamp:    timestamp.Format(time.RFC3339),
	}
}

func NewComplexityEstimatedResult(username, assessmentID string, timestamp time.Time) ComplexityEstimatedResult {
	return ComplexityEstimatedResult{
		ID:           uuid.New().String(),
		Username:     username,
		AssessmentID: assessmentID,
		Timestamp:    timestamp.Format(time.RFC3339),
	}
}

func NewTimeEstimatedResult(username, assessmentID string, timestamp time.Time) TimeEstimatedResult {
	return TimeEstimatedResult{
		ID:           uuid.New().String(),
		Username:     username,
		AssessmentID: assessmentID,
		Timestamp:    timestamp.Format(time.RFC3339),
	}
}

func NewOVADownloadedResult(username, sourceID string, timestamp time.Time) OVADownloadedResult {
	return OVADownloadedResult{
		ID:        uuid.New().String(),
		Username:  username,
		SourceID:  sourceID,
		Timestamp: timestamp.Format(time.RFC3339),
	}
}

func NewVisitedResult(username, orgID string, timestamp time.Time) VisitedResult {
	return VisitedResult{
		ID:        orgID + "_" + username + "_" + timestamp.Format("20060102"),
		Username:  username,
		OrgID:     orgID,
		Timestamp: timestamp.Format(time.RFC3339),
	}
}
