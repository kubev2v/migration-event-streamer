package entity

import "time"

// Visitor represents a visitor tracking document in Elasticsearch
type Visitor struct {
	Index       string `json:"-"`
	ID          string `json:"-"`
	EventSource string `json:"event_source"`
	Username    string `json:"username"`
	OrgID       string `json:"org_id"`
	Date        string `json:"date"`
}

// NewVisitor creates a new Visitor entity with composite ID (username_YYYYMMDD)
func NewVisitor(username, orgID string, timestamp time.Time, eventSource string) *Visitor {
	dateStr := timestamp.Format("2006-01-02")
	docID := orgID + "_" + username + "_" + timestamp.Format("20060102")

	return &Visitor{
		Index:       VisitorIndex,
		ID:          docID,
		EventSource: eventSource,
		Username:    username,
		OrgID:       orgID,
		Date:        dateStr,
	}
}
