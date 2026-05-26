package entity

import "time"

// PartnerCustomer represents a partner-customer relationship document in Elasticsearch
type PartnerCustomer struct {
	ID               string  `json:"id"`
	CustomerUsername string  `json:"customer_username"`
	PartnerID        string  `json:"partner_id"`
	RequestStatus    string  `json:"request_status"`
	Location         string  `json:"location"`
	AcceptedAt       *string `json:"accepted_at,omitempty"`
	TerminatedAt     *string `json:"terminated_at,omitempty"`
	CreatedAt        string  `json:"created_at"`
	EventTime        string  `json:"event_time"`
}

// NewPartnerCustomer creates a new PartnerCustomer entity
func NewPartnerCustomer(id, customerUsername, partnerID, requestStatus, location string, acceptedAt, terminatedAt *time.Time, createdAt time.Time) *PartnerCustomer {
	pc := &PartnerCustomer{
		ID:               id,
		CustomerUsername: customerUsername,
		PartnerID:        partnerID,
		RequestStatus:    requestStatus,
		Location:         location,
		CreatedAt:        createdAt.Format(time.RFC3339),
		EventTime:        time.Now().Format(time.RFC3339),
	}

	if acceptedAt != nil {
		str := acceptedAt.Format(time.RFC3339)
		pc.AcceptedAt = &str
	}

	if terminatedAt != nil {
		str := terminatedAt.Format(time.RFC3339)
		pc.TerminatedAt = &str
	}

	return pc
}
