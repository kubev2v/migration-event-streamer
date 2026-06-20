package processors

import (
	"context"

	"github.com/kubev2v/migration-event-streamer/internal/entity"
	plannerEvents "github.com/kubev2v/migration-planner/pkg/events"
	"go.uber.org/zap"
)

func PartnerCustomerProcessor(_ context.Context, event plannerEvents.PartnerCustomerEventPayload) (entity.PartnerCustomer, error) {
	pc := event.PartnerCustomer

	zap.S().Infow("processing partner_customer event",
		"id", pc.ID,
		"customer_username", pc.CustomerUsername,
		"partner_id", pc.PartnerID,
		"request_status", pc.RequestStatus)

	return entity.NewPartnerCustomer(
		pc.ID,
		pc.CustomerUsername,
		pc.PartnerID,
		pc.RequestStatus,
		pc.AcceptedAt,
		pc.TerminatedAt,
		pc.CreatedAt,
	), nil
}
