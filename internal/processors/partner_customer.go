package processors

import (
	"context"
	"encoding/json"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/kubev2v/migration-event-streamer/internal/entity"
	plannerEvents "github.com/kubev2v/migration-planner/pkg/events"
	"go.uber.org/zap"
)

func PartnerCustomerProcessor(_ context.Context, e cloudevents.Event) (*entity.PartnerCustomer, error) {
	var payload plannerEvents.PartnerCustomerEventPayload
	if err := json.Unmarshal(e.Data(), &payload); err != nil {
		zap.S().Errorw("failed to unmarshal partner_customer event", "error", err)
		return nil, err
	}

	pc := payload.PartnerCustomer

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
		e.Context.GetSource(),
	), nil
}
