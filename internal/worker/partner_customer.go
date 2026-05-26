package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/kubev2v/migration-event-streamer/internal/entity"
	"github.com/kubev2v/migration-event-streamer/internal/pipeline"
	plannerEvents "github.com/kubev2v/migration-planner/pkg/events"
	"go.uber.org/zap"
)

// PartnerCustomerWorker processes partner-customer CloudEvents and writes to Elasticsearch
func PartnerCustomerWorker(ctx context.Context, e cloudevents.Event, w pipeline.ElasticWriter) error {
	var payload plannerEvents.PartnerCustomerEventPayload
	if err := json.Unmarshal(e.Data(), &payload); err != nil {
		zap.S().Errorw("failed to unmarshal partner_customer event", "error", err)
		return err
	}

	pc := payload.PartnerCustomer

	zap.S().Infow("processing partner_customer event",
		"id", pc.ID,
		"customer_username", pc.CustomerUsername,
		"partner_id", pc.PartnerID,
		"request_status", pc.RequestStatus)

	doc := entity.NewPartnerCustomer(
		pc.ID,
		pc.CustomerUsername,
		pc.PartnerID,
		pc.RequestStatus,
		pc.Location,
		pc.AcceptedAt,
		pc.TerminatedAt,
		pc.CreatedAt,
	)

	data, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("failed to marshal partner_customer document: %w", err)
	}

	return w.Overwrite(ctx, entity.Event{
		Index: entity.PartnerCustomerIndex,
		ID:    doc.ID,
		Body:  bytes.NewReader(data),
	})
}
