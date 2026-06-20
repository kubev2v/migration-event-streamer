package elastic

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/kubev2v/migration-event-streamer/internal/entity"
)

type partnerCustomerWriter struct {
	base *baseWriter
}

func (w *partnerCustomerWriter) Write(ctx context.Context, pc entity.PartnerCustomer) error {
	data, err := json.Marshal(pc)
	if err != nil {
		return fmt.Errorf("failed to marshal partner_customer: %w", err)
	}
	return w.base.write(ctx, entity.PartnerCustomerIndex, pc.ID, data)
}
