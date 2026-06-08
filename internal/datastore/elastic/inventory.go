package elastic

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/kubev2v/migration-event-streamer/internal/entity"
)

func (e *ElasticRepository) WriteInventory(ctx context.Context, inv entity.Inventory) error {
	data, err := json.Marshal(inv)
	if err != nil {
		return fmt.Errorf("failed to marshal inventory: %w", err)
	}

	return e.write(ctx, inv.Index, inv.ID, data)
}
