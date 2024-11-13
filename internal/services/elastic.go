package services

import (
	"bytes"
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/tupyy/migration-event-streamer/internal/datastore/models"
	"github.com/tupyy/migration-event-streamer/internal/transform"
	"github.com/tupyy/migration-event-streamer/pkg/datastore"
	"go.uber.org/zap"
)

type Inventory struct {
	dt          datastore.Datastore
	readTimeout time.Duration
}

func NewInventory(dt datastore.Datastore, readTimeout time.Duration) *Inventory {
	return &Inventory{dt: dt, readTimeout: readTimeout}
}

func (e *Inventory) Run(ctx context.Context) {
	zap.S().Infof("start inserting events every %s", e.readTimeout)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		<-time.After(e.readTimeout)

		if err := e.dt.ReadWriteTx(ctx, func(ctx context.Context, reader datastore.Reader, writer datastore.Writer) error {
			// read
			sources, err := reader.Read(ctx)
			if err != nil {
				return err
			}

			if len(sources) == 0 {
				return nil
			}

			w := func(index string, v any) error {
				// marshal and create the event
				data, err := json.Marshal(v)
				if err != nil {
					return err
				}

				event := models.Event{
					Index: index,
					ID:    uuid.New().String(),
					Body:  bytes.NewReader(data),
				}

				// write
				if err := writer.Write(ctx, event); err != nil {
					return err
				}

				return nil
			}

			for _, source := range sources {
				// transform the source inventory to elastic inventory
				os := transform.Os(source)
				dt := transform.Datastore(source)
				inventory := transform.InventorySourceToElastic(source)

				if err := w(inventory.Index, inventory); err != nil {
					zap.S().Warnw("failed to write event", "error", err, "model", os)
				}

				idx := 0
				for {
					if idx < len(os) {
						if err := w(os[idx].Index, os[idx]); err != nil {
							zap.S().Warnw("failed to write event", "error", err, "model", os)
						}
					}

					if idx < len(dt) {
						if err := w(dt[idx].Index, dt[idx]); err != nil {
							zap.S().Warnw("failed to write event", "error", err, "model", os)
						}
					}

					idx++
					if idx >= len(os) && idx >= len(dt) {
						break
					}
				}
			}
			return nil
		}); err != nil {
			zap.S().Errorf("failed to write inventory to elastic: %s", err)
		}
	}
}
