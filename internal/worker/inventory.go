package worker

import (
	"context"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/tupyy/migration-event-streamer/internal/entity"
	"github.com/tupyy/migration-event-streamer/internal/pipeline"
)

func InventoryWorker(ctx context.Context, e cloudevents.Event, w pipeline.Writer[entity.Event]) error {
	//	zap.S().Debugw("write to elastic", "event", e)
	return nil
}

// func InventoryWorker(ctx context.Context, msg *sarama.Message, w datastore.Writer[entity.Event]) {
// 	// zap.S().Infof("start inserting events every %s", e.readTimeout)

// 	// if err := e.dt.ReadWriteTx(ctx, func(ctx context.Context, reader datastore.Reader, writer datastore.Writer) error {
// 	// 	// read
// 	// 	sources, err := reader.Read(ctx)
// 	// 	if err != nil {
// 	// 		return err
// 	// 	}

// 	// 	if len(sources) == 0 {
// 	// 		return nil
// 	// 	}

// 	// 	w := func(index string, v any) error {
// 	// 		// marshal and create the event
// 	// 		data, err := json.Marshal(v)
// 	// 		if err != nil {
// 	// 			return err
// 	// 		}

// 	// 		event := models.Event{
// 	// 			Index: index,
// 	// 			ID:    uuid.New().String(),
// 	// 			Body:  bytes.NewReader(data),
// 	// 		}

// 	// 		// write
// 	// 		if err := writer.Write(ctx, event); err != nil {
// 	// 			return err
// 	// 		}

// 	// 		return nil
// 	// 	}

// 	// 	for _, source := range sources {
// 	// 		// transform the source inventory to elastic inventory
// 	// 		os := transform.Os(source)
// 	// 		dt := transform.Datastore(source)
// 	// 		inventory := transform.InventorySourceToElastic(source)

// 	// 		if err := w(inventory.Index, inventory); err != nil {
// 	// 			zap.S().Warnw("failed to write event", "error", err, "model", os)
// 	// 		}

// 	// 		idx := 0
// 	// 		for {
// 	// 			if idx < len(os) {
// 	// 				if err := w(os[idx].Index, os[idx]); err != nil {
// 	// 					zap.S().Warnw("failed to write event", "error", err, "model", os)
// 	// 				}
// 	// 			}

// 	// 			if idx < len(dt) {
// 	// 				if err := w(dt[idx].Index, dt[idx]); err != nil {
// 	// 					zap.S().Warnw("failed to write event", "error", err, "model", os)
// 	// 				}
// 	// 			}

// 	// 			idx++
// 	// 			if idx >= len(os) && idx >= len(dt) {
// 	// 				break
// 	// 			}
// 	// 		}
// 	// 	}
// 	// 	return nil
// 	// }); err != nil {
// 	// 	zap.S().Errorf("failed to write inventory to elastic: %s", err)
// 	// }
// }

// // func InventorySourceToElastic(source models.Source) models.Inventory {
// // 	inventory := models.Inventory{
// // 		Index:             "inventory",
// // 		ID:                uuid.New().String(),
// // 		EventTime:         time.Now().Format(time.RFC3339),
// // 		SourceID:          source.ID.String(),
// // 		TotalCpuCores:     source.Inventory.Data.Vms.CpuCores.Total,
// // 		TotalMemory:       source.Inventory.Data.Vms.RamGB.Total,
// // 		TotalDisks:        source.Inventory.Data.Vms.DiskCount.Total,
// // 		TotalDiskSpace:    source.Inventory.Data.Vms.DiskGB.Total,
// // 		VMs:               source.Inventory.Data.Vms.Total,
// // 		VMsMigratable:     source.Inventory.Data.Vms.TotalMigratable,
// // 		MigrationWarnings: make([]string, 0, len(source.Inventory.Data.Vms.MigrationWarnings)),
// // 	}
// // 	for _, w := range source.Inventory.Data.Vms.MigrationWarnings {
// // 		inventory.MigrationWarnings = append(inventory.MigrationWarnings, w.Assessment)
// // 	}
// // 	return inventory
// // }

// // func Os(source models.Source) []models.Os {
// // 	os := make([]models.Os, 0, len(source.Inventory.Data.Vms.Os))
// // 	for k, v := range source.Inventory.Data.Vms.Os {
// // 		os = append(os, models.NewOs(source.ID.String(), k, v))
// // 	}
// // 	return os
// // }

// // func Datastore(source models.Source) []models.Datastore {
// // 	dts := make([]models.Datastore, 0, len(source.Inventory.Data.Infra.Datastores))
// // 	for idx, dt := range source.Inventory.Data.Infra.Datastores {
// // 		dts = append(dts, models.NewDatastore(source.ID.String(), idx, dt.FreeCapacityGB, dt.TotalCapacityGB, dt.Type))
// // 	}
// // 	return dts
// // }
