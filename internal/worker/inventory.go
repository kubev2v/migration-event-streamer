package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"
	"github.com/kubev2v/migration-event-streamer/internal/entity"
	"github.com/kubev2v/migration-event-streamer/internal/pipeline"
	"github.com/kubev2v/migration-planner/api/v1alpha1"
)

type jinventory struct {
	Inventory v1alpha1.Inventory `json:"inventory"`
}

func InventoryWorker(ctx context.Context, e cloudevents.Event, w pipeline.Writer[entity.Event]) error {
	var jv jinventory
	if err := json.Unmarshal(e.Data(), &jv); err != nil {
		return err
	}

	// expect to find sourceID in Extentions
	sourceID, ok := e.Extensions()["sourceid"]
	if !ok {
		sourceID = uuid.NewString()
	}

	inventory := InventorySourceToElastic(sourceID.(string), jv.Inventory)
	data, _ := json.Marshal(inventory)

	return w.Write(ctx, entity.Event{
		Index: "inventory",
		ID:    uuid.New().String(),
		Body:  bytes.NewReader(data),
	})
}

func InventorySourceToElastic(sourceID string, i v1alpha1.Inventory) entity.Inventory {
	inventory := entity.Inventory{
		EventTime:         time.Now().Format(time.RFC3339),
		SourceID:          sourceID,
		TotalCpuCores:     i.Vms.CpuCores.Total,
		TotalMemory:       i.Vms.RamGB.Total,
		TotalDisks:        i.Vms.DiskCount.Total,
		TotalDiskSpace:    i.Vms.DiskGB.Total,
		VMs:               i.Vms.Total,
		VMsMigratable:     i.Vms.TotalMigratable,
		MigrationWarnings: make([]string, 0, len(i.Vms.MigrationWarnings)),
	}
	for _, w := range i.Vms.MigrationWarnings {
		inventory.MigrationWarnings = append(inventory.MigrationWarnings, w.Assessment)
	}
	return inventory
}

func Os(sourceID string, i v1alpha1.Inventory) []entity.Os {
	os := make([]entity.Os, 0, len(i.Vms.Os))
	for k, v := range i.Vms.Os {
		os = append(os, entity.NewOs(sourceID, k, v))
	}
	return os
}

func Datastore(sourceID string, i v1alpha1.Inventory) []entity.Datastore {
	dts := make([]entity.Datastore, 0, len(i.Infra.Datastores))
	for idx, dt := range i.Infra.Datastores {
		dts = append(dts, entity.NewDatastore(sourceID, idx, dt.FreeCapacityGB, dt.TotalCapacityGB, dt.Type))
	}
	return dts
}
