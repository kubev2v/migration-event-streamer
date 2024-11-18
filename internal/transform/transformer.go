package transform

import (
	"time"

	"github.com/google/uuid"
	"github.com/tupyy/migration-event-streamer/internal/datastore/models"
)

func InventorySourceToElastic(source models.Source) models.Inventory {
	inventory := models.Inventory{
		Index:             "inventory",
		ID:                uuid.New().String(),
		EventTime:         time.Now().Format(time.RFC3339),
		SourceID:          source.ID.String(),
		TotalCpuCores:     source.Inventory.Data.Vms.CpuCores.Total,
		TotalMemory:       source.Inventory.Data.Vms.RamGB.Total,
		TotalDisks:        source.Inventory.Data.Vms.DiskCount.Total,
		TotalDiskSpace:    source.Inventory.Data.Vms.DiskGB.Total,
		VMs:               source.Inventory.Data.Vms.Total,
		VMsMigratable:     source.Inventory.Data.Vms.TotalMigratable,
		MigrationWarnings: make([]string, 0, len(source.Inventory.Data.Vms.MigrationWarnings)),
	}
	for _, w := range source.Inventory.Data.Vms.MigrationWarnings {
		inventory.MigrationWarnings = append(inventory.MigrationWarnings, w.Assessment)
	}
	return inventory
}

func Os(source models.Source) []models.Os {
	os := make([]models.Os, 0, len(source.Inventory.Data.Vms.Os))
	for k, v := range source.Inventory.Data.Vms.Os {
		os = append(os, models.NewOs(source.ID.String(), k, v))
	}
	return os
}

func Datastore(source models.Source) []models.Datastore {
	dts := make([]models.Datastore, 0, len(source.Inventory.Data.Infra.Datastores))
	for idx, dt := range source.Inventory.Data.Infra.Datastores {
		dts = append(dts, models.NewDatastore(source.ID.String(), idx, dt.FreeCapacityGB, dt.TotalCapacityGB, dt.Type))
	}
	return dts
}
