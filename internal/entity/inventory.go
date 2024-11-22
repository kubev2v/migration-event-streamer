package entity

import (
	"time"

	"github.com/google/uuid"
)

type Datastore struct {
	Index           string `json:"-"`
	ID              string `json:"-"`
	DatastoreIndex  int    `json:"datastore_index"`
	SourceID        string `json:"source_id"`
	EventTime       string `json:"event_time"`
	FreeCapacityGB  int    `json:"freeCapacityGB"`
	TotalCapacityGB int    `json:"totalCapacityGB"`
	Type            string `json:"datastore_type"`
}

func NewDatastore(sourceID string, index, freeCapacityGB, totalCapacityGB int, datastoreType string) Datastore {
	return Datastore{
		Index:           "datastore",
		ID:              uuid.New().String(),
		EventTime:       time.Now().Format(time.RFC3339),
		SourceID:        sourceID,
		DatastoreIndex:  index,
		FreeCapacityGB:  freeCapacityGB,
		TotalCapacityGB: totalCapacityGB,
		Type:            datastoreType,
	}
}

type Os struct {
	Index     string `json:"-"`
	ID        string `json:"-"`
	SourceID  string `json:"source_id"`
	EventTime string `json:"event_time"`
	Type      string `json:"os_type"`
	Count     int    `json:"count"`
}

func NewOs(sourceID string, osType string, count int) Os {
	return Os{
		Index:     "os",
		SourceID:  sourceID,
		ID:        uuid.New().String(),
		EventTime: time.Now().Format(time.RFC3339),
		Type:      osType,
		Count:     count,
	}
}

type Inventory struct {
	ID                string   `json:"-"`
	Index             string   `json:"-"`
	EventTime         string   `json:"event_time"`
	SourceID          string   `json:"source_id"`
	TotalCpuCores     int      `json:"total_cpu_cores"`
	TotalMemory       int      `json:"total_memory"`
	TotalDisks        int      `json:"total_disks"`
	TotalDiskSpace    int      `json:"total_disk_space"`
	VMs               int      `json:"vms"`
	VMsMigratable     int      `json:"vms_migratable"`
	MigrationWarnings []string `json:"migration_warnings"`
}
