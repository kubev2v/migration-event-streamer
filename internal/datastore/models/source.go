package models

import (
	"time"

	api "github.com/kubev2v/migration-planner/api/v1alpha1"
	openapi_types "github.com/oapi-codegen/runtime/types"
)

type Source struct {
	ID        openapi_types.UUID `json:"id" gorm:"primaryKey"`
	CreatedAt time.Time
	UpdatedAt time.Time
	Inventory *JSONField[api.Inventory] `gorm:"type:jsonb"`
}
