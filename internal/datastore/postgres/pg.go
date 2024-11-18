package postgres

import (
	"context"

	"github.com/tupyy/migration-event-streamer/internal/clients"
	"github.com/tupyy/migration-event-streamer/internal/datastore/models"
)

type PgDatastore struct {
	pgClient *clients.PgConnection
}

func NewPgDatastore(pgClient *clients.PgConnection) *PgDatastore {
	return &PgDatastore{pgClient}
}

func (p *PgDatastore) Read(ctx context.Context) ([]models.Source, error) {
	m := []models.Source{}
	tx := p.pgClient.DB().WithContext(ctx).Table("sources").
		Select("id, inventory").
		Order("id").
		Where("inventory IS NOT NULL")

	if err := tx.Find(&m).Error; err != nil {
		return nil, err
	}

	if len(m) == 0 {
		return []models.Source{}, nil
	}

	return m, nil
}
