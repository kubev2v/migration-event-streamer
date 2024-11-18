package datastore

import (
	"context"

	"github.com/tupyy/migration-event-streamer/internal/datastore/models"
)

type Reader interface {
	Read(context.Context) ([]models.Source, error)
}

type Writer interface {
	Write(context.Context, models.Event) error
}

type Datastore interface {
	ReadWriteTx(context.Context, func(context.Context, Reader, Writer) error) error
}
