package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/kubev2v/migration-event-streamer/internal/entity"
	"github.com/kubev2v/migration-event-streamer/internal/pipeline"
)

type basicEvent struct {
	EventTime string
	Event     string
}

func BasicWorker(ctx context.Context, e cloudevents.Event, w pipeline.Writer[entity.Event]) error {
	index := "ui"
	if e.Context.GetType() == "assisted.migrations.events.agent" {
		index = "agent"
	}

	ev := basicEvent{
		EventTime: time.Now().Format(time.RFC3339),
		Event:     "some event",
	}

	body, _ := json.Marshal(ev)

	return w.Write(ctx, entity.Event{
		Index: index,
		ID:    e.Context.GetID(),
		Body:  bytes.NewReader(body),
	})
}
