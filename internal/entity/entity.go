package entity

import (
	"io"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

type Event struct {
	Index string
	ID    string
	Body  io.ReadSeeker
}

type Message struct {
	Event    cloudevents.Event
	CommitCh chan any
}

func NewMessage(e cloudevents.Event) Message {
	return Message{
		Event:    e,
		CommitCh: make(chan any),
	}
}
