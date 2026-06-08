package entity

import (
	cloudevents "github.com/cloudevents/sdk-go/v2"
)

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
