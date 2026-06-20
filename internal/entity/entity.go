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

type PipelineJob struct {
	Data []byte
	Done chan struct{}
}

func NewPipelineJob(data []byte) PipelineJob {
	return PipelineJob{
		Data: data,
		Done: make(chan struct{}),
	}
}

type PipelineError struct {
	Pipeline string
	Err      error
	Ack      chan struct{}
}

func NewPipelineError(pipeline string, err error) PipelineError {
	return PipelineError{
		Pipeline: pipeline,
		Err:      err,
		Ack:      make(chan struct{}),
	}
}
