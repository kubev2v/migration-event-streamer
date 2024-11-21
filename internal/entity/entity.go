package entity

import "io"

type TopicPartition struct {
	Topic     string
	Partition int32
}

type Event struct {
	Index string
	ID    string
	Body  io.ReadSeeker
}
