package infra

type InfraManager interface {
	StartPostgres() error
	StopPostgres() error
	StartKafka() error
	StopKafka() error
	CreateKafkaTopics() error
	StartElasticsearch() error
	StopElasticsearch() error
	StartPlanner() error
	StopPlanner() error
	StartStreamer() error
	StopStreamer() error
	StreamerLogs() string
	PlannerLogs() string
}
