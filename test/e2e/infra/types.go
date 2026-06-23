package infra

const (
	postgresContainerName = "e2e-streamer-test-postgres"
	postgresImage         = "docker.io/library/postgres:17"
	postgresPort          = 5432

	kafkaContainerName = "e2e-streamer-test-kafka"
	kafkaImage         = "docker.io/apache/kafka:4.3.1"
	kafkaPort          = 9092

	esContainerName = "e2e-streamer-test-elasticsearch"
	esImage         = "docker.io/library/elasticsearch:8.7.1"
	esPort          = 9200

	plannerContainerName = "e2e-streamer-test-planner"
	plannerPort          = 3443

	streamerContainerName = "e2e-streamer-test-streamer"

	dbUser     = "planner"
	dbPassword = "adminpass"
	dbName     = "planner"

	e2eNamespace = "migration-planner"
)

var kafkaTopics = []string{
	"assisted.migration.events",
	e2eNamespace + ".events",
}
