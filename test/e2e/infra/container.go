package infra

import (
	"fmt"
	"net/http"
	"time"

	pkgkafka "github.com/kubev2v/migration-event-streamer/pkg/kafka"
	"go.uber.org/zap"
)

type ContainerInfraManager struct {
	runner        *PodmanRunner
	plannerImage  string
	streamerImage string
}

func NewContainerInfraManager(podmanSocket, plannerImage, streamerImage string) (*ContainerInfraManager, error) {
	runner, err := NewPodmanRunner(podmanSocket)
	if err != nil {
		return nil, err
	}
	return &ContainerInfraManager{
		runner:        runner,
		plannerImage:  plannerImage,
		streamerImage: streamerImage,
	}, nil
}

func (c *ContainerInfraManager) StartPostgres() error {
	zap.S().Info("Starting PostgreSQL...")
	_, err := c.runner.StartContainer(
		NewContainerConfig(postgresContainerName, postgresImage).
			WithPort(postgresPort, postgresPort).
			WithEnvVar("POSTGRES_USER", dbUser).
			WithEnvVar("POSTGRES_PASSWORD", dbPassword).
			WithEnvVar("POSTGRES_DB", dbName),
	)
	if err != nil {
		return err
	}
	return c.waitForPostgres(60 * time.Second)
}

func (c *ContainerInfraManager) StopPostgres() error {
	_ = c.runner.StopContainer(postgresContainerName)
	return c.runner.RemoveContainer(postgresContainerName)
}

func (c *ContainerInfraManager) waitForPostgres(timeout time.Duration) error {
	zap.S().Info("Waiting for PostgreSQL to be ready...")
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		exitCode, err := c.runner.Exec(postgresContainerName, []string{
			"pg_isready", "-U", dbUser,
		})
		if err == nil && exitCode == 0 {
			zap.S().Info("PostgreSQL is ready")
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("PostgreSQL did not become ready within %v", timeout)
}

func (c *ContainerInfraManager) StartKafka() error {
	zap.S().Info("Starting Kafka...")
	_, err := c.runner.StartContainer(
		NewContainerConfig(kafkaContainerName, kafkaImage).
			WithPort(kafkaPort, kafkaPort).
			WithEnvVar("KAFKA_NODE_ID", "1").
			WithEnvVar("KAFKA_PROCESS_ROLES", "broker,controller").
			WithEnvVar("KAFKA_LISTENERS", "BROKER://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093").
			WithEnvVar("KAFKA_ADVERTISED_LISTENERS", "BROKER://localhost:9092").
			WithEnvVar("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER").
			WithEnvVar("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER").
			WithEnvVar("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,BROKER:PLAINTEXT").
			WithEnvVar("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093").
			WithEnvVar("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1").
			WithEnvVar("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1").
			WithEnvVar("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1").
			WithEnvVar("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0").
			WithEnvVar("CLUSTER_ID", "e2e-test-cluster-id-000"),
	)
	if err != nil {
		return err
	}
	return c.waitForKafka(60 * time.Second)
}

func (c *ContainerInfraManager) StopKafka() error {
	_ = c.runner.StopContainer(kafkaContainerName)
	return c.runner.RemoveContainer(kafkaContainerName)
}

func (c *ContainerInfraManager) waitForKafka(timeout time.Duration) error {
	zap.S().Info("Waiting for Kafka to be ready...")
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		exitCode, err := c.runner.Exec(kafkaContainerName, []string{
			"/opt/kafka/bin/kafka-broker-api-versions.sh",
			"--bootstrap-server", "localhost:9092",
		})
		if err == nil && exitCode == 0 {
			zap.S().Info("Kafka is ready")
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("kafka did not become ready within %v", timeout)
}

func (c *ContainerInfraManager) CreateKafkaTopics() error {
	zap.S().Info("Creating Kafka topics...")
	brokers := []string{fmt.Sprintf("localhost:%d", kafkaPort)}
	for _, topic := range kafkaTopics {
		if err := pkgkafka.EnsureTopic(brokers, nil, topic, 1, 1); err != nil {
			return fmt.Errorf("failed to create topic %s: %w", topic, err)
		}
	}
	return nil
}

func (c *ContainerInfraManager) StartElasticsearch() error {
	zap.S().Info("Starting Elasticsearch...")
	_, err := c.runner.StartContainer(
		NewContainerConfig(esContainerName, esImage).
			WithPort(esPort, esPort).
			WithEnvVar("discovery.type", "single-node").
			WithEnvVar("xpack.security.enabled", "false").
			WithEnvVar("ES_JAVA_OPTS", "-Xms512m -Xmx512m"),
	)
	if err != nil {
		return err
	}
	return c.waitForElasticsearch(90 * time.Second)
}

func (c *ContainerInfraManager) StopElasticsearch() error {
	_ = c.runner.StopContainer(esContainerName)
	return c.runner.RemoveContainer(esContainerName)
}

func (c *ContainerInfraManager) waitForElasticsearch(timeout time.Duration) error {
	zap.S().Info("Waiting for Elasticsearch to be ready...")
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 2 * time.Second}
	for time.Now().Before(deadline) {
		resp, err := client.Get(fmt.Sprintf("http://localhost:%d/_cluster/health", esPort))
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				zap.S().Info("Elasticsearch is ready")
				return nil
			}
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("elasticsearch did not become ready within %v", timeout)
}

func (c *ContainerInfraManager) StartPlanner() error {
	zap.S().Info("Starting migration-planner...")
	_, err := c.runner.StartContainer(
		NewContainerConfig(plannerContainerName, c.plannerImage).
			WithPort(plannerPort, plannerPort).
			WithEnvVar("DB_TYPE", "pgsql").
			WithEnvVar("DB_HOST", "localhost").
			WithEnvVar("DB_PORT", "5432").
			WithEnvVar("DB_NAME", dbName).
			WithEnvVar("DB_USER", dbUser).
			WithEnvVar("DB_PASS", dbPassword).
			WithEnvVar("MIGRATION_PLANNER_MIGRATIONS_FOLDER", "/app/migrations").
			WithEnvVar("MIGRATION_PLANNER_AUTH", "none").
			WithEnvVar("MIGRATION_PLANNER_AGENT_AUTH_ENABLED", "false").
			WithEnvVar("MIGRATION_PLANNER_LOG_LEVEL", "debug").
			WithEnvVar("KAFKA_ENABLED", "true").
			WithEnvVar("KAFKA_BROKERS", "localhost:9092").
			WithEnvVar("MIGRATION_PLANNER_ISO_PATH", "/dev/null"),
	)
	if err != nil {
		return err
	}
	return c.waitForPlanner(60 * time.Second)
}

func (c *ContainerInfraManager) StopPlanner() error {
	_ = c.runner.StopContainer(plannerContainerName)
	return c.runner.RemoveContainer(plannerContainerName)
}

func (c *ContainerInfraManager) waitForPlanner(timeout time.Duration) error {
	zap.S().Info("Waiting for migration-planner to be ready...")
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 2 * time.Second}
	for time.Now().Before(deadline) {
		resp, err := client.Get(fmt.Sprintf("http://localhost:%d/health", plannerPort))
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				zap.S().Info("migration-planner is ready")
				return nil
			}
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("migration-planner did not become ready within %v", timeout)
}

func (c *ContainerInfraManager) StartStreamer() error {
	zap.S().Info("Starting migration-event-streamer...")
	_, err := c.runner.StartContainer(
		NewContainerConfig(streamerContainerName, c.streamerImage).
			WithCmd(
				"run",
				"--router-input-topic", "assisted.migration.events",
				"--kafka-brokers", "localhost:9092",
				"--elastic-host", "http://localhost:9200",
				"--elastic-indexes", "assessment,os,datastore,partner_customer,user_action",
			).
			WithEnvVar("STREAMER_NAMESPACE", e2eNamespace),
	)
	if err != nil {
		return err
	}
	zap.S().Info("migration-event-streamer started")
	return c.runner.WaitForRunning(streamerContainerName, 30*time.Second)
}

func (c *ContainerInfraManager) StopStreamer() error {
	_ = c.runner.StopContainer(streamerContainerName)
	return c.runner.RemoveContainer(streamerContainerName)
}

func (c *ContainerInfraManager) StreamerLogs() string {
	logs, err := c.runner.Logs(streamerContainerName)
	if err != nil {
		return fmt.Sprintf("failed to get streamer logs: %v", err)
	}
	return logs
}

func (c *ContainerInfraManager) PlannerLogs() string {
	logs, err := c.runner.Logs(plannerContainerName)
	if err != nil {
		return fmt.Sprintf("failed to get planner logs: %v", err)
	}
	return logs
}
