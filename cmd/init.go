package cmd

import (
	"github.com/go-extras/cobraflags"
	"github.com/kubev2v/migration-event-streamer/internal/config"
	"github.com/kubev2v/migration-event-streamer/internal/namespace"
	pkgkafka "github.com/kubev2v/migration-event-streamer/pkg/kafka"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

const (
	inputTopicPartitions        = 3
	inputTopicReplicationFactor = 3

	envTopicPartitions        = 3
	envTopicReplicationFactor = 3
)

func NewInitCommand(cfg *config.Configuration) *cobra.Command {
	var routerInputTopic string

	initCmd := &cobra.Command{
		Use:   "init",
		Short: "Ensure required Kafka topics exist",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			envTopic := namespace.Topic()

			zap.S().Infow("ensuring input topic exists", "topic", routerInputTopic)
			opts := cfg.Kafka.ConnKgoOpts()
			if err := pkgkafka.EnsureTopic(cfg.Kafka.Brokers, opts, routerInputTopic, inputTopicPartitions, inputTopicReplicationFactor); err != nil {
				return err
			}

			zap.S().Infow("ensuring env topic exists", "topic", envTopic)
			if err := pkgkafka.EnsureTopic(cfg.Kafka.Brokers, opts, envTopic, envTopicPartitions, envTopicReplicationFactor); err != nil {
				return err
			}

			zap.S().Info("topic initialization complete")
			return nil
		},
	}

	initCmd.Flags().StringSliceVar(&cfg.Kafka.Brokers, "kafka-brokers", cfg.Kafka.Brokers, "Kafka broker addresses")
	initCmd.Flags().BoolVar(&cfg.Kafka.TLS, "kafka-tls", cfg.Kafka.TLS, "Enable TLS for Kafka connections")
	initCmd.Flags().BoolVar(&cfg.Kafka.SASLEnabled, "kafka-sasl-enabled", cfg.Kafka.SASLEnabled, "Enable SASL authentication for Kafka")
	initCmd.Flags().StringVar(&cfg.Kafka.SASLUsername, "kafka-sasl-username", cfg.Kafka.SASLUsername, "SASL username for Kafka authentication")
	initCmd.Flags().StringVar(&cfg.Kafka.SASLPassword, "kafka-sasl-password", cfg.Kafka.SASLPassword, "SASL password for Kafka authentication")
	initCmd.Flags().StringVar(&routerInputTopic, "router-input-topic", "", "Shared input topic")
	_ = initCmd.MarkFlagRequired("router-input-topic")

	cobraflags.CobraOnInitialize("STREAMER", initCmd)

	return initCmd
}
