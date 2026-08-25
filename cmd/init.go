package cmd

import (
	"context"
	"fmt"
	"time"

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
	var aclPrincipal string

	initCmd := &cobra.Command{
		Use:   "init",
		Short: "Ensure required Kafka topics exist and grant consumer ACLs",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			envTopic := namespace.Topic()

			ctx, cancel := context.WithTimeout(cmd.Context(), 2*time.Minute)
			defer cancel()

			adm, err := pkgkafka.NewAdminClient(cfg.Kafka.Brokers, cfg.Kafka.ConnKgoOpts())
			if err != nil {
				return err
			}
			defer adm.Close()

			zap.S().Infow("ensuring input topic exists", "topic", routerInputTopic)
			if err := pkgkafka.EnsureTopic(ctx, adm, routerInputTopic, inputTopicPartitions, inputTopicReplicationFactor); err != nil {
				return err
			}

			zap.S().Infow("ensuring env topic exists", "topic", envTopic)
			if err := pkgkafka.EnsureTopic(ctx, adm, envTopic, envTopicPartitions, envTopicReplicationFactor); err != nil {
				return err
			}

			// Grant the service (consumer) principal the ACLs it needs to read
			// from the topics we just ensured.
			topics := []string{routerInputTopic, envTopic}
			groups := []string{
				fmt.Sprintf("consumer-group-%s", routerInputTopic),
				fmt.Sprintf("consumer-group-%s", envTopic),
			}
			zap.S().Infow("granting consumer ACLs", "principal", aclPrincipal, "topics", topics, "groups", groups)
			if err := pkgkafka.GrantConsumerACLs(ctx, adm, aclPrincipal, topics, groups); err != nil {
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
	initCmd.Flags().StringVar(&aclPrincipal, "acl-principal", "", "Kafka principal (SASL username) to grant read access on the managed topics and consumer groups")

	cobraflags.CobraOnInitialize("STREAMER", initCmd)

	return initCmd
}
