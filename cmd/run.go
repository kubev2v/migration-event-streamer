package cmd

import (
	"context"
	"fmt"
	"github.com/fatih/color"
	"github.com/go-extras/cobraflags"
	"github.com/jzelinskie/cobrautil/v2"
	"github.com/kubev2v/migration-event-streamer/internal/config"
	"github.com/kubev2v/migration-event-streamer/internal/datastore"
	"github.com/kubev2v/migration-event-streamer/internal/namespace"
	"github.com/kubev2v/migration-event-streamer/internal/pipeline"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func NewRunCommand(cfg *config.Configuration, version, gitCommit string) *cobra.Command {
	var routerInputTopic string

	runCmd := &cobra.Command{
		Use:   "run",
		Short: "Start the streamer",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			zap.S().Infow("starting migration-event-streamer",
				"version", version,
				"git_commit", gitCommit,
				"namespace", namespace.Namespace(),
				"config", cfg.FlatDebugMap(),
			)

			go func() {
				http.Handle("/metrics", promhttp.Handler())
				_ = http.ListenAndServe(fmt.Sprintf(":%d", cfg.MetricsPort), nil)
			}()

			envTopic := namespace.Topic()

			dt, err := createDatastore(cfg, routerInputTopic, envTopic)
			if err != nil {
				return err
			}

			defer func() {
				closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer closeCancel()
				zap.S().Info("shutting down...")
				if err := dt.Close(closeCtx); err != nil {
					zap.S().Errorf("closed with error: %s", err)
				}
				zap.S().Info("streamer shutdown")
			}()

			ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGQUIT)
			defer cancel()

			routerConsumer := dt.MustHaveConsumer("router")
			dispatcherConsumer := dt.MustHaveConsumer("dispatcher")
			producer := dt.MustHaveProducer("producer")

			m, err := pipeline.NewManager(ctx, routerConsumer, dispatcherConsumer, producer)
			if err != nil {
				return err
			}

			m.InitAllPipelines(dt.ElasticRepository())
			m.Start(ctx, cancel)

			zap.S().Infow("pipelines started",
				"router_input_topic", routerInputTopic,
				"env_topic", envTopic,
			)

			<-ctx.Done()
			m.Wait()

			return nil
		},
	}

	registerFlags(runCmd, cfg, &routerInputTopic, &cfg.ElasticSearch.Indexes)
	cobraflags.CobraOnInitialize("STREAMER", runCmd)

	return runCmd
}

func registerFlags(cmd *cobra.Command, cfg *config.Configuration, routerInputTopic *string, elasticIndexes *[]string) {
	nfs := cobrautil.NewNamedFlagSets(cmd)

	kafkaFlags := nfs.FlagSet(color.New(color.FgBlue, color.Bold).Sprint("Kafka"))
	registerKafkaFlags(kafkaFlags, cfg)

	elasticFlags := nfs.FlagSet(color.New(color.FgBlue, color.Bold).Sprint("ElasticSearch"))
	registerElasticFlags(elasticFlags, cfg, elasticIndexes)

	pipelineFlagSet := nfs.FlagSet(color.New(color.FgBlue, color.Bold).Sprint("Pipelines"))
	pipelineFlagSet.StringVar(routerInputTopic, "router-input-topic", "", "Shared input topic")
	_ = cmd.MarkFlagRequired("router-input-topic")

	observabilityFlags := nfs.FlagSet(color.New(color.FgBlue, color.Bold).Sprint("Observability"))
	observabilityFlags.IntVar(&cfg.MetricsPort, "metrics-port", cfg.MetricsPort, "Prometheus metrics port")

	nfs.AddFlagSets(cmd)
}

func registerKafkaFlags(flagSet *pflag.FlagSet, cfg *config.Configuration) {
	flagSet.StringSliceVar(&cfg.Kafka.Brokers, "kafka-brokers", cfg.Kafka.Brokers, "Kafka broker addresses")
	flagSet.StringVar(&cfg.Kafka.ClientID, "kafka-client-id", cfg.Kafka.ClientID, "Kafka client ID")
	flagSet.BoolVar(&cfg.Kafka.TLS, "kafka-tls", cfg.Kafka.TLS, "Enable TLS for Kafka connections")
	flagSet.BoolVar(&cfg.Kafka.SASLEnabled, "kafka-sasl-enabled", cfg.Kafka.SASLEnabled, "Enable SASL authentication for Kafka")
	flagSet.StringVar(&cfg.Kafka.SASLUsername, "kafka-sasl-username", cfg.Kafka.SASLUsername, "SASL username for Kafka authentication")
	flagSet.StringVar(&cfg.Kafka.SASLPassword, "kafka-sasl-password", cfg.Kafka.SASLPassword, "SASL password for Kafka authentication")
}

func registerElasticFlags(flagSet *pflag.FlagSet, cfg *config.Configuration, indexes *[]string) {
	flagSet.StringVar(&cfg.ElasticSearch.Host, "elastic-host", cfg.ElasticSearch.Host, "Elasticsearch host URL")
	flagSet.StringVar(&cfg.ElasticSearch.Username, "elastic-username", cfg.ElasticSearch.Username, "Elasticsearch username")
	flagSet.StringVar(&cfg.ElasticSearch.Password, "elastic-password", cfg.ElasticSearch.Password, "Elasticsearch password")
	flagSet.StringSliceVar(indexes, "elastic-indexes", nil, "Elasticsearch indexes to create")
	flagSet.BoolVar(&cfg.ElasticSearch.SSLInsecureSkipVerify, "elastic-ssl-insecure", cfg.ElasticSearch.SSLInsecureSkipVerify, "Skip SSL verification")
	flagSet.StringVar(&cfg.ElasticSearch.ResponseTimeout, "elastic-response-timeout", cfg.ElasticSearch.ResponseTimeout, "Elasticsearch response timeout")
	flagSet.StringVar(&cfg.ElasticSearch.DialTimeout, "elastic-dial-timeout", cfg.ElasticSearch.DialTimeout, "Elasticsearch dial timeout")
}

func createDatastore(cfg *config.Configuration, routerInputTopic, envTopic string) (*datastore.Datastore, error) {
	dt := datastore.NewDatastore().WithElasticRepository(cfg.ElasticSearch)

	dt.WithKafkaConsumer("router", cfg.Kafka, routerInputTopic, fmt.Sprintf("consumer-group-%s", routerInputTopic))
	dt.WithKafkaConsumer("dispatcher", cfg.Kafka, envTopic, fmt.Sprintf("consumer-group-%s", envTopic))
	dt.WithKafkaProducer("producer", cfg.Kafka)

	if err := dt.Build(); err != nil {
		return nil, err
	}

	return dt, nil
}
