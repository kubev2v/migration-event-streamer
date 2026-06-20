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
	var (
		pipelineFlags    []string
		routerInputTopic string
	)

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

			if err := createAndStartPipelines(ctx, pipelineFlags, routerInputTopic, envTopic, dt); err != nil {
				return err
			}

			<-ctx.Done()

			return nil
		},
	}

	registerFlags(runCmd, cfg, &pipelineFlags, &routerInputTopic, &cfg.ElasticSearch.Indexes)
	cobraflags.CobraOnInitialize("STREAMER", runCmd)

	return runCmd
}

func registerFlags(cmd *cobra.Command, cfg *config.Configuration, pipelineFlags *[]string, routerInputTopic *string, elasticIndexes *[]string) {
	nfs := cobrautil.NewNamedFlagSets(cmd)

	kafkaFlags := nfs.FlagSet(color.New(color.FgBlue, color.Bold).Sprint("Kafka"))
	registerKafkaFlags(kafkaFlags, cfg)

	elasticFlags := nfs.FlagSet(color.New(color.FgBlue, color.Bold).Sprint("ElasticSearch"))
	registerElasticFlags(elasticFlags, cfg, elasticIndexes)

	pipelineFlagSet := nfs.FlagSet(color.New(color.FgBlue, color.Bold).Sprint("Pipelines"))
	pipelineFlagSet.StringArrayVar(pipelineFlags, "pipeline", nil, "Pipeline definition as entity.action (e.g., assessment.created)")
	pipelineFlagSet.StringVar(routerInputTopic, "router-input-topic", "", "Shared input topic")
	_ = cmd.MarkFlagRequired("router-input-topic")

	observabilityFlags := nfs.FlagSet(color.New(color.FgBlue, color.Bold).Sprint("Observability"))
	observabilityFlags.IntVar(&cfg.MetricsPort, "metrics-port", cfg.MetricsPort, "Prometheus metrics port")

	nfs.AddFlagSets(cmd)
}

func registerKafkaFlags(flagSet *pflag.FlagSet, cfg *config.Configuration) {
	flagSet.StringSliceVar(&cfg.Kafka.Brokers, "kafka-brokers", cfg.Kafka.Brokers, "Kafka broker addresses")
	flagSet.StringVar(&cfg.Kafka.ClientID, "kafka-client-id", cfg.Kafka.ClientID, "Kafka client ID")
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

func createAndStartPipelines(ctx context.Context, pipelineFlags []string, routerInputTopic, envTopic string, dt *datastore.Datastore) error {
	routerConsumer := dt.MustHaveConsumer("router")
	dispatcherConsumer := dt.MustHaveConsumer("dispatcher")
	producer := dt.MustHaveProducer("producer")
	repo := dt.ElasticRepository()

	m, err := pipeline.NewManager(ctx, routerConsumer, dispatcherConsumer, producer)
	if err != nil {
		return err
	}

	for _, key := range pipelineFlags {
		switch key {
		case pipeline.AssessmentCreated:
			m.WithAssessmentCreatedPipeline(repo)
		case pipeline.AssessmentDeleted:
			m.WithAssessmentDeletedPipeline(repo)
		case pipeline.UserActionVisited:
			m.WithVisitedPipeline(repo)
		case pipeline.PartnerCustomerUpdated:
			m.WithPartnerCustomerUpdatedPipeline(repo)
		case pipeline.UserActionShared:
			m.WithShareAssessmentPipeline(repo)
		case pipeline.UserActionUnshared:
			m.WithUnshareAssessmentPipeline(repo)
		case pipeline.UserActionSizingRequested:
			m.WithSizingRequestedPipeline(repo)
		case pipeline.UserActionComplexity:
			m.WithComplexityEstimatedPipeline(repo)
		case pipeline.UserActionOVADownloaded:
			m.WithOVADownloadedPipeline(repo)
		default:
			return fmt.Errorf("unknown pipeline %q", key)
		}
		zap.S().Infow("pipeline registered", "key", key)
	}

	m.Start(ctx)

	zap.S().Infow("pipelines started",
		"router_input_topic", routerInputTopic,
		"env_topic", envTopic,
		"pipelines", pipelineFlags,
	)

	return nil
}
