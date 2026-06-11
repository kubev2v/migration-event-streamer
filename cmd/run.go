package cmd

import (
	"context"
	"fmt"
	"github.com/kubev2v/migration-event-streamer/internal/processors"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/fatih/color"
	"github.com/go-extras/cobraflags"
	"github.com/jzelinskie/cobrautil/v2"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v3"

	"github.com/kubev2v/migration-event-streamer/internal/config"
	"github.com/kubev2v/migration-event-streamer/internal/datastore"
	"github.com/kubev2v/migration-event-streamer/internal/pipeline"
)

var (
	InventoryPipeline string = "inventory"
)

type fileConfig struct {
	Pipelines []pipelineDef `yaml:"pipelines"`
	Router    struct {
		InputTopic string `yaml:"inputTopic"`
		Routes     []struct {
			EventType string `yaml:"eventType"`
			Topic     string `yaml:"topic"`
		} `yaml:"routes"`
	} `yaml:"router"`
	Elastic struct {
		IndexPrefix string   `yaml:"indexPrefix"`
		Indexes     []string `yaml:"indexes"`
	} `yaml:"elastic"`
}

type pipelineDef struct {
	Name       string `yaml:"name"`
	Type       string `yaml:"type"`
	InputTopic string `yaml:"inputTopic"`
}

func NewRunCommand(cfg *config.Configuration, version, gitCommit string) *cobra.Command {
	var (
		configFile       string
		pipelineFlags    []string
		routeFlags       []string
		routerInputTopic string
		elasticIndexes   []string
	)

	runCmd := &cobra.Command{
		Use:   "run",
		Short: "Start the streamer",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if configFile != "" {
				fc, err := loadConfigFile(configFile)
				if err != nil {
					return err
				}
				applyFileConfig(cmd, cfg, fc, &pipelineFlags, &routeFlags, &routerInputTopic, &elasticIndexes)
			}

			zap.S().Infow("starting migration-event-streamer",
				"version", version,
				"git_commit", gitCommit,
				"config", cfg.FlatDebugMap(),
			)

			pipelines, err := parsePipelines(pipelineFlags)
			if err != nil {
				return err
			}

			routes, err := parseRoutes(routeFlags)
			if err != nil {
				return err
			}

			cfg.ElasticSearch.Indexes = elasticIndexes

			go func() {
				http.Handle("/metrics", promhttp.Handler())
				_ = http.ListenAndServe(fmt.Sprintf(":%d", cfg.MetricsPort), nil)
			}()

			dt, err := createDatastore(cfg, pipelines, routerInputTopic)
			if err != nil {
				return err
			}

			ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGQUIT)
			defer cancel()

			if err := createPipelines(ctx, pipelines, routes, routerInputTopic, dt); err != nil {
				return err
			}

			<-ctx.Done()

			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()

			zap.S().Info("shutting down...")
			defer func() { zap.S().Info("streamer shutdown") }()

			g, gCtx := errgroup.WithContext(closeCtx)
			g.Go(func() error {
				return dt.Close(gCtx)
			})

			if err := g.Wait(); err != nil {
				zap.S().Errorf("closed with error: %s", err)
				return err
			}

			return nil
		},
	}

	runCmd.Flags().StringVar(&configFile, "config", "", "Path to YAML configuration file")
	registerFlags(runCmd, cfg, &pipelineFlags, &routeFlags, &routerInputTopic, &elasticIndexes)
	cobraflags.CobraOnInitialize("STREAMER", runCmd)

	return runCmd
}

func registerFlags(cmd *cobra.Command, cfg *config.Configuration, pipelineFlags, routeFlags *[]string, routerInputTopic *string, elasticIndexes *[]string) {
	nfs := cobrautil.NewNamedFlagSets(cmd)

	kafkaFlags := nfs.FlagSet(color.New(color.FgBlue, color.Bold).Sprint("Kafka"))
	registerKafkaFlags(kafkaFlags, cfg)

	elasticFlags := nfs.FlagSet(color.New(color.FgBlue, color.Bold).Sprint("ElasticSearch"))
	registerElasticFlags(elasticFlags, cfg, elasticIndexes)

	pipelineFlagSet := nfs.FlagSet(color.New(color.FgBlue, color.Bold).Sprint("Pipelines"))
	pipelineFlagSet.StringArrayVar(pipelineFlags, "pipeline", nil, "Pipeline definition as name:type:inputTopic (repeatable)")
	pipelineFlagSet.StringVar(routerInputTopic, "router-input-topic", "", "Router input topic")
	pipelineFlagSet.StringArrayVar(routeFlags, "route", nil, "Route definition as eventType=topic (repeatable)")

	observabilityFlags := nfs.FlagSet(color.New(color.FgBlue, color.Bold).Sprint("Observability"))
	observabilityFlags.IntVar(&cfg.MetricsPort, "metrics-port", cfg.MetricsPort, "Prometheus metrics port")

	nfs.AddFlagSets(cmd)
}

func registerKafkaFlags(flagSet *pflag.FlagSet, cfg *config.Configuration) {
	flagSet.StringSliceVar(&cfg.Kafka.Brokers, "kafka-brokers", cfg.Kafka.Brokers, "Kafka broker addresses")
	flagSet.StringVar(&cfg.Kafka.ClientID, "kafka-client-id", cfg.Kafka.ClientID, "Kafka client ID")
	flagSet.BoolVar(&cfg.Kafka.TLS, "kafka-tls", cfg.Kafka.TLS, "Enable TLS for Kafka connections")
	flagSet.StringVar(&cfg.Kafka.SASLUsername, "kafka-sasl-username", cfg.Kafka.SASLUsername, "SASL username for Kafka authentication")
	flagSet.StringVar(&cfg.Kafka.SASLPassword, "kafka-sasl-password", cfg.Kafka.SASLPassword, "SASL password for Kafka authentication")
	flagSet.StringVar(&cfg.Kafka.SASLMechanism, "kafka-sasl-mechanism", cfg.Kafka.SASLMechanism, "SASL mechanism (SCRAM-SHA-256 or SCRAM-SHA-512)")
}

func registerElasticFlags(flagSet *pflag.FlagSet, cfg *config.Configuration, indexes *[]string) {
	flagSet.StringVar(&cfg.ElasticSearch.Host, "elastic-host", cfg.ElasticSearch.Host, "Elasticsearch host URL")
	flagSet.StringVar(&cfg.ElasticSearch.Username, "elastic-username", cfg.ElasticSearch.Username, "Elasticsearch username")
	flagSet.StringVar(&cfg.ElasticSearch.Password, "elastic-password", cfg.ElasticSearch.Password, "Elasticsearch password")
	flagSet.StringVar(&cfg.ElasticSearch.IndexPrefix, "elastic-index-prefix", cfg.ElasticSearch.IndexPrefix, "Index prefix")
	flagSet.StringSliceVar(indexes, "elastic-indexes", nil, "Elasticsearch indexes to create")
	flagSet.BoolVar(&cfg.ElasticSearch.SSLInsecureSkipVerify, "elastic-ssl-insecure", cfg.ElasticSearch.SSLInsecureSkipVerify, "Skip SSL verification")
	flagSet.StringVar(&cfg.ElasticSearch.ResponseTimeout, "elastic-response-timeout", cfg.ElasticSearch.ResponseTimeout, "Elasticsearch response timeout")
	flagSet.StringVar(&cfg.ElasticSearch.DialTimeout, "elastic-dial-timeout", cfg.ElasticSearch.DialTimeout, "Elasticsearch dial timeout")
}

func parsePipelines(flags []string) ([]pipelineDef, error) {
	var pipelines []pipelineDef
	for _, f := range flags {
		parts := strings.SplitN(f, ":", 3)
		if len(parts) != 3 {
			return nil, fmt.Errorf("invalid pipeline format %q: expected name:type:inputTopic", f)
		}
		pipelines = append(pipelines, pipelineDef{
			Name:       parts[0],
			Type:       parts[1],
			InputTopic: parts[2],
		})
	}
	return pipelines, nil
}

func parseRoutes(flags []string) (map[string]string, error) {
	routes := make(map[string]string)
	for _, f := range flags {
		parts := strings.SplitN(f, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid route format %q: expected eventType=topic", f)
		}
		routes[parts[0]] = parts[1]
	}
	return routes, nil
}

func createDatastore(cfg *config.Configuration, pipelines []pipelineDef, routerInputTopic string) (*datastore.Datastore, error) {
	dt := datastore.NewDatastore().
		WithElasticRepository(cfg.ElasticSearch)

	for _, p := range pipelines {
		dt.WithKafkaConsumer(p.InputTopic, cfg.Kafka, p.InputTopic, fmt.Sprintf("consumer-group-%s", p.InputTopic))
	}

	if routerInputTopic != "" {
		dt.WithKafkaConsumer(routerInputTopic, cfg.Kafka, routerInputTopic, fmt.Sprintf("consumer-group-%s", routerInputTopic))
	}

	dt.WithKafkaProducer("producer", cfg.Kafka)
	if err := dt.Build(); err != nil {
		return nil, err
	}

	return dt, nil
}

func createPipelines(ctx context.Context, pipelines []pipelineDef, routes map[string]string, routerInputTopic string, dt *datastore.Datastore) error {
	m := pipeline.NewManager()
	for _, p := range pipelines {
		switch p.Type {
		case InventoryPipeline:
			pipeline.AddPipeline(m, ctx, p.Name, dt.MustHaveConsumer(p.InputTopic), processors.InventoryProcessor, dt.ElasticRepository().WriteInventory)
		}
	}

	if len(routes) > 0 && routerInputTopic != "" {
		m.Router(ctx, dt.MustHaveConsumer(routerInputTopic), dt.MustHaveProducer("producer"), routes)
	}

	if err := m.Build(ctx); err != nil {
		return err
	}

	return nil
}

func loadConfigFile(path string) (*fileConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}
	var fc fileConfig
	if err := yaml.Unmarshal(data, &fc); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}
	return &fc, nil
}

func applyFileConfig(cmd *cobra.Command, cfg *config.Configuration, fc *fileConfig, pipelineFlags, routeFlags *[]string, routerInputTopic *string, elasticIndexes *[]string) {
	if len(*pipelineFlags) == 0 {
		for _, p := range fc.Pipelines {
			*pipelineFlags = append(*pipelineFlags, fmt.Sprintf("%s:%s:%s", p.Name, p.Type, p.InputTopic))
		}
	}
	if *routerInputTopic == "" {
		*routerInputTopic = fc.Router.InputTopic
	}
	if len(*routeFlags) == 0 {
		for _, r := range fc.Router.Routes {
			*routeFlags = append(*routeFlags, fmt.Sprintf("%s=%s", r.EventType, r.Topic))
		}
	}
	if len(*elasticIndexes) == 0 {
		*elasticIndexes = fc.Elastic.Indexes
	}
	if f := cmd.Flags().Lookup("elastic-index-prefix"); f != nil && !f.Changed && fc.Elastic.IndexPrefix != "" {
		cfg.ElasticSearch.IndexPrefix = fc.Elastic.IndexPrefix
	}
}
