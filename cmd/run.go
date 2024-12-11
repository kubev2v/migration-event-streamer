package cmd

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/kubev2v/migration-event-streamer/internal/config"
	"github.com/kubev2v/migration-event-streamer/internal/datastore"
	"github.com/kubev2v/migration-event-streamer/internal/logger"
	"github.com/kubev2v/migration-event-streamer/internal/pipeline"
	"github.com/kubev2v/migration-event-streamer/internal/worker"
	basicWorker "github.com/kubev2v/migration-event-streamer/samples/worker"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	InventoryPipeline string = "inventory"
	UiPipeline        string = "ui"
	AgentPipeline     string = "agent"
	GitCommit         string
)

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:          "run",
	Short:        "start the streamer",
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := logger.SetupLogger()
		defer logger.Sync()

		undo := zap.ReplaceGlobals(logger)
		defer undo()

		// read config file
		configData, err := os.ReadFile(cfgFile)
		if err != nil {
			zap.S().Fatalf("failed to read config file %s: %s", cfgFile, err)
		}

		if err := viper.ReadConfig(bytes.NewBuffer(configData)); err != nil {
			zap.S().Fatalf("failed to read config: %v", err)
		}

		var c config.StreamerConfig

		if err := viper.Unmarshal(&c); err != nil {
			zap.S().Fatal("failed to read configuration: %s", err)
		}

		// get env values
		c.Elastic.Username = viper.GetString("elasticsearch_username")
		c.Elastic.Password = viper.GetString("elasticsearch_password")

		zap.S().Infof("git commit: %s", GitCommit)
		zap.S().Debugf("using config: %+v", c)

		// start prometheus
		http.Handle("/metrics", promhttp.Handler())
		go http.ListenAndServe(":8080", nil)

		dt, err := createDatastore(c)
		if err != nil {
			zap.S().Fatal(err)
		}

		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGQUIT)
		defer cancel()

		if err := createPipelines(ctx, c, dt); err != nil {
			zap.S().Fatal(err)
		}

		<-ctx.Done()

		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()

		zap.S().Info("shutting down...")
		defer func() {
			zap.S().Info("streamer shutdown")
		}()

		g, ctx := errgroup.WithContext(closeCtx)
		g.Go(func() error {
			return dt.Close(ctx)
		})

		if err := g.Wait(); err != nil {
			zap.S().Errorf("closed with error: %s", err)
			return err
		}

		return nil
	},
}

func createDatastore(c config.StreamerConfig) (*datastore.Datastore, error) {
	// create datastore
	dt := datastore.NewDatastore().
		WithElasticRepository(c.Elastic)

	for _, p := range c.Pipelines {
		dt.WithKafkaConsumer(p.InputTopic, c.Kafka, p.InputTopic, fmt.Sprintf("consumer-group-%s", p.InputTopic))
	}

	// create consumer for the router if any
	if c.Router.InputTopic != "" {
		dt.WithKafkaConsumer(c.Router.InputTopic, c.Kafka, c.Router.InputTopic, fmt.Sprintf("consumer-group-%s", c.Router.InputTopic))
	}

	dt.WithKafkaProducer("producer", c.Kafka)
	if err := dt.Build(); err != nil {
		return nil, err
	}

	return dt, nil
}

func createPipelines(ctx context.Context, c config.StreamerConfig, dt *datastore.Datastore) error {
	m := pipeline.NewManager()
	for _, p := range c.Pipelines {
		switch p.Type {
		case InventoryPipeline:
			m.ElasticPipeline(ctx, InventoryPipeline, dt.MustHaveConsumer(p.InputTopic), dt.ElasticRepository(), worker.InventoryWorker)
		case UiPipeline:
			m.ElasticPipeline(ctx, UiPipeline, dt.MustHaveConsumer(p.InputTopic), dt.ElasticRepository(), basicWorker.BasicWorker)
		case AgentPipeline:
			m.ElasticPipeline(ctx, AgentPipeline, dt.MustHaveConsumer(p.InputTopic), dt.ElasticRepository(), basicWorker.BasicWorker)
		}
	}

	routes := map[string]string{}
	for _, r := range c.Router.Routes {
		routes[r.EventType] = r.Topic
	}
	if len(routes) > 0 {
		m.Router(ctx, dt.MustHaveConsumer(c.Router.InputTopic), dt.MustHaveProducer("producer"), routes)
	}

	if err := m.Build(ctx); err != nil {
		return err
	}

	return nil
}

func init() {
	viper.SetEnvPrefix("streamer")
	viper.SetConfigType("yaml")
	viper.BindEnv("elasticsearch_username")
	viper.BindEnv("elasticsearch_password")
	viper.AutomaticEnv()

	// set defaults
	viper.Set("elastic.responseTimeout", "90s")
	viper.Set("elastic.dialTimeout", "1s")
	viper.Set("sslInsecureSkipVerify", true)

	rootCmd.AddCommand(runCmd)
}
