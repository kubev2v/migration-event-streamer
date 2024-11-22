package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tupyy/migration-event-streamer/internal/config"
	"github.com/tupyy/migration-event-streamer/internal/datastore"
	"github.com/tupyy/migration-event-streamer/internal/logger"
	"github.com/tupyy/migration-event-streamer/internal/pipeline"
	"github.com/tupyy/migration-event-streamer/internal/worker"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func main() {
	logger := logger.SetupLogger()
	defer logger.Sync()

	undo := zap.ReplaceGlobals(logger)
	defer undo()

	elasticConfig, err := config.GetElasticConfigFromEnv()
	if err != nil {
		panic(err)
	}

	kConfig := config.KafkaConfig{
		Brokers:  []string{"localhost:9092"},
		ClientID: "test-client",
	}

	dt := datastore.NewDatastore().
		WithElasticRepository(elasticConfig, []string{"index1", "index2"}).
		WithKafkaConsumer("test", kConfig, "test", "test-consumer-group").
		WithKafkaConsumer("test1", kConfig, "test-output", "another-group").
		WithKafkaProducer("producer", kConfig)
	if dt.Build(); err != nil {
		zap.S().Fatal(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGQUIT)
	defer cancel()

	m := pipeline.NewManager().
		ElasticPipeline(ctx, "elastic", dt.MustHaveConsumer("test1"), dt.ElasticRepository(), worker.InventoryWorker).
		Router(ctx, dt.MustHaveConsumer("test"), dt.MustHaveProducer("producer"), map[string]string{"sample.basic.event": "test-output"})

	if err := m.Build(ctx); err != nil {
		zap.S().Infof("failed to create pipeline manager: %v", err)
		cancel()
	}

	<-ctx.Done()

	closeCtx, _ := context.WithTimeout(context.Background(), 5*time.Second)

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
		return
	}
}
