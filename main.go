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

	dt, err := datastore.NewDatastore(elasticConfig, kConfig)
	if err != nil {
		panic(err)
	}

	indexes := []string{"os", "datastore", "inventory"}
	if err := dt.CreateIndexes(elasticConfig.Index, indexes); err != nil {
		panic(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGQUIT)
	defer cancel()

	consumer, err := dt.CreateKafkaConsumer("test", "test-consumer-group")
	if err != nil {
		zap.S().Fatalf("failed to create kafka consumer: %s", err)
	}

	m := pipeline.NewManager()

	if err := m.CreateElasticPipeline(ctx, consumer, dt.ElasticRepository(), worker.InventoryWorker); err != nil {
		zap.S().Fatalf("failed to create elastic pipeline: %w", err)
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
	g.Go(func() error {
		return m.Close(ctx)
	})

	if err := g.Wait(); err != nil {
		zap.S().Errorf("closed with error: %s", err)
		return
	}
}
