package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tupyy/migration-event-streamer/internal/clients"
	"github.com/tupyy/migration-event-streamer/internal/datastore"
	"github.com/tupyy/migration-event-streamer/internal/logger"
	"github.com/tupyy/migration-event-streamer/internal/services"
	"go.uber.org/zap"
)

func main() {
	logger := logger.SetupLogger()
	defer logger.Sync()

	undo := zap.ReplaceGlobals(logger)
	defer undo()

	pgConnection, err := clients.NewPgConnectionFromEnv()
	if err != nil {
		panic(err)
	}

	elasticConfig, err := clients.GetElasticConfigFromEnv()
	if err != nil {
		panic(err)
	}

	es, err := clients.NewElasticsearchClient(elasticConfig)
	if err != nil {
		panic(err)
	}

	dt, err := datastore.NewDatastore(pgConnection, es, elasticConfig)
	if err != nil {
		panic(err)
	}

	indexes := []string{"os", "datastore", "inventory"}
	if err := dt.CreateIndexes(elasticConfig.Index, indexes); err != nil {
		panic(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGQUIT)
	defer cancel()

	tickPeriod := 5 * time.Second
	if tick := os.Getenv("TICK_PERIOD"); tick != "" {
		if newPeriod, err := time.ParseDuration(tick); err == nil {
			tickPeriod = newPeriod
		}
	}

	srv := services.NewInventory(dt, tickPeriod)
	go srv.Run(ctx)

	<-ctx.Done()
}
