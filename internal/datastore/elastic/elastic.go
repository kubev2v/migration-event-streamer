package elastic

import (
	"context"
	"fmt"
	"net/http"

	elastic "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/tupyy/migration-event-streamer/internal/config"
	"github.com/tupyy/migration-event-streamer/internal/entity"
	"go.uber.org/zap"
)

// ElasticRepository implements datastore.Writer interface
type ElasticRepository struct {
	client      *elastic.Client
	bulkIndexer esutil.BulkIndexer
	index       string
	docIDPrefix string
}

func NewElasticRepository(config config.ElasticSearchEnvConfig) (*ElasticRepository, error) {
	elasticClient, err := NewElasticsearchClient(config)
	if err != nil {
		return nil, err
	}
	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:        elasticClient,          // The Elasticsearch client
		NumWorkers:    config.NumWorkers,      // The number of worker goroutines
		FlushBytes:    int(config.FlushBytes), // The flush threshold in bytes
		FlushInterval: config.FlushInterval,   // The periodic flush interval
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create the bulk indexer: %w", err)
	}

	elasticDt := &ElasticRepository{
		client:      elasticClient,
		bulkIndexer: bi,
		index:       config.Index,
		docIDPrefix: config.DocIdPrefix,
	}

	return elasticDt, nil
}

func (e *ElasticRepository) Write(ctx context.Context, event entity.Event) error {
	return e.bulkIndexer.Add(ctx, esutil.BulkIndexerItem{
		Index:      fmt.Sprintf("%s_%s", e.index, event.Index),
		Action:     "index",
		DocumentID: event.ID,
		Body:       event.Body,
		OnSuccess: func(ctx context.Context, bii esutil.BulkIndexerItem, biri esutil.BulkIndexerResponseItem) {
			zap.S().Debugf("item sucessfully added: %s", event.ID)
		},
		OnFailure: func(ctx context.Context, bii esutil.BulkIndexerItem, biri esutil.BulkIndexerResponseItem, err error) {
			zap.S().Errorf("failed to add item to indexer: %w", err)
		},
	})
}

func (e *ElasticRepository) CreateIndex(name string) error {
	// check if index exists
	res, err := e.client.Indices.Exists([]string{name})
	if err != nil {
		return fmt.Errorf("failed to check if index %s exists: %w", name, err)
	}
	if res.StatusCode == http.StatusOK {
		res.Body.Close()
		return nil
	}

	// create the index
	res, err = e.client.Indices.Create(name)
	if err != nil {
		return fmt.Errorf("failed to create index %s: %w", name, err)
	}
	if res.IsError() {
		return fmt.Errorf("failed to create index %s: %w", name, err)
	}
	res.Body.Close()
	return nil
}
