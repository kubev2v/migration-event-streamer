package elastic

import (
	"context"
	"fmt"
	"net/http"

	elastic "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/kubev2v/migration-event-streamer/internal/config"
	"github.com/kubev2v/migration-event-streamer/internal/entity"
)

// ElasticRepository implements datastore.Writer interface
type ElasticRepository struct {
	client      *elastic.Client
	indexPrefix string
}

func NewElasticRepository(config config.ElasticSearchConfig) (*ElasticRepository, error) {
	elasticClient, err := NewElasticsearchClient(config)
	if err != nil {
		return nil, err
	}
	elasticDt := &ElasticRepository{
		client:      elasticClient,
		indexPrefix: config.IndexPrefix,
	}

	return elasticDt, nil
}

func (e *ElasticRepository) Write(ctx context.Context, event entity.Event) error {
	req := esapi.IndexRequest{
		Index:      fmt.Sprintf("%s_%s", e.indexPrefix, event.Index),
		DocumentID: event.ID,
		Body:       event.Body,
	}
	res, err := req.Do(ctx, e.client)
	if err != nil {
		return fmt.Errorf("failed to insert document %s: %w", event.ID, err)
	}
	if res.IsError() {
		return fmt.Errorf("failed to index document %s: %s", event.ID, res.Status())
	}
	return nil
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
