package elastic

import (
	"context"
	"fmt"

	"github.com/kubev2v/migration-event-streamer/internal/config"
	"github.com/kubev2v/migration-event-streamer/internal/namespace"
	opensearchapi "github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

type ElasticRepository struct {
	assessment      AssessmentWriter
	userAction      UserActionWriter
	partnerCustomer PartnerCustomerWriter
	base            *baseWriter
}

func NewElasticRepository(cfg config.ElasticSearch) (*ElasticRepository, error) {
	client, err := NewElasticsearchClient(cfg)
	if err != nil {
		return nil, err
	}

	base := &baseWriter{client: client, indexPrefix: namespace.IndexPrefix()}

	return &ElasticRepository{
		assessment:      &assessmentWriter{base: base},
		userAction:      &userActionWriter{base: base},
		partnerCustomer: &partnerCustomerWriter{base: base},
		base:            base,
	}, nil
}

func (e *ElasticRepository) Assessment() AssessmentWriter           { return e.assessment }
func (e *ElasticRepository) UserAction() UserActionWriter           { return e.userAction }
func (e *ElasticRepository) PartnerCustomer() PartnerCustomerWriter { return e.partnerCustomer }

func (e *ElasticRepository) CreateIndex(ctx context.Context, name string) error {
	fullName := fmt.Sprintf("%s_%s", e.base.indexPrefix, name)

	// Check if index exists
	_, err := e.base.client.Indices.Exists(ctx, opensearchapi.IndicesExistsReq{
		Indices: []string{fullName},
	})
	if err == nil {
		// Index exists
		return nil
	}

	// Create index
	_, err = e.base.client.Indices.Create(ctx, opensearchapi.IndicesCreateReq{
		Index: fullName,
	})
	if err != nil {
		return fmt.Errorf("failed to create index %s: %w", name, err)
	}

	return nil
}
