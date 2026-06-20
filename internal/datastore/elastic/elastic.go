package elastic

import (
	"fmt"
	"net/http"

	"github.com/kubev2v/migration-event-streamer/internal/config"
	"github.com/kubev2v/migration-event-streamer/internal/namespace"
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

func (e *ElasticRepository) CreateIndex(name string) error {
	fullName := fmt.Sprintf("%s_%s", e.base.indexPrefix, name)
	res, err := e.base.client.Indices.Exists([]string{fullName})
	if err != nil {
		return fmt.Errorf("failed to check if index %s exists: %w", name, err)
	}
	if res.StatusCode == http.StatusOK {
		_ = res.Body.Close()
		return nil
	}

	res, err = e.base.client.Indices.Create(fullName)
	if err != nil {
		return fmt.Errorf("failed to create index %s: %w", name, err)
	}
	if res.IsError() {
		return fmt.Errorf("failed to create index %s: %w", name, err)
	}
	_ = res.Body.Close()
	return nil
}
