package service

import (
	"encoding/json"
	"fmt"
	"github.com/kubev2v/migration-event-streamer/internal/entity"
	"go.uber.org/zap"
	"io"

	elastic "github.com/elastic/go-elasticsearch/v8"
)

const (
	assessmentIndex = "migration_planner_assessment"
)

type ElasticsearchService struct {
	client *elastic.Client
}

func NewElasticsearchService(address string) (*ElasticsearchService, error) {
	client, err := elastic.NewClient(elastic.Config{
		Addresses: []string{address},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create elasticsearch client: %w", err)
	}
	return &ElasticsearchService{client: client}, nil
}

func (s *ElasticsearchService) GetAssessment(id string) (*entity.Assessment, error) {
	var doc entity.Assessment
	if err := s.getDocument(assessmentIndex, id, &doc); err != nil {
		return nil, err
	}
	zap.S().Infof("Found assessment document: %s", id)
	return &doc, nil
}

func (s *ElasticsearchService) getDocument(index, id string, target any) error {
	res, err := s.client.Get(index, id)
	if err != nil {
		return fmt.Errorf("failed to get document: %w", err)
	}
	defer func() { _ = res.Body.Close() }()

	if res.IsError() {
		return fmt.Errorf("elasticsearch returned %s", res.Status())
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	var hit struct {
		Source json.RawMessage `json:"_source"`
	}
	if err := json.Unmarshal(body, &hit); err != nil {
		return fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return json.Unmarshal(hit.Source, target)
}
