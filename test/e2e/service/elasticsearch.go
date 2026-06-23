package service

import (
	"encoding/json"
	"fmt"
	"io"

	elastic "github.com/elastic/go-elasticsearch/v8"
	"go.uber.org/zap"
)

const (
	assessmentIndex = "migration_planner_assessment"
)

type AssessmentDocument struct {
	AssessmentID string  `json:"assessment_id"`
	EventSource  string  `json:"event_source"`
	Name         string  `json:"name"`
	OrgID        string  `json:"org_id"`
	Username     string  `json:"username"`
	SourceType   string  `json:"source_type"`
	PartnerID    *string `json:"partner_id,omitempty"`
	Status       string  `json:"status"`
	CreatedAt    string  `json:"created_at"`
	SnapshotID   string  `json:"snapshot_id"`
	VCenterID    string  `json:"vcenter_id"`
	TotalVMs     int     `json:"total_vms"`
	TotalHosts   int     `json:"total_hosts"`
}

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

func (s *ElasticsearchService) GetAssessment(id string) (*AssessmentDocument, error) {
	res, err := s.client.Get(assessmentIndex, id)
	if err != nil {
		return nil, fmt.Errorf("failed to get document: %w", err)
	}
	defer func() { _ = res.Body.Close() }()

	if res.IsError() {
		return nil, fmt.Errorf("elasticsearch returned %s", res.Status())
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	var hit struct {
		Source AssessmentDocument `json:"_source"`
	}
	if err := json.Unmarshal(body, &hit); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	zap.S().Infof("Found assessment document: %s", id)
	return &hit.Source, nil
}
