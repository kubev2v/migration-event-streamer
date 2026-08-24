package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/kubev2v/migration-event-streamer/internal/entity"
	opensearch "github.com/opensearch-project/opensearch-go/v4"
	opensearchapi "github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"go.uber.org/zap"
)

const (
	assessmentIndex = "migration_planner_assessment"
	osIndex         = "migration_planner_os"
	datastoreIndex  = "migration_planner_datastore"
	userActionIndex = "migration_planner_user_action"
)

type UserActionDocument struct {
	ActionType   string `json:"action_type"`
	Username     string `json:"username"`
	AssessmentID string `json:"assessment_id,omitempty"`
	SourceID     string `json:"source_id,omitempty"`
	OrgID        string `json:"org_id,omitempty"`
	Timestamp    string `json:"timestamp"`
}

type ElasticsearchService struct {
	client *opensearchapi.Client
}

func NewElasticsearchService(address string) (*ElasticsearchService, error) {
	client, err := opensearchapi.NewClient(opensearchapi.Config{
		Client: opensearch.Config{
			Addresses: []string{address},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create opensearch client: %w", err)
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
	ctx := context.Background()
	res, err := s.client.Document.Get(ctx, opensearchapi.DocumentGetReq{
		Index:      index,
		DocumentID: id,
	})
	if err != nil {
		return fmt.Errorf("failed to get document: %w", err)
	}

	return json.Unmarshal(res.Source, target)
}

func (s *ElasticsearchService) GetUserAction(id string) (*UserActionDocument, error) {
	var doc UserActionDocument
	if err := s.getDocument(userActionIndex, id, &doc); err != nil {
		return nil, err
	}
	zap.S().Infof("Found user action document: %s", id)
	return &doc, nil
}

func (s *ElasticsearchService) searchDocuments(index, field, value string) ([]json.RawMessage, error) {
	query := map[string]any{
		"query": map[string]any{
			"term": map[string]any{
				field + ".keyword": value,
			},
		},
		"size": 1000,
	}
	queryJSON, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal query: %w", err)
	}

	ctx := context.Background()
	res, err := s.client.Search(ctx, &opensearchapi.SearchReq{
		Indices: []string{index},
		Body:    bytes.NewReader(queryJSON),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to search: %w", err)
	}

	sources := make([]json.RawMessage, len(res.Hits.Hits))
	for i, hit := range res.Hits.Hits {
		sources[i] = hit.Source
	}
	return sources, nil
}

func (s *ElasticsearchService) SearchOSDocuments(assessmentID string) ([]entity.AssessmentOS, error) {
	sources, err := s.searchDocuments(osIndex, "assessment_id", assessmentID)
	if err != nil {
		return nil, err
	}
	docs := make([]entity.AssessmentOS, len(sources))
	for i, src := range sources {
		if err := json.Unmarshal(src, &docs[i]); err != nil {
			return nil, fmt.Errorf("failed to unmarshal OS document: %w", err)
		}
	}
	zap.S().Infof("Found %d OS documents for assessment %s", len(docs), assessmentID)
	return docs, nil
}

func (s *ElasticsearchService) SearchDatastoreDocuments(assessmentID string) ([]entity.AssessmentDatastore, error) {
	sources, err := s.searchDocuments(datastoreIndex, "assessment_id", assessmentID)
	if err != nil {
		return nil, err
	}
	docs := make([]entity.AssessmentDatastore, len(sources))
	for i, src := range sources {
		if err := json.Unmarshal(src, &docs[i]); err != nil {
			return nil, fmt.Errorf("failed to unmarshal datastore document: %w", err)
		}
	}
	zap.S().Infof("Found %d datastore documents for assessment %s", len(docs), assessmentID)
	return docs, nil
}

func (s *ElasticsearchService) SearchUserActions(field, value string) ([]UserActionDocument, error) {
	sources, err := s.searchDocuments(userActionIndex, field, value)
	if err != nil {
		return nil, err
	}
	docs := make([]UserActionDocument, len(sources))
	for i, src := range sources {
		if err := json.Unmarshal(src, &docs[i]); err != nil {
			return nil, fmt.Errorf("failed to unmarshal user action document: %w", err)
		}
	}
	zap.S().Infof("Found %d user action documents for %s=%s", len(docs), field, value)
	return docs, nil
}

func (s *ElasticsearchService) RefreshIndex(index string) error {
	ctx := context.Background()
	_, err := s.client.Indices.Refresh(ctx, &opensearchapi.IndicesRefreshReq{
		Index: []string{index},
	})
	if err != nil {
		return fmt.Errorf("failed to refresh index %s: %w", index, err)
	}
	return nil
}
