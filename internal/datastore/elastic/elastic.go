package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	elastic "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/kubev2v/migration-event-streamer/internal/config"
)

// ElasticRepository implements datastore.Writer interface
type ElasticRepository struct {
	client      *elastic.Client
	indexPrefix string
}

func NewElasticRepository(config config.ElasticSearch) (*ElasticRepository, error) {
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

func (e *ElasticRepository) CreateIndex(name string) error {
	// check if index exists
	res, err := e.client.Indices.Exists([]string{name})
	if err != nil {
		return fmt.Errorf("failed to check if index %s exists: %w", name, err)
	}
	if res.StatusCode == http.StatusOK {
		_ = res.Body.Close()
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
	_ = res.Body.Close()
	return nil
}

func (e *ElasticRepository) write(ctx context.Context, index, id string, data []byte) error {
	req := esapi.IndexRequest{
		Index:      fmt.Sprintf("%s_%s", e.indexPrefix, index),
		DocumentID: id,
		Body:       bytes.NewReader(data),
	}
	res, err := req.Do(ctx, e.client)
	if err != nil {
		return fmt.Errorf("failed to insert document %s: %w", id, err)
	}
	defer func() {
		_ = res.Body.Close()
	}()

	if res.IsError() {
		bodyBytes, _ := io.ReadAll(res.Body)
		return fmt.Errorf("failed to index document %s: %s - %s", id, res.Status(), string(bodyBytes))
	}

	return nil
}

// TODO: use at the future for multiple docs
// nolint:unused
func (e *ElasticRepository) updateByQuery(ctx context.Context, req UpdateByQueryRequest) (*UpdateByQueryResult, error) {
	indexName := fmt.Sprintf("%s_%s", e.indexPrefix, req.Index)

	script, params := buildUpdateScript(req.Updates)

	queryBody := map[string]interface{}{
		"script": map[string]interface{}{
			"source": script,
			"lang":   "painless",
			"params": params,
		},
		"query": map[string]interface{}{
			"term": map[string]interface{}{
				req.MatchField: req.MatchValue,
			},
		},
	}

	bodyJSON, err := json.Marshal(queryBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal query body: %w", err)
	}

	updateReq := esapi.UpdateByQueryRequest{
		Index:     []string{indexName},
		Body:      bytes.NewReader(bodyJSON),
		Conflicts: "proceed",
	}

	res, err := updateReq.Do(ctx, e.client)
	if err != nil {
		return nil, fmt.Errorf("failed to execute update by query: %w", err)
	}
	defer func() {
		_ = res.Body.Close()
	}()

	if res.IsError() {
		bodyBytes, _ := io.ReadAll(res.Body)
		return nil, fmt.Errorf("update by query failed: %s - %s", res.Status(), string(bodyBytes))
	}

	// Parse response
	var response struct {
		Total    int64 `json:"total"`
		Updated  int64 `json:"updated"`
		Failures []struct {
			Index string `json:"index"`
			ID    string `json:"id"`
			Cause struct {
				Reason string `json:"reason"`
			} `json:"cause"`
		} `json:"failures"`
	}

	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	result := &UpdateByQueryResult{
		Total:    response.Total,
		Updated:  response.Updated,
		Failed:   int64(len(response.Failures)),
		Failures: make([]UpdateFailure, 0, len(response.Failures)),
	}

	for _, f := range response.Failures {
		result.Failures = append(result.Failures, UpdateFailure{
			Index:      f.Index,
			DocumentID: f.ID,
			Cause:      f.Cause.Reason,
		})
	}

	return result, nil
}

// buildUpdateScript creates a Painless script and params map from the updates
// nolint:unused
func buildUpdateScript(updates map[string]interface{}) (string, map[string]interface{}) {
	if len(updates) == 0 {
		return "", nil
	}

	var scriptParts []string
	params := make(map[string]interface{})

	for field, value := range updates {
		scriptParts = append(scriptParts, fmt.Sprintf("ctx._source.%s = params.%s", field, field))
		params[field] = value
	}

	script := ""
	for i, part := range scriptParts {
		if i > 0 {
			script += "; "
		}
		script += part
	}

	return script, params
}
