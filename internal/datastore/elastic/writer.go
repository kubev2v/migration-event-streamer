package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/kubev2v/migration-event-streamer/internal/entity"
	"go.uber.org/zap"

	elastic "github.com/elastic/go-elasticsearch/v8"
)

type Writer interface {
	Assessment() AssessmentWriter
	UserAction() UserActionWriter
	PartnerCustomer() PartnerCustomerWriter
}

type AssessmentWriter interface {
	WriteCreated(ctx context.Context, result entity.AssessmentCreatedResult) error
	WriteCascadeDelete(ctx context.Context, result entity.AssessmentDeletedResult) error
}

type UserActionWriter interface {
	WriteShareAssessment(ctx context.Context, result entity.ShareAssessmentResult) error
	WriteUnshareAssessment(ctx context.Context, result entity.UnshareAssessmentResult) error
	WriteSizingRequested(ctx context.Context, result entity.SizingRequestedResult) error
	WriteComplexityEstimated(ctx context.Context, result entity.ComplexityEstimatedResult) error
	WriteOVADownloaded(ctx context.Context, result entity.OVADownloadedResult) error
	WriteVisited(ctx context.Context, result entity.VisitedResult) error
}

type PartnerCustomerWriter interface {
	Write(ctx context.Context, pc entity.PartnerCustomer) error
}

type baseWriter struct {
	client      *elastic.Client
	indexPrefix string
}

func (b *baseWriter) write(ctx context.Context, index, id string, data []byte) error {
	req := esapi.IndexRequest{
		Index:      fmt.Sprintf("%s_%s", b.indexPrefix, index),
		DocumentID: id,
		Body:       bytes.NewReader(data),
	}
	res, err := req.Do(ctx, b.client)
	if err != nil {
		return fmt.Errorf("failed to insert document %s: %w", id, err)
	}
	defer func() { _ = res.Body.Close() }()

	if res.IsError() {
		bodyBytes, _ := io.ReadAll(res.Body)
		return fmt.Errorf("failed to index document %s: %s - %s", id, res.Status(), string(bodyBytes))
	}

	zap.S().Infow("successful write", "index", index, "document_id", id, "method", "overwrite")
	return nil
}

func (b *baseWriter) upsert(ctx context.Context, index, id string, data []byte) error {
	body := map[string]any{
		"doc":           json.RawMessage(data),
		"doc_as_upsert": true,
	}

	bodyJSON, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("failed to marshal upsert body: %w", err)
	}

	req := esapi.UpdateRequest{
		Index:      fmt.Sprintf("%s_%s", b.indexPrefix, index),
		DocumentID: id,
		Body:       bytes.NewReader(bodyJSON),
	}

	res, err := req.Do(ctx, b.client)
	if err != nil {
		return fmt.Errorf("failed to upsert document %s: %w", id, err)
	}
	defer func() { _ = res.Body.Close() }()

	if res.IsError() {
		bodyBytes, _ := io.ReadAll(res.Body)
		return fmt.Errorf("failed to upsert document %s: %s - %s", id, res.Status(), string(bodyBytes))
	}

	zap.S().Infow("successful write", "index", index, "document_id", id, "method", "upsert")
	return nil
}

func (b *baseWriter) updateByQuery(ctx context.Context, req UpdateByQueryRequest) (*UpdateByQueryResult, error) {
	indexName := fmt.Sprintf("%s_%s", b.indexPrefix, req.Index)

	script, params := buildUpdateScript(req.Updates)

	queryBody := map[string]any{
		"script": map[string]any{
			"source": script,
			"lang":   "painless",
			"params": params,
		},
		"query": map[string]any{
			"term": map[string]any{
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

	res, err := updateReq.Do(ctx, b.client)
	if err != nil {
		return nil, fmt.Errorf("failed to execute update by query: %w", err)
	}
	defer func() { _ = res.Body.Close() }()

	if res.IsError() {
		bodyBytes, _ := io.ReadAll(res.Body)
		return nil, fmt.Errorf("update by query failed: %s - %s", res.Status(), string(bodyBytes))
	}

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

	zap.S().Infow("successful write", "index", req.Index, "by_field", req.MatchField, "method", "update_by_query")
	return result, nil
}

func buildUpdateScript(updates map[string]any) (string, map[string]any) {
	if len(updates) == 0 {
		return "", nil
	}

	var scriptParts []string
	params := make(map[string]any)

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
