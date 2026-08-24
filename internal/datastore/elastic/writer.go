package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/kubev2v/migration-event-streamer/internal/entity"
	opensearchapi "github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"go.uber.org/zap"
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
	WriteTimeEstimated(ctx context.Context, result entity.TimeEstimatedResult) error
	WriteOVADownloaded(ctx context.Context, result entity.OVADownloadedResult) error
	WriteVisited(ctx context.Context, result entity.VisitedResult) error
}

type PartnerCustomerWriter interface {
	Write(ctx context.Context, pc entity.PartnerCustomer) error
}

type baseWriter struct {
	client      *opensearchapi.Client
	indexPrefix string
}

func (b *baseWriter) write(ctx context.Context, index, id string, data []byte) error {
	req := opensearchapi.IndexReq{
		Index:      fmt.Sprintf("%s_%s", b.indexPrefix, index),
		DocumentID: id,
		Body:       bytes.NewReader(data),
	}
	res, err := b.client.Index(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to insert document %s: %w", id, err)
	}

	zap.S().Infow("successful write", "index", index, "document_id", id, "method", "overwrite", "result", res.Result)
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

	req := opensearchapi.UpdateReq{
		Index:      fmt.Sprintf("%s_%s", b.indexPrefix, index),
		DocumentID: id,
		Body:       bytes.NewReader(bodyJSON),
	}

	res, err := b.client.Update(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to upsert document %s: %w", id, err)
	}

	zap.S().Infow("successful write", "index", index, "document_id", id, "method", "upsert", "result", res.Result)
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

	updateReq := opensearchapi.UpdateByQueryReq{
		Indices: []string{indexName},
		Body:    bytes.NewReader(bodyJSON),
		Params: opensearchapi.UpdateByQueryParams{
			Conflicts: "proceed",
		},
	}

	res, err := b.client.UpdateByQuery(ctx, updateReq)
	if err != nil {
		return nil, fmt.Errorf("failed to execute update by query: %w", err)
	}

	// Response is already parsed - use fields directly
	result := &UpdateByQueryResult{
		Total:    int64(res.Total),
		Updated:  int64(res.Updated),
		Failed:   int64(len(res.Failures)),
		Failures: make([]UpdateFailure, 0, len(res.Failures)),
	}

	for _, f := range res.Failures {
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
