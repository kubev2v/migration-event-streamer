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
	"github.com/kubev2v/migration-event-streamer/internal/entity"
	"go.uber.org/zap"
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

func (e *ElasticRepository) WriteVisitor(ctx context.Context, v *entity.Visitor) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("failed to marshal visitor: %w", err)
	}
	return e.write(ctx, v.Index, v.ID, data)
}

func (e *ElasticRepository) WritePartnerCustomer(ctx context.Context, pc *entity.PartnerCustomer) error {
	data, err := json.Marshal(pc)
	if err != nil {
		return fmt.Errorf("failed to marshal partner_customer: %w", err)
	}
	return e.write(ctx, entity.PartnerCustomerIndex, pc.ID, data)
}

func (e *ElasticRepository) WriteUserAction(ctx context.Context, ua *entity.UserAction) error {
	if ua.ActionType == entity.ActionShareAssessment || ua.ActionType == entity.ActionUnshareAssessment {
		if err := e.updateAssessmentPartner(ctx, *ua.AssessmentID, ua.PartnerID, ua.ActionType); err != nil {
			return fmt.Errorf("failed to handle %s action: %w", ua.ActionType, err)
		}
	}

	data, err := json.Marshal(ua)
	if err != nil {
		return fmt.Errorf("failed to marshal user_action: %w", err)
	}
	return e.write(ctx, entity.UserActionIndex, ua.ID, data)
}

func (e *ElasticRepository) WriteAssessment(ctx context.Context, result entity.AssessmentResult) error {
	switch result.Action {
	case entity.ActionDeleted:
		return e.cascadeDeleteAssessment(ctx, result.DeletedID, result.DeletedAt)
	case entity.ActionCreated:
		return e.writeAssessmentCreated(ctx, result)
	default:
		return nil
	}
}

func (e *ElasticRepository) writeAssessmentCreated(ctx context.Context, result entity.AssessmentResult) error {
	data, err := json.Marshal(result.Assessment)
	if err != nil {
		return fmt.Errorf("failed to marshal assessment: %w", err)
	}
	if err := e.write(ctx, result.Assessment.Index, result.Assessment.ID, data); err != nil {
		return fmt.Errorf("failed to write assessment document: %w", err)
	}

	for _, os := range result.OSEntries {
		d, err := json.Marshal(os)
		if err != nil {
			zap.S().Warnw("failed to marshal OS document", "os_type", os.OSType, "error", err)
			continue
		}
		if err := e.write(ctx, os.Index, os.ID, d); err != nil {
			zap.S().Warnw("failed to write OS document", "os_type", os.OSType, "error", err)
		}
	}

	for _, ds := range result.Datastores {
		d, err := json.Marshal(ds)
		if err != nil {
			zap.S().Warnw("failed to marshal datastore document", "datastore_index", ds.DatastoreIndex, "error", err)
			continue
		}
		if err := e.write(ctx, ds.Index, ds.ID, d); err != nil {
			zap.S().Warnw("failed to write datastore document", "datastore_index", ds.DatastoreIndex, "error", err)
		}
	}

	return nil
}

func (e *ElasticRepository) cascadeDeleteAssessment(ctx context.Context, assessmentID, deletedAt string) error {
	updates := map[string]interface{}{
		"status":     entity.DeletedStatus,
		"deleted_at": deletedAt,
	}

	data, err := json.Marshal(updates)
	if err != nil {
		return err
	}
	if err := e.upsert(ctx, entity.AssessmentIndex, assessmentID, data); err != nil {
		return err
	}

	osResult, err := e.updateByQuery(ctx, UpdateByQueryRequest{
		Index:      entity.OSIndex,
		MatchField: "assessment_id.keyword",
		MatchValue: assessmentID,
		Updates:    updates,
	})
	if err != nil {
		return fmt.Errorf("failed to update OS documents: %w", err)
	}
	zap.S().Infow("cascade delete OS documents", "assessment_id", assessmentID,
		"updated", osResult.Updated, "failed", osResult.Failed)

	datastoreResult, err := e.updateByQuery(ctx, UpdateByQueryRequest{
		Index:      entity.DatastoreIndex,
		MatchField: "assessment_id.keyword",
		MatchValue: assessmentID,
		Updates:    updates,
	})
	if err != nil {
		return fmt.Errorf("failed to update datastore documents: %w", err)
	}
	zap.S().Infow("cascade delete datastore documents", "assessment_id", assessmentID,
		"updated", datastoreResult.Updated, "failed", datastoreResult.Failed)

	return nil
}

func (e *ElasticRepository) updateAssessmentPartner(ctx context.Context, assessmentID string, partnerID *string, actionType string) error {
	var updates map[string]interface{}
	if actionType == entity.ActionShareAssessment {
		updates = map[string]interface{}{"partner_id": *partnerID}
	} else {
		updates = map[string]interface{}{"partner_id": nil}
	}

	for _, index := range []string{entity.AssessmentIndex, entity.OSIndex, entity.DatastoreIndex} {
		matchField := "assessment_id.keyword"
		if index == entity.AssessmentIndex {
			matchField = "id.keyword"
		}

		result, err := e.updateByQuery(ctx, UpdateByQueryRequest{
			Index:      index,
			MatchField: matchField,
			MatchValue: assessmentID,
			Updates:    updates,
		})
		if err != nil {
			return fmt.Errorf("failed to update %s partner_id: %w", index, err)
		}
		zap.S().Infow("updated partner_id", "action", actionType,
			"index", index, "assessment_id", assessmentID, "updated", result.Updated)
	}

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

	zap.S().Infow("successful write", "index", index, "document_id", id, "method", "overwrite")

	return nil
}

func (e *ElasticRepository) upsert(ctx context.Context, index, id string, data []byte) error {
	body := map[string]interface{}{
		"doc":           json.RawMessage(data),
		"doc_as_upsert": true,
	}

	bodyJSON, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("failed to marshal upsert body: %w", err)
	}

	req := esapi.UpdateRequest{
		Index:      fmt.Sprintf("%s_%s", e.indexPrefix, index),
		DocumentID: id,
		Body:       bytes.NewReader(bodyJSON),
	}

	res, err := req.Do(ctx, e.client)
	if err != nil {
		return fmt.Errorf("failed to upsert document %s: %w", id, err)
	}
	defer func() {
		_ = res.Body.Close()
	}()

	if res.IsError() {
		bodyBytes, _ := io.ReadAll(res.Body)
		return fmt.Errorf("failed to upsert document %s: %s - %s", id, res.Status(), string(bodyBytes))
	}

	zap.S().Infow("successful write", "index", index, "document_id", id, "method", "upsert")

	return nil
}

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

	zap.S().Infow("successful write", "index", req.Index, "by_field", req.MatchField, "method", "update_by_query")

	return result, nil
}

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
