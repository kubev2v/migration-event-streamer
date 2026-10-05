package processors

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/kubev2v/migration-event-streamer/internal/entity"
	plannerEvents "github.com/kubev2v/migration-planner/pkg/events/kafka"
	"go.uber.org/zap"
)

func CostEstimatedProcessor(_ context.Context, event costEstimationUserActionEvent) (entity.CostEstimatedResult, error) {
	action := event.UserAction
	var data costEstimationActionData
	if err := json.Unmarshal(action.Data, &data); err != nil {
		return entity.CostEstimatedResult{}, fmt.Errorf("failed to unmarshal cost estimation data: %w", err)
	}

	zap.S().Infow("processing cost estimation event",
		"operation", data.Operation,
		"assessment_id", data.AssessmentID,
		"cluster_id", data.ClusterID)

	return entity.NewCostEstimatedResult(
		action.Username,
		data.Operation,
		data.AssessmentID,
		data.ClusterID,
		data.Scope,
		data.CalculatorVersion,
		data.OutputFormat,
		action.Timestamp,
	), nil
}

func ShareAssessmentProcessor(_ context.Context, event plannerEvents.UserActionEventPayload) (entity.ShareAssessmentResult, error) {
	action := event.UserAction
	dataBytes, err := json.Marshal(action.Data)
	if err != nil {
		return entity.ShareAssessmentResult{}, fmt.Errorf("failed to marshal action data: %w", err)
	}

	var data plannerEvents.ShareAssessmentActionData
	if err := json.Unmarshal(dataBytes, &data); err != nil {
		return entity.ShareAssessmentResult{}, fmt.Errorf("failed to unmarshal share assessment data: %w", err)
	}

	zap.S().Infow("processing share assessment event")

	return entity.NewShareAssessmentResult(
		action.Username,
		data.AssessmentID,
		data.PartnerID,
		data.PartnerName,
		action.Timestamp,
	), nil
}

func UnshareAssessmentProcessor(_ context.Context, event plannerEvents.UserActionEventPayload) (entity.UnshareAssessmentResult, error) {
	action := event.UserAction
	dataBytes, err := json.Marshal(action.Data)
	if err != nil {
		return entity.UnshareAssessmentResult{}, fmt.Errorf("failed to marshal action data: %w", err)
	}

	var data plannerEvents.UnshareAssessmentActionData
	if err := json.Unmarshal(dataBytes, &data); err != nil {
		return entity.UnshareAssessmentResult{}, fmt.Errorf("failed to unmarshal unshare assessment data: %w", err)
	}

	zap.S().Infow("processing unshare assessment event")

	return entity.NewUnshareAssessmentResult(
		action.Username,
		data.AssessmentID,
		action.Timestamp,
	), nil
}

func SizingRequestedProcessor(_ context.Context, event plannerEvents.UserActionEventPayload) (entity.SizingRequestedResult, error) {
	action := event.UserAction
	dataBytes, err := json.Marshal(action.Data)
	if err != nil {
		return entity.SizingRequestedResult{}, fmt.Errorf("failed to marshal action data: %w", err)
	}

	var data plannerEvents.SizingActionData
	if err := json.Unmarshal(dataBytes, &data); err != nil {
		return entity.SizingRequestedResult{}, fmt.Errorf("failed to unmarshal sizing data: %w", err)
	}

	zap.S().Infow("processing sizing request event")

	return entity.NewSizingRequestedResult(
		action.Username,
		data.AssessmentID,
		action.Timestamp,
	), nil
}

func ComplexityEstimatedProcessor(_ context.Context, event plannerEvents.UserActionEventPayload) (entity.ComplexityEstimatedResult, error) {
	action := event.UserAction
	dataBytes, err := json.Marshal(action.Data)
	if err != nil {
		return entity.ComplexityEstimatedResult{}, fmt.Errorf("failed to marshal action data: %w", err)
	}

	var data plannerEvents.ComplexityActionData
	if err := json.Unmarshal(dataBytes, &data); err != nil {
		return entity.ComplexityEstimatedResult{}, fmt.Errorf("failed to unmarshal complexity data: %w", err)
	}

	zap.S().Infow("processing complexity estimation event")

	return entity.NewComplexityEstimatedResult(
		action.Username,
		data.AssessmentID,
		action.Timestamp,
	), nil
}

func TimeEstimatedProcessor(_ context.Context, event plannerEvents.UserActionEventPayload) (entity.TimeEstimatedResult, error) {
	action := event.UserAction
	dataBytes, err := json.Marshal(action.Data)
	if err != nil {
		return entity.TimeEstimatedResult{}, fmt.Errorf("failed to marshal action data: %w", err)
	}

	var data plannerEvents.TimeEstimationActionData
	if err := json.Unmarshal(dataBytes, &data); err != nil {
		return entity.TimeEstimatedResult{}, fmt.Errorf("failed to unmarshal time estimation data: %w", err)
	}

	zap.S().Infow("processing time estimation event")

	return entity.NewTimeEstimatedResult(
		action.Username,
		data.AssessmentID,
		action.Timestamp,
	), nil
}

func OVADownloadedProcessor(_ context.Context, event plannerEvents.UserActionEventPayload) (entity.OVADownloadedResult, error) {
	action := event.UserAction
	dataBytes, err := json.Marshal(action.Data)
	if err != nil {
		return entity.OVADownloadedResult{}, fmt.Errorf("failed to marshal action data: %w", err)
	}

	var data plannerEvents.OVADownloadActionData
	if err := json.Unmarshal(dataBytes, &data); err != nil {
		return entity.OVADownloadedResult{}, fmt.Errorf("failed to unmarshal ova download data: %w", err)
	}

	zap.S().Infow("processing ova download event")

	return entity.NewOVADownloadedResult(
		action.Username,
		data.SourceID,
		data.DownloadURLID,
		action.Timestamp,
	), nil
}

func VisitedProcessor(_ context.Context, event plannerEvents.UserActionEventPayload) (entity.VisitedResult, error) {
	action := event.UserAction
	dataBytes, err := json.Marshal(action.Data)
	if err != nil {
		return entity.VisitedResult{}, fmt.Errorf("failed to marshal action data: %w", err)
	}

	var data plannerEvents.VisitorActionData
	if err := json.Unmarshal(dataBytes, &data); err != nil {
		return entity.VisitedResult{}, fmt.Errorf("failed to unmarshal visitor data: %w", err)
	}

	zap.S().Infow("processing visited event")

	return entity.NewVisitedResult(
		action.Username,
		data.OrgID,
		action.Timestamp,
	), nil
}
