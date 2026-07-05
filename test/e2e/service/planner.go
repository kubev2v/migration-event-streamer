package service

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/google/uuid"
	"github.com/kubev2v/migration-planner/api/v1alpha1"
	"go.uber.org/zap"
)

type PlannerService struct {
	baseURL    string
	httpClient *http.Client
}

func NewPlannerService(baseURL string) *PlannerService {
	return &PlannerService{
		baseURL:    baseURL,
		httpClient: &http.Client{},
	}
}

func (s *PlannerService) DeleteAssessment(id string) error {
	zap.S().Infof("Deleting assessment %s...", id)

	resp, err := s.doRequest(http.MethodDelete, fmt.Sprintf("/api/v1/assessments/%s", id), nil)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete assessment failed: status %d, body: %s", resp.StatusCode, string(body))
	}

	zap.S().Infof("Assessment deleted: %s", id)
	return nil
}

func (s *PlannerService) ListAssessments() error {
	zap.S().Info("Listing assessments...")

	resp, err := s.doRequest(http.MethodGet, "/api/v1/assessments", nil)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("list assessments failed: status %d, body: %s", resp.StatusCode, string(body))
	}

	return nil
}

func (s *PlannerService) CalculateComplexity(id string, clusterID string) error {
	zap.S().Infof("Calculating complexity for assessment %s, cluster %s...", id, clusterID)

	body := map[string]any{"clusterId": clusterID}
	reqBody, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := s.doRequest(http.MethodPost, fmt.Sprintf("/api/v1/assessments/%s/complexity-estimation", id), reqBody)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("calculate complexity failed: status %d, body: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

func (s *PlannerService) CalculateTimeEstimation(id string, clusterID string) error {
	zap.S().Infof("Calculating time estimation for assessment %s, cluster %s...", id, clusterID)

	body := map[string]any{"clusterId": clusterID}
	reqBody, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := s.doRequest(http.MethodPost, fmt.Sprintf("/api/v1/assessments/%s/migration-estimation", id), reqBody)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("calculate time estimation failed: status %d, body: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

func (s *PlannerService) CreateAssessment(name, sourceType string, sourceID *uuid.UUID, inventory *v1alpha1.Inventory) (*v1alpha1.Assessment, error) {
	zap.S().Infof("Creating assessment %q...", name)

	body := map[string]any{
		"name":       name,
		"sourceType": sourceType,
		"inventory":  inventory,
	}
	if sourceID != nil {
		body["sourceId"] = sourceID.String()
	}

	reqBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := s.doRequest(http.MethodPost, "/api/v1/assessments", reqBody)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("create assessment failed: status %d, body: %s", resp.StatusCode, string(respBody))
	}

	var assessment v1alpha1.Assessment
	if err := json.Unmarshal(respBody, &assessment); err != nil {
		return nil, fmt.Errorf("failed to unmarshal assessment: %w", err)
	}

	zap.S().Infof("Assessment created: %s", assessment.Id)
	return &assessment, nil
}

func (s *PlannerService) doRequest(method, path string, body []byte) (*http.Response, error) {
	url := fmt.Sprintf("%s%s", s.baseURL, path)

	var req *http.Request
	var err error
	if body != nil {
		req, err = http.NewRequest(method, url, bytes.NewReader(body))
	} else {
		req, err = http.NewRequest(method, url, nil)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	zap.S().Infof("[PlannerService] %s %s", method, url)
	return s.httpClient.Do(req)
}
