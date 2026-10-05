package processors

import (
	"encoding/json"
	"time"
)

// Unlike the planner, We cannot import cost-estimation types because that's a private repository.
// Hence, we declare the same producer structs here.

type costEstimationUserActionEvent struct {
	UserAction costEstimationUserAction `json:"user_action"`
}

type costEstimationUserAction struct {
	Username  string          `json:"username"`
	Timestamp time.Time       `json:"timestamp"`
	Data      json.RawMessage `json:"data"`
}

type costEstimationActionData struct {
	Operation         string `json:"operation"`
	AssessmentID      string `json:"assessment_id"`
	ClusterID         string `json:"cluster_id"`
	Scope             string `json:"scope"`
	CalculatorVersion string `json:"calculator_version"`
	OutputFormat      string `json:"output_format"`
}
