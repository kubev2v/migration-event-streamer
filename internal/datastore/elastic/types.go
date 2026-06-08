package elastic

// UpdateByQueryRequest represents a bulk update operation on documents matching a field condition
type UpdateByQueryRequest struct {
	Index      string                 // Index name (will be prefixed)
	MatchField string                 // Field to match on (e.g., "assessment_id")
	MatchValue interface{}            // Value to match (e.g., "assessment-123")
	Updates    map[string]interface{} // Fields to update with their new values
}

// UpdateByQueryResult contains the results of a bulk update operation
type UpdateByQueryResult struct {
	Total    int64           // Total documents matched
	Updated  int64           // Successfully updated
	Failed   int64           // Failed to update
	Failures []UpdateFailure // Details of failures
}

// UpdateFailure represents a single document update failure
type UpdateFailure struct {
	Index      string `json:"index"`
	DocumentID string `json:"id"`
	Cause      string `json:"cause"`
}
