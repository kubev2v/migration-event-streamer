package pipeline

import "strings"

// ExtractAction returns the action segment from a CloudEvent type.
// "assisted.migration.assessment.created" -> "created"
func ExtractAction(ceType string) string {
	if i := strings.LastIndex(ceType, "."); i >= 0 {
		return ceType[i+1:]
	}
	return ceType
}
