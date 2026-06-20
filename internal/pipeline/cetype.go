package pipeline

import "strings"

// ExtractEntityAction returns the entity.action segment from a CloudEvent type.
// "assisted.migration.assessment.created" -> "assessment.created"
func ExtractEntityAction(ceType string) string {
	parts := strings.Split(ceType, ".")
	if len(parts) >= 2 {
		return parts[len(parts)-2] + "." + parts[len(parts)-1]
	}
	return ceType
}
