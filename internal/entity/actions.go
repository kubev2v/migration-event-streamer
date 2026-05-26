package entity

// Action types extracted from the last segment of CloudEvent types.
// e.g. "assisted.migration.assessment.created" -> "created"
const (
	AssessmentActionCreated = "created"
	AssessmentActionDeleted = "deleted"

	UserActionShareAssessment   = "assessment_shared"
	UserActionUnshareAssessment = "assessment_unshared"

	UserActionSizingRequested     = "sizing_requested"
	UserActionComplexityEstimated = "complexity_estimated"
	UserActionOVADownloaded       = "ova_downloaded"
)
