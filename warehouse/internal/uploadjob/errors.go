package uploadjob

import (
	"encoding/json"
	"fmt"

	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/manager"
)

// ErrorHandler handles upload job errors
type ErrorHandler struct {
	Mapper manager.Manager
}

// MatchUploadJobErrorType matches the error type for an upload job
func (h *ErrorHandler) MatchUploadJobErrorType(err error) model.JobErrorType {
	if err == nil {
		return model.UncategorizedError
	}

	errorString := err.Error()

	// Check for permission errors
	if h.Mapper.IsPermissionError(errorString) {
		return model.PermissionError
	}

	// Check for alter column errors
	if h.Mapper.IsAlterColumnError(errorString) {
		return model.AlterColumnError
	}

	// Check for resource not found errors
	if h.Mapper.IsResourceNotFoundError(errorString) {
		return model.ResourceNotFoundError
	}

	// Check for column count errors
	if h.Mapper.IsColumnCountError(errorString) {
		return model.ColumnCountError
	}

	// Check for column size errors
	if h.Mapper.IsColumnSizeError(errorString) {
		return model.ColumnSizeError
	}

	// Check for insufficient resource errors
	if h.Mapper.IsInsufficientResourceError(errorString) {
		return model.InsufficientResourceError
	}

	// Check for concurrent queries errors
	if h.Mapper.IsConcurrentQueriesError(errorString) {
		return model.ConcurrentQueriesError
	}

	return model.UncategorizedError
}

// UploadStatusOpts represents options for setting upload status
type UploadStatusOpts struct {
	Status          string
	ReportingMetric types.PUReportedMetric
}

// extractAndUpdateUploadErrorsByState extracts and augment errors in format
// { "internal_processing_failed": { "errors": ["account-locked", "account-locked"] }}
// from a particular upload.
func extractAndUpdateUploadErrorsByState(message json.RawMessage, state string, statusError error) (map[string]map[string]interface{}, error) {
	var uploadErrors map[string]map[string]interface{}
	err := json.Unmarshal(message, &uploadErrors)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal error into upload errors: %v", err)
	}

	if uploadErrors == nil {
		uploadErrors = make(map[string]map[string]interface{})
	}

	if _, ok := uploadErrors[state]; !ok {
		uploadErrors[state] = make(map[string]interface{})
	}
	errorByState := uploadErrors[state]

	// increment attempts for errored stage
	if attempt, ok := errorByState["attempt"]; ok {
		errorByState["attempt"] = int(attempt.(float64)) + 1
	} else {
		errorByState["attempt"] = 1
	}

	// append errors for errored stage
	if errList, ok := errorByState["errors"]; ok {
		errorByState["errors"] = append(errList.([]interface{}), statusError.Error())
	} else {
		errorByState["errors"] = []string{statusError.Error()}
	}

	return uploadErrors, nil
}
