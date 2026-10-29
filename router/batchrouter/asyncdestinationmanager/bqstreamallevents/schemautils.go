package bqstreamallevents

import (
	"errors"
	"slices"
	"strings"

	"cloud.google.com/go/auth"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func findNewColumns(eventSchema, warehouseSchema whutils.ModelTableSchema) []whutils.ColumnInfo {
	var newColumns []whutils.ColumnInfo
	for column, dataType := range eventSchema {
		if _, exists := warehouseSchema[column]; !exists {
			newColumns = append(newColumns, whutils.ColumnInfo{
				Name: column,
				Type: dataType,
			})
		}
	}
	return newColumns
}

func checkAndIgnoreAlreadyExistError(err error) bool {
	if e, ok := errors.AsType[*googleapi.Error](err); ok {
		// 409 is returned when we try to create a table that already exists
		// 400 is returned for all kinds of invalid input - so we need to check the error message too
		if e.Code == 409 || (e.Code == 400 && strings.Contains(strings.ToLower(e.Message), "already exists in schema")) {
			return true
		}
	}
	return false
}

// terminalTokenError reports whether the OAuth token endpoint rejected the
// service-account credentials themselves (e.g. a deleted key or account).
func terminalTokenError(err error) bool {
	e, ok := errors.AsType[*auth.Error](err)
	if !ok {
		return false
	}
	var response struct {
		Error       string `json:"error"`
		Description string `json:"error_description"`
	}
	if jsonrs.Unmarshal(e.Body, &response) != nil {
		return false
	}
	switch response.Error {
	case "invalid_client", "unauthorized_client":
		return true
	case "invalid_grant":
		// invalid_grant also covers host clock skew ("Check your iat and exp values"),
		// which a retry can fix, so only abort when Google names the key or account.
		return strings.Contains(response.Description, "Invalid JWT Signature") ||
			strings.Contains(response.Description, "account not found")
	}
	return false
}

// rateLimitedBigQueryError reports whether a 403 is BigQuery throttling rather than a permission denial.
func rateLimitedBigQueryError(e *googleapi.Error) bool {
	return slices.ContainsFunc(e.Errors, func(item googleapi.ErrorItem) bool {
		return item.Reason == "rateLimitExceeded" || item.Reason == "userRateLimitExceeded" || item.Reason == "quotaExceeded"
	})
}

// shouldAbort classifies an error as terminal (abort the jobs) vs retryable.
func shouldAbort(err error) bool {
	// AsType sees only the first match; walk joined errors so an earlier
	// transient branch cannot hide a terminal sibling.
	type multiError interface{ Unwrap() []error }
	if joined, ok := err.(multiError); ok {
		if slices.ContainsFunc(joined.Unwrap(), shouldAbort) {
			return true
		}
	} else if wrapped := errors.Unwrap(err); wrapped != nil && shouldAbort(wrapped) {
		return true
	}
	if terminalTokenError(err) {
		return true
	}
	if e, ok := errors.AsType[*googleapi.Error](err); ok {
		return e.Code == 401 || (e.Code == 403 && !rateLimitedBigQueryError(e))
	}
	switch status.Code(err) {
	case codes.PermissionDenied, codes.Unauthenticated, codes.FailedPrecondition, codes.Unimplemented, codes.DataLoss:
		return true
	default:
		return false
	}
}
