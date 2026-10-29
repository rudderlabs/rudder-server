package bqstreamallevents

import (
	"errors"
	"slices"
	"strings"

	"golang.org/x/oauth2"
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

var errTerminalAuth = errors.New("terminal BigQuery authentication error")

func terminalOAuthError(err error) bool {
	e, ok := errors.AsType[*oauth2.RetrieveError](err)
	if !ok {
		return false
	}
	if terminalOAuthCode(e.ErrorCode) {
		return true
	}
	// The service-account JWT token source leaves ErrorCode empty, with the
	// OAuth error code only in the response body.
	var response struct {
		Error string `json:"error"`
	}
	return jsonrs.Unmarshal(e.Body, &response) == nil && terminalOAuthCode(response.Error)
}

func terminalOAuthCode(code string) bool {
	switch code {
	case "invalid_grant", "invalid_client", "unauthorized_client":
		return true
	}
	return false
}

func retryableBigQueryError(e *googleapi.Error) bool {
	for _, item := range e.Errors {
		switch item.Reason {
		case "rateLimitExceeded", "userRateLimitExceeded", "quotaExceeded", "backendError", "internalError":
			return true
		}
	}
	// BigQuery also reports concurrent query quotas as a 400 without a
	// structured reason (see the warehouse BigQuery error mappings).
	message := strings.ToLower(e.Message)
	return e.Code == 400 && (strings.Contains(message, "exceeded quota for concurrent queries") ||
		strings.Contains(message, "too many concurrent queries"))
}

func flattenedTerminalAuthError(message string) bool {
	// Only inspect text within a flattened credential error. Table names and
	// other metadata context must not turn transient schema errors terminal.
	for _, prefix := range []string{"oauth2: cannot fetch token:", "transport: per-RPC creds failed due to error:"} {
		if _, credentialError, found := strings.Cut(message, prefix); found {
			return strings.Contains(credentialError, "invalid_grant") || strings.Contains(credentialError, "Invalid JWT Signature")
		}
	}
	return false
}

// shouldAbort classifies an error as terminal (abort the jobs) vs retryable.
func shouldAbort(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, errTerminalAuth) {
		return true
	}
	// AsType sees only the first match; walk joined errors so an earlier
	// transient or already-exists branch cannot hide a terminal sibling.
	type multiError interface{ Unwrap() []error }
	if joined, ok := err.(multiError); ok {
		if slices.ContainsFunc(joined.Unwrap(), shouldAbort) {
			return true
		}
	} else if wrapped := errors.Unwrap(err); wrapped != nil && shouldAbort(wrapped) {
		return true
	}
	if terminalOAuthError(err) {
		return true
	}
	if e, ok := errors.AsType[*googleapi.Error](err); ok {
		if !checkAndIgnoreAlreadyExistError(e) && !retryableBigQueryError(e) {
			switch e.Code {
			case 400, 401, 403:
				return true
			}
		}
		// A retryable REST error must not be overridden by an unrelated
		// token marker in its message (e.g. an event-derived table name).
		return false
	}
	switch status.Code(err) {
	case codes.PermissionDenied, codes.Unauthenticated, codes.FailedPrecondition, codes.Unimplemented, codes.DataLoss:
		return true
	}

	// gRPC flattens per-RPC credential errors with %v, losing their chain.
	// Keep text matching confined to the credential error, not schema context.
	return flattenedTerminalAuthError(err.Error())
}
