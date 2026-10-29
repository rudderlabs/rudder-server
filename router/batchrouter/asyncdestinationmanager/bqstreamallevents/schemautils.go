package bqstreamallevents

import (
	"errors"
	"net/url"
	"strings"

	"cloud.google.com/go/auth"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"

	bigqueryintegration "github.com/rudderlabs/rudder-server/warehouse/integrations/bigquery"
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

// invalidGrantError reports whether the OAuth token endpoint rejected the
// service-account credentials (e.g. an unknown key or account).
func invalidGrantError(err error) bool {
	e, ok := errors.AsType[*auth.Error](err)
	if !ok {
		return false
	}
	var response struct {
		Error string `json:"error"`
	}
	return jsonrs.Unmarshal(e.Body, &response) == nil && response.Error == "invalid_grant"
}

// shouldAbort classifies an error as terminal (abort the jobs) vs retryable.
func shouldAbort(err error) bool {
	if invalidGrantError(err) || errors.Is(err, bigqueryintegration.ErrIncompatibleCredentials) {
		return true
	}
	// The auth library reports an unparseable key as a plain string inside the
	// HTTP transport error. Match only there: other error text can carry event values.
	if e, ok := errors.AsType[*url.Error](err); ok && strings.HasPrefix(e.Err.Error(), "failed to parse private key") {
		return true
	}
	switch status.Code(err) {
	case codes.PermissionDenied, codes.Unauthenticated, codes.FailedPrecondition, codes.Unimplemented, codes.DataLoss:
		return true
	default:
		return false
	}
}
