package bqstreamallevents

import (
	"errors"
	"strings"

	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
	var e *googleapi.Error
	if errors.As(err, &e) {
		// 409 is returned when we try to create a table that already exists
		// 400 is returned for all kinds of invalid input - so we need to check the error message too
		if e.Code == 409 || (e.Code == 400 && strings.Contains(strings.ToLower(e.Message), "already exists in schema")) {
			return true
		}
	}
	return false
}

// shouldAbort classifies an error as terminal (abort the jobs) vs retryable
func shouldAbort(err error) bool {
	switch status.Code(err) {
	case codes.PermissionDenied, codes.Unauthenticated, codes.FailedPrecondition, codes.Unimplemented, codes.DataLoss:
		return true
	default:
		return false
	}
}
