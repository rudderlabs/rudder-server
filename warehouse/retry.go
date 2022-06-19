package warehouse

import (
	"errors"
	"fmt"
	"strings"

	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var retryQueryPlaceHolder = "<retry>"

type RetryRequest struct {
	WorkspaceID     string
	SourceID        string
	DestinationID   string
	DestinationType string
	IntervalInHours int64   // Optional, if provided we will retry based on the interval provided
	UploadIds       []int64 // Optional, if provided we will retry the upload ids provided
	ForceRetry      bool
	API             UploadAPIT
}

type RetryResponse struct {
	Message    string
	StatusCode int32
}

func (retryReq *RetryRequest) RetryWHUploads() (response RetryResponse, err error) {
	// Request validation
	err = retryReq.validateReq()
	defer func() {
		if err != nil {
			retryReq.API.log.Errorf(`WH: Error occurred while retrying upload jobs for workspaceId: %s, sourceId: %s, destinationId: %s with error: %s`,
				retryReq.WorkspaceID,
				retryReq.SourceID,
				retryReq.DestinationID,
				err.Error(),
			)
			response = RetryResponse{
				Message:    err.Error(),
				StatusCode: 400,
			}
		}
	}()
	if err != nil {
		return
	}

	// Getting the corresponding sourceIDs from workspaceID (sourceIDsByWorkspace)
	// Also, validating if the sourceIDs list is empty or provided sourceId is present in the
	// sources list ot not.
	sourceIDs := retryReq.getSourceIDs()
	if len(sourceIDs) == 0 {
		err = fmt.Errorf("unauthorized request")
		return
	}
	if retryReq.SourceID != "" && !misc.ContainsString(sourceIDs, retryReq.SourceID) {
		err = fmt.Errorf("no such sourceID exists")
		return
	}

	// Retry request should trigger on these cases.
	// 1. Either provide the retry interval.
	// 2. Or provide the List of Upload id's that needs to be re-triggered.
	uploadsRetried, err := retryReq.retryUploads(sourceIDs)
	if err != nil {
		return
	}

	response = RetryResponse{
		Message:    successMessage(uploadsRetried),
		StatusCode: 200,
	}
	return
}

func (retryReq *RetryRequest) getSourceIDs() (sourceIDs []string) {
	sourceIDsByWorkspaceLock.RLock()
	defer sourceIDsByWorkspaceLock.RUnlock()
	var ok bool
	if sourceIDs, ok = sourceIDsByWorkspace[retryReq.WorkspaceID]; !ok {
		sourceIDs = []string{}
	}
	return sourceIDs
}

func (retryReq *RetryRequest) retryUploads(sourceIDs []string) (rowsAffected int64, err error) {
	var clausesQuery string
	var clauses []string
	var clausesArgs []interface{}

	// Preparing clauses query
	clauses, clausesArgs = retryReq.whereClauses(sourceIDs)
	for i, clause := range clauses {
		clausesQuery = clausesQuery + strings.Replace(clause, retryQueryPlaceHolder, fmt.Sprintf("$%d", i+1), 1)
		if i != len(clauses)-1 {
			clausesQuery += " AND "
		}
	}

	// Preparing the prepared statement
	preparedStatement := fmt.Sprintf(`
		UPDATE wh_uploads
		SET
			metadata = metadata || '{"retried": true, "priority": 50}' || jsonb_build_object('nextRetryTime', NOW() - INTERVAL '1 HOUR'),
			status = 'waiting',
			updated_at = NOW()
		WHERE %[1]s`,
		clausesQuery,
	)

	// Executing the statement
	result, err := retryReq.API.dbHandle.Exec(preparedStatement, clausesArgs...)
	if err != nil {
		return
	}

	// Getting rows affected
	rowsAffected, err = result.RowsAffected()
	return
}

func (retryReq *RetryRequest) whereClauses(sourceIDs []string) (clauses []string, clausesArgs []interface{}) {
	// SourceID
	if retryReq.SourceID != "" {
		clauses = append(clauses, fmt.Sprintf(`source_id = %s`, retryQueryPlaceHolder))
		clausesArgs = append(clausesArgs, retryReq.SourceID)
	} else {
		clauses = append(clauses, fmt.Sprintf(`source_id = ANY(%s)`, retryQueryPlaceHolder))
		clausesArgs = append(clausesArgs, pq.Array(sourceIDs))
	}

	// DestinationID
	if retryReq.DestinationID != "" {
		clauses = append(clauses, fmt.Sprintf(`destination_id = %s`, retryQueryPlaceHolder))
		clausesArgs = append(clausesArgs, retryReq.DestinationID)
	}

	// DestinationType
	if retryReq.DestinationType != "" {
		clauses = append(clauses, fmt.Sprintf(`destination_type = %s`, retryQueryPlaceHolder))
		clausesArgs = append(clausesArgs, retryReq.DestinationType)
	}

	// Status deciding based on forcefully retrying
	if !retryReq.ForceRetry {
		clauses = append(clauses, fmt.Sprintf("status = %s", retryQueryPlaceHolder))
		clausesArgs = append(clausesArgs, Aborted)
	}

	// UploadIDs or Retry Interval
	if len(retryReq.UploadIds) != 0 {
		clauses = append(clauses, fmt.Sprintf("id = ANY(%s)", retryQueryPlaceHolder))
		clausesArgs = append(clausesArgs, pq.Array(retryReq.UploadIds))
	} else {
		clauses = append(clauses, fmt.Sprintf("created_at > NOW() - %s * INTERVAL '1 HOUR'", retryQueryPlaceHolder))
		clausesArgs = append(clausesArgs, retryReq.IntervalInHours)
	}
	return
}

func successMessage(uploadsRetried int64) string {
	if uploadsRetried == 0 {
		return "No retried uploads to sync for this destination"
	}
	return fmt.Sprintf("Retried successfully %d syncs", uploadsRetried)
}

func (retryReq *RetryRequest) validateReq() (err error) {
	if !retryReq.API.enabled || retryReq.API.log == nil || retryReq.API.dbHandle == nil {
		err = errors.New("warehouse service is not initialized")
		return
	}

	if retryReq.SourceID == "" && retryReq.DestinationID == "" && retryReq.WorkspaceID == "" {
		err = errors.New("please provide valid request parameters while retrying jobs with workspaceId or sourceId or destinationId")
		return
	}

	if len(retryReq.UploadIds) == 0 && retryReq.IntervalInHours <= 0 {
		err = errors.New("please provide valid request parameters while retrying jobs with UploadIds or IntervalInHours")
		return
	}
	return
}
