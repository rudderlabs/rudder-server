package warehouse

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/lib/pq"
)

type RetryReq struct {
	WorkspaceID   string
	DestinationID string

	IntervalInHours int64   // Optional, if provided we will retry based on the interval provided
	UploadIds       []int64 // Optional, if provided we will retry the upload ids provided

	API        UploadAPIT
	ForceRetry bool
}

type RetryRes struct {
	Message    string
	StatusCode int32
}

func (retryReq *RetryReq) RetryWHUploads() (response RetryRes, err error) {
	// Request validation
	err = retryReq.validateReq()
	defer func() {
		if err != nil {
			retryReq.API.log.Errorf("WH: Error occurred while retrying upload jobs with error: %s", err.Error())
			response = RetryRes{
				Message:    err.Error(),
				StatusCode: 400,
			}
		}
	}()
	if err != nil {
		return
	}

	// Retrying uploads
	uploadsRetried, err := retryReq.retryUploads()
	if err != nil {
		return
	}

	response = RetryRes{
		Message:    retryReq.successMessage(uploadsRetried),
		StatusCode: 200,
	}
	return
}

func (retryReq *RetryReq) retryUploads() (rowsAffected int64, err error) {
	var retryClause, statusClause string
	if len(retryReq.UploadIds) != 0 {
		retryClause = `id = ANY($1)`
	} else {
		retryClause = `destination_id = $1 AND created_at > NOW() - $2 * INTERVAL '1 HOUR'`
	}
	if !retryReq.ForceRetry {
		statusClause = fmt.Sprintf("AND status = '%s'", Aborted)
	}

	sqlPreparedStatement := fmt.Sprintf(`
		UPDATE wh_uploads
		SET
			metadata = metadata || '{"retried": true, "priority": 50}' || jsonb_build_object('nextRetryTime', NOW() - INTERVAL '1 HOUR'),
			status = 'waiting',
			updated_at = NOW()
		WHERE %[1]s %[2]s`,
		retryClause,
		statusClause,
	)
	retryReq.API.log.Info(sqlPreparedStatement)

	var res sql.Result
	if len(retryReq.UploadIds) != 0 {
		res, err = retryReq.API.dbHandle.Exec(sqlPreparedStatement, pq.Array(retryReq.UploadIds))
	} else {
		res, err = retryReq.API.dbHandle.Exec(sqlPreparedStatement, retryReq.DestinationID, retryReq.IntervalInHours)
	}
	if err != nil {
		return
	}

	// Getting rows affected
	rowsAffected, err = res.RowsAffected()
	if err != nil {
		return
	}
	return
}

func (retryReq *RetryReq) successMessage(uploadsRetried int64) string {
	if uploadsRetried == 0 {
		return "No retried uploads to sync for this destination"
	}
	return fmt.Sprintf("Retried successfully %d syncs", uploadsRetried)
}

// Retry request should trigger on these cases.
// 1. Either provide the retry interval.
// 2. List of Upload id's that needs to be re-triggered.
func (retryReq RetryReq) validateReq() (err error) {
	if !retryReq.API.enabled || retryReq.API.log == nil || retryReq.API.dbHandle == nil {
		err = errors.New("warehouse api's are not initialized")
		return
	}

	// Checking for either IntervalInHours or UploadIds present.
	if retryReq.IntervalInHours == 0 && len(retryReq.UploadIds) == 0 {
		err = errors.New("please verify either uploadIDs or retryIntervalInHours is provided in the request")
		return
	}

	// Check for the valid destination id as per the connection map present in warehouse.
	_, ok := connectionsMap[retryReq.DestinationID]
	if !ok {
		pkgLogger.Errorf(`Unauthorized request for workspaceId: %s in sourceId: %s`, retryReq.WorkspaceID, retryReq.DestinationID)
		err = errors.New("unauthorized request")
		return
	}
	return
}
