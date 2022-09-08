package warehouse

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

const (
	retryQueryPlaceHolder = "<retry>"
)

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
	Count      int64
	Message    string
	StatusCode int32
}

func (retryReq *RetryRequest) RetryWHUploads(ctx context.Context) (response RetryResponse, err error) {
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
		err = fmt.Errorf("failed validating request, error: %s", err.Error())
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
	clausesQuery, clausesArgs := retryReq.clausesQuery(sourceIDs)
	uploadsRetried, err := retryReq.API.warehouseDBHandle.RetryUploads(ctx, clausesQuery, clausesArgs)
	if err != nil {
		err = fmt.Errorf("failed retrying uploads, error: %s", err.Error())
		return
	}

	response = RetryResponse{
		Count:      uploadsRetried,
		StatusCode: 200,
	}
	return
}

func (retryReq *RetryRequest) UploadsToRetry(ctx context.Context) (response RetryResponse, err error) {
	// Request validation
	err = retryReq.validateReq()
	defer func() {
		if err != nil {
			retryReq.API.log.Errorf(`WH: Error occurred while fetching upload jobs to retry for workspaceId: %s, sourceId: %s, destinationId: %s with error: %s`,
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
		err = fmt.Errorf("failed validating request, error: %s", err.Error())
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
	clausesQuery, clausesArgs := retryReq.clausesQuery(sourceIDs)
	count, err := retryReq.API.warehouseDBHandle.CountUploadsToRetry(ctx, clausesQuery, clausesArgs)
	if err != nil {
		err = fmt.Errorf("failed counting uploads to retry, error: %s", err.Error())
		return
	}

	response = RetryResponse{
		Count:      count,
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

func (retryReq *RetryRequest) clausesQuery(sourceIDs []string) (string, []interface{}) {
	var (
		clauses      []string
		clausesArgs  []interface{}
		clausesQuery string
	)

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

	for i, clause := range clauses {
		clausesQuery = clausesQuery + strings.Replace(clause, retryQueryPlaceHolder, fmt.Sprintf("$%d", i+1), 1)
		if i != len(clauses)-1 {
			clausesQuery += " AND "
		}
	}
	return clausesQuery, clausesArgs
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
