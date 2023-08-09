package warehouse

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/lib/pq"
	"golang.org/x/exp/slices"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
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
				StatusCode: http.StatusBadRequest,
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
	if retryReq.SourceID != "" && !slices.Contains(sourceIDs, retryReq.SourceID) {
		err = fmt.Errorf("no such sourceID exists")
		return
	}

	// Retry request should trigger on these cases.
	// 1. Either provide the retry interval.
	// 2. Or provide the List of Upload id's that needs to be re-triggered.
	uploadsRetried, err := retryReq.API.warehouseDBHandle.RetryUploads(ctx, retryReq.clausesQuery(sourceIDs)...)
	if err != nil {
		err = fmt.Errorf("failed retrying uploads, error: %s", err.Error())
		return
	}

	response = RetryResponse{
		Count:      uploadsRetried,
		StatusCode: http.StatusOK,
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
				StatusCode: http.StatusBadRequest,
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
	if retryReq.SourceID != "" && !slices.Contains(sourceIDs, retryReq.SourceID) {
		err = fmt.Errorf("no such sourceID exists")
		return
	}

	// Retry request should trigger on these cases.
	// 1. Either provide the retry interval.
	// 2. Or provide the List of Upload id's that needs to be re-triggered.
	count, err := retryReq.API.warehouseDBHandle.GetUploadsCount(ctx, retryReq.clausesQuery(sourceIDs)...)
	if err != nil {
		err = fmt.Errorf("failed counting uploads to retry, error: %s", err.Error())
		return
	}

	response = RetryResponse{
		Count:      count,
		StatusCode: http.StatusOK,
	}
	return
}

func (retryReq *RetryRequest) getSourceIDs() []string {
	return retryReq.API.bcManager.SourceIDsByWorkspace()[retryReq.WorkspaceID]
}

func (retryReq *RetryRequest) clausesQuery(sourceIDs []string) []FilterClause {
	var clauses []FilterClause

	// SourceID
	if retryReq.SourceID != "" {
		clauses = append(clauses, FilterClause{
			Clause:    fmt.Sprintf(`source_id = %s`, queryPlaceHolder),
			ClauseArg: retryReq.SourceID,
		})
	} else {
		clauses = append(clauses, FilterClause{
			Clause:    fmt.Sprintf(`source_id = ANY(%s)`, queryPlaceHolder),
			ClauseArg: pq.Array(sourceIDs),
		})
	}

	// DestinationID
	if retryReq.DestinationID != "" {
		clauses = append(clauses, FilterClause{
			Clause:    fmt.Sprintf(`destination_id = %s`, queryPlaceHolder),
			ClauseArg: retryReq.DestinationID,
		})
	}

	// DestinationType
	if retryReq.DestinationType != "" {
		clauses = append(clauses, FilterClause{
			Clause:    fmt.Sprintf(`destination_type = %s`, queryPlaceHolder),
			ClauseArg: retryReq.DestinationType,
		})
	}

	// Status deciding based on forcefully retrying
	if !retryReq.ForceRetry {
		clauses = append(clauses, FilterClause{
			Clause:    fmt.Sprintf("status = %s", queryPlaceHolder),
			ClauseArg: model.Aborted,
		})
	}

	// UploadIDs or Retry Interval
	if len(retryReq.UploadIds) != 0 {
		clauses = append(clauses, FilterClause{
			Clause:    fmt.Sprintf("id = ANY(%s)", queryPlaceHolder),
			ClauseArg: pq.Array(retryReq.UploadIds),
		})
	} else {
		clauses = append(clauses, FilterClause{
			Clause:    fmt.Sprintf("created_at > NOW() - %s * INTERVAL '1 HOUR'", queryPlaceHolder),
			ClauseArg: retryReq.IntervalInHours,
		})
	}
	return clauses
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
