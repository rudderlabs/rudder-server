package warehouse

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rudderlabs/rudder-server/proto/warehouse"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"strings"
	"time"
)

type RetryReq struct {
	WorkspaceID   string
	DestinationID string

	IntervalInHours int64   // Optional, if provided we will retry based on the interval provided
	UploadIds       []int64 // Optional, if provided we will retry the upload ids provided

	API UploadAPIT
}

func (retryReq *RetryReq) RetryWHUploads() (response *proto.RetryWHUploadsResponse, err error) {
	// Request validation
	err = retryReq.validateReq()
	defer func() {
		if err != nil {
			retryReq.API.log.Errorf("WH: Error occurred while retrying upload jobs with error: ", err.Error())
			response = &proto.RetryWHUploadsResponse{
				Message:    err.Error(),
				StatusCode: 400,
			}
		}
	}()
	if err != nil {
		return
	}

	// Generating query
	query := retryReq.getQueryToRetry(`id, source_id, destination_id, metadata`)
	retryReq.API.log.Debug(query)

	// Getting corresponding uploads
	uploadsToRetry, err := retryReq.getUploadsToRetry(query)
	if err != nil {
		return
	}

	// Checking if there are no upload jobs to be retried
	if len(uploadsToRetry) == 0 {
		err = nil
		response = &proto.RetryWHUploadsResponse{
			Message:    "No retried uploads to sync for this destination",
			StatusCode: 200,
		}
		return
	}

	// Retrying uploads
	err = retryReq.retryUploads(uploadsToRetry)
	if err != nil {
		return
	}

	response = &proto.RetryWHUploadsResponse{
		Message:    fmt.Sprintf("Retried successfully %d syncs", len(uploadsToRetry)),
		StatusCode: 200,
	}
	return
}

func (retryReq *RetryReq) getQueryToRetry(selectedFields string) string {
	if len(retryReq.UploadIds) != 0 {
		return fmt.Sprintf(`SELECT %s FROM %s WHERE id IN (%s)`,
			selectedFields,
			warehouseutils.WarehouseUploadsTable,
			strings.Trim(strings.Replace(fmt.Sprint(retryReq.UploadIds), " ", ",", -1), "[]"),
		)
	}
	return fmt.Sprintf(`SELECT %s FROM %s WHERE destination_id = '%s' AND created_at > INTERVAL - %d HOUR`,
		selectedFields, warehouseutils.WarehouseUploadsTable,
		retryReq.DestinationID,
		retryReq.IntervalInHours,
	)
}

func (retryReq *RetryReq) getUploadsToRetry(retryStatement string) (uploads []UploadJobT, err error) {
	rows, err := retryReq.API.dbHandle.Query(retryStatement)
	if err == sql.ErrNoRows {
		err = nil
		return
	}
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var uploadJob UploadJobT
		var upload UploadT

		err = rows.Scan(&upload.ID, &upload.SourceID, &upload.DestinationID, &upload.Metadata)
		if err != nil {
			return
		}

		uploadJob.upload = &upload
		uploadJob.dbHandle = retryReq.API.dbHandle
		uploads = append(uploads, uploadJob)
	}
	return
}

func (retryReq *RetryReq) retryUploads(uploads []UploadJobT) (err error) {
	// Begin transaction
	txn, err := retryReq.API.dbHandle.Begin()
	if err != nil {
		return
	}

	// Updating status, metadata, updated_at for retried jobs
	for _, upload := range uploads {
		upload.uploadLock.Lock()
		defer upload.uploadLock.Unlock()

		var metadata map[string]interface{}
		unmarshallErr := json.Unmarshal(upload.upload.Metadata, &metadata)
		if unmarshallErr != nil {
			metadata = make(map[string]interface{})
		}
		metadata["nextRetryTime"] = time.Now().Add(-time.Hour * 1).Format(time.RFC3339)
		metadata["retried"] = true
		metadata["priority"] = 50

		metadataJSON, err := json.Marshal(metadata)
		if err != nil {
			return err
		}

		uploadColumns := []UploadColumnT{
			{Column: "status", Value: Waiting},
			{Column: "metadata", Value: metadataJSON},
			{Column: "updated_at", Value: timeutil.Now()},
		}
		err = upload.setUploadColumns(UploadColumnsOpts{Fields: uploadColumns, Txn: txn})
		if err != nil {
			return
		}
	}

	// Committing transaction
	err = txn.Commit()
	return
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
