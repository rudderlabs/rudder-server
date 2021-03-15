package warehouse

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	destinationdebugger "github.com/rudderlabs/rudder-server/services/destination-debugger"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func (wh *HandleT) recordDeliveryStatus(destID string, uploadID int64) {
	if !destinationdebugger.HasUploadEnabled(destID) {
		return
	}
	var (
		sourceID      string
		destinationID string
		status        string
		errorCode     string
		errorResp     string
		updatedAt     time.Time
		tableName     string
		tableStatus   string
		attemptNum    int
	)
	successfulTableUploads := make([]string, 0)
	failedTableUploads := make([]string, 0)

	sqlStatement := fmt.Sprintf(`select source_id, destination_id, status, error, updated_at from %s where id=%d`, warehouseutils.WarehouseUploadsTable, uploadID)
	row := wh.dbHandle.QueryRow(sqlStatement)
	err := row.Scan(&sourceID, &destinationID, &status, &errorResp, &updatedAt)
	if err != nil && err != sql.ErrNoRows {
		panic(fmt.Errorf("Query: %s\nfailed with Error : %w", sqlStatement, err))
	}

	sqlStatement = fmt.Sprintf(`select table_name, status from %s where wh_upload_id=%d`, warehouseutils.WarehouseTableUploadsTable, uploadID)
	rows, err := wh.dbHandle.Query(sqlStatement)
	if err != nil && err != sql.ErrNoRows {
		panic(fmt.Errorf("Query: %s\nfailed with Error : %w", sqlStatement, err))
	}
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&tableName, &tableStatus)
		if tableStatus == TableUploadExported {
			successfulTableUploads = append(successfulTableUploads, tableName)
		} else {
			failedTableUploads = append(failedTableUploads, tableName)
		}
	}

	var errJSON map[string]map[string]interface{}
	err = json.Unmarshal([]byte(errorResp), &errJSON)
	if err != nil {
		panic(fmt.Errorf("Unmarshalling: %s failed with Error : %w", errorResp, err))
	}
	if stateErr, ok := errJSON[status]; ok {
		if attempt, ok := stateErr["Attempt"]; ok {
			if floatAttempt, ok := attempt.(float64); ok {
				attemptNum = attemptNum + int(floatAttempt)
			}
		}
	}
	if attemptNum == 0 {
		attemptNum = 1
	}
	var errorRespB []byte
	if errorResp == "{}" {
		errorCode = "200"
	} else {
		errorCode = "400"

	}
	errorRespB, _ = json.Marshal(ErrorResponseT{Error: errorResp})

	payloadMap := map[string]interface{}{
		"lastSyncedAt":             updatedAt,
		"successful_table_uploads": successfulTableUploads,
		"failed_table_uploads":     failedTableUploads,
		"uploadID":                 uploadID,
		"error":                    errorResp,
	}
	payload, _ := json.Marshal(payloadMap)
	deliveryStatus := destinationdebugger.DeliveryStatusT{
		DestinationID: destinationID,
		SourceID:      sourceID,
		Payload:       payload,
		AttemptNum:    attemptNum,
		JobState:      status,
		ErrorCode:     errorCode,
		ErrorResponse: errorRespB,
	}
	destinationdebugger.RecordEventDeliveryStatus(destinationID, &deliveryStatus)
}

// syncLiveWarehouseStatus fetch last 10 records order by updated_at desc and sends uploadIds in reverse order to recordDeliveryStatus.
// that way we can fetch last 10 records order by updated_at asc
func (wh *HandleT) syncLiveWarehouseStatus(sourceID string, destinationID string) {
	sqlStatement := fmt.Sprintf(`select id from %s where source_id='%s' and destination_id='%s' order by updated_at desc limit %d`, warehouseutils.WarehouseUploadsTable, sourceID, destinationID, warehouseSyncPreFetchCount)
	rows, err := wh.dbHandle.Query(sqlStatement)
	if err != nil && err != sql.ErrNoRows {
		panic(fmt.Errorf("Query: %s\nfailed with Error : %w", sqlStatement, err))
	}
	defer rows.Close()
	var uploadIDs []int64
	for rows.Next() {
		var uploadID int64
		err = rows.Scan(&uploadID)
		if err != nil {
			panic(fmt.Errorf("Failed to scan result from query: %s\nwith Error : %w", sqlStatement, err))
		}
		uploadIDs = append(uploadIDs, uploadID)
	}

	for index := range uploadIDs {
		wh.recordDeliveryStatus(destinationID, uploadIDs[len(uploadIDs)-1-index])
	}
}
