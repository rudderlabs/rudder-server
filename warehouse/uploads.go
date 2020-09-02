package warehouse

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type UploadT struct {
	ID                 int64
	Namespace          string
	SourceID           string
	DestinationID      string
	DestinationType    string
	StartStagingFileID int64
	EndStagingFileID   int64
	StartLoadFileID    int64
	EndLoadFileID      int64
	Status             string
	Schema             warehouseutils.SchemaT
	Error              json.RawMessage
	Timings            []map[string]string
	FirstAttemptAt     time.Time
	LastAttemptAt      time.Time
	Attempts           int64
}

type UploadColumnT struct {
	Column string
	Value  interface{}
}

type UploadHandleT struct {
	upload   UploadT
	dbHandle *sql.DB
}

const (
	UploadStatusField          = "status"
	UploadStartLoadFileIDField = "start_load_file_id"
	UploadEndLoadFileIDField   = "end_load_file_id"
	UploadUpdatedAtField       = "updated_at"
	UploadTimingsField         = "timings"
	UploadSchemaField          = "schema"
	UploadLastExecAtField      = "last_exec_at"
)

// getUploadTimings returns timings json column
// eg. timings: [{exporting_data: 2020-04-21 15:16:19.687716, exported_data: 2020-04-21 15:26:34.344356}]
func (uploadHandle *UploadHandleT) getUploadTimings() (timings []map[string]string) {
	var rawJSON json.RawMessage
	sqlStatement := fmt.Sprintf(`SELECT timings FROM %s WHERE id=%d`, warehouseutils.WarehouseUploadsTable, uploadHandle.upload.ID)
	err := uploadHandle.dbHandle.QueryRow(sqlStatement).Scan(&rawJSON)
	if err != nil {
		return
	}
	err = json.Unmarshal(rawJSON, &timings)
	return
}

// getNewTimings appends current status with current time to timings column
// eg. status: exported_data, timings: [{exporting_data: 2020-04-21 15:16:19.687716] -> [{exporting_data: 2020-04-21 15:16:19.687716, exported_data: 2020-04-21 15:26:34.344356}]
func (uploadHandle *UploadHandleT) getNewTimings(status string) []byte {
	timings := uploadHandle.getUploadTimings()
	timing := map[string]string{status: timeutil.Now().Format(misc.RFC3339Milli)}
	timings = append(timings, timing)
	marshalledTimings, err := json.Marshal(timings)
	if err != nil {
		panic(err)
	}
	return marshalledTimings
}

func (uploadHandle *UploadHandleT) getUploadFirstAttemptTime() (timing time.Time) {
	var firstTiming sql.NullString
	sqlStatement := fmt.Sprintf(`SELECT timings->0 as firstTimingObj FROM %s WHERE id=%d`, warehouseutils.WarehouseUploadsTable, uploadHandle.upload.ID)
	err := uploadHandle.dbHandle.QueryRow(sqlStatement).Scan(&firstTiming)
	if err != nil {
		return
	}
	_, timing = warehouseutils.TimingFromJSONString(firstTiming)
	return timing
}

func (uploadHandle *UploadHandleT) setUploadStatus(status string, additionalFields ...UploadColumnT) (err error) {
	logger.Infof("WH: Setting status of %s for wh_upload:%v", status, uploadHandle.upload.ID)
	marshalledTimings := uploadHandle.getNewTimings(status)
	opts := []UploadColumnT{
		{Column: UploadStatusField, Value: status},
		{Column: UploadTimingsField, Value: marshalledTimings},
		{Column: UploadUpdatedAtField, Value: timeutil.Now()},
	}

	additionalFields = append(additionalFields, opts...)

	uploadHandle.setUploadColumns(
		additionalFields...,
	)
	return
}

// SetSchema
func (uploadHandle *UploadHandleT) setSchema(schema interface{}) error {
	return uploadHandle.setUploadColumns(
		UploadColumnT{Column: UploadSchemaField, Value: schema},
	)
}

// SetUploadColumns sets any column values passed as args in UploadColumnT format for WarehouseUploadsTable
func (uploadHandle *UploadHandleT) setUploadColumns(fields ...UploadColumnT) (err error) {
	var columns string
	values := []interface{}{uploadHandle.upload.ID}
	// setting values using syntax $n since Exec can correctly format time.Time strings
	for idx, f := range fields {
		// start with $2 as $1 is upload.ID
		columns += fmt.Sprintf(`%s=$%d`, f.Column, idx+2)
		if idx < len(fields)-1 {
			columns += ","
		}
		values = append(values, f.Value)
	}
	sqlStatement := fmt.Sprintf(`UPDATE %s SET %s WHERE id=$1`, warehouseutils.WarehouseUploadsTable, columns)
	_, err = dbHandle.Exec(sqlStatement, values...)
	if err != nil {
		panic(err)
	}
	return
}

func (uploadHandle *UploadHandleT) setUploadError(statusError error, state string) (err error) {
	logger.Errorf("WH: Failed during %s stage: %v\n", state, statusError.Error())
	upload := uploadHandle.upload
	dbHandle := uploadHandle.dbHandle

	uploadHandle.setUploadStatus(state)
	var e map[string]map[string]interface{}
	json.Unmarshal(uploadHandle.upload.Error, &e)
	if e == nil {
		e = make(map[string]map[string]interface{})
	}
	if _, ok := e[state]; !ok {
		e[state] = make(map[string]interface{})
	}
	errorByState := e[state]
	// increment attempts for errored stage
	if attempt, ok := errorByState["attempt"]; ok {
		errorByState["attempt"] = int(attempt.(float64)) + 1
	} else {
		errorByState["attempt"] = 1
	}
	// append errors for errored stage
	if errList, ok := errorByState["errors"]; ok {
		errorByState["errors"] = append(errList.([]interface{}), statusError.Error())
	} else {
		errorByState["errors"] = []string{statusError.Error()}
	}
	// abort after configured retry attempts
	if errorByState["attempt"].(int) > minRetryAttempts {
		firstTiming := uploadHandle.getUploadFirstAttemptTime(upload, dbHandle)
		if !firstTiming.IsZero() && (timeutil.Now().Sub(firstTiming) > retryTimeWindow) {
			state = warehouseutils.AbortedState
		}
	}
	serializedErr, _ := json.Marshal(&e)
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1, error=$2, updated_at=$3 WHERE id=$4`, warehouseutils.WarehouseUploadsTable)
	_, err = dbHandle.Exec(sqlStatement, state, serializedErr, timeutil.Now(), upload.ID)
	if err != nil {
		panic(err)
	}
	return
}
