package warehouseutils

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

const (
	GeneratingLoadFileState       = "generationg_load_file"
	GeneratingLoadFileFailedState = "generationg_load_file_failed"
	GeneratedLoadFileState        = "generated_load_file"
	UpdatingSchemaState           = "updating_schema"
	UpdatingSchemaFailedState     = "updating_schema_failed"
	UpdatedSchemaState            = "updated_schema"
	ExportingDataState            = "exporting_data"
	ExportingDataFailedState      = "exporting_data_failed"
	ExportedDataState             = "exported_data"
	AbortedState                  = "aborted"
)

const (
	StagingFileSucceededState = "succeeded"
	StagingFileFailedState    = "failed"
	StagingFileExecutingState = "executing"
	StagingFileAbortedState   = "aborted"
	StagingFileWaitingState   = "waiting"
)

var (
	warehouseUploadsTable      string
	warehouseSchemasTable      string
	warehouseLoadFilesTable    string
	warehouseStagingFilesTable string
	maxRetry                   int
)

var ObjectStorageMap = map[string]string{
	"RS": "S3",
	"BQ": "GCS",
}

func init() {
	config.Initialize()
	loadConfig()
}

func loadConfig() {
	warehouseUploadsTable = config.GetString("Warehouse.uploadsTable", "wh_uploads")
	warehouseSchemasTable = config.GetString("Warehouse.schemasTable", "wh_schemas")
	warehouseLoadFilesTable = config.GetString("Warehouse.loadFilesTable", "wh_load_files")
	warehouseStagingFilesTable = config.GetString("Warehouse.stagingFilesTable", "wh_staging_files")
	maxRetry = config.GetInt("Warehouse.maxRetry", 3)
}

type WarehouseT struct {
	Source      backendconfig.SourceT
	Destination backendconfig.DestinationT
}

type ConfigT struct {
	DbHandle  *sql.DB
	Upload    UploadT
	Warehouse WarehouseT
	Stage     string
}

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
	Schema             map[string]map[string]string
	Error              json.RawMessage
	// Error              map[string]map[string]interface{}
}

func GetCurrentSchema(dbHandle *sql.DB, warehouse WarehouseT) (map[string]map[string]string, error) {
	var rawSchema json.RawMessage
	sqlStatement := fmt.Sprintf(`SELECT schema FROM %[1]s WHERE (%[1]s.source_id='%[2]s' AND %[1]s.destination_id='%[3]s') ORDER BY %[1]s.id DESC`, warehouseSchemasTable, warehouse.Source.ID, warehouse.Destination.ID)
	err := dbHandle.QueryRow(sqlStatement).Scan(&rawSchema)
	if err != nil {
		if err == sql.ErrNoRows {
			return make(map[string]map[string]string), nil
		}
		misc.AssertError(err)
	}
	var schemaMapInterface map[string]interface{}
	err = json.Unmarshal(rawSchema, &schemaMapInterface)
	misc.AssertError(err)
	schema := make(map[string]map[string]string)
	for key, val := range schemaMapInterface {
		y := make(map[string]string)
		x := val.(map[string]interface{})
		for k, v := range x {
			y[k] = v.(string)
		}
		schema[key] = y
	}
	return schema, nil
}

type SchemaDiffT struct {
	Tables        []string
	ColumnMaps    map[string]map[string]string
	UpdatedSchema map[string]map[string]string
}

func GetSchemaDiff(currentSchema, uploadSchema map[string]map[string]string) (diff SchemaDiffT) {
	diff = SchemaDiffT{
		Tables:        []string{},
		ColumnMaps:    make(map[string]map[string]string),
		UpdatedSchema: make(map[string]map[string]string),
	}
	for tableName, uploadColumnMap := range uploadSchema {
		currentColumnsMap, ok := currentSchema[tableName]
		if !ok {
			diff.Tables = append(diff.Tables, tableName)
			diff.ColumnMaps[tableName] = uploadColumnMap
			diff.UpdatedSchema[tableName] = uploadColumnMap
		} else {
			diff.UpdatedSchema[tableName] = currentSchema[tableName]
			diff.ColumnMaps[tableName] = make(map[string]string)
			for columnName, columnVal := range uploadColumnMap {
				if _, ok := currentColumnsMap[columnName]; !ok {
					diff.ColumnMaps[tableName][columnName] = columnVal
					diff.UpdatedSchema[tableName][columnName] = columnVal
				}
			}
		}
	}
	return
}

func SetUploadStatus(upload UploadT, status string, dbHandle *sql.DB) (err error) {
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1, updated_at=$2 WHERE id=$3`, warehouseUploadsTable)
	_, err = dbHandle.Exec(sqlStatement, status, time.Now(), upload.ID)
	misc.AssertError(err)
	return
}

func SetUploadError(upload UploadT, statusError error, state string, dbHandle *sql.DB) (err error) {
	SetUploadStatus(upload, ExportingDataFailedState, dbHandle)
	var e map[string]map[string]interface{}
	json.Unmarshal(upload.Error, &e)
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
	if errorByState["attempt"].(int) > maxRetry {
		state = AbortedState
	}
	serializedErr, _ := json.Marshal(&e)
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1, error=$2, updated_at=$3 WHERE id=$4`, warehouseUploadsTable)
	_, err = dbHandle.Exec(sqlStatement, state, serializedErr, time.Now(), upload.ID)
	misc.AssertError(err)
	return
}

func SetStagingFilesStatus(ids []int64, status string, dbHandle *sql.DB) (err error) {
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1, updated_at=$2 WHERE id=ANY($3)`, warehouseStagingFilesTable)
	_, err = dbHandle.Exec(sqlStatement, status, time.Now(), pq.Array(ids))
	misc.AssertError(err)
	return
}

func SetStagingFilesError(ids []int64, status string, dbHandle *sql.DB, statusError error) (err error) {
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1, error=$2, updated_at=$3 WHERE id=ANY($4)`, warehouseStagingFilesTable)
	_, err = dbHandle.Exec(sqlStatement, status, statusError.Error(), time.Now(), pq.Array(ids))
	misc.AssertError(err)
	return
}

func UpdateCurrentSchema(wh WarehouseT, uploadID int64, currentSchema, schema map[string]map[string]string, dbHandle *sql.DB) (err error) {
	marshalledSchema, err := json.Marshal(schema)
	if len(currentSchema) == 0 {
		sqlStatement := fmt.Sprintf(`INSERT INTO %s (wh_upload_id, source_id, namespace, destination_id, destination_type, schema, created_at)
									   VALUES ($1, $2, $3, $4, $5, $6, $7)`, warehouseSchemasTable)
		stmt, err := dbHandle.Prepare(sqlStatement)
		misc.AssertError(err)
		defer stmt.Close()

		_, err = stmt.Exec(uploadID, wh.Source.ID, strings.ToLower(wh.Source.Name), wh.Destination.ID, wh.Destination.DestinationDefinition.Name, marshalledSchema, time.Now())
	} else {
		sqlStatement := fmt.Sprintf(`UPDATE %s SET schema=$1 WHERE source_id=$2 AND destination_id=$3`, warehouseSchemasTable)
		_, err = dbHandle.Exec(sqlStatement, marshalledSchema, wh.Source.ID, wh.Destination.ID)
	}
	misc.AssertError(err)
	return
}

func GetLoadFileLocations(dbHandle *sql.DB, sourceId string, destinationId string, tableName string, start, end int64) (locations []string, err error) {
	sqlStatement := fmt.Sprintf(`SELECT location FROM %[1]s
								WHERE ( %[1]s.source_id='%[2]s' AND %[1]s.destination_id='%[3]s' AND %[1]s.table_name='%[4]s' AND %[1]s.id > %[5]v AND %[1]s.id <= %[6]v)`,
		warehouseLoadFilesTable, sourceId, destinationId, tableName, start, end)
	rows, err := dbHandle.Query(sqlStatement)
	defer rows.Close()
	misc.AssertError(err)

	for rows.Next() {
		var location string
		err := rows.Scan(&location)
		misc.AssertError(err)
		locations = append(locations, location)
	}
	return
}

func GetS3Location(location string) string {
	str1 := strings.Replace(location, "https", "s3", 1)
	str2 := strings.Replace(str1, ".s3.amazonaws.com", "", 1)
	return str2
}

func GetGCSLocation(location string) string {
	str1 := strings.Replace(location, "https", "gs", 1)
	str2 := strings.Replace(str1, "storage.googleapis.com/", "", 1)
	return str2
}

func GetS3Locations(locations []string) (s3Locations []string, err error) {
	for _, location := range locations {
		s3Locations = append(s3Locations, GetS3Location((location)))
	}
	return
}

func GetGCSLocations(locations []string) (gcsLocations []string, err error) {
	for _, location := range locations {
		gcsLocations = append(gcsLocations, GetGCSLocation((location)))
	}
	return
}

func JSONSchemaToMap(rawMsg json.RawMessage) map[string]map[string]string {
	var schema map[string]map[string]string
	err := json.Unmarshal(rawMsg, &schema)
	misc.AssertError(err)
	return schema
}

func DestStat(statType string, statName string, id string) *stats.RudderStats {
	return stats.NewBatchDestStat(fmt.Sprintf("warehouse.%s", statName), statType, id)
}
