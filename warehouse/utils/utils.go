package warehouseutils

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

const (
	WaitingState                  = "waiting"
	ExecutingState                = "executing"
	GeneratingLoadFileState       = "generating_load_file"
	GeneratingLoadFileFailedState = "generating_load_file_failed"
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
	warehouseTableUploadsTable string
	warehouseSchemasTable      string
	warehouseLoadFilesTable    string
	warehouseStagingFilesTable string
	maxRetry                   int
	serverIP                   string
)

var ObjectStorageMap = map[string]string{
	"RS":        "S3",
	"BQ":        "GCS",
	"SNOWFLAKE": "S3",
}

func init() {
	config.Initialize()
	loadConfig()
}

func loadConfig() {
	warehouseUploadsTable = config.GetString("Warehouse.uploadsTable", "wh_uploads")
	warehouseTableUploadsTable = config.GetString("Warehouse.tableUploadsTable", "wh_table_uploads")
	warehouseSchemasTable = config.GetString("Warehouse.schemasTable", "wh_schemas")
	warehouseLoadFilesTable = config.GetString("Warehouse.loadFilesTable", "wh_load_files")
	warehouseStagingFilesTable = config.GetString("Warehouse.stagingFilesTable", "wh_staging_files")
	maxRetry = config.GetInt("Warehouse.maxRetry", 3)
}

type WarehouseT struct {
	Source      backendconfig.SourceT
	Destination backendconfig.DestinationT
}

type DestinationT struct {
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
}

type CurrentSchemaT struct {
	Namespace string
	Schema    map[string]map[string]string
}

type StagingFileT struct {
	Schema           map[string]map[string]interface{}
	BatchDestination DestinationT
	Location         string
}

func GetCurrentSchema(dbHandle *sql.DB, warehouse WarehouseT) (CurrentSchemaT, error) {
	var rawSchema json.RawMessage
	var namespace string
	sqlStatement := fmt.Sprintf(`SELECT namespace, schema FROM %[1]s WHERE (%[1]s.source_id='%[2]s' AND %[1]s.destination_id='%[3]s') ORDER BY %[1]s.id DESC`, warehouseSchemasTable, warehouse.Source.ID, warehouse.Destination.ID)
	err := dbHandle.QueryRow(sqlStatement).Scan(&namespace, &rawSchema)
	if err != nil {
		if err == sql.ErrNoRows {
			return CurrentSchemaT{}, nil
		}
		if err != nil {
			panic(err)
		}
	}
	var schemaMapInterface map[string]interface{}
	err = json.Unmarshal(rawSchema, &schemaMapInterface)
	if err != nil {
		panic(err)
	}
	schema := make(map[string]map[string]string)
	for key, val := range schemaMapInterface {
		y := make(map[string]string)
		x := val.(map[string]interface{})
		for k, v := range x {
			y[k] = v.(string)
		}
		schema[key] = y
	}
	currentSchema := CurrentSchemaT{Namespace: namespace, Schema: schema}
	return currentSchema, nil
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
	logger.Infof("WH: Setting status of %s for wh_upload:%v", status, upload.ID)
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1, updated_at=$2 WHERE id=$3`, warehouseUploadsTable)
	_, err = dbHandle.Exec(sqlStatement, status, time.Now(), upload.ID)
	if err != nil {
		panic(err)
	}
	return
}

func SetUploadError(upload UploadT, statusError error, state string, dbHandle *sql.DB) (err error) {
	logger.Errorf("WH: Failed during %s stage: %v\n", state, statusError.Error())
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
	if err != nil {
		panic(err)
	}
	return
}

func SetStagingFilesStatus(ids []int64, status string, dbHandle *sql.DB) (err error) {
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1, updated_at=$2 WHERE id=ANY($3)`, warehouseStagingFilesTable)
	_, err = dbHandle.Exec(sqlStatement, status, time.Now(), pq.Array(ids))
	if err != nil {
		panic(err)
	}
	return
}

func SetStagingFilesError(ids []int64, status string, dbHandle *sql.DB, statusError error) (err error) {
	logger.Errorf("WH: Failed processing staging files: %v", statusError.Error())
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1, error=$2, updated_at=$3 WHERE id=ANY($4)`, warehouseStagingFilesTable)
	_, err = dbHandle.Exec(sqlStatement, status, statusError.Error(), time.Now(), pq.Array(ids))
	if err != nil {
		panic(err)
	}
	return
}

func SetTableUploadStatus(status string, uploadID int64, tableName string, dbHandle *sql.DB) (err error) {
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1, updated_at=$2, last_exec_time=$2 WHERE wh_upload_id=$3 AND table_name=$4`, warehouseTableUploadsTable)
	logger.Infof("WH: Setting table upload status: %v", sqlStatement)
	_, err = dbHandle.Exec(sqlStatement, status, time.Now(), uploadID, tableName)
	if err != nil {
		panic(err)
	}
	return
}

func SetTableUploadError(status string, uploadID int64, tableName string, statusError error, dbHandle *sql.DB) (err error) {
	logger.Errorf("WH: Failed uploading table-%s for upload-%v: %v", tableName, uploadID, statusError.Error())
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1, updated_at=$2, error=$3 WHERE wh_upload_id=$4 AND table_name=$5`, warehouseTableUploadsTable)
	logger.Infof("WH: Setting table upload error: %v", sqlStatement)
	_, err = dbHandle.Exec(sqlStatement, status, time.Now(), statusError.Error(), uploadID, tableName)
	if err != nil {
		panic(err)
	}
	return
}

func GetTableUploadStatus(uploadID int64, tableName string, dbHandle *sql.DB) (status string, err error) {
	sqlStatement := fmt.Sprintf(`SELECT status from %s WHERE wh_upload_id=%d AND table_name='%s'`, warehouseTableUploadsTable, uploadID, tableName)
	err = dbHandle.QueryRow(sqlStatement).Scan(&status)
	return
}

func UpdateCurrentSchema(namespace string, wh WarehouseT, uploadID int64, currentSchema, schema map[string]map[string]string, dbHandle *sql.DB) (err error) {
	marshalledSchema, err := json.Marshal(schema)
	if len(currentSchema) == 0 {
		sqlStatement := fmt.Sprintf(`INSERT INTO %s (wh_upload_id, source_id, namespace, destination_id, destination_type, schema, created_at)
									   VALUES ($1, $2, $3, $4, $5, $6, $7)`, warehouseSchemasTable)
		stmt, err := dbHandle.Prepare(sqlStatement)
		if err != nil {
			panic(err)
		}
		defer stmt.Close()

		_, err = stmt.Exec(uploadID, wh.Source.ID, namespace, wh.Destination.ID, wh.Destination.DestinationDefinition.Name, marshalledSchema, time.Now())
	} else {
		sqlStatement := fmt.Sprintf(`UPDATE %s SET schema=$1 WHERE source_id=$2 AND destination_id=$3`, warehouseSchemasTable)
		_, err = dbHandle.Exec(sqlStatement, marshalledSchema, wh.Source.ID, wh.Destination.ID)
	}
	if err != nil {
		panic(err)
	}
	return
}

func GetLoadFileLocations(dbHandle *sql.DB, sourceId string, destinationId string, tableName string, start, end int64) (locations []string, err error) {
	sqlStatement := fmt.Sprintf(`SELECT location from %[1]s right join (
		SELECT  staging_file_id, MAX(id) AS id FROM wh_load_files
		WHERE ( source_id='%[2]s'
			AND destination_id='%[3]s'
			AND table_name='%[4]s'
			AND id >= %[5]v
			AND id <= %[6]v)
		GROUP BY staging_file_id ) uniqueStagingFiles
		ON  wh_load_files.id = uniqueStagingFiles.id `,
		warehouseLoadFilesTable,
		sourceId,
		destinationId,
		tableName,
		start,
		end)
	rows, err := dbHandle.Query(sqlStatement)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var location string
		err := rows.Scan(&location)
		if err != nil {
			panic(err)
		}
		locations = append(locations, location)
	}
	return
}

func GetLoadFileLocation(dbHandle *sql.DB, sourceId string, destinationId string, tableName string, start, end int64) (location string, err error) {
	sqlStatement := fmt.Sprintf(`SELECT location FROM %[1]s
								WHERE ( %[1]s.source_id='%[2]s' AND %[1]s.destination_id='%[3]s' AND %[1]s.table_name='%[4]s' AND %[1]s.id >= %[5]v AND %[1]s.id <= %[6]v) LIMIT 1`,
		warehouseLoadFilesTable, sourceId, destinationId, tableName, start, end)
	err = dbHandle.QueryRow(sqlStatement).Scan(&location)
	if err != nil {
		panic(err)
	}
	return
}

func GetS3Location(location string) (string, string) {
	r, _ := regexp.Compile("\\.s3.*\\.amazonaws\\.com")
	subLocation := r.FindString(location)
	regionTokens := strings.Split(subLocation, ".")
	var region string
	if len(regionTokens) == 5 {
		region = regionTokens[2]
	}
	str1 := r.ReplaceAllString(location, "")
	str2 := strings.Replace(str1, "https", "s3", 1)
	return region, str2
}

func GetS3LocationFolder(location string) string {
	_, s3Location := GetS3Location(location)
	lastPos := strings.LastIndex(s3Location, "/")
	return s3Location[:lastPos]
}

func GetGCSLocation(location string) string {
	str1 := strings.Replace(location, "https", "gs", 1)
	str2 := strings.Replace(str1, "storage.googleapis.com/", "", 1)
	return str2
}

func GetS3Locations(locations []string) (s3Locations []string, err error) {
	for _, location := range locations {
		_, s3Location := GetS3Location(location)
		s3Locations = append(s3Locations, s3Location)
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
	if err != nil {
		panic(err)
	}
	return schema
}

func DestStat(statType string, statName string, id string) *stats.RudderStats {
	return stats.NewBatchDestStat(fmt.Sprintf("warehouse.%s", statName), statType, id)
}

func Datatype(in interface{}) string {
	if _, ok := in.(bool); ok {
		return "boolean"
	}

	if _, ok := in.(int); ok {
		return "int"
	}

	if _, ok := in.(float64); ok {
		return "float"
	}

	if str, ok := in.(string); ok {
		isTimestamp, _ := regexp.MatchString(`^([\+-]?\d{4})((-)((0[1-9]|1[0-2])(-([12]\d|0[1-9]|3[01])))([T\s]((([01]\d|2[0-3])((:)[0-5]\d))([\:]\d+)?)?(:[0-5]\d([\.]\d+)?)?([zZ]|([\+-])([01]\d|2[0-3]):?([0-5]\d)?)?)?)$`, str)

		if isTimestamp {
			return "datetime"
		}
	}

	return "string"
}

//ToSafeDBString to remove special characters
func ToSafeDBString(provider string, str string) string {
	res := ""
	if str != "" {
		r := []rune(str)
		_, err := strconv.ParseInt(string(r[0]), 10, 64)
		if err == nil {
			str = "_" + str
		}
		regexForNotAlphaNumeric := regexp.MustCompile("[^a-zA-Z0-9_]+")
		res = regexForNotAlphaNumeric.ReplaceAllString(str, "")

	}
	if res == "" {
		res = "STRINGEMPTY"
	}
	if _, ok := ReservedKeywords[provider][strings.ToUpper(str)]; ok {
		res = fmt.Sprintf(`_%s`, res)
	}
	return res
}

func GetIP() string {
	if serverIP != "" {
		return serverIP
	}

	serverIP := ""
	ip, err := misc.GetOutboundIP()
	if err == nil {
		serverIP = ip.String()
	}
	return serverIP
}

func GetSlaveWorkerId(workerIdx int, slaveID string) string {
	return fmt.Sprintf("%v-%v-%v", GetIP(), workerIdx, slaveID)
}
