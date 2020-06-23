package warehouseutils

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"

	"github.com/iancoleman/strcase"

	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
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
	GeneratingStagingFileFailed   = "generating_staging_file_failed"
	GeneratedStagingFile          = "generated_staging_file"
)

const (
	RS        = "RS"
	BQ        = "BQ"
	SNOWFLAKE = "SNOWFLAKE"
	POSTGRES  = "POSTGRES"
)

const (
	StagingFileSucceededState = "succeeded"
	StagingFileFailedState    = "failed"
	StagingFileExecutingState = "executing"
	StagingFileAbortedState   = "aborted"
	StagingFileWaitingState   = "waiting"
)

// warehouse table names
const (
	WarehouseStagingFilesTable = "wh_staging_files"
	WarehouseLoadFilesTable    = "wh_load_files"
	WarehouseUploadsTable      = "wh_uploads"
	WarehouseTableUploadsTable = "wh_table_uploads"
	WarehouseSchemasTable      = "wh_schemas"
)

const (
	DiscardsTable = "rudder_discards"
	SyncFrequency = "syncFrequency"
	SyncStartAt   = "syncStartAt"
)

const (
	UploadStatusField          = "status"
	UploadStartLoadFileIDField = "start_load_file_id"
	UploadEndLoadFileIDField   = "end_load_file_id"
	UploadUpdatedAtField       = "updated_at"
	UploadTimingsField         = "timings"
	UploadSchemaField          = "schema"
	UploadLastExecAtField      = "last_exec_at"
)

const (
	UsersTable      = "users"
	IdentifiesTable = "identifies"
)

var (
	maxRetry int
	serverIP string
)

var ObjectStorageMap = map[string]string{
	"RS": "S3",
	"BQ": "GCS",
}

var SnowflakeStorageMap = map[string]string{
	"AWS":   "S3",
	"GCP":   "GCS",
	"AZURE": "AZURE_BLOB",
}

func init() {
	loadConfig()
}

func loadConfig() {
	maxRetry = config.GetInt("Warehouse.maxRetry", 3)
}

type WarehouseT struct {
	Source      backendconfig.SourceT
	Destination backendconfig.DestinationT
	Namespace   string
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
	Timings            []map[string]string
}

type CurrentSchemaT struct {
	Schema map[string]map[string]string
}

type StagingFileT struct {
	Schema           map[string]map[string]interface{}
	BatchDestination DestinationT
	Location         string
	FirstEventAt     string
	LastEventAt      string
	TotalEvents      int
}

func GetCurrentSchema(dbHandle *sql.DB, warehouse WarehouseT) (map[string]map[string]string, error) {
	var rawSchema json.RawMessage
	sqlStatement := fmt.Sprintf(`SELECT schema FROM %[1]s WHERE (%[1]s.destination_id='%[2]s' AND %[1]s.namespace='%[3]s') ORDER BY %[1]s.id DESC`, WarehouseSchemasTable, warehouse.Destination.ID, warehouse.Namespace)

	err := dbHandle.QueryRow(sqlStatement).Scan(&rawSchema)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
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

	// deep copy currentschema to avoid mutating currentSchema by doing diff.UpdatedSchema = currentSchema
	for tableName, columnMap := range currentSchema {
		diff.UpdatedSchema[tableName] = make(map[string]string)
		for columnName, columnType := range columnMap {
			diff.UpdatedSchema[tableName][columnName] = columnType
		}
	}
	for tableName, uploadColumnMap := range uploadSchema {
		currentColumnsMap, ok := currentSchema[tableName]
		if !ok {
			diff.Tables = append(diff.Tables, tableName)
			diff.ColumnMaps[tableName] = uploadColumnMap
			diff.UpdatedSchema[tableName] = uploadColumnMap
		} else {
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

func CompareSchema(sch1, sch2 map[string]map[string]string) bool {
	eq := reflect.DeepEqual(sch1, sch2)
	return eq
}

type UploadColumnT struct {
	Column string
	Value  interface{}
}

// GetUploadTimings returns timings json column
// eg. timings: [{exporting_data: 2020-04-21 15:16:19.687716, exported_data: 2020-04-21 15:26:34.344356}]
func GetUploadTimings(upload UploadT, dbHandle *sql.DB) (timings []map[string]string) {
	var rawJSON json.RawMessage
	sqlStatement := fmt.Sprintf(`SELECT timings FROM %s WHERE id=%d`, WarehouseUploadsTable, upload.ID)
	err := dbHandle.QueryRow(sqlStatement).Scan(&rawJSON)
	if err != nil {
		return
	}
	err = json.Unmarshal(rawJSON, &timings)
	return
}

// GetNewTimings appends current status with current time to timings column
// eg. status: exported_data, timings: [{exporting_data: 2020-04-21 15:16:19.687716] -> [{exporting_data: 2020-04-21 15:16:19.687716, exported_data: 2020-04-21 15:26:34.344356}]
func GetNewTimings(upload UploadT, dbHandle *sql.DB, status string) []byte {
	timings := GetUploadTimings(upload, dbHandle)
	timing := map[string]string{status: timeutil.Now().Format(misc.RFC3339Milli)}
	timings = append(timings, timing)
	marshalledTimings, err := json.Marshal(timings)
	if err != nil {
		panic(err)
	}
	return marshalledTimings
}

func SetUploadStatus(upload UploadT, status string, dbHandle *sql.DB, additionalFields ...UploadColumnT) (err error) {
	logger.Infof("WH: Setting status of %s for wh_upload:%v", status, upload.ID)
	marshalledTimings := GetNewTimings(upload, dbHandle, status)
	opts := []UploadColumnT{
		{Column: UploadStatusField, Value: status},
		{Column: UploadTimingsField, Value: marshalledTimings},
		{Column: UploadUpdatedAtField, Value: timeutil.Now()},
	}

	additionalFields = append(additionalFields, opts...)

	SetUploadColumns(
		upload,
		dbHandle,
		additionalFields...,
	)
	return
}

// SetUploadColumns sets any column values passed as args in UploadColumnT format for WarehouseUploadsTable
func SetUploadColumns(upload UploadT, dbHandle *sql.DB, fields ...UploadColumnT) (err error) {
	var columns string
	values := []interface{}{upload.ID}
	// setting values using syntax $n since Exec can correctly format time.Time strings
	for idx, f := range fields {
		// start with $2 as $1 is upload.ID
		columns += fmt.Sprintf(`%s=$%d`, f.Column, idx+2)
		if idx < len(fields)-1 {
			columns += ","
		}
		values = append(values, f.Value)
	}
	sqlStatement := fmt.Sprintf(`UPDATE %s SET %s WHERE id=$1`, WarehouseUploadsTable, columns)
	_, err = dbHandle.Exec(sqlStatement, values...)
	if err != nil {
		panic(err)
	}
	return
}

func SetUploadError(upload UploadT, statusError error, state string, dbHandle *sql.DB) (err error) {
	logger.Errorf("WH: Failed during %s stage: %v\n", state, statusError.Error())
	SetUploadStatus(upload, state, dbHandle)
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
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1, error=$2, updated_at=$3 WHERE id=$4`, WarehouseUploadsTable)
	_, err = dbHandle.Exec(sqlStatement, state, serializedErr, timeutil.Now(), upload.ID)
	if err != nil {
		panic(err)
	}
	return
}

func SetStagingFilesStatus(ids []int64, status string, dbHandle *sql.DB) (err error) {
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1, updated_at=$2 WHERE id=ANY($3)`, WarehouseStagingFilesTable)
	_, err = dbHandle.Exec(sqlStatement, status, timeutil.Now(), pq.Array(ids))
	if err != nil {
		panic(err)
	}
	return
}

func SetStagingFilesError(ids []int64, status string, dbHandle *sql.DB, statusError error) (err error) {
	logger.Errorf("WH: Failed processing staging files: %v", statusError.Error())
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1, error=$2, updated_at=$3 WHERE id=ANY($4)`, WarehouseStagingFilesTable)
	_, err = dbHandle.Exec(sqlStatement, status, misc.QuoteLiteral(statusError.Error()), timeutil.Now(), pq.Array(ids))
	if err != nil {
		panic(err)
	}
	return
}

func SetTableUploadStatus(status string, uploadID int64, tableName string, dbHandle *sql.DB) (err error) {
	// set last_exec_time only if status is executing
	execValues := []interface{}{status, timeutil.Now(), uploadID, tableName}
	var lastExec string
	if status == ExecutingState {
		// setting values using syntax $n since Exec can correctlt format time.Time strings
		lastExec = fmt.Sprintf(`, last_exec_time=$%d`, len(execValues)+1)
		execValues = append(execValues, timeutil.Now())
	}
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1, updated_at=$2 %s WHERE wh_upload_id=$3 AND table_name=$4`, WarehouseTableUploadsTable, lastExec)
	logger.Infof("WH: Setting table upload status: %v", sqlStatement)
	_, err = dbHandle.Exec(sqlStatement, execValues...)
	if err != nil {
		panic(err)
	}
	return
}

func SetTableUploadError(status string, uploadID int64, tableName string, statusError error, dbHandle *sql.DB) (err error) {
	logger.Errorf("WH: Failed uploading table-%s for upload-%v: %v", tableName, uploadID, statusError.Error())
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1, updated_at=$2, error=$3 WHERE wh_upload_id=$4 AND table_name=$5`, WarehouseTableUploadsTable)
	logger.Infof("WH: Setting table upload error: %v", sqlStatement)
	_, err = dbHandle.Exec(sqlStatement, status, timeutil.Now(), misc.QuoteLiteral(statusError.Error()), uploadID, tableName)
	if err != nil {
		panic(err)
	}
	return
}

func GetTableUploadStatus(uploadID int64, tableName string, dbHandle *sql.DB) (status string, err error) {
	sqlStatement := fmt.Sprintf(`SELECT status from %s WHERE wh_upload_id=%d AND table_name='%s'`, WarehouseTableUploadsTable, uploadID, tableName)
	err = dbHandle.QueryRow(sqlStatement).Scan(&status)
	return
}

func GetNamespace(source backendconfig.SourceT, destination backendconfig.DestinationT, dbHandle *sql.DB) (namespace string, exists bool) {
	sqlStatement := fmt.Sprintf(`SELECT namespace FROM %s WHERE source_id='%s' AND destination_id='%s' ORDER BY id DESC`, WarehouseSchemasTable, source.ID, destination.ID)
	err := dbHandle.QueryRow(sqlStatement).Scan(&namespace)
	if err != nil && err != sql.ErrNoRows {
		panic(err)
	}
	return namespace, len(namespace) > 0
}

func UpdateCurrentSchema(namespace string, wh WarehouseT, uploadID int64, schema map[string]map[string]string, dbHandle *sql.DB) (err error) {
	var count int
	sqlStatement := fmt.Sprintf(`SELECT count(*) FROM %s WHERE source_id='%s' AND destination_id='%s' AND namespace='%s'`, WarehouseSchemasTable, wh.Source.ID, wh.Destination.ID, namespace)
	err = dbHandle.QueryRow(sqlStatement).Scan(&count)
	if err != nil {
		panic(err)
	}

	marshalledSchema, err := json.Marshal(schema)
	if count == 0 {
		sqlStatement := fmt.Sprintf(`INSERT INTO %s (wh_upload_id, source_id, namespace, destination_id, destination_type, schema, created_at)
									   VALUES ($1, $2, $3, $4, $5, $6, $7)`, WarehouseSchemasTable)
		stmt, err := dbHandle.Prepare(sqlStatement)
		if err != nil {
			panic(err)
		}
		defer stmt.Close()

		_, err = stmt.Exec(uploadID, wh.Source.ID, namespace, wh.Destination.ID, wh.Destination.DestinationDefinition.Name, marshalledSchema, timeutil.Now())
	} else {
		sqlStatement := fmt.Sprintf(`UPDATE %s SET schema=$1 WHERE source_id=$2 AND destination_id=$3 AND namespace=$4`, WarehouseSchemasTable)
		_, err = dbHandle.Exec(sqlStatement, marshalledSchema, wh.Source.ID, wh.Destination.ID, namespace)
	}
	if err != nil {
		panic(err)
	}
	return
}

func HasLoadFiles(dbHandle *sql.DB, sourceId string, destinationId string, tableName string, start, end int64) bool {
	sqlStatement := fmt.Sprintf(`SELECT count(*) FROM %[1]s
								WHERE ( %[1]s.source_id='%[2]s' AND %[1]s.destination_id='%[3]s' AND %[1]s.table_name='%[4]s' AND %[1]s.id >= %[5]v AND %[1]s.id <= %[6]v)`,
		WarehouseLoadFilesTable, sourceId, destinationId, tableName, start, end)
	var count int64
	err := dbHandle.QueryRow(sqlStatement).Scan(&count)
	if err != nil {
		panic(err)
	}
	return count > 0
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
		WarehouseLoadFilesTable,
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
	sqlStatement := fmt.Sprintf(`SELECT location from %[1]s right join (
		SELECT  staging_file_id, MAX(id) AS id FROM %[1]s
		WHERE ( source_id='%[2]s'
			AND destination_id='%[3]s'
			AND table_name='%[4]s'
			AND id >= %[5]v
			AND id <= %[6]v)
		GROUP BY staging_file_id ) uniqueStagingFiles
		ON  wh_load_files.id = uniqueStagingFiles.id `,
		WarehouseLoadFilesTable,
		sourceId,
		destinationId,
		tableName,
		start,
		end)
	err = dbHandle.QueryRow(sqlStatement).Scan(&location)
	if err != nil && err != sql.ErrNoRows {
		panic(err)
	}
	return
}

// GetObjectFolder returns the folder path for the storage object based on the storage provider
// eg. For provider as S3: https://test-bucket.s3.amazonaws.com/test-object.csv --> s3://test-bucket/test-object.csv
func GetObjectFolder(provider string, location string) (folder string) {
	switch provider {
	case "S3":
		folder = GetS3LocationFolder(location)
		break
	case "GCS":
		folder = GetGCSLocationFolder(location, GCSLocationOptionsT{TLDFormat: "gcs"})
		break
	case "AZURE_BLOB":
		folder = GetAzureBlobLocationFolder(location)
		break
	}
	return
}

// GetObjectName extracts object/key objectName from different buckets locations
// ex: https://bucket-endpoint/bucket-name/object -> object
func GetObjectName(providerConfig interface{}, location string) (objectName string, err error) {
	var config map[string]interface{}
	var ok bool
	var bucketProvider string
	if config, ok = providerConfig.(map[string]interface{}); !ok {
		return "", errors.New("failed to cast destination config interface{} to map[string]interface{}")
	}
	if bucketProvider, ok = config["bucketProvider"].(string); !ok {
		return "", errors.New("failed to get bucket information")
	}
	fm, err := filemanager.New(&filemanager.SettingsT{
		Provider: bucketProvider,
		Config:   config,
	})
	if err != nil {
		return "", err
	}
	return fm.GetObjectNameFromLocation(location), nil
}

// GetS3Location parses path-style location http url to return in s3:// format
// https://test-bucket.s3.amazonaws.com/test-object.csv --> s3://test-bucket/test-object.csv
func GetS3Location(location string) (s3Location string, region string) {
	r, _ := regexp.Compile("\\.s3.*\\.amazonaws\\.com")
	subLocation := r.FindString(location)
	regionTokens := strings.Split(subLocation, ".")
	if len(regionTokens) == 5 {
		region = regionTokens[2]
	}
	str1 := r.ReplaceAllString(location, "")
	s3Location = strings.Replace(str1, "https", "s3", 1)
	return
}

// GetS3LocationFolder returns the folder path for an s3 object
// https://test-bucket.s3.amazonaws.com/myfolder/test-object.csv --> s3://test-bucket/myfolder
func GetS3LocationFolder(location string) string {
	s3Location, _ := GetS3Location(location)
	lastPos := strings.LastIndex(s3Location, "/")
	return s3Location[:lastPos]
}

type GCSLocationOptionsT struct {
	TLDFormat string
}

// GetGCSLocation parses path-style location http url to return in gcs:// format
// https://storage.googleapis.com/test-bucket/test-object.csv --> gcs://test-bucket/test-object.csv
// tldFormat is used to set return format "<tldFormat>://..."
func GetGCSLocation(location string, options GCSLocationOptionsT) string {
	tld := "gs"
	if options.TLDFormat != "" {
		tld = options.TLDFormat
	}
	str1 := strings.Replace(location, "https", tld, 1)
	str2 := strings.Replace(str1, "storage.googleapis.com/", "", 1)
	return str2
}

// GetGCSLocationFolder returns the folder path for an gcs object
// https://storage.googleapis.com/test-bucket/myfolder/test-object.csv --> gcs://test-bucket/myfolder
func GetGCSLocationFolder(location string, options GCSLocationOptionsT) string {
	s3Location := GetGCSLocation(location, options)
	lastPos := strings.LastIndex(s3Location, "/")
	return s3Location[:lastPos]
}

func GetGCSLocations(locations []string, options GCSLocationOptionsT) (gcsLocations []string) {
	for _, location := range locations {
		gcsLocations = append(gcsLocations, GetGCSLocation(location, options))
	}
	return
}

// GetAzureBlobLocation parses path-style location http url to return in azure:// format
// https://myproject.blob.core.windows.net/test-bucket/test-object.csv  --> azure://myproject.blob.core.windows.net/test-bucket/test-object.csv
func GetAzureBlobLocation(location string) string {
	str1 := strings.Replace(location, "https", "azure", 1)
	return str1
}

// GetAzureBlobLocationFolder returns the folder path for an azure storage object
// https://myproject.blob.core.windows.net/test-bucket/myfolder/test-object.csv  --> azure://myproject.blob.core.windows.net/myfolder
func GetAzureBlobLocationFolder(location string) string {
	s3Location := GetAzureBlobLocation(location)
	lastPos := strings.LastIndex(s3Location, "/")
	return s3Location[:lastPos]
}

func GetS3Locations(locations []string) (s3Locations []string) {
	for _, location := range locations {
		s3Location, _ := GetS3Location(location)
		s3Locations = append(s3Locations, s3Location)
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

func DestStat(statType string, statName string, id string) stats.RudderStats {
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

/*
ToSafeNamespace convert name of the namespace to one acceptable by warehouse
1. removes symbols and joins continuous letters and numbers with single underscore and if first char is a number will append a underscore before the first number
2. adds an underscore if the name is a reserved keyword in the warehouse
3. truncate the length of namespace to 127 characters
4. return "stringempty" if name is empty after conversion
examples:
omega     to omega
omega v2  to omega_v2
9mega     to _9mega
mega&     to mega
ome$ga    to ome_ga
omega$    to omega
ome_ ga   to ome_ga
9mega________-________90 to _9mega_90
Cízǔ to C_z
*/
func ToSafeNamespace(provider string, name string) string {
	var extractedValues []string
	var extractedValue string
	for _, c := range name {
		asciiValue := int(c)
		if (asciiValue >= 65 && asciiValue <= 90) || (asciiValue >= 97 && asciiValue <= 122) || (asciiValue >= 48 && asciiValue <= 57) {
			extractedValue += string(c)
		} else {
			if extractedValue != "" {
				extractedValues = append(extractedValues, extractedValue)
			}
			extractedValue = ""
		}
	}

	if extractedValue != "" {
		extractedValues = append(extractedValues, extractedValue)
	}
	namespace := strings.Join(extractedValues, "_")
	namespace = strcase.ToSnake(namespace)
	if namespace != "" && int(namespace[0]) >= 48 && int(namespace[0]) <= 57 {
		namespace = "_" + namespace
	}
	if namespace == "" {
		namespace = "stringempty"
	}
	if _, ok := ReservedKeywords[provider][strings.ToUpper(namespace)]; ok {
		namespace = fmt.Sprintf(`_%s`, namespace)
	}
	return misc.TruncateStr(namespace, 127)
}

/*
ToProviderCase converts string provided to case generally accepted in the warehouse for table, column, schema names etc
eg. columns are uppercased in SNOWFLAKE and lowercased etc in REDSHIFT, BIGQUERY etc
*/
func ToProviderCase(provider string, str string) string {
	if strings.ToUpper(provider) == "SNOWFLAKE" {
		str = strings.ToUpper(str)
	}
	return str
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

func SnowflakeCloudProvider(config interface{}) string {
	c := config.(map[string]interface{})
	provider, ok := c["cloudProvider"].(string)
	if provider == "" || !ok {
		provider = "AWS"
	}
	return provider
}

func ObjectStorageType(destType string, config interface{}) string {
	if destType != "SNOWFLAKE" && destType != "POSTGRES" {
		return ObjectStorageMap[destType]
	}
	if destType == "POSTGRES" {
		c := config.(map[string]interface{})
		provider, _ := c["bucketProvider"].(string)
		return provider
	}
	c := config.(map[string]interface{})
	provider, ok := c["cloudProvider"].(string)
	if provider == "" || !ok {
		provider = "AWS"
	}
	return SnowflakeStorageMap[provider]
}

func GetConfigValue(key string, warehouse WarehouseT) (val string) {
	config := warehouse.Destination.Config
	if config[key] != nil {
		val, _ = config[key].(string)
	}
	return val
}

func SortColumnKeysFromColumnMap(columnMap map[string]string) []string {
	columnKeys := make([]string, 0, len(columnMap))
	for k := range columnMap {
		columnKeys = append(columnKeys, k)
	}
	sort.Strings(columnKeys)
	return columnKeys
}
