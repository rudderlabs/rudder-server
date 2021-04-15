package warehouseutils

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/iancoleman/strcase"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

const (
	RS         = "RS"
	BQ         = "BQ"
	SNOWFLAKE  = "SNOWFLAKE"
	POSTGRES   = "POSTGRES"
	CLICKHOUSE = "CLICKHOUSE"
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
	DiscardsTable           = "rudder_discards"
	IdentityMergeRulesTable = "rudder_identity_merge_rules"
	IdentityMappingsTable   = "rudder_identity_mappings"
	SyncFrequency           = "syncFrequency"
	SyncStartAt             = "syncStartAt"
	ExcludeWindow           = "excludeWindow"
	ExcludeWindowStartTime  = "excludeWindowStartTime"
	ExcludeWindowEndTime    = "excludeWindowEndTime"
)

const (
	UsersTable      = "users"
	IdentifiesTable = "identifies"
)

const (
	BQLoadedAtFormat = "2006-01-02 15:04:05.999999 Z"
	BQUuidTSFormat   = "2006-01-02 15:04:05 Z"
)

var (
	serverIP                  string
	IdentityEnabledWarehouses []string
	enableIDResolution        bool
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

var pkgLogger logger.LoggerI

func init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("warehouse").Child("utils")
}

func loadConfig() {
	IdentityEnabledWarehouses = []string{"SNOWFLAKE"}
	enableIDResolution = config.GetBool("Warehouse.enableIDResolution", false)
}

type WarehouseT struct {
	Source      backendconfig.SourceT
	Destination backendconfig.DestinationT
	Namespace   string
	Type        string
	Identifier  string
}

type DestinationT struct {
	Source      backendconfig.SourceT
	Destination backendconfig.DestinationT
}

type SchemaT map[string]map[string]string
type TableSchemaT map[string]string

type StagingFileT struct {
	Schema           map[string]map[string]interface{}
	BatchDestination DestinationT
	Location         string
	FirstEventAt     string
	LastEventAt      string
	TotalEvents      int
}

type UploaderI interface {
	GetSchemaInWarehouse() SchemaT
	GetTableSchemaInWarehouse(tableName string) TableSchemaT
	GetTableSchemaInUpload(tableName string) TableSchemaT
	GetLoadFileLocations(options GetLoadFileLocationsOptionsT) []string
	GetSampleLoadFileLocation(tableName string) (string, error)
	GetSingleLoadFileLocation(tableName string) (string, error)
}

type GetLoadFileLocationsOptionsT struct {
	Table   string
	StartID int64
	EndID   int64
	Limit   int64
}

func IDResolutionEnabled() bool {
	return enableIDResolution
}

type TableSchemaDiffT struct {
	Exists                         bool
	TableToBeCreated               bool
	ColumnMap                      map[string]string
	UpdatedSchema                  map[string]string
	StringColumnsToBeAlteredToText []string
}

type QueryResult struct {
	Columns []string
	Values  [][]string
}

func TimingFromJSONString(str sql.NullString) (status string, recordedTime time.Time) {
	timingsMap := gjson.Parse(str.String).Map()
	for s, t := range timingsMap {
		return s, t.Time()
	}
	return // zero values
}

func GetFirstTiming(str sql.NullString) (status string, recordedTime time.Time) {
	timingsMap := gjson.Parse(str.String).Array()
	if len(timingsMap) > 0 {
		for s, t := range timingsMap[0].Map() {
			return s, t.Time()
		}
	}
	return // zero values
}

func GetLastTiming(str sql.NullString) (status string, recordedTime time.Time) {
	timingsMap := gjson.Parse(str.String).Array()
	if len(timingsMap) > 0 {
		for s, t := range timingsMap[len(timingsMap)-1].Map() {
			return s, t.Time()
		}
	}
	return // zero values
}

func GetLastFailedStatus(str sql.NullString) (status string) {
	timingsMap := gjson.Parse(str.String).Array()
	if len(timingsMap) > 0 {
		for index := len(timingsMap) - 1; index >= 0; index-- {
			for s := range timingsMap[index].Map() {
				if strings.Contains(s, "failed") {
					return s
				}
			}
		}
	}
	return // zero values
}

func GetNamespace(source backendconfig.SourceT, destination backendconfig.DestinationT, dbHandle *sql.DB) (namespace string, exists bool) {
	sqlStatement := fmt.Sprintf(`SELECT namespace FROM %s WHERE source_id='%s' AND destination_id='%s' ORDER BY id DESC`, WarehouseSchemasTable, source.ID, destination.ID)
	err := dbHandle.QueryRow(sqlStatement).Scan(&namespace)
	if err != nil && err != sql.ErrNoRows {
		panic(fmt.Errorf("Query: %s failed with Error : %w", sqlStatement, err))
	}
	return namespace, len(namespace) > 0
}

func GetTableFirstEventAt(dbHandle *sql.DB, sourceId string, destinationId string, tableName string, start, end int64) (firstEventAt string) {
	sqlStatement := fmt.Sprintf(`SELECT first_event_at FROM %[7]s where id = ( SELECT staging_file_id FROM %[1]s WHERE ( source_id='%[2]s'
			AND destination_id='%[3]s'
			AND table_name='%[4]s'
			AND id >= %[5]v
			AND id <= %[6]v) ORDER BY staging_file_id ASC LIMIT 1)`,
		WarehouseLoadFilesTable,
		sourceId,
		destinationId,
		tableName,
		start,
		end,
		WarehouseStagingFilesTable)
	err := dbHandle.QueryRow(sqlStatement).Scan(&firstEventAt)
	if err != nil && err != sql.ErrNoRows {
		panic(fmt.Errorf("Query: %s failed with Error : %w", sqlStatement, err))
	}
	return
}

// GetObjectFolder returns the folder path for the storage object based on the storage provider
// eg. For provider as S3: https://test-bucket.s3.amazonaws.com/test-object.csv --> s3://test-bucket/test-object.csv
func GetObjectFolder(provider string, location string) (folder string) {
	switch provider {
	case "S3":
		folder = GetS3LocationFolder(location)
	case "GCS":
		folder = GetGCSLocationFolder(location, GCSLocationOptionsT{TLDFormat: "gcs"})
	case "AZURE_BLOB":
		folder = GetAzureBlobLocationFolder(location)
	}
	return
}

// GetObjectFolder returns the folder path for the storage object based on the storage provider
// eg. For provider as S3: https://test-bucket.s3.amazonaws.com/test-object.csv --> s3://test-bucket/test-object.csv
func GetObjectLocation(provider string, location string) (folder string) {
	switch provider {
	case "S3":
		folder, _ = GetS3Location(location)
	case "GCS":
		folder = GetGCSLocation(location, GCSLocationOptionsT{TLDFormat: "gcs"})
	case "AZURE_BLOB":
		folder = GetAzureBlobLocation(location)
	}
	return
}

// GetObjectName extracts object/key objectName from different buckets locations
// ex: https://bucket-endpoint/bucket-name/object -> object
func GetObjectName(location string, providerConfig interface{}, objectProvider string) (objectName string, err error) {
	var config map[string]interface{}
	var ok bool
	if config, ok = providerConfig.(map[string]interface{}); !ok {
		return "", errors.New("failed to cast destination config interface{} to map[string]interface{}")
	}
	fm, err := filemanager.New(&filemanager.SettingsT{
		Provider: objectProvider,
		Config:   config,
	})
	if err != nil {
		return "", err
	}
	return fm.GetObjectNameFromLocation(location)
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
	schema := make(map[string]map[string]string)
	err := json.Unmarshal(rawMsg, &schema)
	if err != nil {
		panic(fmt.Errorf("Unmarshalling: %s failed with Error : %w", string(rawMsg), err))
	}
	return schema
}
func JSONTimingsToMap(rawMsg json.RawMessage) []map[string]string {
	timings := make([]map[string]string, 0)
	err := json.Unmarshal(rawMsg, &timings)
	if err != nil {
		panic(fmt.Errorf("Unmarshalling: %s failed with Error : %w", string(rawMsg), err))
	}
	return timings
}

func DestStat(statType string, statName string, id string) stats.RudderStats {
	return stats.NewTaggedStat(fmt.Sprintf("warehouse.%s", statName), statType, stats.Tags{"destID": id})
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
	if destType == "RS" || destType == "BQ" {
		return ObjectStorageMap[destType]
	}
	c := config.(map[string]interface{})
	if destType == "SNOWFLAKE" {
		provider, ok := c["cloudProvider"].(string)
		if provider == "" || !ok {
			provider = "AWS"
		}
		return SnowflakeStorageMap[provider]
	}
	provider, _ := c["bucketProvider"].(string)
	return provider
}

func GetConfigValue(key string, warehouse WarehouseT) (val string) {
	config := warehouse.Destination.Config
	if config[key] != nil {
		val, _ = config[key].(string)
	}
	return val
}

func GetConfigValueBoolString(key string, warehouse WarehouseT) string {
	config := warehouse.Destination.Config
	if config[key] != nil {
		if val, ok := config[key].(bool); ok {
			if val {
				return "true"
			}
		}
	}
	return "false"
}

func GetConfigValueAsMap(key string, config map[string]interface{}) map[string]interface{} {
	value := map[string]interface{}{}
	if config[key] != nil {
		if val, ok := config[key].(map[string]interface{}); ok {
			return val
		}
	}
	return value
}

func SortColumnKeysFromColumnMap(columnMap map[string]string) []string {
	columnKeys := make([]string, 0, len(columnMap))
	for k := range columnMap {
		columnKeys = append(columnKeys, k)
	}
	sort.Strings(columnKeys)
	return columnKeys
}

func IdentityMergeRulesTableName(warehouse WarehouseT) string {
	return fmt.Sprintf(`%s_%s_%s`, IdentityMergeRulesTable, warehouse.Namespace, warehouse.Destination.ID)
}

func IdentityMappingsTableName(warehouse WarehouseT) string {
	return fmt.Sprintf(`%s_%s_%s`, IdentityMappingsTable, warehouse.Namespace, warehouse.Destination.ID)
}

func IdentityMappingsUniqueMappingConstraintName(warehouse WarehouseT) string {
	return fmt.Sprintf(`unique_merge_property_%s_%s`, warehouse.Namespace, warehouse.Destination.ID)
}

func GetWarehouseIdentifier(destType string, sourceID string, destinationID string) string {
	return fmt.Sprintf("%s:%s:%s", destType, sourceID, destinationID)
}

func DoubleQuoteAndJoinByComma(strs []string) string {
	var quotedSlice []string
	for _, str := range strs {
		quotedSlice = append(quotedSlice, fmt.Sprintf("%q", str))
	}
	return strings.Join(quotedSlice, ",")
}
