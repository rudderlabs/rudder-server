package warehouseutils

import (
	"bytes"
	"context"
	"crypto/sha512"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/iancoleman/strcase"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/awsutils"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/tunnelling"
)

const (
	DestinationType = "destinationType"
)

const (
	RS             = "RS"
	BQ             = "BQ"
	SNOWFLAKE      = "SNOWFLAKE"
	POSTGRES       = "POSTGRES"
	CLICKHOUSE     = "CLICKHOUSE"
	MSSQL          = "MSSQL"
	AZURE_SYNAPSE  = "AZURE_SYNAPSE"
	DELTALAKE      = "DELTALAKE"
	S3_DATALAKE    = "S3_DATALAKE"
	GCS_DATALAKE   = "GCS_DATALAKE"
	AZURE_DATALAKE = "AZURE_DATALAKE"
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
	WarehouseAsyncJobTable     = "wh_async_jobs"
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
	UsersView       = "users_view"
	IdentifiesTable = "identifies"
)

const (
	DatalakeTimeWindowFormat = "2006/01/02/15"
)

const (
	CTStagingTablePrefix = "setup_test_staging"
)

const (
	WAREHOUSE               = "warehouse"
	RUDDER_MISSING_DATATYPE = "warehouse_rudder_missing_datatype"
	MISSING_DATATYPE        = "<missing_datatype>"
)

const (
	stagingTablePrefix = "rudder_staging_"
)

// Object storages
const (
	S3         = "S3"
	AZURE_BLOB = "AZURE_BLOB"
	GCS        = "GCS"
	MINIO      = "MINIO"
)

// Cloud providers
const (
	AWS   = "AWS"
	GCP   = "GCP"
	AZURE = "AZURE"
)

const (
	AWSAccessKey         = "accessKey"
	AWSAccessSecret      = "accessKeyID"
	AWSBucketNameConfig  = "bucketName"
	AWSS3Prefix          = "prefix"
	MinioAccessKeyID     = "accessKeyID"
	MinioSecretAccessKey = "secretAccessKey"
)

// DeprecatedColumnsRegex
// This regex is used to identify deprecated columns in the warehouse
// Example: abc-deprecated-dba626a7-406a-4757-b3e0-3875559c5840
var DeprecatedColumnsRegex = regexp.MustCompile(`.*-deprecated-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

var (
	IdentityEnabledWarehouses []string
	enableIDResolution        bool
	AWSCredsExpiryInS         int64
)

var WHDestNameMap = map[string]string{
	BQ:             "bigquery",
	RS:             "redshift",
	MSSQL:          "mssql",
	POSTGRES:       "postgres",
	SNOWFLAKE:      "snowflake",
	CLICKHOUSE:     "clickhouse",
	DELTALAKE:      "deltalake",
	S3_DATALAKE:    "s3_datalake",
	GCS_DATALAKE:   "gcs_datalake",
	AZURE_DATALAKE: "azure_datalake",
	AZURE_SYNAPSE:  "azure_synapse",
}

var ObjectStorageMap = map[string]string{
	RS:             S3,
	S3_DATALAKE:    S3,
	BQ:             GCS,
	GCS_DATALAKE:   GCS,
	AZURE_DATALAKE: AZURE_BLOB,
}

var SnowflakeStorageMap = map[string]string{
	AWS:   S3,
	GCP:   GCS,
	AZURE: AZURE_BLOB,
}

var DiscardsSchema = map[string]string{
	"table_name":   "string",
	"row_id":       "string",
	"column_name":  "string",
	"column_value": "string",
	"received_at":  "datetime",
	"uuid_ts":      "datetime",
}

const (
	LOAD_FILE_TYPE_CSV     = "csv"
	LOAD_FILE_TYPE_JSON    = "json"
	LOAD_FILE_TYPE_PARQUET = "parquet"
	TestConnectionTimeout  = 15 * time.Second
)

var (
	pkgLogger              logger.Logger
	TimeWindowDestinations []string
	WarehouseDestinations  []string
)

var (
	S3PathStyleRegex     = regexp.MustCompile(`https?://s3([.-](?P<region>[^.]+))?.amazonaws\.com/(?P<bucket>[^/]+)/(?P<keyname>.*)`)
	S3VirtualHostedRegex = regexp.MustCompile(`https?://(?P<bucket>[^/]+).s3([.-](?P<region>[^.]+))?.amazonaws\.com/(?P<keyname>.*)`)
)

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("warehouse").Child("utils")
}

func loadConfig() {
	IdentityEnabledWarehouses = []string{SNOWFLAKE, BQ}
	TimeWindowDestinations = []string{S3_DATALAKE, GCS_DATALAKE, AZURE_DATALAKE}
	WarehouseDestinations = []string{RS, BQ, SNOWFLAKE, POSTGRES, CLICKHOUSE, MSSQL, AZURE_SYNAPSE, S3_DATALAKE, GCS_DATALAKE, AZURE_DATALAKE, DELTALAKE}
	config.RegisterBoolConfigVariable(false, &enableIDResolution, false, "Warehouse.enableIDResolution")
	config.RegisterInt64ConfigVariable(3600, &AWSCredsExpiryInS, true, 1, "Warehouse.awsCredsExpiryInS")
}

type DeleteByMetaData struct {
	JobRunId  string `json:"job_run_id"`
	TaskRunId string `json:"task_run_id"`
	StartTime string `json:"start_time"`
}

type DeleteByParams struct {
	SourceId  string
	JobRunId  string
	TaskRunId string
	StartTime string
}

type ColumnInfo struct {
	Name  string
	Value interface{}
	Type  string
}

type Destination struct {
	Source      backendconfig.SourceT
	Destination backendconfig.DestinationT
}

type Schema model.Schema

type KeyValue struct {
	Key   string
	Value interface{}
}

type Uploader interface {
	GetSchemaInWarehouse() model.Schema
	GetLocalSchema() model.Schema
	UpdateLocalSchema(schema model.Schema) error
	GetTableSchemaInWarehouse(tableName string) model.TableSchema
	GetTableSchemaInUpload(tableName string) model.TableSchema
	GetLoadFilesMetadata(options GetLoadFilesOptions) []LoadFile
	GetSampleLoadFileLocation(tableName string) (string, error)
	GetSingleLoadFile(tableName string) (LoadFile, error)
	ShouldOnDedupUseNewRecord() bool
	UseRudderStorage() bool
	GetLoadFileGenStartTIme() time.Time
	GetLoadFileType() string
	GetFirstLastEvent() (time.Time, time.Time)
}

type GetLoadFilesOptions struct {
	Table   string
	StartID int64
	EndID   int64
	Limit   int64
}

type LoadFile struct {
	Location string
	Metadata json.RawMessage
}

func IDResolutionEnabled() bool {
	return enableIDResolution
}

type TableSchemaDiff struct {
	Exists           bool
	TableToBeCreated bool
	ColumnMap        model.TableSchema
	UpdatedSchema    model.TableSchema
	AlteredColumnMap model.TableSchema
}

type QueryResult struct {
	Columns []string
	Values  [][]string
}

type PendingEventsRequest struct {
	SourceID  string `json:"source_id"`
	TaskRunID string `json:"task_run_id"`
}

type PendingEventsResponse struct {
	PendingEvents            bool  `json:"pending_events"`
	PendingStagingFilesCount int64 `json:"pending_staging_files"`
	PendingUploadCount       int64 `json:"pending_uploads"`
	AbortedEvents            bool  `json:"aborted_events"`
}

type TriggerUploadRequest struct {
	SourceID      string `json:"source_id"`
	DestinationID string `json:"destination_id"`
}

func TimingFromJSONString(str sql.NullString) (status string, recordedTime time.Time) {
	timingsMap := gjson.Parse(str.String).Map()
	for s, t := range timingsMap {
		return s, t.Time()
	}
	return // zero values
}

func GetNamespace(source backendconfig.SourceT, destination backendconfig.DestinationT, dbHandle *sql.DB) (namespace string, exists bool) {
	sqlStatement := fmt.Sprintf(`
		SELECT
		  namespace
		FROM
		  %s
		WHERE
		  source_id = '%s'
		  AND destination_id = '%s'
		ORDER BY
		  id DESC;
`,
		WarehouseSchemasTable,
		source.ID,
		destination.ID,
	)
	err := dbHandle.QueryRow(sqlStatement).Scan(&namespace)
	if err != nil && err != sql.ErrNoRows {
		panic(fmt.Errorf("query: %s failed with Error : %w", sqlStatement, err))
	}
	return namespace, len(namespace) > 0
}

// GetObjectFolder returns the folder path for the storage object based on the storage provider
// eg. For provider as S3: https://test-bucket.s3.amazonaws.com/test-object.csv --> s3://test-bucket/test-object.csv
func GetObjectFolder(provider, location string) (folder string) {
	switch provider {
	case S3:
		folder = GetS3LocationFolder(location)
	case GCS:
		folder = GetGCSLocationFolder(location, GCSLocationOptions{TLDFormat: "gcs"})
	case AZURE_BLOB:
		folder = GetAzureBlobLocationFolder(location)
	}
	return
}

// GetObjectFolderForDeltalake returns the folder path for the storage object based on the storage provider for delta lake
// eg. For provider as S3: https://<bucket-name>.s3.amazonaws.com/<directory-name> --> s3://<bucket-name>/<directory-name>
// eg. For provider as GCS: https://storage.cloud.google.com/<bucket-name>/<directory-name> --> gs://<bucket-name>/<directory-name>
// eg. For provider as AZURE_BLOB: https://<storage-account-name>.blob.core.windows.net/<container-name>/<directory-name> --> wasbs://<container-name>@<storage-account-name>.blob.core.windows.net/<directory-name>
func GetObjectFolderForDeltalake(provider, location string) (folder string) {
	switch provider {
	case S3:
		folder = GetS3LocationFolder(location)
	case GCS:
		folder = GetGCSLocationFolder(location, GCSLocationOptions{TLDFormat: "gs"})
	case AZURE_BLOB:
		blobUrl, _ := url.Parse(location)
		blobUrlParts := azblob.NewBlobURLParts(*blobUrl)
		accountName := strings.Replace(blobUrlParts.Host, ".blob.core.windows.net", "", 1)
		blobLocation := fmt.Sprintf("wasbs://%s@%s.blob.core.windows.net/%s", blobUrlParts.ContainerName, accountName, blobUrlParts.BlobName)
		lastPos := strings.LastIndex(blobLocation, "/")
		folder = blobLocation[:lastPos]
	}
	return
}

func GetColumnsFromTableSchema(schema model.TableSchema) []string {
	keys := reflect.ValueOf(schema).MapKeys()
	strKeys := make([]string, len(keys))
	for i := 0; i < len(keys); i++ {
		strKeys[i] = keys[i].String()
	}
	return strKeys
}

// GetObjectLocation returns the folder path for the storage object based on the storage provider
// e.g. For provider as S3: https://test-bucket.s3.amazonaws.com/test-object.csv --> s3://test-bucket/test-object.csv
func GetObjectLocation(provider, location string) (objectLocation string) {
	switch provider {
	case S3:
		objectLocation, _ = GetS3Location(location)
	case GCS:
		objectLocation = GetGCSLocation(location, GCSLocationOptions{TLDFormat: "gcs"})
	case AZURE_BLOB:
		objectLocation = GetAzureBlobLocation(location)
	}
	return
}

// GetObjectName extracts object/key objectName from different buckets locations
// ex: https://bucket-endpoint/bucket-name/object -> object
func GetObjectName(location string, providerConfig interface{}, objectProvider string) (objectName string, err error) {
	var destConfig map[string]interface{}
	var ok bool
	if destConfig, ok = providerConfig.(map[string]interface{}); !ok {
		return "", errors.New("failed to cast destination config interface{} to map[string]interface{}")
	}
	fm, err := filemanager.DefaultFileManagerFactory.New(&filemanager.SettingsT{
		Provider: objectProvider,
		Config:   destConfig,
	})
	if err != nil {
		return "", err
	}
	return fm.GetObjectNameFromLocation(location)
}

// CaptureRegexGroup returns capture as per the regex provided
func CaptureRegexGroup(r *regexp.Regexp, pattern string) (groups map[string]string, err error) {
	if !r.MatchString(pattern) {
		err = fmt.Errorf("regex does not match pattern %s", pattern)
		return
	}
	m := r.FindStringSubmatch(pattern)
	groups = make(map[string]string)
	for i, name := range r.SubexpNames() {
		if name == "" {
			continue
		}
		if i > 0 && i <= len(m) {
			groups[name] = m[i]
		}
	}
	return
}

// GetS3Location parses path-style location http url to return in s3:// format
// [Path-style access] https://s3.amazonaws.com/test-bucket/test-object.csv --> s3://test-bucket/test-object.csv
// [Virtual-hosted–style access] https://test-bucket.s3.amazonaws.com/test-object.csv --> s3://test-bucket/test-object.csv
// TODO: Handle non regex matches.
func GetS3Location(location string) (s3Location, region string) {
	for _, s3Regex := range []*regexp.Regexp{S3VirtualHostedRegex, S3PathStyleRegex} {
		var groups map[string]string
		groups, err := CaptureRegexGroup(s3Regex, location)
		if err == nil {
			region = groups["region"]
			s3Location = fmt.Sprintf("s3://%s/%s", groups["bucket"], groups["keyname"])
			return
		}
	}
	return
}

// GetS3LocationFolder returns the folder path for a s3 object
// https://test-bucket.s3.amazonaws.com/myfolder/test-object.csv --> s3://test-bucket/myfolder
func GetS3LocationFolder(location string) string {
	s3Location, _ := GetS3Location(location)
	lastPos := strings.LastIndex(s3Location, "/")
	return s3Location[:lastPos]
}

type GCSLocationOptions struct {
	TLDFormat string
}

// GetGCSLocation parses path-style location http url to return in gcs:// format
// https://storage.googleapis.com/test-bucket/test-object.csv --> gcs://test-bucket/test-object.csv
// tldFormat is used to set return format "<tldFormat>://..."
func GetGCSLocation(location string, options GCSLocationOptions) string {
	tld := "gs"
	if options.TLDFormat != "" {
		tld = options.TLDFormat
	}
	str1 := strings.Replace(location, "https", tld, 1)
	str2 := strings.Replace(str1, "storage.googleapis.com/", "", 1)
	return str2
}

// GetGCSLocationFolder returns the folder path for a gcs object
// https://storage.googleapis.com/test-bucket/myfolder/test-object.csv --> gcs://test-bucket/myfolder
func GetGCSLocationFolder(location string, options GCSLocationOptions) string {
	s3Location := GetGCSLocation(location, options)
	lastPos := strings.LastIndex(s3Location, "/")
	return s3Location[:lastPos]
}

func GetGCSLocations(loadFiles []LoadFile, options GCSLocationOptions) (gcsLocations []string) {
	for _, loadFile := range loadFiles {
		gcsLocations = append(gcsLocations, GetGCSLocation(loadFile.Location, options))
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

func GetS3Locations(loadFiles []LoadFile) []LoadFile {
	for idx, loadFile := range loadFiles {
		loadFiles[idx].Location, _ = GetS3Location(loadFile.Location)
	}
	return loadFiles
}

func JSONSchemaToMap(rawMsg json.RawMessage) model.Schema {
	schema := make(model.Schema)
	err := json.Unmarshal(rawMsg, &schema)
	if err != nil {
		panic(fmt.Errorf("unmarshalling: %s failed with Error : %w", string(rawMsg), err))
	}
	return schema
}

func DestStat(statType, statName, id string) stats.Measurement {
	return stats.Default.NewTaggedStat(fmt.Sprintf("warehouse.%s", statName), statType, stats.Tags{"destID": id})
}

/*
ToSafeNamespace convert name of the namespace to one acceptable by warehouse
1. Remove symbols and joins continuous letters and numbers with single underscore and if first char is a number will append an underscore before the first number
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
func ToSafeNamespace(provider, name string) string {
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
ToProviderCase converts string provided to case generally accepted in the warehouse for table, column, schema names etc.
e.g. columns are uppercase in SNOWFLAKE and lowercase etc. in REDSHIFT, BIGQUERY etc
*/
func ToProviderCase(provider, str string) string {
	if strings.ToUpper(provider) == SNOWFLAKE {
		str = strings.ToUpper(str)
	}
	return str
}

func SnowflakeCloudProvider(config interface{}) string {
	c := config.(map[string]interface{})
	provider, ok := c["cloudProvider"].(string)
	if provider == "" || !ok {
		provider = AWS
	}
	return provider
}

func ObjectStorageType(destType string, config interface{}, useRudderStorage bool) string {
	c := config.(map[string]interface{})
	if useRudderStorage {
		return S3
	}
	if _, ok := ObjectStorageMap[destType]; ok {
		return ObjectStorageMap[destType]
	}
	if destType == SNOWFLAKE {
		provider, ok := c["cloudProvider"].(string)
		if provider == "" || !ok {
			provider = AWS
		}
		return SnowflakeStorageMap[provider]
	}
	provider, _ := c["bucketProvider"].(string)
	return provider
}

func GetConfigValue(key string, warehouse model.Warehouse) (val string) {
	configKey := fmt.Sprintf("Warehouse.pipeline.%s.%s.%s", warehouse.Source.ID, warehouse.Destination.ID, key)
	if config.IsSet(configKey) {
		return config.GetString(configKey, "")
	}
	destConfig := warehouse.Destination.Config
	if destConfig[key] != nil {
		val, _ = destConfig[key].(string)
	}
	return val
}

func GetConfigValueBoolString(key string, warehouse model.Warehouse) string {
	destConfig := warehouse.Destination.Config
	if destConfig[key] != nil {
		if val, ok := destConfig[key].(bool); ok {
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

func SortColumnKeysFromColumnMap(columnMap model.TableSchema) []string {
	columnKeys := make([]string, 0, len(columnMap))
	for k := range columnMap {
		columnKeys = append(columnKeys, k)
	}
	sort.Strings(columnKeys)
	return columnKeys
}

func IdentityMergeRulesTableName(warehouse model.Warehouse) string {
	return fmt.Sprintf(`%s_%s_%s`, IdentityMergeRulesTable, warehouse.Namespace, warehouse.Destination.ID)
}

func IdentityMergeRulesWarehouseTableName(provider string) string {
	return ToProviderCase(provider, IdentityMergeRulesTable)
}

func IdentityMappingsWarehouseTableName(provider string) string {
	return ToProviderCase(provider, IdentityMappingsTable)
}

func IdentityMappingsTableName(warehouse model.Warehouse) string {
	return fmt.Sprintf(`%s_%s_%s`, IdentityMappingsTable, warehouse.Namespace, warehouse.Destination.ID)
}

func IdentityMappingsUniqueMappingConstraintName(warehouse model.Warehouse) string {
	return fmt.Sprintf(`unique_merge_property_%s_%s`, warehouse.Namespace, warehouse.Destination.ID)
}

func GetWarehouseIdentifier(destType, sourceID, destinationID string) string {
	return fmt.Sprintf("%s:%s:%s", destType, sourceID, destinationID)
}

func DoubleQuoteAndJoinByComma(elems []string) string {
	var quotedSlice []string
	for _, elem := range elems {
		quotedSlice = append(quotedSlice, fmt.Sprintf("%q", elem))
	}
	return strings.Join(quotedSlice, ",")
}

func GetTempFileExtension(destType string) string {
	if destType == BQ {
		return "json.gz"
	}
	return "csv.gz"
}

func GetTimeWindow(ts time.Time) time.Time {
	ts = ts.UTC()

	// create and return time struct for window
	return time.Date(ts.Year(), ts.Month(), ts.Day(), ts.Hour(), 0, 0, 0, time.UTC)
}

// GetTablePathInObjectStorage returns the path of the table relative to the object storage bucket
// <$WAREHOUSE_DATALAKE_FOLDER_NAME>/<namespace>/tableName
func GetTablePathInObjectStorage(namespace, tableName string) string {
	return fmt.Sprintf("%s/%s/%s", config.GetString("WAREHOUSE_DATALAKE_FOLDER_NAME", "rudder-datalake"), namespace, tableName)
}

// JoinWithFormatting returns joined string for keys with the provided formatting function.
func JoinWithFormatting(keys []string, format func(idx int, str string) string, separator string) string {
	output := make([]string, len(keys))
	for idx, str := range keys {
		output[idx] += format(idx, str)
	}
	return strings.Join(output, separator)
}

func CreateAWSSessionConfig(destination *backendconfig.DestinationT, serviceName string) (*awsutils.SessionConfig, error) {
	if !misc.IsConfiguredToUseRudderObjectStorage(destination.Config) &&
		(misc.HasAWSRoleARNInConfig(destination.Config) || misc.HasAWSKeysInConfig(destination.Config)) {
		return awsutils.NewSimpleSessionConfigForDestination(destination, serviceName)
	}
	accessKeyID, accessKey := misc.GetRudderObjectStorageAccessKeys()
	return &awsutils.SessionConfig{
		AccessKeyID: accessKeyID,
		AccessKey:   accessKey,
		Service:     serviceName,
	}, nil
}

func GetTemporaryS3Cred(destination *backendconfig.DestinationT) (string, string, string, error) {
	sessionConfig, err := CreateAWSSessionConfig(destination, s3.ServiceID)
	if err != nil {
		return "", "", "", err
	}

	awsSession, err := awsutils.CreateSession(sessionConfig)
	if err != nil {
		return "", "", "", err
	}

	// Role already provides temporary credentials
	// so we shouldn't call sts.GetSessionToken again
	if sessionConfig.RoleBasedAuth {
		creds, err := awsSession.Config.Credentials.Get()
		if err != nil {
			return "", "", "", err
		}
		return creds.AccessKeyID, creds.SecretAccessKey, creds.SessionToken, nil
	}

	// Create an STS client from just a session.
	svc := sts.New(awsSession)

	sessionTokenOutput, err := svc.GetSessionToken(&sts.GetSessionTokenInput{DurationSeconds: &AWSCredsExpiryInS})
	if err != nil {
		return "", "", "", err
	}
	return *sessionTokenOutput.Credentials.AccessKeyId, *sessionTokenOutput.Credentials.SecretAccessKey, *sessionTokenOutput.Credentials.SessionToken, err
}

type Tag struct {
	Name  string
	Value string
}

func NewTimerStat(name string, extraTags ...Tag) stats.Measurement {
	tags := stats.Tags{
		"module": WAREHOUSE,
	}
	for _, extraTag := range extraTags {
		tags[extraTag.Name] = extraTag.Value
	}
	return stats.Default.NewTaggedStat(name, stats.TimerType, tags)
}

func NewCounterStat(name string, extraTags ...Tag) stats.Measurement {
	tags := stats.Tags{
		"module": WAREHOUSE,
	}
	for _, extraTag := range extraTags {
		tags[extraTag.Name] = extraTag.Value
	}
	return stats.Default.NewTaggedStat(name, stats.CountType, tags)
}

func WHCounterStat(name string, warehouse *model.Warehouse, extraTags ...Tag) stats.Measurement {
	tags := stats.Tags{
		"module":      WAREHOUSE,
		"destType":    warehouse.Type,
		"workspaceId": warehouse.WorkspaceID,
		"destID":      warehouse.Destination.ID,
		"sourceID":    warehouse.Source.ID,
	}
	for _, extraTag := range extraTags {
		tags[extraTag.Name] = extraTag.Value
	}
	return stats.Default.NewTaggedStat(name, stats.CountType, tags)
}

func formatSSLFile(content string) (formattedContent string) {
	formattedContent = strings.ReplaceAll(content, "\n", "")
	// Add new line at the end of -----BEGIN CERTIFICATE-----
	formattedContent = strings.Replace(formattedContent, "-----BEGIN CERTIFICATE-----", "-----BEGIN CERTIFICATE-----\n", 1)
	// Add new line at the end of -----BEGIN RSA PRIVATE KEY-----
	formattedContent = strings.Replace(formattedContent, "-----BEGIN RSA PRIVATE KEY-----", "-----BEGIN RSA PRIVATE KEY-----\n", 1)
	// Add new line at the start and end of -----END CERTIFICATE-----
	formattedContent = strings.Replace(formattedContent, "-----END CERTIFICATE-----", "\n-----END CERTIFICATE-----\n", 1)
	// Add new line at the start and end of -----END RSA PRIVATE KEY-----
	formattedContent = strings.Replace(formattedContent, "-----END RSA PRIVATE KEY-----", "\n-----END RSA PRIVATE KEY-----\n", 1)
	return formattedContent
}

type WriteSSLKeyError struct {
	errorText string
	errorTag  string
}

func (err *WriteSSLKeyError) IsError() bool {
	return err.errorText != ""
}

func (err *WriteSSLKeyError) Error() string {
	return err.errorText
}

func (err *WriteSSLKeyError) GetErrTag() string {
	return err.errorTag
}

// WriteSSLKeys writes the ssl key(s) present in the destination config
// to the file system, this function checks whether a given config is
// already written to the file system, writes to the file system if the
// content is not already written
func WriteSSLKeys(destination backendconfig.DestinationT) WriteSSLKeyError {
	var err error
	var existingChecksum string
	var directoryName string
	if directoryName, err = misc.CreateTMPDIR(); err != nil {
		return WriteSSLKeyError{fmt.Sprintf("Error creating SSL root TMP directory for destination %v", err), "tmp_dir_failure"}
	}
	clientKeyConfig := destination.Config["clientKey"]
	clientCertConfig := destination.Config["clientCert"]
	serverCAConfig := destination.Config["serverCA"]
	if clientKeyConfig == nil || clientCertConfig == nil || serverCAConfig == nil {
		return WriteSSLKeyError{fmt.Sprintf("Error extracting ssl information; invalid config passed for destination %s", destination.ID), "certs_nil_value"}
	}
	clientKey := formatSSLFile(clientKeyConfig.(string))
	clientCert := formatSSLFile(clientCertConfig.(string))
	serverCert := formatSSLFile(serverCAConfig.(string))
	sslDirPath := fmt.Sprintf("%s/dest-ssls/%s", directoryName, destination.ID)
	if err = os.MkdirAll(sslDirPath, 0o700); err != nil {
		return WriteSSLKeyError{fmt.Sprintf("Error creating SSL root directory for destination %s %v", destination.ID, err), "dest_ssl_create_err"}
	}
	combinedString := fmt.Sprintf("%s%s%s", clientKey, clientCert, serverCert)
	h := sha512.New()
	h.Write([]byte(combinedString))
	sslHash := fmt.Sprintf("%x", h.Sum(nil))
	clientCertPemFile := fmt.Sprintf("%s/client-cert.pem", sslDirPath)
	clientKeyPemFile := fmt.Sprintf("%s/client-key.pem", sslDirPath)
	serverCertPemFile := fmt.Sprintf("%s/server-ca.pem", sslDirPath)
	checkSumFile := fmt.Sprintf("%s/checksum", sslDirPath)
	if fileContent, fileReadErr := os.ReadFile(checkSumFile); fileReadErr == nil {
		existingChecksum = string(fileContent)
	}
	if existingChecksum == sslHash {
		// Permission files already written to FS
		return WriteSSLKeyError{}
	}
	if err = os.WriteFile(clientCertPemFile, []byte(clientCert), 0o600); err != nil {
		return WriteSSLKeyError{fmt.Sprintf("Error saving file %s error::%v", clientCertPemFile, err), "client_cert_create_err"}
	}
	if err = os.WriteFile(clientKeyPemFile, []byte(clientKey), 0o600); err != nil {
		return WriteSSLKeyError{fmt.Sprintf("Error saving file %s error::%v", clientKeyPemFile, err), "client_key_create_err"}
	}
	if err = os.WriteFile(serverCertPemFile, []byte(serverCert), 0o600); err != nil {
		return WriteSSLKeyError{fmt.Sprintf("Error saving file %s error::%v", serverCertPemFile, err), "server_cert_create_err"}
	}
	if err = os.WriteFile(checkSumFile, []byte(sslHash), 0o600); err != nil {
		return WriteSSLKeyError{fmt.Sprintf("Error saving file %s error::%v", checkSumFile, err), "ssl_hash_create_err"}
	}
	return WriteSSLKeyError{}
}

func GetSSLKeyDirPath(destinationID string) (whSSLRootDir string) {
	var err error
	var directoryName string
	if directoryName, err = misc.CreateTMPDIR(); err != nil {
		pkgLogger.Errorf("Error creating SSL root TMP directory for destination %v", err)
		return
	}
	sslDirPath := fmt.Sprintf("%s/dest-ssls/%s", directoryName, destinationID)
	return sslDirPath
}

func GetLoadFileType(wh string) string {
	switch wh {
	case BQ:
		return LOAD_FILE_TYPE_JSON
	case RS:
		return LOAD_FILE_TYPE_CSV
	case S3_DATALAKE, GCS_DATALAKE, AZURE_DATALAKE:
		return LOAD_FILE_TYPE_PARQUET
	case DELTALAKE:
		return LOAD_FILE_TYPE_CSV
	default:
		return LOAD_FILE_TYPE_CSV
	}
}

func GetLoadFileFormat(whType string) string {
	switch whType {
	case BQ:
		return "json.gz"
	case S3_DATALAKE, GCS_DATALAKE, AZURE_DATALAKE:
		return "parquet"
	case RS:
		return "csv.gz"
	case DELTALAKE:
		return "csv.gz"
	default:
		return "csv.gz"
	}
}

func PostRequestWithTimeout(ctx context.Context, url string, payload []byte, timeout time.Duration) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(payload))
	if err != nil {
		return []byte{}, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(config.GetWorkspaceToken(), "")

	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return []byte{}, err
	}

	var respBody []byte
	if resp != nil && resp.Body != nil {
		respBody, _ = io.ReadAll(resp.Body)
		func() { httputil.CloseResponse(resp) }()
	}

	return respBody, nil
}

func GetDateRangeList(start, end time.Time, dateFormat string) (dateRange []string) {
	if (start == time.Time{} || end == time.Time{}) {
		return
	}
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		dateRange = append(dateRange, d.Format(dateFormat))
	}
	return
}

type FilterBy struct {
	Key   string
	Value interface{}
}

func StagingTablePrefix(provider string) string {
	return ToProviderCase(provider, stagingTablePrefix)
}

func StagingTableName(provider, tableName string, tableNameLimit int) string {
	randomNess := RandHex()
	prefix := StagingTablePrefix(provider)
	stagingTableName := fmt.Sprintf(`%s%s_%s`, prefix, tableName, randomNess)
	return misc.TruncateStr(stagingTableName, tableNameLimit)
}

// RandHex returns a random hex string of length 32
func RandHex() string {
	u := misc.FastUUID()
	var buf [32]byte
	hex.Encode(buf[:], u[:])
	return string(buf[:])
}

func ExtractTunnelInfoFromDestinationConfig(config map[string]interface{}) *tunnelling.TunnelInfo {
	if tunnelEnabled := ReadAsBool("useSSH", config); !tunnelEnabled {
		return nil
	}

	return &tunnelling.TunnelInfo{
		Config: config,
	}
}

func ReadAsBool(key string, config map[string]interface{}) bool {
	if _, ok := config[key]; ok {
		if val, ok := config[key].(bool); ok {
			return val
		}
	}
	return false
}
