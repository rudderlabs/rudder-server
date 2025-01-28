package warehouseutils

import (
	"bytes"
	"crypto/sha512"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/iancoleman/strcase"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/awsutil"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/awsutils"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

const (
	RS                = "RS"
	BQ                = "BQ"
	SNOWFLAKE         = "SNOWFLAKE"
	SnowpipeStreaming = "SNOWPIPE_STREAMING"
	POSTGRES          = "POSTGRES"
	CLICKHOUSE        = "CLICKHOUSE"
	MSSQL             = "MSSQL"
	AzureSynapse      = "AZURE_SYNAPSE"
	DELTALAKE         = "DELTALAKE"
	S3Datalake        = "S3_DATALAKE"
	GCSDatalake       = "GCS_DATALAKE"
	AzureDatalake     = "AZURE_DATALAKE"
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
	WAREHOUSE             = "warehouse"
	RudderMissingDatatype = "warehouse_rudder_missing_datatype"
)

const (
	stagingTablePrefix = "rudder_staging_"
)

// Object storages
const (
	S3                 = "S3"
	AzureBlob          = "AZURE_BLOB"
	GCS                = "GCS"
	MINIO              = "MINIO"
	DigitalOceanSpaces = "DIGITAL_OCEAN_SPACES"
)

// Cloud providers
const (
	AWS   = "AWS"
	GCP   = "GCP"
	AZURE = "AZURE"
)

var (
	pkgLogger          logger.Logger
	enableIDResolution bool
	awsCredsExpiryInS  config.ValueLoader[int64]

	TimeWindowDestinations    = []string{S3Datalake, GCSDatalake, AzureDatalake}
	WarehouseDestinations     = []string{RS, BQ, SNOWFLAKE, POSTGRES, CLICKHOUSE, MSSQL, AzureSynapse, S3Datalake, GCSDatalake, AzureDatalake, DELTALAKE}
	IdentityEnabledWarehouses = []string{SNOWFLAKE, BQ}
	S3PathStyleRegex          = regexp.MustCompile(`https?://s3([.-](?P<region>[^.]+))?.amazonaws\.com/(?P<bucket>[^/]+)/(?P<keyname>.*)`)
	S3VirtualHostedRegex      = regexp.MustCompile(`https?://(?P<bucket>[^/]+).s3([.-](?P<region>[^.]+))?.amazonaws\.com/(?P<keyname>.*)`)

	WarehouseDestinationMap = lo.SliceToMap(WarehouseDestinations, func(destination string) (string, struct{}) {
		return destination, struct{}{}
	})
)

var WHDestNameMap = map[string]string{
	BQ:            "bigquery",
	RS:            "redshift",
	MSSQL:         "mssql",
	POSTGRES:      "postgres",
	SNOWFLAKE:     "snowflake",
	CLICKHOUSE:    "clickhouse",
	DELTALAKE:     "deltalake",
	S3Datalake:    "s3_datalake",
	GCSDatalake:   "gcs_datalake",
	AzureDatalake: "azure_datalake",
	AzureSynapse:  "azure_synapse",
}

var ObjectStorageMap = map[string]string{
	RS:            S3,
	S3Datalake:    S3,
	BQ:            GCS,
	GCSDatalake:   GCS,
	AzureDatalake: AzureBlob,
}

var SnowflakeStorageMap = map[string]string{
	AWS:   S3,
	GCP:   GCS,
	AZURE: AzureBlob,
}

var DiscardsSchema = map[string]string{
	"table_name":   "string",
	"row_id":       "string",
	"column_name":  "string",
	"column_value": "string",
	"received_at":  "datetime",
	"uuid_ts":      "datetime",
	"reason":       "string",
}

const (
	LoadFileTypeCsv     = "csv"
	LoadFileTypeJson    = "json"
	LoadFileTypeParquet = "parquet"
)

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("warehouse").Child("utils")
}

func loadConfig() {
	enableIDResolution = config.GetBoolVar(false, "Warehouse.enableIDResolution")
	awsCredsExpiryInS = config.GetReloadableInt64Var(3600, 1, "Warehouse.awsCredsExpiryInS")
}

type DeleteByMetaData struct {
	JobRunId  string    `json:"job_run_id"`
	TaskRunId string    `json:"task_run_id"`
	StartTime time.Time `json:"start_time"`
}

type DeleteByParams struct {
	SourceId  string
	JobRunId  string
	TaskRunId string
	StartTime time.Time
}

type ColumnInfo struct {
	Name  string
	Value interface{}
	Type  string
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

type (
	ModelWarehouse   = model.Warehouse
	ModelTableSchema = model.TableSchema
)

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

type SourceIDDestinationID struct {
	SourceID      string `json:"source_id"`
	DestinationID string `json:"destination_id"`
}

type FetchTableInfo struct {
	SourceID      string   `json:"source_id"`
	DestinationID string   `json:"destination_id"`
	Namespace     string   `json:"namespace"`
	Tables        []string `json:"tables"`
}

func TimingFromJSONString(str sql.NullString) (status string, recordedTime time.Time) {
	timingsMap := gjson.Parse(str.String).Map()
	for s, t := range timingsMap {
		return s, t.Time()
	}
	return // zero values
}

// GetObjectFolder returns the folder path for the storage object based on the storage provider
// eg. For provider as S3: https://test-bucket.s3.amazonaws.com/test-object.csv --> s3://test-bucket/test-object.csv
func GetObjectFolder(provider, location string) (folder string) {
	switch provider {
	case S3:
		folder = GetS3LocationFolder(location)
	case GCS:
		folder = GetGCSLocationFolder(location, GCSLocationOptions{TLDFormat: "gcs"})
	case AzureBlob:
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
	case AzureBlob:
		blobUrl, _ := url.Parse(location)
		blobUrlParts := azblob.NewBlobURLParts(*blobUrl)
		accountName := strings.Replace(blobUrlParts.Host, ".blob.core.windows.net", "", 1)
		blobLocation := fmt.Sprintf("wasbs://%s@%s.blob.core.windows.net/%s", blobUrlParts.ContainerName, accountName, blobUrlParts.BlobName)
		folder = GetLocationFolder(blobLocation)
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
	case AzureBlob:
		objectLocation = GetAzureBlobLocation(location)
	}
	return
}

func SanitizeJSON(input json.RawMessage) json.RawMessage {
	v := bytes.ReplaceAll(input, []byte(`\u0000`), []byte(""))
	if len(v) == 0 {
		v = []byte(`{}`)
	}
	return v
}

func SanitizeString(input string) string {
	return strings.ReplaceAll(input, "\u0000", "")
}

// GetObjectName extracts object/key objectName from different buckets locations
// ex: https://bucket-endpoint/bucket-name/object -> object
func GetObjectName(location string, providerConfig interface{}, objectProvider string) (objectName string, err error) {
	var destConfig map[string]interface{}
	var ok bool
	if destConfig, ok = providerConfig.(map[string]interface{}); !ok {
		return "", errors.New("failed to cast destination config interface{} to map[string]interface{}")
	}
	fm, err := filemanager.New(&filemanager.Settings{
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
	return GetLocationFolder(s3Location)
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
	return GetLocationFolder(GetGCSLocation(location, options))
}

func GetGCSLocations(loadFiles []LoadFile, options GCSLocationOptions) (gcsLocations []string) {
	for _, loadFile := range loadFiles {
		gcsLocations = append(gcsLocations, GetGCSLocation(loadFile.Location, options))
	}
	return
}

func GetLocationFolder(location string) string {
	return location[:strings.LastIndex(location, "/")]
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
	return GetLocationFolder(GetAzureBlobLocation(location))
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
	skipNamespaceSnakeCasing := config.GetBool(fmt.Sprintf("Warehouse.%s.skipNamespaceSnakeCasing", WHDestNameMap[provider]), false)
	if !skipNamespaceSnakeCasing {
		namespace = strcase.ToSnake(namespace)
	}
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
	upperCaseProvider := strings.ToUpper(provider)
	if upperCaseProvider == SNOWFLAKE || upperCaseProvider == SnowpipeStreaming {
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

func CreateAWSSessionConfig(destination *backendconfig.DestinationT, serviceName string) (*awsutil.SessionConfig, error) {
	if !misc.IsConfiguredToUseRudderObjectStorage(destination.Config) &&
		(misc.HasAWSRoleARNInConfig(destination.Config) || misc.HasAWSKeysInConfig(destination.Config)) {
		return awsutils.NewSimpleSessionConfigForDestination(destination, serviceName)
	}
	accessKeyID, accessKey := misc.GetRudderObjectStorageAccessKeys()
	return &awsutil.SessionConfig{
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

	awsSession, err := awsutil.CreateSession(sessionConfig)
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

	expiryInSec := awsCredsExpiryInS.Load()
	sessionTokenOutput, err := svc.GetSessionToken(&sts.GetSessionTokenInput{DurationSeconds: &expiryInSec})
	if err != nil {
		return "", "", "", err
	}
	return *sessionTokenOutput.Credentials.AccessKeyId, *sessionTokenOutput.Credentials.SecretAccessKey, *sessionTokenOutput.Credentials.SessionToken, err
}

type Tag struct {
	Name  string
	Value string
}

func WHCounterStat(s stats.Stats, name string, warehouse *model.Warehouse, extraTags ...Tag) stats.Measurement {
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
	return s.NewTaggedStat(name, stats.CountType, tags)
}

// FormatPemContent formats the content of certificates and keys by adding necessary newlines around specific markers.
func FormatPemContent(content string) string {
	// Remove all existing newline characters
	formattedContent := strings.ReplaceAll(content, "\n", " ")

	// Add a newline after specific BEGIN markers
	formattedContent = strings.Replace(formattedContent, "-----BEGIN CERTIFICATE-----", "-----BEGIN CERTIFICATE-----\n", 1)
	formattedContent = strings.Replace(formattedContent, "-----BEGIN RSA PRIVATE KEY-----", "-----BEGIN RSA PRIVATE KEY-----\n", 1)
	formattedContent = strings.Replace(formattedContent, "-----BEGIN ENCRYPTED PRIVATE KEY-----", "-----BEGIN ENCRYPTED PRIVATE KEY-----\n", 1)
	formattedContent = strings.Replace(formattedContent, "-----BEGIN PRIVATE KEY-----", "-----BEGIN PRIVATE KEY-----\n", 1)

	// Add a newline before and after specific END markers
	formattedContent = strings.Replace(formattedContent, "-----END CERTIFICATE-----", "\n-----END CERTIFICATE-----\n", 1)
	formattedContent = strings.Replace(formattedContent, "-----END RSA PRIVATE KEY-----", "\n-----END RSA PRIVATE KEY-----\n", 1)
	formattedContent = strings.Replace(formattedContent, "-----END ENCRYPTED PRIVATE KEY-----", "\n-----END ENCRYPTED PRIVATE KEY-----\n", 1)
	formattedContent = strings.Replace(formattedContent, "-----END PRIVATE KEY-----", "\n-----END PRIVATE KEY-----\n", 1)

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
	clientKey := FormatPemContent(clientKeyConfig.(string))
	clientCert := FormatPemContent(clientCertConfig.(string))
	serverCert := FormatPemContent(serverCAConfig.(string))
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

func GetLoadFileType(destType string) string {
	switch destType {
	case BQ:
		return LoadFileTypeJson
	case RS:
		return LoadFileTypeCsv
	case S3Datalake, GCSDatalake, AzureDatalake:
		return LoadFileTypeParquet
	case DELTALAKE:
		if config.GetBool("Warehouse.deltalake.useParquetLoadFiles", false) {
			return LoadFileTypeParquet
		}
		return LoadFileTypeCsv
	default:
		return LoadFileTypeCsv
	}
}

func GetLoadFileFormat(loadFileType string) string {
	switch loadFileType {
	case LoadFileTypeJson:
		return "json.gz"
	case LoadFileTypeParquet:
		return "parquet"
	case LoadFileTypeCsv:
		return "csv.gz"
	default:
		return "csv.gz"
	}
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

func ReadAsBool(key string, config map[string]interface{}) bool {
	if _, ok := config[key]; ok {
		if val, ok := config[key].(bool); ok {
			return val
		}
	}
	return false
}

func GetConnectionTimeout(destType, destID string) time.Duration {
	destIDLevelConfig := fmt.Sprintf("Warehouse.%s.%s.connectionTimeout", destType, destID)
	destTypeLevelConfig := fmt.Sprintf("Warehouse.%s.connectionTimeout", destType)
	warehouseLevelConfig := "Warehouse.connectionTimeout"

	defaultTimeout := int64(3)
	defaultTimeoutUnits := time.Hour

	if config.IsSet(destIDLevelConfig) {
		return config.GetDuration(destIDLevelConfig, defaultTimeout, defaultTimeoutUnits)
	}
	if config.IsSet(destTypeLevelConfig) {
		return config.GetDuration(destTypeLevelConfig, defaultTimeout, defaultTimeoutUnits)
	}
	return config.GetDuration(warehouseLevelConfig, defaultTimeout, defaultTimeoutUnits)
}
