package clickhouse

import (
	"compress/gzip"
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/internal/service/loadfiles/downloader"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"golang.org/x/exp/slices"

	"github.com/cenkalti/backoff/v4"

	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/ClickHouse/clickhouse-go"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const (
	host           = "host"
	dbName         = "database"
	user           = "user"
	password       = "password"
	port           = "port"
	secure         = "secure"
	skipVerify     = "skipVerify"
	caCertificate  = "caCertificate"
	Cluster        = "cluster"
	partitionField = "received_at"
)

var (
	pkgLogger                    logger.Logger
	clickhouseDefaultDateTime, _ = time.Parse(time.RFC3339, "1970-01-01 00:00:00")
)

// clickhouse doesn't support bool, they recommend to use Uint8 and set 1,0
var rudderDataTypesMapToClickHouse = map[string]string{
	"int":             "Int64",
	"array(int)":      "Array(Int64)",
	"float":           "Float64",
	"array(float)":    "Array(Float64)",
	"string":          "String",
	"array(string)":   "Array(String)",
	"datetime":        "DateTime",
	"array(datetime)": "Array(DateTime)",
	"boolean":         "UInt8",
	"array(boolean)":  "Array(UInt8)",
}

var clickhouseSpecificColumnNameMappings = map[string]string{
	"event":      "LowCardinality(String)",
	"event_text": "LowCardinality(String)",
}

var datatypeDefaultValuesMap = map[string]interface{}{
	"int":      0,
	"float":    0.0,
	"boolean":  0,
	"datetime": clickhouseDefaultDateTime,
}

var clickhouseDataTypesMapToRudder = map[string]string{
	"Int8":                             "int",
	"Int16":                            "int",
	"Int32":                            "int",
	"Int64":                            "int",
	"Array(Int64)":                     "array(int)",
	"Array(Nullable(Int64))":           "array(int)",
	"Float32":                          "float",
	"Float64":                          "float",
	"Array(Float64)":                   "array(float)",
	"Array(Nullable(Float64))":         "array(float)",
	"String":                           "string",
	"Array(String)":                    "array(string)",
	"Array(Nullable(String))":          "array(string)",
	"DateTime":                         "datetime",
	"Array(DateTime)":                  "array(datetime)",
	"Array(Nullable(DateTime))":        "array(datetime)",
	"UInt8":                            "boolean",
	"Array(UInt8)":                     "array(boolean)",
	"Array(Nullable(UInt8))":           "array(boolean)",
	"LowCardinality(String)":           "string",
	"LowCardinality(Nullable(String))": "string",
	"Nullable(Int8)":                   "int",
	"Nullable(Int16)":                  "int",
	"Nullable(Int32)":                  "int",
	"Nullable(Int64)":                  "int",
	"Nullable(Float32)":                "float",
	"Nullable(Float64)":                "float",
	"Nullable(String)":                 "string",
	"Nullable(DateTime)":               "datetime",
	"Nullable(UInt8)":                  "boolean",
	"SimpleAggregateFunction(anyLast, Nullable(Int8))":     "int",
	"SimpleAggregateFunction(anyLast, Nullable(Int16))":    "int",
	"SimpleAggregateFunction(anyLast, Nullable(Int32))":    "int",
	"SimpleAggregateFunction(anyLast, Nullable(Int64))":    "int",
	"SimpleAggregateFunction(anyLast, Nullable(Float32))":  "float",
	"SimpleAggregateFunction(anyLast, Nullable(Float64))":  "float",
	"SimpleAggregateFunction(anyLast, Nullable(String))":   "string",
	"SimpleAggregateFunction(anyLast, Nullable(DateTime))": "datetime",
	"SimpleAggregateFunction(anyLast, Nullable(UInt8))":    "boolean",
}

var errorsMappings = []model.JobError{
	{
		Type:   model.PermissionError,
		Format: regexp.MustCompile(`code: 516, message: .*: Authentication failed: password is incorrect, or there is no user with such name`),
	},
	{
		Type:   model.InsufficientResourceError,
		Format: regexp.MustCompile(`code: 241, message: Memory limit .* exceeded: would use .*, maximum: .*`),
	},
}

type Clickhouse struct {
	DB                          *sql.DB
	Namespace                   string
	ObjectStorage               string
	Warehouse                   model.Warehouse
	Uploader                    warehouseutils.Uploader
	stats                       stats.Stats
	ConnectTimeout              time.Duration
	Logger                      logger.Logger
	QueryDebugLogs              string
	BlockSize                   string
	PoolSize                    string
	ReadTimeout                 string
	WriteTimeout                string
	Compress                    bool
	DisableNullable             bool
	ExecTimeOutInSeconds        time.Duration
	CommitTimeOutInSeconds      time.Duration
	LoadTableFailureRetries     int
	NumWorkersDownloadLoadFiles int
	S3EngineEnabledWorkspaceIDs []string
	LoadFileDownloader          downloader.Downloader
}

type Credentials struct {
	Host          string
	DBName        string
	User          string
	Password      string
	Port          string
	Secure        string
	SkipVerify    string
	TLSConfigName string
	timeout       time.Duration
}

type clickHouseStat struct {
	numRowsLoadFile       stats.Measurement
	downloadLoadFilesTime stats.Measurement
	syncLoadFileTime      stats.Measurement
	commitTime            stats.Measurement
	failRetries           stats.Measurement
	execTimeouts          stats.Measurement
	commitTimeouts        stats.Measurement
}

// newClickHouseStat Creates a new clickHouseStat instance
func (ch *Clickhouse) newClickHouseStat(tableName string) *clickHouseStat {
	warehouse := ch.Warehouse

	tags := map[string]string{
		"workspaceId": warehouse.WorkspaceID,
		"destination": warehouse.Destination.ID,
		"destType":    warehouse.Type,
		"source":      warehouse.Source.ID,
		"namespace":   warehouse.Namespace,
		"identifier":  warehouse.Identifier,
		"tableName":   tableName,
	}

	numRowsLoadFile := ch.stats.NewTaggedStat("warehouse.clickhouse.numRowsLoadFile", stats.CountType, tags)
	downloadLoadFilesTime := ch.stats.NewTaggedStat("warehouse.clickhouse.downloadLoadFilesTime", stats.TimerType, tags)
	syncLoadFileTime := ch.stats.NewTaggedStat("warehouse.clickhouse.syncLoadFileTime", stats.TimerType, tags)
	commitTime := ch.stats.NewTaggedStat("warehouse.clickhouse.commitTime", stats.TimerType, tags)
	failRetries := ch.stats.NewTaggedStat("warehouse.clickhouse.failedRetries", stats.CountType, tags)
	execTimeouts := ch.stats.NewTaggedStat("warehouse.clickhouse.execTimeouts", stats.CountType, tags)
	commitTimeouts := ch.stats.NewTaggedStat("warehouse.clickhouse.commitTimeouts", stats.CountType, tags)

	return &clickHouseStat{
		numRowsLoadFile:       numRowsLoadFile,
		downloadLoadFilesTime: downloadLoadFilesTime,
		syncLoadFileTime:      syncLoadFileTime,
		commitTime:            commitTime,
		failRetries:           failRetries,
		execTimeouts:          execTimeouts,
		commitTimeouts:        commitTimeouts,
	}
}

func Init() {
	pkgLogger = logger.NewLogger().Child("warehouse").Child("clickhouse")
}

// ConnectToClickhouse connects to clickhouse with provided credentials
func (ch *Clickhouse) ConnectToClickhouse(cred Credentials, includeDBInConn bool) (*sql.DB, error) {
	dsn := url.URL{
		Scheme: "tcp",
		Host:   fmt.Sprintf("%s:%s", cred.Host, cred.Port),
	}

	values := url.Values{
		"username":      []string{cred.User},
		"password":      []string{cred.Password},
		"block_size":    []string{ch.BlockSize},
		"pool_size":     []string{ch.PoolSize},
		"debug":         []string{ch.QueryDebugLogs},
		"secure":        []string{cred.Secure},
		"skip_verify":   []string{cred.SkipVerify},
		"tls_config":    []string{cred.TLSConfigName},
		"read_timeout":  []string{ch.ReadTimeout},
		"write_timeout": []string{ch.WriteTimeout},
		"compress":      []string{strconv.FormatBool(ch.Compress)},
	}

	if includeDBInConn {
		values.Add("database", cred.DBName)
	}
	if cred.timeout > 0 {
		values.Add("timeout", fmt.Sprintf("%d", cred.timeout/time.Second))
	}

	dsn.RawQuery = values.Encode()

	var (
		err error
		db  *sql.DB
	)

	if db, err = sql.Open("clickhouse", dsn.String()); err != nil {
		return nil, fmt.Errorf("clickhouse connection error : (%v)", err)
	}
	return db, nil
}

func NewClickhouse() *Clickhouse {
	return &Clickhouse{
		Logger: pkgLogger,
	}
}

func WithConfig(h *Clickhouse, config *config.Config) {
	h.QueryDebugLogs = config.GetString("Warehouse.clickhouse.queryDebugLogs", "false")
	h.BlockSize = config.GetString("Warehouse.clickhouse.blockSize", "1000000")
	h.PoolSize = config.GetString("Warehouse.clickhouse.poolSize", "100")
	h.ReadTimeout = config.GetString("Warehouse.clickhouse.readTimeout", "300")
	h.WriteTimeout = config.GetString("Warehouse.clickhouse.writeTimeout", "1800")
	h.Compress = config.GetBool("Warehouse.clickhouse.compress", false)
	h.DisableNullable = config.GetBool("Warehouse.clickhouse.disableNullable", false)
	h.ExecTimeOutInSeconds = config.GetDuration("Warehouse.clickhouse.execTimeOutInSeconds", 600, time.Second)
	h.CommitTimeOutInSeconds = config.GetDuration("Warehouse.clickhouse.commitTimeOutInSeconds", 600, time.Second)
	h.LoadTableFailureRetries = config.GetInt("Warehouse.clickhouse.loadTableFailureRetries", 3)
	h.NumWorkersDownloadLoadFiles = config.GetInt("Warehouse.clickhouse.numWorkersDownloadLoadFiles", 8)
	h.S3EngineEnabledWorkspaceIDs = config.GetStringSlice("Warehouse.clickhouse.s3EngineEnabledWorkspaceIDs", nil)
}

/*
registerTLSConfig will create a global map, use different names for the different tls config.
clickhouse will access the config by mentioning the key in connection string
*/
func registerTLSConfig(key, certificate string) {
	tlsConfig := &tls.Config{}
	caCert := []byte(certificate)
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	tlsConfig.RootCAs = caCertPool
	_ = clickhouse.RegisterTLSConfig(key, tlsConfig)
}

// getConnectionCredentials gives clickhouse credentials
func (ch *Clickhouse) getConnectionCredentials() Credentials {
	tlsName := ""
	certificate := warehouseutils.GetConfigValue(caCertificate, ch.Warehouse)
	if strings.TrimSpace(certificate) != "" {
		// each destination will have separate tls config, hence using destination id as tlsName
		tlsName = ch.Warehouse.Destination.ID
		registerTLSConfig(tlsName, certificate)
	}
	credentials := Credentials{
		Host:          warehouseutils.GetConfigValue(host, ch.Warehouse),
		DBName:        warehouseutils.GetConfigValue(dbName, ch.Warehouse),
		User:          warehouseutils.GetConfigValue(user, ch.Warehouse),
		Password:      warehouseutils.GetConfigValue(password, ch.Warehouse),
		Port:          warehouseutils.GetConfigValue(port, ch.Warehouse),
		Secure:        warehouseutils.GetConfigValueBoolString(secure, ch.Warehouse),
		SkipVerify:    warehouseutils.GetConfigValueBoolString(skipVerify, ch.Warehouse),
		TLSConfigName: tlsName,
		timeout:       ch.ConnectTimeout,
	}
	return credentials
}

// ColumnsWithDataTypes creates columns and its datatype into sql format for creating table
func (ch *Clickhouse) ColumnsWithDataTypes(tableName string, columns model.TableSchema, notNullableColumns []string) string {
	var arr []string
	for columnName, dataType := range columns {
		codec := ch.getClickHouseCodecForColumnType(dataType, tableName)
		columnType := ch.getClickHouseColumnTypeForSpecificTable(tableName, columnName, rudderDataTypesMapToClickHouse[dataType], misc.Contains(notNullableColumns, columnName))
		arr = append(arr, fmt.Sprintf(`%q %s %s`, columnName, columnType, codec))
	}
	return strings.Join(arr, ",")
}

func (ch *Clickhouse) getClickHouseCodecForColumnType(columnType, tableName string) string {
	if columnType == "datetime" {
		if ch.DisableNullable && !(tableName == warehouseutils.IdentifiesTable || tableName == warehouseutils.UsersTable) {
			return "Codec(DoubleDelta, LZ4)"
		}
	}
	return ""
}

func getClickhouseColumnTypeForSpecificColumn(columnName, columnType string, isNullable bool) string {
	specificColumnType := columnType
	if strings.Contains(specificColumnType, "Array") {
		return specificColumnType
	}
	if isNullable {
		specificColumnType = fmt.Sprintf("Nullable(%s)", specificColumnType)
	}
	if _, ok := clickhouseSpecificColumnNameMappings[columnName]; ok {
		specificColumnType = clickhouseSpecificColumnNameMappings[columnName]
	}

	return specificColumnType
}

// getClickHouseColumnTypeForSpecificTable gets suitable columnType based on the tableName
func (ch *Clickhouse) getClickHouseColumnTypeForSpecificTable(tableName, columnName, columnType string, notNullableKey bool) string {
	if notNullableKey || (tableName != warehouseutils.IdentifiesTable && ch.DisableNullable) {
		return getClickhouseColumnTypeForSpecificColumn(columnName, columnType, false)
	}
	// Nullable is not disabled for users and identity table
	if tableName == warehouseutils.UsersTable {
		return fmt.Sprintf(`SimpleAggregateFunction(anyLast, %s)`, getClickhouseColumnTypeForSpecificColumn(columnName, columnType, true))
	}
	return getClickhouseColumnTypeForSpecificColumn(columnName, columnType, true)
}

func (*Clickhouse) DeleteBy([]string, warehouseutils.DeleteByParams) error {
	return fmt.Errorf(warehouseutils.NotImplementedErrorCode)
}

func generateArgumentString(length int) string {
	var args []string
	for i := 0; i < length; i++ {
		args = append(args, "?")
	}
	return strings.Join(args, ",")
}

func (ch *Clickhouse) castStringToArray(data, dataType string) interface{} {
	switch dataType {
	case "array(int)":
		dataInt := make([]int64, 0)
		err := json.Unmarshal([]byte(data), &dataInt)
		if err != nil {
			ch.Logger.Error("Error while unmarshalling data into array of int: %s", err.Error())
		}
		return dataInt
	case "array(float)":
		dataFloat := make([]float64, 0)
		err := json.Unmarshal([]byte(data), &dataFloat)
		if err != nil {
			ch.Logger.Error("Error while unmarshalling data into array of float: %s", err.Error())
		}
		return dataFloat
	case "array(string)":
		dataInterface := make([]interface{}, 0)
		err := json.Unmarshal([]byte(data), &dataInterface)
		if err != nil {
			ch.Logger.Error("Error while unmarshalling data into array of interface: %s", err.Error())
		}
		dataString := make([]string, 0)
		for _, value := range dataInterface {
			if _, ok := value.(string); ok {
				dataString = append(dataString, value.(string))
			} else {
				bytes, _ := json.Marshal(value)
				dataString = append(dataString, string(bytes))
			}
		}
		return dataString
	case "array(datetime)":
		dataTime := make([]time.Time, 0)
		err := json.Unmarshal([]byte(data), &dataTime)
		if err != nil {
			ch.Logger.Error("Error while unmarshalling data into array of date time: %s", err.Error())
		}
		return dataTime
	case "array(boolean)":
		dataInt := make([]int32, 0)
		dataBool := make([]bool, 0)

		// Since we are converting true/false to 1/0 in warehouse slave
		// We need to unmarshal into []int32 first to load the data into the table
		// If it is unsuccessful, we unmarshal for []bool
		if err := json.Unmarshal([]byte(data), &dataInt); err == nil {
			for _, value := range dataInt {
				dataBool = append(dataBool, value != 0)
			}
			return dataBool
		}

		err := json.Unmarshal([]byte(data), &dataBool)
		if err != nil {
			ch.Logger.Error("Error while unmarshalling data into array of bool: %s", err.Error())
			return dataBool
		}
		return dataBool
	}
	return data
}

// typecastDataFromType typeCasts string data to the mentioned data type
func (ch *Clickhouse) typecastDataFromType(data, dataType string) interface{} {
	var dataI interface{}
	var err error
	switch dataType {
	case "int":
		dataI, err = strconv.Atoi(data)
	case "float":
		dataI, err = strconv.ParseFloat(data, 64)
	case "datetime":
		dataI, err = time.Parse(time.RFC3339, data)
	case "boolean":
		var b bool
		b, err = strconv.ParseBool(data)
		dataI = 0
		if b {
			dataI = 1
		}
	default:
		if strings.Contains(dataType, "array") {
			dataI = ch.castStringToArray(data, dataType)
		} else {
			return data
		}
	}
	if err != nil {
		if ch.DisableNullable {
			return datatypeDefaultValuesMap[dataType]
		}
		return nil
	}
	return dataI
}

// loadTable loads table to clickhouse from the load files
func (ch *Clickhouse) loadTable(tableName string, tableSchemaInUpload model.TableSchema) (err error) {
	if ch.UseS3CopyEngineForLoading() {
		return ch.loadByCopyCommand(tableName, tableSchemaInUpload)
	}
	return ch.loadByDownloadingLoadFiles(tableName, tableSchemaInUpload)
}

func (ch *Clickhouse) UseS3CopyEngineForLoading() bool {
	if !slices.Contains(ch.S3EngineEnabledWorkspaceIDs, ch.Warehouse.WorkspaceID) {
		return false
	}
	return ch.ObjectStorage == warehouseutils.S3 || ch.ObjectStorage == warehouseutils.MINIO
}

func (ch *Clickhouse) loadByDownloadingLoadFiles(tableName string, tableSchemaInUpload model.TableSchema) (err error) {
	ch.Logger.Infof("%s LoadTable Started", ch.GetLogIdentifier(tableName))
	defer ch.Logger.Infof("%s LoadTable Completed", ch.GetLogIdentifier(tableName))

	// Clickhouse stats
	chStats := ch.newClickHouseStat(tableName)

	downloadStart := time.Now()
	fileNames, err := ch.LoadFileDownloader.Download(context.TODO(), tableName)
	chStats.downloadLoadFilesTime.Since(downloadStart)
	if err != nil {
		return
	}
	defer misc.RemoveFilePaths(fileNames...)

	operation := func() error {
		tableError := ch.loadTablesFromFilesNamesWithRetry(tableName, tableSchemaInUpload, fileNames, chStats)
		err = tableError.err
		if !tableError.enableRetry {
			return nil
		}
		return tableError.err
	}

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewConstantBackOff(1*time.Second), uint64(ch.LoadTableFailureRetries))
	retryError := backoff.RetryNotify(operation, backoffWithMaxRetry, func(error error, t time.Duration) {
		err = fmt.Errorf("%s Error occurred while retrying for load tables with error: %v", ch.GetLogIdentifier(tableName), error)
		ch.Logger.Error(err)
		chStats.failRetries.Count(1)
	})
	if retryError != nil {
		err = retryError
	}
	return
}

func (ch *Clickhouse) credentials() (accessKeyID, secretAccessKey string, err error) {
	if ch.ObjectStorage == warehouseutils.S3 {
		return warehouseutils.GetConfigValue(warehouseutils.AWSAccessSecret, ch.Warehouse), warehouseutils.GetConfigValue(warehouseutils.AWSAccessKey, ch.Warehouse), nil
	}
	if ch.ObjectStorage == warehouseutils.MINIO {
		return warehouseutils.GetConfigValue(warehouseutils.MinioAccessKeyID, ch.Warehouse), warehouseutils.GetConfigValue(warehouseutils.MinioSecretAccessKey, ch.Warehouse), nil
	}
	return "", "", errors.New("objectStorage not supported for loading using S3 engine")
}

func (ch *Clickhouse) loadByCopyCommand(tableName string, tableSchemaInUpload model.TableSchema) error {
	ch.Logger.Infof("LoadTable By COPY command Started for table: %s", tableName)

	strKeys := warehouseutils.GetColumnsFromTableSchema(tableSchemaInUpload)
	sort.Strings(strKeys)
	sortedColumnNames := strings.Join(strKeys, ",")
	sortedColumnNamesWithDataTypes := warehouseutils.JoinWithFormatting(strKeys, func(idx int, name string) string {
		return fmt.Sprintf(`%s %s`, name, rudderDataTypesMapToClickHouse[tableSchemaInUpload[name]])
	}, ",")

	csvObjectLocation, err := ch.Uploader.GetSampleLoadFileLocation(tableName)
	if err != nil {
		return fmt.Errorf("sample load file location with error: %w", err)
	}
	loadFolderDir, _ := path.Split(csvObjectLocation)
	loadFolder := loadFolderDir + "*.csv.gz"

	accessKeyID, secretAccessKey, err := ch.credentials()
	if err != nil {
		return fmt.Errorf("auth credentials during load for copy with error: %w", err)
	}

	sqlStatement := fmt.Sprintf(`
		INSERT INTO %[1]q.%[2]q (
			%[3]s
		)
		SELECT
		  *
		FROM
		  s3(
			'%[4]s',
		  	'%[5]s',
		  	'%[6]s',
			'CSV',
			'%[7]s',
			'gz'
		  )
			settings
				date_time_input_format = 'best_effort',
				input_format_csv_arrays_as_nested_csv = 1;
		`,
		ch.Namespace,                   // 1
		tableName,                      // 2
		sortedColumnNames,              // 3
		loadFolder,                     // 4
		accessKeyID,                    // 5
		secretAccessKey,                // 6
		sortedColumnNamesWithDataTypes, // 7
	)
	_, err = ch.DB.Exec(sqlStatement)
	if err != nil {
		return fmt.Errorf("executing insert query for load table with error: %w", err)
	}

	ch.Logger.Infof("LoadTable By COPY command Completed for table: %s", tableName)
	return nil
}

type tableError struct {
	enableRetry bool
	err         error
}

func (ch *Clickhouse) loadTablesFromFilesNamesWithRetry(tableName string, tableSchemaInUpload model.TableSchema, fileNames []string, chStats *clickHouseStat) (terr tableError) {
	ch.Logger.Debugf("%s LoadTablesFromFilesNamesWithRetry Started", ch.GetLogIdentifier(tableName))
	defer ch.Logger.Debugf("%s LoadTablesFromFilesNamesWithRetry Completed", ch.GetLogIdentifier(tableName))

	var txn *sql.Tx
	var err error

	onError := func(err error) {
		if txn != nil {
			go func() {
				ch.Logger.Debugf("%s Rollback Started for loading in table", ch.GetLogIdentifier(tableName))
				_ = txn.Rollback()
				ch.Logger.Debugf("%s Rollback Completed for loading in table", ch.GetLogIdentifier(tableName))
			}()
		}
		terr.err = err
		ch.Logger.Errorf("%s OnError for loading in table with error: %v", ch.GetLogIdentifier(tableName), err)
	}

	ch.Logger.Debugf("%s Beginning a transaction in db for loading in table", ch.GetLogIdentifier(tableName))
	txn, err = ch.DB.Begin()
	if err != nil {
		err = fmt.Errorf("%s Error while beginning a transaction in db for loading in table with error:%v", ch.GetLogIdentifier(tableName), err)
		onError(err)
		return
	}
	ch.Logger.Debugf("%s Completed a transaction in db for loading in table", ch.GetLogIdentifier(tableName))

	// sort column names
	sortedColumnKeys := warehouseutils.SortColumnKeysFromColumnMap(tableSchemaInUpload)
	sortedColumnString := warehouseutils.DoubleQuoteAndJoinByComma(sortedColumnKeys)

	sqlStatement := fmt.Sprintf(`INSERT INTO %q.%q (%v) VALUES (%s)`, ch.Namespace, tableName, sortedColumnString, generateArgumentString(len(sortedColumnKeys)))
	ch.Logger.Debugf("%s Preparing statement exec in db for loading in table for query:%s", ch.GetLogIdentifier(tableName), sqlStatement)
	stmt, err := txn.Prepare(sqlStatement)
	if err != nil {
		err = fmt.Errorf("%s Error while preparing statement for transaction in db for loading in table for query:%s error:%v", ch.GetLogIdentifier(tableName), sqlStatement, err)
		onError(err)
		return
	}
	ch.Logger.Debugf("%s Prepared statement exec in db for loading in table", ch.GetLogIdentifier(tableName))

	for _, objectFileName := range fileNames {
		syncStart := time.Now()

		var gzipFile *os.File
		gzipFile, err = os.Open(objectFileName)
		if err != nil {
			err = fmt.Errorf("%s Error opening file using os.Open for file:%s while loading to table with error:%v", ch.GetLogIdentifier(tableName), objectFileName, err.Error())
			onError(err)
			return
		}

		var gzipReader *gzip.Reader
		gzipReader, err = gzip.NewReader(gzipFile)
		if err != nil {
			rruntime.GoForWarehouse(func() {
				misc.RemoveFilePaths(objectFileName)
			})
			_ = gzipFile.Close()
			err = fmt.Errorf("%s Error reading file using gzip.NewReader for file:%s while loading to table with error:%v", ch.GetLogIdentifier(tableName), gzipFile.Name(), err.Error())
			onError(err)
			return
		}

		csvReader := csv.NewReader(gzipReader)
		var csvRowsProcessedCount int
		for {
			var record []string
			record, err = csvReader.Read()
			if err != nil {
				if err == io.EOF {
					ch.Logger.Debugf("%s File reading completed while reading csv file for loading in table for objectFileName:%s", ch.GetLogIdentifier(tableName), objectFileName)
					break
				}
				err = fmt.Errorf("%s Error while reading csv file %s for loading in table with error:%v", ch.GetLogIdentifier(tableName), objectFileName, err)
				onError(err)
				return
			}
			if len(sortedColumnKeys) != len(record) {
				err = fmt.Errorf(`%s Load file CSV columns for a row mismatch number found in upload schema. Columns in CSV row: %d, Columns in upload schema of table with sortedColumnKeys: %d. Processed rows in csv file until mismatch: %d`, ch.GetLogIdentifier(tableName), len(record), len(sortedColumnKeys), csvRowsProcessedCount)
				onError(err)
				return
			}
			var recordInterface []interface{}
			for index, value := range record {
				columnName := sortedColumnKeys[index]
				columnDataType := tableSchemaInUpload[columnName]
				data := ch.typecastDataFromType(value, columnDataType)
				recordInterface = append(recordInterface, data)
			}

			stmtCtx, stmtCancel := context.WithCancel(context.Background())
			misc.RunWithTimeout(func() {
				ch.Logger.Debugf("%s Starting Prepared statement exec", ch.GetLogIdentifier(tableName))
				_, err = stmt.ExecContext(stmtCtx, recordInterface...)
				ch.Logger.Debugf("%s Completed Prepared statement exec", ch.GetLogIdentifier(tableName))
			}, func() {
				ch.Logger.Debugf("%s Cancelling and closing statement", ch.GetLogIdentifier(tableName))
				stmtCancel()
				go func() {
					_ = stmt.Close()
				}()
				err = fmt.Errorf("%s Timed out exec table for objectFileName: %s", ch.GetLogIdentifier(tableName), objectFileName)
				terr.enableRetry = true
				chStats.execTimeouts.Count(1)
			}, ch.ExecTimeOutInSeconds)

			if err != nil {
				err = fmt.Errorf("%s Error in inserting statement for loading in table with error:%v", ch.GetLogIdentifier(tableName), err)
				onError(err)
				return
			}
			csvRowsProcessedCount++
		}

		chStats.numRowsLoadFile.Count(csvRowsProcessedCount)

		_ = gzipReader.Close()
		_ = gzipFile.Close()

		chStats.syncLoadFileTime.Since(syncStart)
	}

	misc.RunWithTimeout(func() {
		defer chStats.commitTime.RecordDuration()()

		ch.Logger.Debugf("%s Committing transaction", ch.GetLogIdentifier(tableName))
		if err = txn.Commit(); err != nil {
			err = fmt.Errorf("%s Error while committing transaction as there was error while loading in table with error:%v", ch.GetLogIdentifier(tableName), err)
			return
		}
		ch.Logger.Debugf("%v Committed transaction", ch.GetLogIdentifier(tableName))
	}, func() {
		err = fmt.Errorf("%s Timed out while committing", ch.GetLogIdentifier(tableName))
		terr.enableRetry = true
		chStats.commitTimeouts.Count(1)
	}, ch.CommitTimeOutInSeconds)

	if err != nil {
		err = fmt.Errorf("%s Error occurred while committing with error:%v", ch.GetLogIdentifier(tableName), err)
		onError(err)
		return
	}
	ch.Logger.Infof("%s Completed loading the table", ch.GetLogIdentifier(tableName))
	return
}

func (ch *Clickhouse) schemaExists(schemaName string) (exists bool, err error) {
	var count int64
	sqlStatement := "SELECT count(*) FROM system.databases WHERE name = ?"
	err = ch.DB.QueryRow(sqlStatement, schemaName).Scan(&count)
	// ignore err if no results for query
	if err == sql.ErrNoRows {
		err = nil
	}
	exists = count > 0
	return
}

// createSchema creates a database in clickhouse
func (ch *Clickhouse) createSchema() (err error) {
	var schemaExists bool
	schemaExists, err = ch.schemaExists(ch.Namespace)
	if err != nil {
		ch.Logger.Errorf("CH: Error checking if database: %s exists: %v", ch.Namespace, err)
		return err
	}
	if schemaExists {
		ch.Logger.Infof("CH: Skipping creating database: %s since it already exists", ch.Namespace)
		return
	}
	dbHandle, err := ch.ConnectToClickhouse(ch.getConnectionCredentials(), false)
	if err != nil {
		return err
	}
	defer dbHandle.Close()
	cluster := warehouseutils.GetConfigValue(Cluster, ch.Warehouse)
	clusterClause := ""
	if len(strings.TrimSpace(cluster)) > 0 {
		clusterClause = fmt.Sprintf(`ON CLUSTER %q`, cluster)
	}
	sqlStatement := fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %q %s`, ch.Namespace, clusterClause)
	ch.Logger.Infof("CH: Creating database in clickhouse for ch:%s : %v", ch.Warehouse.Destination.ID, sqlStatement)
	_, err = dbHandle.Exec(sqlStatement)
	return
}

/*
createUsersTable creates a user's table with engine AggregatingMergeTree,
this lets us choose aggregation logic before merging records with same user id.
current behaviour is to replace user  properties with the latest non-null values
*/
func (ch *Clickhouse) createUsersTable(name string, columns model.TableSchema) (err error) {
	sortKeyFields := []string{"id"}
	notNullableColumns := []string{"received_at", "id"}
	clusterClause := ""
	engine := "AggregatingMergeTree"
	engineOptions := ""
	cluster := warehouseutils.GetConfigValue(Cluster, ch.Warehouse)
	if len(strings.TrimSpace(cluster)) > 0 {
		clusterClause = fmt.Sprintf(`ON CLUSTER %q`, cluster)
		engine = fmt.Sprintf(`%s%s`, "Replicated", engine)
		engineOptions = `'/clickhouse/{cluster}/tables/{database}/{table}', '{replica}'`
	}
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %q.%q %s ( %v )  ENGINE = %s(%s) ORDER BY %s PARTITION BY toDate(%s)`, ch.Namespace, name, clusterClause, ch.ColumnsWithDataTypes(name, columns, notNullableColumns), engine, engineOptions, getSortKeyTuple(sortKeyFields), partitionField)
	ch.Logger.Infof("CH: Creating table in clickhouse for ch:%s : %v", ch.Warehouse.Destination.ID, sqlStatement)
	_, err = ch.DB.Exec(sqlStatement)
	return
}

func getSortKeyTuple(sortKeyFields []string) string {
	tuple := "("
	for index, field := range sortKeyFields {
		if index == len(sortKeyFields)-1 {
			tuple += fmt.Sprintf(`%q`, field)
		} else {
			tuple += fmt.Sprintf(`%q,`, field)
		}
	}
	tuple += ")"
	return tuple
}

// CreateTable creates table with engine ReplacingMergeTree(), this is used for dedupe event data and replace it will the latest data if duplicate data found. This logic is handled by clickhouse
// The engine differs from MergeTree in that it removes duplicate entries with the same sorting key value.
func (ch *Clickhouse) CreateTable(tableName string, columns model.TableSchema) (err error) {
	sortKeyFields := []string{"received_at", "id"}
	if tableName == warehouseutils.DiscardsTable {
		sortKeyFields = []string{"received_at"}
	}
	if strings.HasPrefix(tableName, warehouseutils.CTStagingTablePrefix) {
		sortKeyFields = []string{"id"}
	}
	var sqlStatement string
	if tableName == warehouseutils.UsersTable {
		return ch.createUsersTable(tableName, columns)
	}
	clusterClause := ""
	engine := "ReplacingMergeTree"
	engineOptions := ""
	cluster := warehouseutils.GetConfigValue(Cluster, ch.Warehouse)
	if len(strings.TrimSpace(cluster)) > 0 {
		clusterClause = fmt.Sprintf(`ON CLUSTER %q`, cluster)
		engine = fmt.Sprintf(`%s%s`, "Replicated", engine)
		engineOptions = `'/clickhouse/{cluster}/tables/{database}/{table}', '{replica}'`
	}
	var orderByClause string
	if len(sortKeyFields) > 0 {
		orderByClause = fmt.Sprintf(`ORDER BY %s`, getSortKeyTuple(sortKeyFields))
	}

	var partitionByClause string
	if _, ok := columns[partitionField]; ok {
		partitionByClause = fmt.Sprintf(`PARTITION BY toDate(%s)`, partitionField)
	}

	sqlStatement = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %q.%q %s ( %v ) ENGINE = %s(%s) %s %s`, ch.Namespace, tableName, clusterClause, ch.ColumnsWithDataTypes(tableName, columns, sortKeyFields), engine, engineOptions, orderByClause, partitionByClause)

	ch.Logger.Infof("CH: Creating table in clickhouse for ch:%s : %v", ch.Warehouse.Destination.ID, sqlStatement)
	_, err = ch.DB.Exec(sqlStatement)
	return
}

func (ch *Clickhouse) DropTable(tableName string) (err error) {
	cluster := warehouseutils.GetConfigValue(Cluster, ch.Warehouse)
	clusterClause := ""
	if len(strings.TrimSpace(cluster)) > 0 {
		clusterClause = fmt.Sprintf(`ON CLUSTER %q`, cluster)
	}
	sqlStatement := fmt.Sprintf(`DROP TABLE %q.%q %s `, ch.Warehouse.Namespace, tableName, clusterClause)
	_, err = ch.DB.Exec(sqlStatement)
	return
}

func (ch *Clickhouse) AddColumns(tableName string, columnsInfo []warehouseutils.ColumnInfo) (err error) {
	var (
		query         string
		queryBuilder  strings.Builder
		cluster       string
		clusterClause string
	)

	cluster = warehouseutils.GetConfigValue(Cluster, ch.Warehouse)
	if len(strings.TrimSpace(cluster)) > 0 {
		clusterClause = fmt.Sprintf(`ON CLUSTER %q`, cluster)
	}

	queryBuilder.WriteString(fmt.Sprintf(`
		ALTER TABLE
		  %q.%q %s`,
		ch.Namespace,
		tableName,
		clusterClause,
	))

	for _, columnInfo := range columnsInfo {
		columnType := ch.getClickHouseColumnTypeForSpecificTable(
			tableName,
			columnInfo.Name,
			rudderDataTypesMapToClickHouse[columnInfo.Type],
			false,
		)
		queryBuilder.WriteString(fmt.Sprintf(` ADD COLUMN IF NOT EXISTS %q %s,`, columnInfo.Name, columnType))
	}

	query = strings.TrimSuffix(queryBuilder.String(), ",")
	query += ";"

	ch.Logger.Infof("CH: Adding columns for destinationID: %s, tableName: %s with query: %v", ch.Warehouse.Destination.ID, tableName, query)
	_, err = ch.DB.Exec(query)
	return
}

func (ch *Clickhouse) CreateSchema() (err error) {
	if len(ch.Uploader.GetSchemaInWarehouse()) > 0 {
		return nil
	}
	err = ch.createSchema()
	return err
}

func (*Clickhouse) AlterColumn(_, _, _ string) (model.AlterTableResponse, error) {
	return model.AlterTableResponse{}, nil
}

// TestConnection is used destination connection tester to test the clickhouse connection
func (ch *Clickhouse) TestConnection(warehouse model.Warehouse) (err error) {
	ch.Warehouse = warehouse
	ch.DB, err = ch.ConnectToClickhouse(ch.getConnectionCredentials(), true)
	if err != nil {
		return
	}
	defer ch.DB.Close()

	ctx, cancel := context.WithTimeout(context.TODO(), ch.ConnectTimeout)
	defer cancel()

	err = ch.DB.PingContext(ctx)
	if err == context.DeadlineExceeded {
		return fmt.Errorf("connection testing timed out after %d sec", ch.ConnectTimeout/time.Second)
	}
	if err != nil {
		return err
	}

	return nil
}

func (ch *Clickhouse) Setup(warehouse model.Warehouse, uploader warehouseutils.Uploader) (err error) {
	ch.Warehouse = warehouse
	ch.Namespace = warehouse.Namespace
	ch.Uploader = uploader
	ch.stats = stats.Default
	ch.ObjectStorage = warehouseutils.ObjectStorageType(warehouseutils.CLICKHOUSE, warehouse.Destination.Config, ch.Uploader.UseRudderStorage())
	ch.LoadFileDownloader = downloader.NewDownloader(&warehouse, uploader, ch.NumWorkersDownloadLoadFiles)

	ch.DB, err = ch.ConnectToClickhouse(ch.getConnectionCredentials(), true)
	return err
}

func (*Clickhouse) CrashRecover(_ model.Warehouse) (err error) {
	return
}

// FetchSchema queries clickhouse and returns the schema associated with provided namespace
func (ch *Clickhouse) FetchSchema(warehouse model.Warehouse) (schema, unrecognizedSchema model.Schema, err error) {
	ch.Warehouse = warehouse
	ch.Namespace = warehouse.Namespace
	dbHandle, err := ch.ConnectToClickhouse(ch.getConnectionCredentials(), true)
	if err != nil {
		return
	}
	defer dbHandle.Close()

	schema = make(model.Schema)
	unrecognizedSchema = make(model.Schema)

	sqlStatement := fmt.Sprintf(`
		SELECT
		  table,
		  name,
		  type
		FROM
		  system.columns
		WHERE
		  database = '%s'
	`,
		ch.Namespace,
	)

	rows, err := dbHandle.Query(sqlStatement)
	if err != nil && err != sql.ErrNoRows {
		if exception, ok := err.(*clickhouse.Exception); ok && exception.Code == 81 {
			ch.Logger.Infof("CH: No database found while fetching schema: %s from  destination:%v, query: %v", ch.Namespace, ch.Warehouse.Destination.Name, sqlStatement)
			return schema, unrecognizedSchema, nil
		}
		ch.Logger.Errorf("CH: Error in fetching schema from clickhouse destination:%v, query: %v", ch.Warehouse.Destination.ID, sqlStatement)
		return
	}
	if err == sql.ErrNoRows {
		ch.Logger.Infof("CH: No rows, while fetching schema: %s from destination:%v, query: %v", ch.Namespace, ch.Warehouse.Destination.Name, sqlStatement)
		return schema, unrecognizedSchema, nil
	}
	defer rows.Close()
	for rows.Next() {
		var tName, cName, cType string
		err = rows.Scan(&tName, &cName, &cType)
		if err != nil {
			ch.Logger.Errorf("CH: Error in processing fetched schema from clickhouse destination:%v", ch.Warehouse.Destination.ID)
			return
		}
		if _, ok := schema[tName]; !ok {
			schema[tName] = make(model.TableSchema)
		}
		if datatype, ok := clickhouseDataTypesMapToRudder[cType]; ok {
			schema[tName][cName] = datatype
		} else {
			if _, ok := unrecognizedSchema[tName]; !ok {
				unrecognizedSchema[tName] = make(model.TableSchema)
			}
			unrecognizedSchema[tName][cName] = warehouseutils.MISSING_DATATYPE

			warehouseutils.WHCounterStat(warehouseutils.RUDDER_MISSING_DATATYPE, &ch.Warehouse, warehouseutils.Tag{Name: "datatype", Value: cType}).Count(1)
		}
	}
	return
}

func (ch *Clickhouse) LoadUserTables() (errorMap map[string]error) {
	errorMap = map[string]error{warehouseutils.IdentifiesTable: nil}
	err := ch.loadTable(warehouseutils.IdentifiesTable, ch.Uploader.GetTableSchemaInUpload(warehouseutils.IdentifiesTable))
	if err != nil {
		errorMap[warehouseutils.IdentifiesTable] = err
		return
	}

	if len(ch.Uploader.GetTableSchemaInUpload(warehouseutils.UsersTable)) == 0 {
		return
	}
	errorMap[warehouseutils.UsersTable] = nil
	err = ch.loadTable(warehouseutils.UsersTable, ch.Uploader.GetTableSchemaInUpload(warehouseutils.UsersTable))
	if err != nil {
		errorMap[warehouseutils.UsersTable] = err
		return
	}
	return
}

func (ch *Clickhouse) LoadTable(tableName string) error {
	err := ch.loadTable(tableName, ch.Uploader.GetTableSchemaInUpload(tableName))
	return err
}

func (ch *Clickhouse) Cleanup() {
	if ch.DB != nil {
		_ = ch.DB.Close()
	}
}

func (*Clickhouse) LoadIdentityMergeRulesTable() (err error) {
	return
}

func (*Clickhouse) LoadIdentityMappingsTable() (err error) {
	return
}

func (*Clickhouse) DownloadIdentityRules(*misc.GZipWriter) (err error) {
	return
}

func (*Clickhouse) IsEmpty(_ model.Warehouse) (empty bool, err error) {
	return
}

func (ch *Clickhouse) GetTotalCountInTable(ctx context.Context, tableName string) (int64, error) {
	var (
		total        int64
		err          error
		sqlStatement string
	)
	sqlStatement = fmt.Sprintf(`
		SELECT count(*) FROM "%[1]s"."%[2]s";
	`,
		ch.Namespace,
		tableName,
	)
	err = ch.DB.QueryRowContext(ctx, sqlStatement).Scan(&total)
	return total, err
}

func (ch *Clickhouse) Connect(warehouse model.Warehouse) (client.Client, error) {
	ch.Warehouse = warehouse
	ch.Namespace = warehouse.Namespace
	ch.ObjectStorage = warehouseutils.ObjectStorageType(
		warehouseutils.CLICKHOUSE,
		warehouse.Destination.Config,
		misc.IsConfiguredToUseRudderObjectStorage(ch.Warehouse.Destination.Config),
	)
	dbHandle, err := ch.ConnectToClickhouse(ch.getConnectionCredentials(), true)
	if err != nil {
		return client.Client{}, err
	}

	return client.Client{Type: client.SQLClient, SQL: dbHandle}, err
}

func (ch *Clickhouse) GetLogIdentifier(args ...string) string {
	if len(args) == 0 {
		return fmt.Sprintf("[%s][%s][%s][%s]", ch.Warehouse.Type, ch.Warehouse.Source.ID, ch.Warehouse.Destination.ID, ch.Warehouse.Namespace)
	}
	return fmt.Sprintf("[%s][%s][%s][%s][%s]", ch.Warehouse.Type, ch.Warehouse.Source.ID, ch.Warehouse.Destination.ID, ch.Warehouse.Namespace, strings.Join(args, "]["))
}

func (ch *Clickhouse) LoadTestTable(_, tableName string, payloadMap map[string]interface{}, _ string) (err error) {
	var columns []string
	var recordInterface []interface{}

	for key, value := range payloadMap {
		recordInterface = append(recordInterface, value)
		columns = append(columns, key)
	}

	sqlStatement := fmt.Sprintf(`INSERT INTO %q.%q (%v) VALUES (%s)`,
		ch.Namespace,
		tableName,
		strings.Join(columns, ","),
		generateArgumentString(len(columns)),
	)
	txn, err := ch.DB.Begin()
	if err != nil {
		return
	}

	stmt, err := txn.Prepare(sqlStatement)
	if err != nil {
		return
	}

	if _, err = stmt.Exec(recordInterface...); err != nil {
		return
	}

	if err = stmt.Close(); err != nil {
		return
	}
	if err = txn.Commit(); err != nil {
		return
	}
	return
}

func (ch *Clickhouse) SetConnectionTimeout(timeout time.Duration) {
	ch.ConnectTimeout = timeout
}

func (ch *Clickhouse) ErrorMappings() []model.JobError {
	return errorsMappings
}
