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
	"math/rand"
	"net/url"
	"os"
	"path"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go"
	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	sqlmw "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/types"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/service/loadfiles/downloader"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const (
	partitionField = "received_at"
)

var clickhouseDefaultDateTime, _ = time.Parse(time.RFC3339, "1970-01-01 00:00:00")

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
	DB                 *sqlmw.DB
	Namespace          string
	ObjectStorage      string
	Warehouse          model.Warehouse
	Uploader           warehouseutils.Uploader
	connectTimeout     time.Duration
	LoadFileDownloader downloader.Downloader

	conf   *config.Config
	logger logger.Logger
	stats  stats.Stats

	config struct {
		queryDebugLogs              string
		blockSize                   string
		poolSize                    string
		readTimeout                 string
		writeTimeout                string
		compress                    bool
		disableNullable             bool
		execTimeOutInSeconds        time.Duration
		commitTimeOutInSeconds      time.Duration
		loadTableFailureRetries     int
		numWorkersDownloadLoadFiles int
		s3EngineEnabledWorkspaceIDs []string
		slowQueryThreshold          time.Duration
		randomLoadDelay             func(string) time.Duration
	}
}

type credentials struct {
	host       string
	database   string
	user       string
	password   string
	port       string
	secure     string
	skipVerify string
	tlsConfig  string
	timeout    time.Duration
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
	tags := stats.Tags{
		"workspaceId": ch.Warehouse.WorkspaceID,
		"destination": ch.Warehouse.Destination.ID,
		"destType":    ch.Warehouse.Type,
		"source":      ch.Warehouse.Source.ID,
		"identifier":  ch.Warehouse.Identifier,
		"tableName":   warehouseutils.TableNameForStats(tableName),
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

func New(conf *config.Config, log logger.Logger, stat stats.Stats) *Clickhouse {
	ch := &Clickhouse{}

	ch.conf = conf
	ch.logger = log.Child("integrations").Child("clickhouse")
	ch.stats = stat

	ch.config.queryDebugLogs = conf.GetString("Warehouse.clickhouse.queryDebugLogs", "false")
	ch.config.blockSize = conf.GetString("Warehouse.clickhouse.blockSize", "1000000")
	ch.config.poolSize = conf.GetString("Warehouse.clickhouse.poolSize", "100")
	ch.config.readTimeout = conf.GetString("Warehouse.clickhouse.readTimeout", "300")
	ch.config.writeTimeout = conf.GetString("Warehouse.clickhouse.writeTimeout", "1800")
	ch.config.compress = conf.GetBool("Warehouse.clickhouse.compress", false)
	ch.config.disableNullable = conf.GetBool("Warehouse.clickhouse.disableNullable", false)
	ch.config.execTimeOutInSeconds = conf.GetDuration("Warehouse.clickhouse.execTimeOutInSeconds", 600, time.Second)
	ch.config.commitTimeOutInSeconds = conf.GetDuration("Warehouse.clickhouse.commitTimeOutInSeconds", 600, time.Second)
	ch.config.loadTableFailureRetries = conf.GetInt("Warehouse.clickhouse.loadTableFailureRetries", 3)
	ch.config.numWorkersDownloadLoadFiles = conf.GetInt("Warehouse.clickhouse.numWorkersDownloadLoadFiles", 8)
	ch.config.s3EngineEnabledWorkspaceIDs = conf.GetStringSlice("Warehouse.clickhouse.s3EngineEnabledWorkspaceIDs", nil)
	ch.config.slowQueryThreshold = conf.GetDuration("Warehouse.clickhouse.slowQueryThreshold", 5, time.Minute)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	ch.config.randomLoadDelay = func(workspaceID string) time.Duration {
		maxDelay := conf.GetDurationVar(
			0,
			time.Second,
			fmt.Sprintf("Warehouse.clickhouse.%s.maxLoadDelay", workspaceID),
			"Warehouse.clickhouse.maxLoadDelay",
		)
		return time.Duration(float64(maxDelay) * (1 - r.Float64()))
	}

	return ch
}

func (ch *Clickhouse) connectToClickhouse(includeDBInConn bool) (*sqlmw.DB, error) {
	cred, err := ch.connectionCredentials()
	if err != nil {
		return nil, fmt.Errorf("could not get connection credentials: %w", err)
	}

	values := url.Values{
		"username":      []string{cred.user},
		"password":      []string{cred.password},
		"block_size":    []string{ch.config.blockSize},
		"pool_size":     []string{ch.config.poolSize},
		"debug":         []string{ch.config.queryDebugLogs},
		"secure":        []string{cred.secure},
		"skip_verify":   []string{cred.skipVerify},
		"tls_config":    []string{cred.tlsConfig},
		"read_timeout":  []string{ch.config.readTimeout},
		"write_timeout": []string{ch.config.writeTimeout},
		"compress":      []string{strconv.FormatBool(ch.config.compress)},
	}
	if includeDBInConn {
		values.Add("database", cred.database)
	}
	if cred.timeout > 0 {
		values.Add("timeout", fmt.Sprintf("%d", cred.timeout/time.Second))
	}

	dsn := url.URL{
		Scheme:   "tcp",
		Host:     fmt.Sprintf("%s:%s", cred.host, cred.port),
		RawQuery: values.Encode(),
	}

	db, err := sql.Open("clickhouse", dsn.String())
	if err != nil {
		return nil, fmt.Errorf("opening connection: %w", err)
	}

	middleware := sqlmw.New(
		db,
		sqlmw.WithStats(ch.stats),
		sqlmw.WithLogger(ch.logger),
		sqlmw.WithKeyAndValues(ch.defaultLogFields()),
		sqlmw.WithQueryTimeout(ch.connectTimeout),
		sqlmw.WithSlowQueryThreshold(ch.config.slowQueryThreshold),
	)
	return middleware, nil
}

// connectionCredentials returns the credentials for connecting to clickhouse
// Each destination will have separate tls config, hence using destination id as tlsName
func (ch *Clickhouse) connectionCredentials() (*credentials, error) {
	credentials := &credentials{
		host:       ch.Warehouse.GetStringDestinationConfig(ch.conf, model.HostSetting),
		database:   ch.Warehouse.GetStringDestinationConfig(ch.conf, model.DatabaseSetting),
		user:       ch.Warehouse.GetStringDestinationConfig(ch.conf, model.UserSetting),
		password:   ch.Warehouse.GetStringDestinationConfig(ch.conf, model.PasswordSetting),
		port:       ch.Warehouse.GetStringDestinationConfig(ch.conf, model.PortSetting),
		secure:     strconv.FormatBool(ch.Warehouse.GetBoolDestinationConfig(model.SecureSetting)),
		skipVerify: strconv.FormatBool(ch.Warehouse.GetBoolDestinationConfig(model.SkipVerifySetting)),
		timeout:    ch.connectTimeout,
	}

	certificate := ch.Warehouse.GetStringDestinationConfig(ch.conf, model.CACertificateSetting)
	if strings.TrimSpace(certificate) != "" {
		if err := registerTLSConfig(ch.Warehouse.Destination.ID, certificate); err != nil {
			return nil, fmt.Errorf("registering tls config: %w", err)
		}

		credentials.tlsConfig = ch.Warehouse.Destination.ID
	}
	return credentials, nil
}

// registerTLSConfig will create a global map, use different names for the different tls config.
// clickhouse will access the config by mentioning the key in connection string
func registerTLSConfig(key, certificate string) error {
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM([]byte(certificate))

	return clickhouse.RegisterTLSConfig(key, &tls.Config{
		RootCAs: caCertPool,
	})
}

func (ch *Clickhouse) defaultLogFields() []any {
	return []any{
		logfield.SourceID, ch.Warehouse.Source.ID,
		logfield.SourceType, ch.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, ch.Warehouse.Destination.ID,
		logfield.DestinationType, ch.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, ch.Warehouse.WorkspaceID,
		logfield.Namespace, ch.Namespace,
	}
}

// ColumnsWithDataTypes creates columns and its datatype into sql format for creating table
func (ch *Clickhouse) ColumnsWithDataTypes(tableName string, columns model.TableSchema, notNullableColumns []string) string {
	var arr []string
	for columnName, dataType := range columns {
		codec := ch.getClickHouseCodecForColumnType(dataType, tableName)
		columnType := ch.getClickHouseColumnTypeForSpecificTable(tableName, columnName, rudderDataTypesMapToClickHouse[dataType], slices.Contains(notNullableColumns, columnName))
		arr = append(arr, fmt.Sprintf(`%q %s %s`, columnName, columnType, codec))
	}
	return strings.Join(arr, ",")
}

func (ch *Clickhouse) getClickHouseCodecForColumnType(columnType, tableName string) string {
	if columnType == "datetime" {
		if ch.config.disableNullable && !(tableName == warehouseutils.IdentifiesTable || tableName == warehouseutils.UsersTable) {
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
	if notNullableKey || (tableName != warehouseutils.IdentifiesTable && ch.config.disableNullable) {
		return getClickhouseColumnTypeForSpecificColumn(columnName, columnType, false)
	}
	// Nullable is not disabled for users and identity table
	if tableName == warehouseutils.UsersTable {
		return fmt.Sprintf(`SimpleAggregateFunction(anyLast, %s)`, getClickhouseColumnTypeForSpecificColumn(columnName, columnType, true))
	}
	return getClickhouseColumnTypeForSpecificColumn(columnName, columnType, true)
}

func (*Clickhouse) DeleteBy(context.Context, []string, warehouseutils.DeleteByParams) error {
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
			ch.logger.Error("Error while unmarshalling data into array of int: %s", err.Error())
		}
		return dataInt
	case "array(float)":
		dataFloat := make([]float64, 0)
		err := json.Unmarshal([]byte(data), &dataFloat)
		if err != nil {
			ch.logger.Error("Error while unmarshalling data into array of float: %s", err.Error())
		}
		return dataFloat
	case "array(string)":
		dataInterface := make([]interface{}, 0)
		err := json.Unmarshal([]byte(data), &dataInterface)
		if err != nil {
			ch.logger.Error("Error while unmarshalling data into array of interface: %s", err.Error())
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
			ch.logger.Error("Error while unmarshalling data into array of date time: %s", err.Error())
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
			ch.logger.Error("Error while unmarshalling data into array of bool: %s", err.Error())
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
		if ch.config.disableNullable {
			return datatypeDefaultValuesMap[dataType]
		}
		return nil
	}
	return dataI
}

// loadTable loads table to clickhouse from the load files
func (ch *Clickhouse) loadTable(ctx context.Context, tableName string, tableSchemaInUpload model.TableSchema) (err error) {
	if delay := ch.config.randomLoadDelay(ch.Warehouse.WorkspaceID); delay > 0 {
		if err = misc.SleepCtx(ctx, delay); err != nil {
			return
		}
	}
	if ch.UseS3CopyEngineForLoading() {
		return ch.loadByCopyCommand(ctx, tableName, tableSchemaInUpload)
	}
	return ch.loadByDownloadingLoadFiles(ctx, tableName, tableSchemaInUpload)
}

func (ch *Clickhouse) UseS3CopyEngineForLoading() bool {
	if !slices.Contains(ch.config.s3EngineEnabledWorkspaceIDs, ch.Warehouse.WorkspaceID) {
		return false
	}
	return ch.ObjectStorage == warehouseutils.S3 || ch.ObjectStorage == warehouseutils.MINIO
}

func (ch *Clickhouse) loadByDownloadingLoadFiles(ctx context.Context, tableName string, tableSchemaInUpload model.TableSchema) (err error) {
	ch.logger.Infof("%s LoadTable Started", ch.GetLogIdentifier(tableName))
	defer ch.logger.Infof("%s LoadTable Completed", ch.GetLogIdentifier(tableName))

	// Clickhouse stats
	chStats := ch.newClickHouseStat(tableName)

	downloadStart := time.Now()
	fileNames, err := ch.LoadFileDownloader.Download(ctx, tableName)
	chStats.downloadLoadFilesTime.Since(downloadStart)
	if err != nil {
		return
	}
	defer misc.RemoveFilePaths(fileNames...)

	operation := func() error {
		tableError := ch.loadTablesFromFilesNamesWithRetry(ctx, tableName, tableSchemaInUpload, fileNames, chStats)
		err = tableError.err
		if !tableError.enableRetry {
			return nil
		}
		return tableError.err
	}

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewConstantBackOff(1*time.Second), uint64(ch.config.loadTableFailureRetries))
	retryError := backoff.RetryNotify(operation, backoffWithMaxRetry, func(error error, t time.Duration) {
		err = fmt.Errorf("%s Error occurred while retrying for load tables with error: %v", ch.GetLogIdentifier(tableName), error)
		ch.logger.Error(err)
		chStats.failRetries.Count(1)
	})
	if retryError != nil {
		err = retryError
	}
	return
}

func (ch *Clickhouse) credentials() (accessKeyID, secretAccessKey string, err error) {
	if ch.ObjectStorage == warehouseutils.S3 {
		return ch.Warehouse.GetStringDestinationConfig(ch.conf, model.AWSAccessSecretSetting), ch.Warehouse.GetStringDestinationConfig(ch.conf, model.AWSAccessKeySetting), nil
	}
	if ch.ObjectStorage == warehouseutils.MINIO {
		return ch.Warehouse.GetStringDestinationConfig(ch.conf, model.MinioAccessKeyIDSetting), ch.Warehouse.GetStringDestinationConfig(ch.conf, model.MinioSecretAccessKeySetting), nil
	}
	return "", "", errors.New("objectStorage not supported for loading using S3 engine")
}

func (ch *Clickhouse) loadByCopyCommand(ctx context.Context, tableName string, tableSchemaInUpload model.TableSchema) error {
	ch.logger.Infof("LoadTable By COPY command Started for table: %s", tableName)

	strKeys := warehouseutils.GetColumnsFromTableSchema(tableSchemaInUpload)
	sort.Strings(strKeys)
	sortedColumnNames := strings.Join(strKeys, ",")
	sortedColumnNamesWithDataTypes := warehouseutils.JoinWithFormatting(strKeys, func(idx int, name string) string {
		return fmt.Sprintf(`%s %s`, name, rudderDataTypesMapToClickHouse[tableSchemaInUpload[name]])
	}, ",")

	csvObjectLocation, err := ch.Uploader.GetSampleLoadFileLocation(ctx, tableName)
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
	_, err = ch.DB.ExecContext(ctx, sqlStatement)
	if err != nil {
		return fmt.Errorf("executing insert query for load table with error: %w", err)
	}

	ch.logger.Infof("LoadTable By COPY command Completed for table: %s", tableName)
	return nil
}

type tableError struct {
	enableRetry bool
	err         error
}

func (ch *Clickhouse) loadTablesFromFilesNamesWithRetry(ctx context.Context, tableName string, tableSchemaInUpload model.TableSchema, fileNames []string, chStats *clickHouseStat) (terr tableError) {
	ch.logger.Debugf("%s LoadTablesFromFilesNamesWithRetry Started", ch.GetLogIdentifier(tableName))
	defer ch.logger.Debugf("%s LoadTablesFromFilesNamesWithRetry Completed", ch.GetLogIdentifier(tableName))

	var txn *sqlmw.Tx
	var err error

	onError := func(err error) {
		if txn != nil {
			go func() {
				ch.logger.Debugf("%s Rollback Started for loading in table", ch.GetLogIdentifier(tableName))
				_ = txn.Rollback()
				ch.logger.Debugf("%s Rollback Completed for loading in table", ch.GetLogIdentifier(tableName))
			}()
		}
		terr.err = err
		ch.logger.Errorf("%s OnError for loading in table with error: %v", ch.GetLogIdentifier(tableName), err)
	}

	ch.logger.Debugf("%s Beginning a transaction in db for loading in table", ch.GetLogIdentifier(tableName))
	txn, err = ch.DB.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		err = fmt.Errorf("%s Error while beginning a transaction in db for loading in table with error:%v", ch.GetLogIdentifier(tableName), err)
		onError(err)
		return
	}
	ch.logger.Debugf("%s Completed a transaction in db for loading in table", ch.GetLogIdentifier(tableName))

	// sort column names
	sortedColumnKeys := warehouseutils.SortColumnKeysFromColumnMap(tableSchemaInUpload)
	sortedColumnString := warehouseutils.DoubleQuoteAndJoinByComma(sortedColumnKeys)

	sqlStatement := fmt.Sprintf(`INSERT INTO %q.%q (%v) VALUES (%s)`, ch.Namespace, tableName, sortedColumnString, generateArgumentString(len(sortedColumnKeys)))
	ch.logger.Debugf("%s Preparing statement exec in db for loading in table for query:%s", ch.GetLogIdentifier(tableName), sqlStatement)
	stmt, err := txn.PrepareContext(ctx, sqlStatement)
	if err != nil {
		err = fmt.Errorf("%s Error while preparing statement for transaction in db for loading in table for query:%s error:%v", ch.GetLogIdentifier(tableName), sqlStatement, err)
		onError(err)
		return
	}
	ch.logger.Debugf("%s Prepared statement exec in db for loading in table", ch.GetLogIdentifier(tableName))

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
				if errors.Is(err, io.EOF) {
					ch.logger.Debugf("%s File reading completed while reading csv file for loading in table for objectFileName:%s", ch.GetLogIdentifier(tableName), objectFileName)
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

			stmtCtx, stmtCancel := context.WithCancel(ctx)
			misc.RunWithTimeout(func() {
				ch.logger.Debugf("%s Starting Prepared statement exec", ch.GetLogIdentifier(tableName))
				_, err = stmt.ExecContext(stmtCtx, recordInterface...)
				ch.logger.Debugf("%s Completed Prepared statement exec", ch.GetLogIdentifier(tableName))
				stmtCancel()
			}, func() {
				ch.logger.Debugf("%s Cancelling and closing statement", ch.GetLogIdentifier(tableName))
				stmtCancel()
				go func() {
					_ = stmt.Close()
				}()
				err = fmt.Errorf("%s Timed out exec table for objectFileName: %s", ch.GetLogIdentifier(tableName), objectFileName)
				terr.enableRetry = true
				chStats.execTimeouts.Count(1)
			}, ch.config.execTimeOutInSeconds)

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

		ch.logger.Debugf("%s Committing transaction", ch.GetLogIdentifier(tableName))
		if err = txn.Commit(); err != nil {
			err = fmt.Errorf("%s Error while committing transaction as there was error while loading in table with error:%v", ch.GetLogIdentifier(tableName), err)
			return
		}
		ch.logger.Debugf("%v Committed transaction", ch.GetLogIdentifier(tableName))
	}, func() {
		err = fmt.Errorf("%s Timed out while committing", ch.GetLogIdentifier(tableName))
		terr.enableRetry = true
		chStats.commitTimeouts.Count(1)
	}, ch.config.commitTimeOutInSeconds)

	if err != nil {
		err = fmt.Errorf("%s Error occurred while committing with error:%v", ch.GetLogIdentifier(tableName), err)
		onError(err)
		return
	}
	ch.logger.Infof("%s Completed loading the table", ch.GetLogIdentifier(tableName))
	return
}

func (ch *Clickhouse) schemaExists(ctx context.Context, schemaName string) (exists bool, err error) {
	var count int64
	sqlStatement := "SELECT count(*) FROM system.databases WHERE name = ?"
	err = ch.DB.QueryRowContext(ctx, sqlStatement, schemaName).Scan(&count)
	// ignore err if no results for query
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	exists = count > 0
	return
}

/*
createUsersTable creates a user's table with engine AggregatingMergeTree,
this lets us choose aggregation logic before merging records with same user id.
current behaviour is to replace user  properties with the latest non-null values
*/
func (ch *Clickhouse) createUsersTable(ctx context.Context, name string, columns model.TableSchema) (err error) {
	sortKeyFields := []string{"id"}
	notNullableColumns := []string{"received_at", "id"}
	clusterClause := ""
	engine := "AggregatingMergeTree"
	engineOptions := ""
	cluster := ch.Warehouse.GetStringDestinationConfig(ch.conf, model.ClusterSetting)
	if len(strings.TrimSpace(cluster)) > 0 {
		clusterClause = fmt.Sprintf(`ON CLUSTER %q`, cluster)
		engine = fmt.Sprintf(`%s%s`, "Replicated", engine)
		engineOptions = fmt.Sprintf(`'/clickhouse/{cluster}/tables/%s/{database}/{table}', '{replica}'`, uuid.New().String())
	}
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %q.%q %s ( %v )  ENGINE = %s(%s) ORDER BY %s PARTITION BY toDate(%s)`, ch.Namespace, name, clusterClause, ch.ColumnsWithDataTypes(name, columns, notNullableColumns), engine, engineOptions, getSortKeyTuple(sortKeyFields), partitionField)
	ch.logger.Infof("CH: Creating table in clickhouse for ch:%s : %v", ch.Warehouse.Destination.ID, sqlStatement)
	_, err = ch.DB.ExecContext(ctx, sqlStatement)
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
func (ch *Clickhouse) CreateTable(ctx context.Context, tableName string, columns model.TableSchema) (err error) {
	sortKeyFields := []string{"received_at", "id"}
	if tableName == warehouseutils.DiscardsTable {
		sortKeyFields = []string{"received_at"}
	}
	if strings.HasPrefix(tableName, warehouseutils.CTStagingTablePrefix) {
		sortKeyFields = []string{"id"}
	}
	var sqlStatement string
	if tableName == warehouseutils.UsersTable {
		return ch.createUsersTable(ctx, tableName, columns)
	}
	clusterClause := ""
	engine := "ReplacingMergeTree"
	engineOptions := ""
	cluster := ch.Warehouse.GetStringDestinationConfig(ch.conf, model.ClusterSetting)
	if len(strings.TrimSpace(cluster)) > 0 {
		clusterClause = fmt.Sprintf(`ON CLUSTER %q`, cluster)
		engine = fmt.Sprintf(`%s%s`, "Replicated", engine)
		engineOptions = fmt.Sprintf(`'/clickhouse/{cluster}/tables/%s/{database}/{table}', '{replica}'`, uuid.New().String())
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

	ch.logger.Infof("CH: Creating table in clickhouse for ch:%s : %v", ch.Warehouse.Destination.ID, sqlStatement)
	_, err = ch.DB.ExecContext(ctx, sqlStatement)
	return
}

func (ch *Clickhouse) DropTable(ctx context.Context, tableName string) (err error) {
	sqlStatement := fmt.Sprintf(`DROP TABLE %q.%q %s `, ch.Warehouse.Namespace, tableName, ch.clusterClause())
	_, err = ch.DB.ExecContext(ctx, sqlStatement)
	return
}

func (ch *Clickhouse) AddColumns(ctx context.Context, tableName string, columnsInfo []warehouseutils.ColumnInfo) (err error) {
	var (
		query        string
		queryBuilder strings.Builder
	)

	queryBuilder.WriteString(fmt.Sprintf(`
		ALTER TABLE
		  %q.%q %s`,
		ch.Namespace,
		tableName,
		ch.clusterClause(),
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

	ch.logger.Infof("CH: Adding columns for destinationID: %s, tableName: %s with query: %v", ch.Warehouse.Destination.ID, tableName, query)
	_, err = ch.DB.ExecContext(ctx, query)
	return
}

func (ch *Clickhouse) CreateSchema(ctx context.Context) error {
	if !ch.Uploader.IsWarehouseSchemaEmpty() {
		return nil
	}

	if schemaExists, err := ch.schemaExists(ctx, ch.Namespace); err != nil {
		return fmt.Errorf("checking if database: %s exists: %v", ch.Namespace, err)
	} else if schemaExists {
		return nil
	}

	db, err := ch.connectToClickhouse(false)
	if err != nil {
		return fmt.Errorf("connecting to clickhouse: %v", err)
	}
	defer func() { _ = db.Close() }()

	ch.logger.Infow("Creating schema", append(ch.defaultLogFields(), "clusterClause", ch.clusterClause()))

	query := fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %q %s`, ch.Namespace, ch.clusterClause())
	if _, err = db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("creating database: %v", err)
	}
	return nil
}

func (ch *Clickhouse) clusterClause() string {
	if cluster := ch.Warehouse.GetStringDestinationConfig(ch.conf, model.ClusterSetting); len(strings.TrimSpace(cluster)) > 0 {
		return fmt.Sprintf(`ON CLUSTER %q`, cluster)
	}
	return ""
}

func (*Clickhouse) AlterColumn(_ context.Context, _, _, _ string) (model.AlterTableResponse, error) {
	return model.AlterTableResponse{}, nil
}

// TestConnection is used destination connection tester to test the clickhouse connection
func (ch *Clickhouse) TestConnection(ctx context.Context, _ model.Warehouse) error {
	err := ch.DB.PingContext(ctx)
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("connection timeout: %w", err)
	}
	if err != nil {
		return fmt.Errorf("pinging: %w", err)
	}

	return nil
}

func (ch *Clickhouse) Setup(_ context.Context, warehouse model.Warehouse, uploader warehouseutils.Uploader) (err error) {
	ch.Warehouse = warehouse
	ch.Namespace = warehouse.Namespace
	ch.Uploader = uploader
	ch.ObjectStorage = warehouseutils.ObjectStorageType(warehouseutils.CLICKHOUSE, warehouse.Destination.Config, ch.Uploader.UseRudderStorage())
	ch.LoadFileDownloader = downloader.NewDownloader(&warehouse, uploader, ch.config.numWorkersDownloadLoadFiles)

	if ch.DB, err = ch.connectToClickhouse(true); err != nil {
		return fmt.Errorf("connecting to clickhouse: %w", err)
	}
	return err
}

// FetchSchema queries clickhouse and returns the schema associated with provided namespace
func (ch *Clickhouse) FetchSchema(ctx context.Context) (model.Schema, error) {
	schema := make(model.Schema)

	sqlStatement := `
		SELECT
		  table,
		  name,
		  type
		FROM
		  system.columns
		WHERE
		  database = ?
	`

	rows, err := ch.DB.QueryContext(ctx, sqlStatement, ch.Namespace)
	if errors.Is(err, sql.ErrNoRows) {
		return schema, nil
	}
	if err != nil {
		var clickhouseErr *clickhouse.Exception
		if errors.As(err, &clickhouseErr) && clickhouseErr.Code == 81 {
			return schema, nil
		}
		return nil, fmt.Errorf("fetching schema: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var tableName, columnName, columnType string

		if err := rows.Scan(&tableName, &columnName, &columnType); err != nil {
			return nil, fmt.Errorf("scanning schema: %w", err)
		}

		if _, ok := schema[tableName]; !ok {
			schema[tableName] = make(model.TableSchema)
		}
		if datatype, ok := clickhouseDataTypesMapToRudder[columnType]; ok {
			schema[tableName][columnName] = datatype
		} else {
			warehouseutils.WHCounterStat(ch.stats, warehouseutils.RudderMissingDatatype, &ch.Warehouse, warehouseutils.Tag{Name: "datatype", Value: columnType}).Count(1)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("fetching schema: %w", err)
	}

	return schema, nil
}

func (ch *Clickhouse) LoadUserTables(ctx context.Context) (errorMap map[string]error) {
	errorMap = map[string]error{warehouseutils.IdentifiesTable: nil}
	err := ch.loadTable(ctx, warehouseutils.IdentifiesTable, ch.Uploader.GetTableSchemaInUpload(warehouseutils.IdentifiesTable))
	if err != nil {
		errorMap[warehouseutils.IdentifiesTable] = err
		return
	}

	if len(ch.Uploader.GetTableSchemaInUpload(warehouseutils.UsersTable)) == 0 {
		return
	}
	errorMap[warehouseutils.UsersTable] = nil
	err = ch.loadTable(ctx, warehouseutils.UsersTable, ch.Uploader.GetTableSchemaInUpload(warehouseutils.UsersTable))
	if err != nil {
		errorMap[warehouseutils.UsersTable] = err
		return
	}
	return
}

func (ch *Clickhouse) LoadTable(ctx context.Context, tableName string) (*types.LoadTableStats, error) {
	preLoadTableCount, err := ch.totalCountIntable(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("pre load table count: %w", err)
	}

	err = ch.loadTable(ctx, tableName, ch.Uploader.GetTableSchemaInUpload(tableName))
	if err != nil {
		return nil, fmt.Errorf("loading table: %w", err)
	}

	postLoadTableCount, err := ch.totalCountIntable(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("post load table count: %w", err)
	}

	return &types.LoadTableStats{
		RowsInserted: postLoadTableCount - preLoadTableCount,
	}, nil
}

func (ch *Clickhouse) Cleanup(_ context.Context) {
	if ch.DB != nil {
		_ = ch.DB.Close()
	}
}

func (*Clickhouse) LoadIdentityMergeRulesTable(_ context.Context) (err error) {
	return
}

func (*Clickhouse) LoadIdentityMappingsTable(_ context.Context) (err error) {
	return
}

func (*Clickhouse) DownloadIdentityRules(context.Context, *misc.GZipWriter) (err error) {
	return
}

func (*Clickhouse) IsEmpty(_ context.Context, _ model.Warehouse) (empty bool, err error) {
	return
}

func (ch *Clickhouse) totalCountIntable(ctx context.Context, tableName string) (int64, error) {
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

func (ch *Clickhouse) Connect(_ context.Context, warehouse model.Warehouse) (client.Client, error) {
	ch.Warehouse = warehouse
	ch.Namespace = warehouse.Namespace
	ch.ObjectStorage = warehouseutils.ObjectStorageType(
		warehouseutils.CLICKHOUSE,
		warehouse.Destination.Config,
		misc.IsConfiguredToUseRudderObjectStorage(ch.Warehouse.Destination.Config),
	)

	db, err := ch.connectToClickhouse(true)
	if err != nil {
		return client.Client{}, fmt.Errorf("connecting to clickhouse: %w", err)
	}

	return client.Client{Type: client.SQLClient, SQL: db.DB}, err
}

func (ch *Clickhouse) GetLogIdentifier(args ...string) string {
	if len(args) == 0 {
		return fmt.Sprintf("[%s][%s][%s][%s]", ch.Warehouse.Type, ch.Warehouse.Source.ID, ch.Warehouse.Destination.ID, ch.Warehouse.Namespace)
	}
	return fmt.Sprintf("[%s][%s][%s][%s][%s]", ch.Warehouse.Type, ch.Warehouse.Source.ID, ch.Warehouse.Destination.ID, ch.Warehouse.Namespace, strings.Join(args, "]["))
}

func (ch *Clickhouse) LoadTestTable(ctx context.Context, _, tableName string, payloadMap map[string]interface{}, _ string) (err error) {
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
	txn, err := ch.DB.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return
	}

	stmt, err := txn.PrepareContext(ctx, sqlStatement)
	if err != nil {
		return
	}

	if _, err = stmt.ExecContext(ctx, recordInterface...); err != nil {
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
	ch.connectTimeout = timeout
}

func (*Clickhouse) ErrorMappings() []model.JobError {
	return errorsMappings
}
