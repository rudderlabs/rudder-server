package clickhouse

import (
	"compress/gzip"
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/rudderlabs/rudder-server/services/stats"

	"github.com/ClickHouse/clickhouse-go"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	queryDebugLogs              string
	blockSize                   string
	poolSize                    string
	readTimeout                 string
	writeTimeout                string
	compress                    bool
	pkgLogger                   logger.LoggerI
	disableNullable             bool
	execTimeOutInSeconds        time.Duration
	commitTimeOutInSeconds      time.Duration
	loadTableFailureRetries     int
	numWorkersDownloadLoadFiles int
)
var clikhouseDefaultDateTime, _ = time.Parse(time.RFC3339, "1970-01-01 00:00:00")

const (
	host          = "host"
	dbName        = "database"
	user          = "user"
	password      = "password"
	port          = "port"
	secure        = "secure"
	skipVerify    = "skipVerify"
	caCertificate = "caCertificate"
	cluster       = "cluster"
)
const partitionField = "received_at"

// clickhouse doesnt support bool, they recommend to use Uint8 and set 1,0

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
	"datetime": clikhouseDefaultDateTime,
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
	"SimpleAggregateFunction(anyLast, Nullable(Floats32))": "float",
	"SimpleAggregateFunction(anyLast, Nullable(Floats64))": "float",
	"SimpleAggregateFunction(anyLast, Nullable(String))":   "string",
	"SimpleAggregateFunction(anyLast, Nullable(DateTime))": "datetime",
	"SimpleAggregateFunction(anyLast, Nullable(UInt8))":    "boolean",
}

type HandleT struct {
	Db            *sql.DB
	Namespace     string
	ObjectStorage string
	Warehouse     warehouseutils.WarehouseT
	Uploader      warehouseutils.UploaderI
	stats         stats.Stats
}

type credentialsT struct {
	host          string
	dbName        string
	user          string
	password      string
	port          string
	secure        string
	skipVerify    string
	tlsConfigName string
}

type clickHouseStatT struct {
	numRowsLoadFile       stats.RudderStats
	downloadLoadFilesTime stats.RudderStats
	syncLoadFileTime      stats.RudderStats
	execRowTime           stats.RudderStats
	commitTime            stats.RudderStats
	failRetries           stats.RudderStats
	execTimeouts          stats.RudderStats
	commitTimeouts        stats.RudderStats
}

// newClickHouseStat Creates a new clickHouseStatT instance
func (proc *HandleT) newClickHouseStat(tableName string) *clickHouseStatT {
	warehouse := proc.Warehouse

	tags := map[string]string{
		"destination": warehouse.Destination.ID,
		"destType":    warehouse.Type,
		"source":      warehouse.Source.ID,
		"namespace":   warehouse.Namespace,
		"identifier":  warehouse.Identifier,
		"tableName":   tableName,
	}

	numRowsLoadFile := proc.stats.NewTaggedStat("warehouse.clickhouse.numRowsLoadFile", stats.CountType, tags)
	downloadLoadFilesTime := proc.stats.NewTaggedStat("warehouse.clickhouse.downloadLoadFilesTime", stats.TimerType, tags)
	syncLoadFileTime := proc.stats.NewTaggedStat("warehouse.clickhouse.syncLoadFileTime", stats.TimerType, tags)
	execRowTime := proc.stats.NewTaggedStat("warehouse.clickhouse.execRowTime", stats.TimerType, tags)
	commitTime := proc.stats.NewTaggedStat("warehouse.clickhouse.commitTime", stats.TimerType, tags)
	failRetries := proc.stats.NewTaggedStat("warehouse.clickhouse.failedRetries", stats.CountType, tags)
	execTimeouts := proc.stats.NewTaggedStat("warehouse.clickhouse.execTimeouts", stats.CountType, tags)
	commitTimeouts := proc.stats.NewTaggedStat("warehouse.clickhouse.commitTimeouts", stats.CountType, tags)

	return &clickHouseStatT{
		numRowsLoadFile:       numRowsLoadFile,
		downloadLoadFilesTime: downloadLoadFilesTime,
		syncLoadFileTime:      syncLoadFileTime,
		execRowTime:           execRowTime,
		commitTime:            commitTime,
		failRetries:           failRetries,
		execTimeouts:          execTimeouts,
		commitTimeouts:        commitTimeouts,
	}
}

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("warehouse").Child("clickhouse")
}

// connect connects to warehouse with provided credentials
func connect(cred credentialsT, includeDBInConn bool) (*sql.DB, error) {
	var dbNameParam string
	if includeDBInConn {
		dbNameParam = fmt.Sprintf(`database=%s`, cred.dbName)
	}

	url := fmt.Sprintf("tcp://%s:%s?&username=%s&password=%s&block_size=%s&pool_size=%s&debug=%s&secure=%s&skip_verify=%s&tls_config=%s&%s&read_timeout=%s&write_timeout=%s&compress=%t",
		cred.host,
		cred.port,
		cred.user,
		cred.password,
		blockSize,
		poolSize,
		queryDebugLogs,
		cred.secure,
		cred.skipVerify,
		cred.tlsConfigName,
		dbNameParam,
		readTimeout,
		writeTimeout,
		compress,
	)

	var err error
	var db *sql.DB

	if db, err = sql.Open("clickhouse", url); err != nil {
		return nil, fmt.Errorf("clickhouse connection error : (%v)", err)
	}
	return db, nil
}

func loadConfig() {
	config.RegisterStringConfigVariable("true", &queryDebugLogs, true, "Warehouse.clickhouse.queryDebugLogs")
	config.RegisterStringConfigVariable("1000000", &blockSize, true, "Warehouse.clickhouse.blockSize")
	config.RegisterStringConfigVariable("100", &poolSize, true, "Warehouse.clickhouse.poolSize")
	config.RegisterBoolConfigVariable(false, &disableNullable, false, "Warehouse.clickhouse.disableNullable")
	config.RegisterStringConfigVariable("300", &readTimeout, true, "Warehouse.clickhouse.readTimeout")
	config.RegisterStringConfigVariable("1800", &writeTimeout, true, "Warehouse.clickhouse.writeTimeout")
	config.RegisterBoolConfigVariable(false, &compress, true, "Warehouse.clickhouse.compress")
	config.RegisterDurationConfigVariable(time.Duration(120), &execTimeOutInSeconds, true, time.Second, "Warehouse.clickhouse.execTimeOutInSeconds")
	config.RegisterDurationConfigVariable(time.Duration(240), &commitTimeOutInSeconds, true, time.Second, "Warehouse.clickhouse.commitTimeOutInSeconds")
	config.RegisterIntConfigVariable(3, &loadTableFailureRetries, true, 1, "Warehouse.clickhouse.loadTableFailureRetries")
	config.RegisterIntConfigVariable(8, &numWorkersDownloadLoadFiles, true, 1, "Warehouse.clickhouse.numWorkersDownloadLoadFiles")
}

/*
 registerTLSConfig will create a global map, use different names for the different tls config.
 clickhouse will access the config by mentioning the key in connection string
*/
func registerTLSConfig(key string, certificate string) {
	tlsConfig := &tls.Config{}
	caCert := []byte(certificate)
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	tlsConfig.RootCAs = caCertPool
	clickhouse.RegisterTLSConfig(key, tlsConfig)
}

// getConnectionCredentials gives clickhouse credentials
func (ch *HandleT) getConnectionCredentials() credentialsT {
	tlsName := ""
	certificate := warehouseutils.GetConfigValue(caCertificate, ch.Warehouse)
	if len(strings.TrimSpace(certificate)) != 0 {
		// each destination will have separate tls config, hence using destination id as tlsName
		tlsName = ch.Warehouse.Destination.ID
		registerTLSConfig(tlsName, certificate)
	}
	credentials := credentialsT{
		host:          warehouseutils.GetConfigValue(host, ch.Warehouse),
		dbName:        warehouseutils.GetConfigValue(dbName, ch.Warehouse),
		user:          warehouseutils.GetConfigValue(user, ch.Warehouse),
		password:      warehouseutils.GetConfigValue(password, ch.Warehouse),
		port:          warehouseutils.GetConfigValue(port, ch.Warehouse),
		secure:        warehouseutils.GetConfigValueBoolString(secure, ch.Warehouse),
		skipVerify:    warehouseutils.GetConfigValueBoolString(skipVerify, ch.Warehouse),
		tlsConfigName: tlsName,
	}
	return credentials
}

// columnsWithDataTypes creates columns and its datatype into sql format for creating table
func columnsWithDataTypes(tableName string, columns map[string]string, notNullableColumns []string) string {
	var arr []string
	for columnName, dataType := range columns {
		codec := getClickHouseCodecForColumnType(dataType, tableName)
		columnType := getClickHouseColumnTypeForSpecificTable(tableName, columnName, rudderDataTypesMapToClickHouse[dataType], misc.ContainsString(notNullableColumns, columnName))
		arr = append(arr, fmt.Sprintf(`%s %s %s`, columnName, columnType, codec))
	}
	return strings.Join(arr[:], ",")
}

func getClickHouseCodecForColumnType(columnType string, tableName string) string {
	switch columnType {
	case "datetime":
		if disableNullable && !(tableName == warehouseutils.IdentifiesTable || tableName == warehouseutils.UsersTable) {
			return "Codec(DoubleDelta, LZ4)"
		}
	}
	return ""
}

func getClickhouseColumnTypeForSpecificColumn(columnName string, columnType string, isNullable bool) string {
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
func getClickHouseColumnTypeForSpecificTable(tableName string, columnName string, columnType string, notNullableKey bool) string {
	if notNullableKey || (tableName != warehouseutils.IdentifiesTable && disableNullable) {
		return getClickhouseColumnTypeForSpecificColumn(columnName, columnType, false)
	}
	// Nullable is not disabled for users and identity table
	if tableName == warehouseutils.UsersTable {
		return fmt.Sprintf(`SimpleAggregateFunction(anyLast, %s)`, getClickhouseColumnTypeForSpecificColumn(columnName, columnType, true))
	}
	return getClickhouseColumnTypeForSpecificColumn(columnName, columnType, true)
}

// DownloadLoadFiles downloads load files for the tableName and gives file names
func (ch *HandleT) DownloadLoadFiles(tableName string) ([]string, error) {
	pkgLogger.Infof("%s DownloadLoadFiles Started", ch.GetLogIdentifier(tableName))
	defer pkgLogger.Infof("%s DownloadLoadFiles Completed", ch.GetLogIdentifier(tableName))
	objects := ch.Uploader.GetLoadFilesMetadata(warehouseutils.GetLoadFilesOptionsT{Table: tableName})
	storageProvider := warehouseutils.ObjectStorageType(ch.Warehouse.Destination.DestinationDefinition.Name, ch.Warehouse.Destination.Config, ch.Uploader.UseRudderStorage())
	downloader, err := filemanager.New(&filemanager.SettingsT{
		Provider: storageProvider,
		Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
			Provider:         storageProvider,
			Config:           ch.Warehouse.Destination.Config,
			UseRudderStorage: ch.Uploader.UseRudderStorage(),
		}),
	})
	if err != nil {
		pkgLogger.Errorf("%s Error in setting up a downloader with Error: %v", ch.GetLogIdentifier(tableName, downloader.GetProvider()), err)
		return nil, err
	}
	var fileNames []string
	var dErr error
	var fileNamesLock sync.RWMutex

	var jobs = make([]misc.RWCJob, 0)
	for _, object := range objects {
		jobs = append(jobs, object)
	}

	misc.RunWithConcurrency(&misc.RWCConfig{
		Factor: numWorkersDownloadLoadFiles,
		Jobs:   &jobs,
		Run: func(job interface{}) {
			loadFile := job.(warehouseutils.LoadFileT)
			fileName, err := ch.downloadLoadFile(&loadFile, tableName, downloader)
			if err != nil {
				pkgLogger.Errorf("%s Error occurred while downloading fileName: %s, Error: %v", ch.GetLogIdentifier(tableName), fileName, err)
				dErr = err
				return
			}
			fileNamesLock.Lock()
			fileNames = append(fileNames, fileName)
			fileNamesLock.Unlock()
		},
	})
	return fileNames, dErr
}

func (ch *HandleT) downloadLoadFile(object *warehouseutils.LoadFileT, tableName string, downloader filemanager.FileManager) (fileName string, err error) {
	pkgLogger.Debugf("%s DownloadLoadFile Started", ch.GetLogIdentifier(tableName, downloader.GetProvider()))
	defer pkgLogger.Debugf("%s DownloadLoadFile Completed", ch.GetLogIdentifier(tableName, downloader.GetProvider()))

	objectName, err := warehouseutils.GetObjectName(object.Location, ch.Warehouse.Destination.Config, ch.ObjectStorage)
	if err != nil {
		pkgLogger.Errorf("%s Error in converting object location to object key for location: %s, error: %v", ch.GetLogIdentifier(tableName, downloader.GetProvider()), object.Location, err)
		return
	}

	dirName := "/rudder-warehouse-load-uploads-tmp/"
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		pkgLogger.Errorf("%s Error in getting tmp directory for downloading load file for Location: %s, error: %v", ch.GetLogIdentifier(tableName, downloader.GetProvider()), object.Location, err)
		return
	}

	ObjectPath := tmpDirPath + dirName + fmt.Sprintf(`%s_%s_%d/`, ch.Warehouse.Destination.DestinationDefinition.Name, ch.Warehouse.Destination.ID, time.Now().Unix()) + objectName
	err = os.MkdirAll(filepath.Dir(ObjectPath), os.ModePerm)
	if err != nil {
		pkgLogger.Errorf("%s Error in making tmp directory for downloading load file for Location: %s, error: %v", ch.GetLogIdentifier(tableName, downloader.GetProvider()), object.Location, err)
		return
	}

	objectFile, err := os.Create(ObjectPath)
	if err != nil {
		pkgLogger.Errorf("%s Error in creating file in tmp directory for downloading load file for Location: %s, error: %v", ch.GetLogIdentifier(tableName, downloader.GetProvider()), tableName, object.Location, err)
		return
	}

	err = downloader.Download(objectFile, objectName)
	if err != nil {
		pkgLogger.Errorf("%s Error in downloading file in tmp directory for downloading load file for Location: %s, error: %v", ch.GetLogIdentifier(tableName, downloader.GetProvider()), tableName, object.Location, err)
		return
	}
	fileName = objectFile.Name()
	if err = objectFile.Close(); err != nil {
		pkgLogger.Errorf("%s Error in closing downloaded file in tmp directory for downloading load file for Location: %s, error: %v", ch.GetLogIdentifier(tableName, downloader.GetProvider()), tableName, object.Location, err)
		return
	}
	return fileName, err
}

func generateArgumentString(arg string, length int) string {
	var args []string
	for i := 0; i < length; i++ {
		args = append(args, "?")
	}
	return strings.Join(args, ",")
}

func castStringToArray(data string, dataType string) interface{} {
	switch dataType {
	case "array(int)":
		dataInt := make([]int64, 0)
		err := json.Unmarshal([]byte(data), &dataInt)
		if err != nil {
			pkgLogger.Error("Error while unmarshalling data into array of int: %s", err.Error())
		}
		return dataInt
	case "array(float)":
		dataFloat := make([]float64, 0)
		err := json.Unmarshal([]byte(data), &dataFloat)
		if err != nil {
			pkgLogger.Error("Error while unmarshalling data into array of float: %s", err.Error())
		}
		return dataFloat
	case "array(string)":
		dataInterface := make([]interface{}, 0)
		err := json.Unmarshal([]byte(data), &dataInterface)
		if err != nil {
			pkgLogger.Error("Error while unmarshalling data into array of interface: %s", err.Error())
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
			pkgLogger.Error("Error while unmarshalling data into array of date time: %s", err.Error())
		}
		return dataTime
	case "array(boolean)":
		dataBool := make([]bool, 0)
		err := json.Unmarshal([]byte(data), &dataBool)
		if err != nil {
			pkgLogger.Error("Error while unmarshalling data into array of bool: %s", err.Error())
			return dataBool
		}
		dataInt := make([]int32, len(dataBool))
		for _, val := range dataBool {
			if val {
				dataInt = append(dataInt, 1)
			} else {
				dataInt = append(dataInt, 0)
			}
		}
		return dataBool
	}
	return data
}

// typecastDataFromType typeCasts string data to the mentioned data type
func typecastDataFromType(data string, dataType string) interface{} {
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
			dataI = castStringToArray(data, dataType)
		} else {
			return data
		}

	}
	if err != nil {
		if disableNullable {
			return datatypeDefaultValuesMap[dataType]
		}
		return nil
	}
	return dataI
}

// loadTable loads table to clickhouse from the load files
func (ch *HandleT) loadTable(tableName string, tableSchemaInUpload warehouseutils.TableSchemaT) (err error) {
	pkgLogger.Infof("%s LoadTable Started", ch.GetLogIdentifier(tableName))
	defer pkgLogger.Infof("%s LoadTable Completed", ch.GetLogIdentifier(tableName))

	// Clickhouse stats
	chStats := ch.newClickHouseStat(tableName)

	chStats.downloadLoadFilesTime.Start()
	fileNames, err := ch.DownloadLoadFiles(tableName)
	chStats.downloadLoadFilesTime.End()
	if err != nil {
		return
	}
	defer misc.RemoveFilePaths(fileNames...)

	operation := func() error {
		tableError := ch.loadTablesFromFilesNamesWithRetry(tableName, tableSchemaInUpload, fileNames, chStats)
		if !tableError.enableRetry {
			return nil
		}
		return tableError.err
	}

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewConstantBackOff(1*time.Second), uint64(loadTableFailureRetries))
	err = backoff.RetryNotify(operation, backoffWithMaxRetry, func(error error, t time.Duration) {
		err = fmt.Errorf("%s Error occurred while retrying for load tables with error: %v", ch.GetLogIdentifier(tableName), error)
		pkgLogger.Error(err)
		chStats.failRetries.Count(1)
	})
	return
}

type tableError struct {
	enableRetry bool
	err         error
}

func (ch *HandleT) loadTablesFromFilesNamesWithRetry(tableName string, tableSchemaInUpload warehouseutils.TableSchemaT, fileNames []string, chStats *clickHouseStatT) (terr tableError) {
	pkgLogger.Debugf("%s LoadTablesFromFilesNamesWithRetry Started", ch.GetLogIdentifier(tableName))
	defer pkgLogger.Debugf("%s LoadTablesFromFilesNamesWithRetry Completed", ch.GetLogIdentifier(tableName))

	var txn *sql.Tx
	var err error

	onError := func(err error) {
		if txn != nil {
			go func() {
				pkgLogger.Debugf("%s Rollback Started for loading in table", ch.GetLogIdentifier(tableName))
				txn.Rollback()
				pkgLogger.Debugf("%s Rollback Completed for loading in table", ch.GetLogIdentifier(tableName))
			}()
		}
		terr.err = err
		pkgLogger.Errorf("%s OnError for loading in table with error: %v", ch.GetLogIdentifier(tableName), err)
	}

	pkgLogger.Debugf("%s Beginning a transaction in db for loading in table", ch.GetLogIdentifier(tableName))
	txn, err = ch.Db.Begin()
	if err != nil {
		err = fmt.Errorf("%s Error while beginning a transaction in db for loading in table with error:%v", ch.GetLogIdentifier(tableName), err)
		onError(err)
		return
	}
	pkgLogger.Debugf("%s Completed a transaction in db for loading in table", ch.GetLogIdentifier(tableName))

	// sort column names
	sortedColumnKeys := warehouseutils.SortColumnKeysFromColumnMap(tableSchemaInUpload)
	sortedColumnString := strings.Join(sortedColumnKeys, ", ")

	sqlStatement := fmt.Sprintf(`INSERT INTO "%s"."%s" (%v) VALUES (%s)`, ch.Namespace, tableName, sortedColumnString, generateArgumentString("?", len(sortedColumnKeys)))
	pkgLogger.Debugf("%s Preparing statement exec in db for loading in table for query:%s", ch.GetLogIdentifier(tableName), sqlStatement)
	stmt, err := txn.Prepare(sqlStatement)
	if err != nil {
		err = fmt.Errorf("%s Error while preparing statement for transaction in db for loading in table for query:%s error:%v", ch.GetLogIdentifier(tableName), sqlStatement, err)
		onError(err)
		return
	}
	pkgLogger.Debugf("%s Prepared statement exec in db for loading in table", ch.GetLogIdentifier(tableName))

	for _, objectFileName := range fileNames {
		chStats.syncLoadFileTime.Start()

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
			rruntime.Go(func() {
				misc.RemoveFilePaths(objectFileName)
			})
			gzipFile.Close()
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
					pkgLogger.Debugf("%s File reading completed while reading csv file for loading in table for objectFileName:%s", ch.GetLogIdentifier(tableName), objectFileName)
					break
				} else {
					err = fmt.Errorf("%s Error while reading csv file %s for loading in table with error:%v", ch.GetLogIdentifier(tableName), objectFileName, err)
					onError(err)
					return
				}
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
				data := typecastDataFromType(value, columnDataType)
				recordInterface = append(recordInterface, data)
			}

			stmtCtx, stmtCancel := context.WithCancel(context.Background())
			misc.RunWithTimeout(func() {
				pkgLogger.Debugf("%s Starting Prepared statement exec", ch.GetLogIdentifier(tableName))
				chStats.execRowTime.Start()
				_, err = stmt.ExecContext(stmtCtx, recordInterface...)
				chStats.execRowTime.End()
				pkgLogger.Debugf("%s Completed Prepared statement exec", ch.GetLogIdentifier(tableName))
			}, func() {
				pkgLogger.Debugf("%s Cancelling and closing statement", ch.GetLogIdentifier(tableName))
				stmtCancel()
				go func() {
					stmt.Close()
				}()
				err = fmt.Errorf("%s Timed out exec table for objectFileName: %s", ch.GetLogIdentifier(tableName), objectFileName)
				terr.enableRetry = true
				chStats.execTimeouts.Count(1)
			}, execTimeOutInSeconds)

			if err != nil {
				err = fmt.Errorf("%s Error in inserting statement for loading in table with error:%v", ch.GetLogIdentifier(tableName), err)
				onError(err)
				return
			}
			csvRowsProcessedCount++
		}

		chStats.numRowsLoadFile.Count(csvRowsProcessedCount)

		gzipReader.Close()
		gzipFile.Close()

		chStats.syncLoadFileTime.End()
	}

	misc.RunWithTimeout(func() {
		chStats.commitTime.Start()
		defer chStats.commitTime.End()

		pkgLogger.Debugf("%s Committing transaction", ch.GetLogIdentifier(tableName))
		if err = txn.Commit(); err != nil {
			err = fmt.Errorf("%s Error while committing transaction as there was error while loading in table with error:%v", ch.GetLogIdentifier(tableName), err)
			return
		}
		pkgLogger.Debugf("%v Committed transaction", ch.GetLogIdentifier(tableName))
	}, func() {
		err = fmt.Errorf("%s Timed out while committing", ch.GetLogIdentifier(tableName))
		terr.enableRetry = true
		chStats.commitTimeouts.Count(1)
	}, commitTimeOutInSeconds)

	if err != nil {
		err = fmt.Errorf("%s Error occurred while committing with error:%v", ch.GetLogIdentifier(tableName), err)
		onError(err)
		return
	}
	pkgLogger.Infof("%s Completed loading the table", ch.GetLogIdentifier(tableName))
	return
}

func (ch *HandleT) schemaExists(schemaname string) (exists bool, err error) {
	sqlStatement := fmt.Sprintf(`SELECT 1`)
	_, err = ch.Db.Exec(sqlStatement)
	if err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok && exception.Code == 81 {
			pkgLogger.Debugf("CH: No database found while checking for schema: %s from  destination:%v, query: %v", ch.Namespace, ch.Warehouse.Destination.Name, sqlStatement)
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// createSchema creates a database in clickhouse
func (ch *HandleT) createSchema() (err error) {
	var schemaExists bool
	schemaExists, err = ch.schemaExists(ch.Namespace)
	if err != nil {
		pkgLogger.Errorf("CH: Error checking if database: %s exists: %v", ch.Namespace, err)
		return err
	}
	if schemaExists {
		pkgLogger.Infof("CH: Skipping creating database: %s since it already exists", ch.Namespace)
		return
	}
	dbHandle, err := connect(ch.getConnectionCredentials(), false)
	if err != nil {
		return err
	}
	defer dbHandle.Close()
	cluster := warehouseutils.GetConfigValue(cluster, ch.Warehouse)
	clusterClause := ""
	if len(strings.TrimSpace(cluster)) > 0 {
		clusterClause = fmt.Sprintf(`ON CLUSTER "%s"`, cluster)
	}
	sqlStatement := fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS "%s" %s`, ch.Namespace, clusterClause)
	pkgLogger.Infof("CH: Creating database in clickhouse for ch:%s : %v", ch.Warehouse.Destination.ID, sqlStatement)
	_, err = dbHandle.Exec(sqlStatement)
	return
}

/*
 createUsersTable creates a users table with engine AggregatingMergeTree,
 this lets us choose aggregation logic before merging records with same user id.
 current behaviour is to replace user  properties with latest non null values
*/
func (ch *HandleT) createUsersTable(name string, columns map[string]string) (err error) {
	sortKeyFields := []string{"id"}
	notNullableColumns := []string{"received_at", "id"}
	clusterClause := ""
	engine := "AggregatingMergeTree"
	engineOptions := ""
	cluster := warehouseutils.GetConfigValue(cluster, ch.Warehouse)
	if len(strings.TrimSpace(cluster)) > 0 {
		clusterClause = fmt.Sprintf(`ON CLUSTER "%s"`, cluster)
		engine = fmt.Sprintf(`%s%s`, "Replicated", engine)
		engineOptions = `'/clickhouse/{cluster}/tables/{database}/{table}', '{replica}'`
	}
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s"."%s" %s ( %v )  ENGINE = %s(%s) ORDER BY %s PARTITION BY toDate(%s)`, ch.Namespace, name, clusterClause, columnsWithDataTypes(name, columns, notNullableColumns), engine, engineOptions, getSortKeyTuple(sortKeyFields), partitionField)
	pkgLogger.Infof("CH: Creating table in clickhouse for ch:%s : %v", ch.Warehouse.Destination.ID, sqlStatement)
	_, err = ch.Db.Exec(sqlStatement)
	return
}

func getSortKeyTuple(sortKeyFields []string) string {
	tuple := "("
	for index, field := range sortKeyFields {
		if index == len(sortKeyFields)-1 {
			tuple += fmt.Sprintf(`"%s"`, field)
		} else {
			tuple += fmt.Sprintf(`"%s",`, field)
		}

	}
	tuple += ")"
	return tuple
}

// createTable creates table with engine ReplacingMergeTree(), this is used for dedupe event data and replace it will latest data if duplicate data found. This logic is handled by clickhouse
// The engine differs from MergeTree in that it removes duplicate entries with the same sorting key value.
func (ch *HandleT) CreateTable(tableName string, columns map[string]string) (err error) {
	sortKeyFields := []string{"received_at", "id"}
	if tableName == warehouseutils.DiscardsTable {
		sortKeyFields = []string{"received_at"}
	}
	var sqlStatement string
	if tableName == warehouseutils.UsersTable {
		return ch.createUsersTable(tableName, columns)
	}
	clusterClause := ""
	engine := "ReplacingMergeTree"
	engineOptions := ""
	cluster := warehouseutils.GetConfigValue(cluster, ch.Warehouse)
	if len(strings.TrimSpace(cluster)) > 0 {
		clusterClause = fmt.Sprintf(`ON CLUSTER "%s"`, cluster)
		engine = fmt.Sprintf(`%s%s`, "Replicated", engine)
		engineOptions = `'/clickhouse/{cluster}/tables/{database}/{table}', '{replica}'`
	}
	sqlStatement = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s"."%s" %s ( %v )  ENGINE = %s(%s) ORDER BY %s PARTITION BY toDate(%s)`, ch.Namespace, tableName, clusterClause, columnsWithDataTypes(tableName, columns, sortKeyFields), engine, engineOptions, getSortKeyTuple(sortKeyFields), partitionField)

	pkgLogger.Infof("CH: Creating table in clickhouse for ch:%s : %v", ch.Warehouse.Destination.ID, sqlStatement)
	_, err = ch.Db.Exec(sqlStatement)
	return
}

// AddColumn adds column:columnName with dataType columnType to the tableName
func (ch *HandleT) AddColumn(tableName string, columnName string, columnType string) (err error) {
	sqlStatement := fmt.Sprintf(`ALTER TABLE "%s"."%s" ADD COLUMN IF NOT EXISTS %s %s`, ch.Namespace, tableName, columnName, getClickHouseColumnTypeForSpecificTable(tableName, columnName, rudderDataTypesMapToClickHouse[columnType], false))
	pkgLogger.Infof("CH: Adding column in clickhouse for ch:%s : %v", ch.Warehouse.Destination.ID, sqlStatement)
	_, err = ch.Db.Exec(sqlStatement)
	return
}

func (ch *HandleT) CreateSchema() (err error) {
	if len(ch.Uploader.GetSchemaInWarehouse()) > 0 {
		return nil
	}
	err = ch.createSchema()
	return err
}

func (ch *HandleT) AlterColumn(tableName string, columnName string, columnType string) (err error) {
	return
}

// TestConnection is used destination connection tester to test the clickhouse connection
func (ch *HandleT) TestConnection(warehouse warehouseutils.WarehouseT) (err error) {
	ch.Warehouse = warehouse
	ch.Db, err = connect(ch.getConnectionCredentials(), true)
	if err != nil {
		return
	}
	defer ch.Db.Close()

	timeOut := 5 * time.Second

	ctx, cancel := context.WithTimeout(context.TODO(), timeOut)
	defer cancel()

	err = ch.Db.PingContext(ctx)
	if err == context.DeadlineExceeded {
		return fmt.Errorf("connection testing timed out after %d sec", timeOut/time.Second)
	}
	if err != nil {
		return err
	}

	return nil

}

func (ch *HandleT) Setup(warehouse warehouseutils.WarehouseT, uploader warehouseutils.UploaderI) (err error) {
	ch.Warehouse = warehouse
	ch.Namespace = warehouse.Namespace
	ch.Uploader = uploader
	ch.stats = stats.DefaultStats
	ch.ObjectStorage = warehouseutils.ObjectStorageType(warehouseutils.CLICKHOUSE, warehouse.Destination.Config, ch.Uploader.UseRudderStorage())

	ch.Db, err = connect(ch.getConnectionCredentials(), true)
	return err
}

func (ch *HandleT) CrashRecover(warehouse warehouseutils.WarehouseT) (err error) {
	return
}

// FetchSchema queries clickhouse and returns the schema associated with provided namespace
func (ch *HandleT) FetchSchema(warehouse warehouseutils.WarehouseT) (schema warehouseutils.SchemaT, err error) {
	ch.Warehouse = warehouse
	ch.Namespace = warehouse.Namespace
	dbHandle, err := connect(ch.getConnectionCredentials(), true)
	if err != nil {
		return
	}
	defer dbHandle.Close()

	schema = make(warehouseutils.SchemaT)
	sqlStatement := fmt.Sprintf(`select table, name, type
									from system.columns
									where database = '%s'`, ch.Namespace)

	rows, err := dbHandle.Query(sqlStatement)
	if err != nil && err != sql.ErrNoRows {
		if exception, ok := err.(*clickhouse.Exception); ok && exception.Code == 81 {
			pkgLogger.Infof("CH: No database found while fetching schema: %s from  destination:%v, query: %v", ch.Namespace, ch.Warehouse.Destination.Name, sqlStatement)
			return schema, nil
		}
		pkgLogger.Errorf("CH: Error in fetching schema from clickhouse destination:%v, query: %v", ch.Warehouse.Destination.ID, sqlStatement)
		return
	}
	if err == sql.ErrNoRows {
		pkgLogger.Infof("CH: No rows, while fetching schema: %s from destination:%v, query: %v", ch.Namespace, ch.Warehouse.Destination.Name, sqlStatement)
		return schema, nil
	}
	defer rows.Close()
	for rows.Next() {
		var tName, cName, cType string
		err = rows.Scan(&tName, &cName, &cType)
		if err != nil {
			pkgLogger.Errorf("CH: Error in processing fetched schema from clickhouse destination:%v", ch.Warehouse.Destination.ID)
			return
		}
		if _, ok := schema[tName]; !ok {
			schema[tName] = make(map[string]string)
		}
		if datatype, ok := clickhouseDataTypesMapToRudder[cType]; ok {
			schema[tName][cName] = datatype
		}
	}
	return
}

func (ch *HandleT) LoadUserTables() (errorMap map[string]error) {
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

func (ch *HandleT) LoadTable(tableName string) error {
	err := ch.loadTable(tableName, ch.Uploader.GetTableSchemaInUpload(tableName))
	return err
}

func (ch *HandleT) Cleanup() {
	if ch.Db != nil {
		ch.Db.Close()
	}
}

func (ch *HandleT) LoadIdentityMergeRulesTable() (err error) {
	return
}

func (ch *HandleT) LoadIdentityMappingsTable() (err error) {
	return
}

func (ch *HandleT) DownloadIdentityRules(*misc.GZipWriter) (err error) {
	return
}

func (ch *HandleT) IsEmpty(warehouse warehouseutils.WarehouseT) (empty bool, err error) {
	return
}

func (ch *HandleT) GetTotalCountInTable(tableName string) (total int64, err error) {
	sqlStatement := fmt.Sprintf(`SELECT count(*) FROM "%[1]s"."%[2]s"`, ch.Namespace, tableName)
	err = ch.Db.QueryRow(sqlStatement).Scan(&total)
	if err != nil {
		pkgLogger.Errorf(`CH: Error getting total count in table %s:%s`, ch.Namespace, tableName)
	}
	return
}

func (ch *HandleT) Connect(warehouse warehouseutils.WarehouseT) (client.Client, error) {
	ch.Warehouse = warehouse
	ch.Namespace = warehouse.Namespace
	dbHandle, err := connect(ch.getConnectionCredentials(), true)
	if err != nil {
		return client.Client{}, err
	}

	return client.Client{Type: client.SQLClient, SQL: dbHandle}, err
}

func (ch *HandleT) GetLogIdentifier(args ...string) string {
	if len(args) == 0 {
		return fmt.Sprintf("[%s][%s]", ch.Warehouse.Identifier, ch.Warehouse.Namespace)
	}
	return fmt.Sprintf("[%s][%s][%s]", ch.Warehouse.Identifier, ch.Warehouse.Namespace, strings.Join(args, "]["))
}
