package clickhouse

import (
	"compress/gzip"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	stagingTablePrefix string
	maxParallelLoads   int
	queryDebugLogs     string
	blockSize          string
	poolSize           string
)

const (
	host          = "host"
	dbName        = "database"
	user          = "user"
	password      = "password"
	port          = "port"
	secure        = "secure"
	skipVerify    = "skipVerify"
	caCertificate = "caCertificate"
)
const partitionField = "received_at"

// clickhouse doesnt support bool, they recommend to use Uint8 and set 1,0

var rudderDataTypesMapToClickHouse = map[string]string{
	"int":      "Int64",
	"float":    "Float64",
	"string":   "String",
	"datetime": "DateTime",
	"boolean":  "UInt8",
}

var clickhouseDataTypesMapToRudder = map[string]string{
	"Int8":               "int",
	"Int16":              "int",
	"Int32":              "int",
	"Int64":              "int",
	"Float32":            "float",
	"Float64":            "float",
	"String":             "string",
	"DateTime":           "datetime",
	"UInt8":              "boolean",
	"Nullable(Int8)":     "int",
	"Nullable(Int16)":    "int",
	"Nullable(Int32)":    "int",
	"Nullable(Int64)":    "int",
	"Nullable(Float32)":  "float",
	"Nullable(Float64)":  "float",
	"Nullable(String)":   "string",
	"Nullable(DateTime)": "datetime",
	"Nullable(UInt8)":    "boolean",
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
	DbHandle      *sql.DB
	Db            *sql.DB
	Namespace     string
	CurrentSchema map[string]map[string]string
	Warehouse     warehouseutils.WarehouseT
	Upload        warehouseutils.UploadT
	ObjectStorage string
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

var primaryKeyMap = map[string]string{
	warehouseutils.UsersTable:      "id",
	warehouseutils.IdentifiesTable: "id",
	warehouseutils.DiscardsTable:   "row_id",
}

// connect connects to warehouse with provided credentials
func connect(cred credentialsT) (*sql.DB, error) {
	url := fmt.Sprintf("tcp://%s:%s?&username=%s&password=%s&database=%s&block_size=%s&pool_size=%s&debug=%s&secure=%s&skip_verify=%s&tls_config=%s",
		cred.host,
		cred.port,
		cred.user,
		cred.password,
		cred.dbName,
		blockSize,
		poolSize,
		queryDebugLogs,
		cred.secure,
		cred.skipVerify,
		cred.tlsConfigName,
	)

	var err error
	var db *sql.DB

	if db, err = sql.Open("clickhouse", url); err != nil {
		return nil, fmt.Errorf("clickhouse connection error : (%v)", err)
	}
	return db, nil
}

func init() {
	loadConfig()
}

func loadConfig() {
	stagingTablePrefix = "rudder_staging_"
	maxParallelLoads = config.GetInt("Warehouse.clickhouse.maxParallelLoads", 3)
	queryDebugLogs = config.GetString("Warehouse.clickhouse.queryDebugLogs", "false")
	blockSize = config.GetString("Warehouse.clickhouse.blockSize", "1000")
	poolSize = config.GetString("Warehouse.clickhouse.poolSize", "10")

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
		if misc.ContainsString(notNullableColumns, columnName) {
			arr = append(arr, fmt.Sprintf(`%s %s`, columnName, getClickHouseColumnTypeForSpecificTable(tableName, rudderDataTypesMapToClickHouse[dataType], true)))
		} else {
			arr = append(arr, fmt.Sprintf(`%s %s`, columnName, getClickHouseColumnTypeForSpecificTable(tableName, rudderDataTypesMapToClickHouse[dataType], false)))
		}

	}
	return strings.Join(arr[:], ",")
}

// getClickHouseColumnTypeForSpecificTable gets suitable columnType based on the tableName
func getClickHouseColumnTypeForSpecificTable(tableName string, columnType string, notNullableKey bool) string {
	if notNullableKey {
		return columnType
	}
	if tableName == warehouseutils.UsersTable {
		return fmt.Sprintf(`SimpleAggregateFunction(anyLast, Nullable(%s))`, columnType)
	}
	return fmt.Sprintf(`Nullable(%s)`, columnType)
}

func (ch *HandleT) CrashRecover(config warehouseutils.ConfigT) (err error) {
	return
}

// FetchSchema queries clickhouse and returns the schema associated with provided namespace
func (ch *HandleT) FetchSchema(warehouse warehouseutils.WarehouseT, namespace string) (schema map[string]map[string]string, err error) {
	ch.Warehouse = warehouse
	dbHandle, err := connect(ch.getConnectionCredentials())
	if err != nil {
		return
	}
	defer dbHandle.Close()
	schema = make(map[string]map[string]string)
	sqlStatement := fmt.Sprintf(`select table, name, type
									from system.columns
									where database = '%s'`, namespace)

	rows, err := dbHandle.Query(sqlStatement)
	if err != nil && err != sql.ErrNoRows {
		logger.Errorf("CH: Error in fetching schema from clickhouse destination:%v, query: %v", ch.Warehouse.Destination.ID, sqlStatement)
		return
	}
	if err == sql.ErrNoRows {
		return schema, nil
	}
	defer rows.Close()
	for rows.Next() {
		var tName, cName, cType string
		err = rows.Scan(&tName, &cName, &cType)
		if err != nil {
			logger.Errorf("CH: Error in processing fetched schema from clickhouse destination:%v", ch.Warehouse.Destination.ID)
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

// Export starts exporting data to clickhouse
func (ch *HandleT) Export() (err error) {
	logger.Infof("CH: Starting export to clickhouse for source:%s and wh_upload:%v", ch.Warehouse.Source.ID, ch.Upload.ID)
	err = warehouseutils.SetUploadStatus(ch.Upload, warehouseutils.ExportingDataState, ch.DbHandle)
	if err != nil {
		panic(err)
	}
	timer := warehouseutils.DestStat(stats.TimerType, "upload_time", ch.Warehouse.Destination.ID)
	timer.Start()
	errList := ch.load()
	timer.End()
	if len(errList) > 0 {
		errStr := ""
		for idx, err := range errList {
			errStr += err.Error()
			if idx < len(errList)-1 {
				errStr += ", "
			}
		}
		warehouseutils.SetUploadError(ch.Upload, errors.New(errStr), warehouseutils.ExportingDataFailedState, ch.DbHandle)
		return errors.New(errStr)
	}
	err = warehouseutils.SetUploadStatus(ch.Upload, warehouseutils.ExportedDataState, ch.DbHandle)
	if err != nil {
		panic(err)
	}
	return
}

// DownloadLoadFiles downloads load files for the tableName and gives file names
func (ch *HandleT) DownloadLoadFiles(tableName string) ([]string, error) {
	objectLocations, _ := warehouseutils.GetLoadFileLocations(ch.DbHandle, ch.Warehouse.Source.ID, ch.Warehouse.Destination.ID, tableName, ch.Upload.StartLoadFileID, ch.Upload.EndLoadFileID)
	var fileNames []string
	for _, objectLocation := range objectLocations {
		object, err := warehouseutils.GetObjectName(ch.Warehouse.Destination.Config, objectLocation)
		if err != nil {
			logger.Errorf("CH: Error in converting object location to object key for table:%s: %s,%v", tableName, objectLocation, err)
			return nil, err
		}
		dirName := "/rudder-warehouse-load-uploads-tmp/"
		tmpDirPath, err := misc.CreateTMPDIR()
		if err != nil {
			logger.Errorf("CH: Error in getting tmp directory for downloading load file for table:%s: %s, %v", tableName, objectLocation, err)
			return nil, err
		}
		ObjectPath := tmpDirPath + dirName + fmt.Sprintf(`%s_%s_%d/`, ch.Warehouse.Destination.DestinationDefinition.Name, ch.Warehouse.Destination.ID, time.Now().Unix()) + object
		err = os.MkdirAll(filepath.Dir(ObjectPath), os.ModePerm)
		if err != nil {
			logger.Errorf("CH: Error in making tmp directory for downloading load file for table:%s: %s, %s %v", tableName, objectLocation, err)
			return nil, err
		}
		objectFile, err := os.Create(ObjectPath)
		if err != nil {
			logger.Errorf("CH: Error in creating file in tmp directory for downloading load file for table:%s: %s, %v", tableName, objectLocation, err)
			return nil, err
		}
		downloader, err := filemanager.New(&filemanager.SettingsT{
			Provider: warehouseutils.ObjectStorageType(ch.Warehouse.Destination.DestinationDefinition.Name, ch.Warehouse.Destination.Config),
			Config:   ch.Warehouse.Destination.Config,
		})
		err = downloader.Download(objectFile, object)
		if err != nil {
			logger.Errorf("CH: Error in downloading file in tmp directory for downloading load file for table:%s: %s, %v", tableName, objectLocation, err)
			return nil, err
		}
		fileName := objectFile.Name()
		if err = objectFile.Close(); err != nil {
			logger.Errorf("CH: Error in closing downloaded file in tmp directory for downloading load file for table:%s: %s, %v", tableName, objectLocation, err)
			return nil, err
		}
		fileNames = append(fileNames, fileName)
	}
	return fileNames, nil

}
func generateArgumentString(arg string, length int) string {
	var args []string
	for i := 0; i < length; i++ {
		args = append(args, "?")
	}
	return strings.Join(args, ",")
}

// typeCastDataFromType typeCasts string data to the mentioned data type
func typeCastDataFromType(data string, datatype string) interface{} {
	switch datatype {
	case "int":
		i, err := strconv.Atoi(data)
		if err != nil {
			return nil
		}
		return i
	case "float":
		f, err := strconv.ParseFloat(data, 64)
		if err != nil {
			return nil
		}
		return f
	case "datetime":
		t, err := time.Parse(time.RFC3339, data)
		if err != nil {
			return ""
		}
		return t
	case "boolean":
		b, err := strconv.ParseBool(data)
		if err != nil {
			return nil
		}
		if b {
			return 1
		}
		return 0
	}
	return data
}

// loadTable loads table to clickhouse from the load files
func (ch *HandleT) loadTable(tableName string, columnMap map[string]string) (err error) {
	status, _ := warehouseutils.GetTableUploadStatus(ch.Upload.ID, tableName, ch.DbHandle)
	if status == warehouseutils.ExportedDataState {
		logger.Infof("CH: Skipping load for table:%s as it has been successfully loaded earlier", tableName)
		return
	}

	if !warehouseutils.HasLoadFiles(ch.DbHandle, ch.Warehouse.Source.ID, ch.Warehouse.Destination.ID, tableName, ch.Upload.StartLoadFileID, ch.Upload.EndLoadFileID) {
		warehouseutils.SetTableUploadStatus(warehouseutils.ExportedDataState, ch.Upload.ID, tableName, ch.DbHandle)
		return
	}
	logger.Infof("CH: Starting load for table:%s", tableName)
	warehouseutils.SetTableUploadStatus(warehouseutils.ExecutingState, ch.Upload.ID, tableName, ch.DbHandle)
	timer := warehouseutils.DestStat(stats.TimerType, "single_table_upload_time", ch.Warehouse.Destination.ID)
	timer.Start()
	defer timer.End()

	// sort column names
	sortedColumnKeys := warehouseutils.SortColumnKeysFromColumnMap(columnMap)
	sortedColumnString := strings.Join(sortedColumnKeys, ", ")

	fileNames, err := ch.DownloadLoadFiles(tableName)
	if err != nil {
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, ch.Upload.ID, tableName, err, ch.DbHandle)
		return
	}
	defer misc.RemoveFilePaths(fileNames...)

	txn, err := ch.Db.Begin()
	if err != nil {
		logger.Errorf("CH: Error while beginning a transaction in db for loading in table:%s: %v", tableName, err)
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, ch.Upload.ID, tableName, err, ch.DbHandle)
		return
	}

	stmt, err := txn.Prepare(fmt.Sprintf(`INSERT INTO "%s"."%s" (%v) VALUES (%s)`, ch.Namespace, tableName, sortedColumnString, generateArgumentString("?", len(sortedColumnKeys))))
	if err != nil {
		logger.Errorf("CH: Error while preparing statement for  transaction in db for loading in  table:%s: %v", tableName, err)
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, ch.Upload.ID, tableName, err, ch.DbHandle)
		return
	}

	for _, objectFileName := range fileNames {
		var gzipFile *os.File
		gzipFile, err = os.Open(objectFileName)
		if err != nil {
			logger.Errorf("CH: Error opening file using os.Open for file:%s while loading to table %s", objectFileName, tableName)
			warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, ch.Upload.ID, tableName, err, ch.DbHandle)
			return
		}

		var gzipReader *gzip.Reader
		gzipReader, err = gzip.NewReader(gzipFile)
		if err != nil {
			logger.Errorf("CH: Error reading file using gzip.NewReader for file:%s while loading to table %s", gzipFile, tableName)
			rruntime.Go(func() {
				misc.RemoveFilePaths(objectFileName)
			})
			warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, ch.Upload.ID, tableName, err, ch.DbHandle)
			gzipFile.Close()
			return

		}
		csvReader := csv.NewReader(gzipReader)
		for {
			var record []string
			record, err = csvReader.Read()
			if err != nil {
				if err == io.EOF {
					logger.Infof("CH: File reading completed while reading csv file for loading in table:%s: %s", tableName, objectFileName)
					break
				} else {
					logger.Errorf("CH: Error while reading csv file for loading in table:%s: %v", tableName, err)
					warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, ch.Upload.ID, tableName, err, ch.DbHandle)
					txn.Rollback()
					return
				}
			}
			var recordInterface []interface{}
			for index, value := range record {
				columnName := sortedColumnKeys[index]
				columnDataType := columnMap[columnName]
				data := typeCastDataFromType(value, columnDataType)
				recordInterface = append(recordInterface, data)

			}

			_, err = stmt.Exec(recordInterface...)
			if err != nil {
				logger.Errorf("CH: Error in exec statement for loading in table:%s: %v", tableName, err)
				warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, ch.Upload.ID, tableName, err, ch.DbHandle)
				txn.Rollback()
				return
			}

		}
		gzipReader.Close()
		gzipFile.Close()
	}
	if err = txn.Commit(); err != nil {
		logger.Errorf("CH: Error while committing transaction as there was error while loading in table:%s: %v", tableName, err)
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, ch.Upload.ID, tableName, err, ch.DbHandle)
		return
	}
	warehouseutils.SetTableUploadStatus(warehouseutils.ExportedDataState, ch.Upload.ID, tableName, ch.DbHandle)
	logger.Infof("CH: Complete load for table:%s", tableName)
	return
}

// load tables into clickhouse
func (ch *HandleT) load() (errList []error) {
	logger.Infof("CH: Starting load for all %v tables\n", len(ch.Upload.Schema))
	var wg sync.WaitGroup
	loadChan := make(chan struct{}, maxParallelLoads)
	wg.Add(len(ch.Upload.Schema))
	for tableName, columnMap := range ch.Upload.Schema {
		tName := tableName
		cMap := columnMap
		loadChan <- struct{}{}
		rruntime.Go(func() {
			loadError := ch.loadTable(tName, cMap)
			if loadError != nil {
				errList = append(errList, loadError)
			}
			wg.Done()
			<-loadChan
		})
	}
	wg.Wait()
	logger.Infof("CH: Completed load for all tables\n")
	return
}

// createSchema creates a database in clickhouse
func (ch *HandleT) createSchema() (err error) {
	sqlStatement := fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS "%s"`, ch.Namespace)
	logger.Infof("CH: Creating database in clickhouse for ch:%s : %v", ch.Warehouse.Destination.ID, sqlStatement)
	_, err = ch.Db.Exec(sqlStatement)
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
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s"."%s" ( %v ) ENGINE = AggregatingMergeTree() ORDER BY %s PARTITION BY toDate(%s)`, ch.Namespace, name, columnsWithDataTypes(name, columns, notNullableColumns), getSortKeyTuple(sortKeyFields), partitionField)
	logger.Infof("CH: Creating table in clickhouse for ch:%s : %v", ch.Warehouse.Destination.ID, sqlStatement)
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
func (ch *HandleT) createTable(tableName string, columns map[string]string) (err error) {
	sortKeyFields := []string{"received_at", "id"}
	if tableName == warehouseutils.DiscardsTable {
		sortKeyFields = []string{"received_at"}
	}
	var sqlStatement string
	if tableName == warehouseutils.UsersTable {
		return ch.createUsersTable(tableName, columns)
	}
	sqlStatement = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s"."%s" ( %v ) ENGINE = ReplacingMergeTree() ORDER BY %s PARTITION BY toDate(%s)`, ch.Namespace, tableName, columnsWithDataTypes(tableName, columns, sortKeyFields), getSortKeyTuple(sortKeyFields), partitionField)

	logger.Infof("CH: Creating table in clickhouse for ch:%s : %v", ch.Warehouse.Destination.ID, sqlStatement)
	_, err = ch.Db.Exec(sqlStatement)
	return
}

// tableExists will check if tableName exists in the current namespace which is the database name
func (ch *HandleT) tableExists(tableName string) (exists bool, err error) {
	sqlStatement := fmt.Sprintf(` EXISTS TABLE "%s"."%s"`, ch.Namespace, tableName)
	err = ch.Db.QueryRow(sqlStatement).Scan(&exists)
	return
}

// addColumn adds column:columnName with dataType columnType to the tableName
func (ch *HandleT) addColumn(tableName string, columnName string, columnType string) (err error) {
	sqlStatement := fmt.Sprintf(`ALTER TABLE "%s"."%s" ADD COLUMN IF NOT EXISTS %s %s`, ch.Namespace, tableName, columnName, getClickHouseColumnTypeForSpecificTable(tableName, rudderDataTypesMapToClickHouse[columnType], false))
	logger.Infof("CH: Adding column in clickhouse for ch:%s : %v", ch.Warehouse.Destination.ID, sqlStatement)
	_, err = ch.Db.Exec(sqlStatement)
	return
}

func (ch *HandleT) updateSchema() (updatedSchema map[string]map[string]string, err error) {
	diff := warehouseutils.GetSchemaDiff(ch.CurrentSchema, ch.Upload.Schema)
	updatedSchema = diff.UpdatedSchema
	if len(ch.CurrentSchema) == 0 {
		err = ch.createSchema()
		if err != nil {
			return nil, err
		}
	}

	processedTables := make(map[string]bool)
	for _, tableName := range diff.Tables {
		tableExists, err := ch.tableExists(tableName)
		if err != nil {
			return nil, err
		}
		if !tableExists {
			err = ch.createTable(fmt.Sprintf(`%s`, tableName), diff.ColumnMaps[tableName])
			if err != nil {
				return nil, err
			}
			processedTables[tableName] = true
		}
	}
	for tableName, columnMap := range diff.ColumnMaps {
		// skip adding columns when table didn't exist previously and was created in the prev statement
		// this to make sure all columns in the the columnMap exists in the table in snowflake
		if _, ok := processedTables[tableName]; ok {
			continue
		}
		if len(columnMap) > 0 {
			for columnName, columnType := range columnMap {
				err := ch.addColumn(tableName, columnName, columnType)
				if err != nil {
					logger.Errorf("CH: Column %s already exists on %s.%s \nResponse: %v", columnName, ch.Namespace, tableName, err)
					return nil, err
				}
			}
		}
	}
	return
}

// MigrateSchema will handle
func (ch *HandleT) MigrateSchema() (err error) {
	timer := warehouseutils.DestStat(stats.TimerType, "migrate_schema_time", ch.Warehouse.Destination.ID)
	timer.Start()
	warehouseutils.SetUploadStatus(ch.Upload, warehouseutils.UpdatingSchemaState, ch.DbHandle)
	logger.Infof("CH: Updating schema for clickhouse schema name: %s", ch.Namespace)
	updatedSchema, err := ch.updateSchema()
	if err != nil {
		warehouseutils.SetUploadError(ch.Upload, err, warehouseutils.UpdatingSchemaFailedState, ch.DbHandle)
		return
	}
	ch.CurrentSchema = updatedSchema
	err = warehouseutils.SetUploadStatus(ch.Upload, warehouseutils.UpdatedSchemaState, ch.DbHandle)
	if err != nil {
		panic(err)
	}
	err = warehouseutils.UpdateCurrentSchema(ch.Namespace, ch.Warehouse, ch.Upload.ID, updatedSchema, ch.DbHandle)
	timer.End()
	if err != nil {
		warehouseutils.SetUploadError(ch.Upload, err, warehouseutils.UpdatingSchemaFailedState, ch.DbHandle)
		return
	}
	return
}

// Process starts processing to export to clickhouse
func (ch *HandleT) Process(config warehouseutils.ConfigT) (err error) {
	ch.DbHandle = config.DbHandle
	ch.Warehouse = config.Warehouse
	ch.Upload = config.Upload
	ch.ObjectStorage = warehouseutils.ObjectStorageType(warehouseutils.CLICKHOUSE, config.Warehouse.Destination.Config)

	currSchema, err := warehouseutils.GetCurrentSchema(ch.DbHandle, ch.Warehouse)
	if err != nil {
		panic(err)
	}
	ch.CurrentSchema = currSchema
	ch.Namespace = ch.Upload.Namespace

	ch.Db, err = connect(ch.getConnectionCredentials())
	if err != nil {
		warehouseutils.SetUploadError(ch.Upload, err, warehouseutils.UpdatingSchemaFailedState, ch.DbHandle)
		return err
	}
	defer ch.Db.Close()
	if err = ch.MigrateSchema(); err == nil {
		ch.Export()
	}
	return
}

// TestConnection is used destination connection tester to test the clickhouse connection
func (ch *HandleT) TestConnection(config warehouseutils.ConfigT) (err error) {
	ch.Warehouse = config.Warehouse
	ch.Db, err = connect(ch.getConnectionCredentials())
	if err != nil {
		return
	}
	defer ch.Db.Close()
	pingResultChannel := make(chan error, 1)
	rruntime.Go(func() {
		pingResultChannel <- ch.Db.Ping()
	})
	var timeOut time.Duration = 5
	select {
	case err = <-pingResultChannel:
	case <-time.After(timeOut * time.Second):
		err = errors.New(fmt.Sprintf("connection testing timed out after %v sec", timeOut))
	}
	return
}
