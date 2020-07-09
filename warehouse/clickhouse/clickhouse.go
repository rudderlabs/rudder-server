package clickhouse

import (
	"compress/gzip"
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

	_ "github.com/ClickHouse/clickhouse-go"
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
)

const (
	host     = "host"
	dbName   = "database"
	user     = "user"
	password = "password"
	port     = "port"
	sslMode  = "sslMode"
)

// clickhouse doesnt support bool, they recommend to use Uint8 and set 1,0

var rudderDataTypesMapToClickHouse = map[string]string{
	"int":      "Int64",
	"float":    "Float64",
	"string":   "String",
	"datetime": "DateTime",
	"boolean":  "UInt8",
}

//TODO: add addition clickhouse types which maps to rudder transformer data types
var clickhouseDataTypesMapToRudder = map[string]string{
	"Int64":    "int",
	"Float64":  "float",
	"String":   "string",
	"DateTime": "datetime",
	"boolean":  "boolean",
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
	host     string
	dbName   string
	user     string
	password string
	port     string
	sslMode  string
}

var primaryKeyMap = map[string]string{
	warehouseutils.UsersTable:      "id",
	warehouseutils.IdentifiesTable: "id",
	warehouseutils.DiscardsTable:   "row_id",
}
var partitionKeyMap = map[string]string{
	warehouseutils.UsersTable:      "id",
	warehouseutils.IdentifiesTable: "id",
	warehouseutils.DiscardsTable:   "row_id, column_name, table_name",
}

func connect(cred credentialsT) (*sql.DB, error) {
	url := fmt.Sprintf("tcp://%s:%s?debug=true&username=%s&password=%s&database=%s",
		cred.host,
		cred.port,
		cred.user,
		cred.password,
		cred.dbName,
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
}

func (ch *HandleT) getConnectionCredentials() credentialsT {
	return credentialsT{
		host:     warehouseutils.GetConfigValue(host, ch.Warehouse),
		dbName:   warehouseutils.GetConfigValue(dbName, ch.Warehouse),
		user:     warehouseutils.GetConfigValue(user, ch.Warehouse),
		password: warehouseutils.GetConfigValue(password, ch.Warehouse),
		port:     warehouseutils.GetConfigValue(port, ch.Warehouse),
		sslMode:  warehouseutils.GetConfigValue(sslMode, ch.Warehouse),
	}
}

func columnsWithDataTypes(columns map[string]string, prefix string, sortKeyField string) string {
	var arr []string
	for name, dataType := range columns {
		if name == sortKeyField {
			arr = append(arr, fmt.Sprintf(`%s%s %s`, prefix, name, rudderDataTypesMapToClickHouse[dataType]))
		} else {
			arr = append(arr, fmt.Sprintf(`%s%s Nullable(%s)`, prefix, name, rudderDataTypesMapToClickHouse[dataType]))
		}

	}
	return strings.Join(arr[:], ",")
}

func columnsWithDataTypesForUsersTable(columns map[string]string, prefix string, sortKeyField string) string {
	var arr []string
	for name, dataType := range columns {
		if name == sortKeyField {
			arr = append(arr, fmt.Sprintf(`%s%s %s`, prefix, name, rudderDataTypesMapToClickHouse[dataType]))
		} else {
			arr = append(arr, fmt.Sprintf(`%s%s SimpleAggregateFunction(anyLast, Nullable(%s))`, prefix, name, rudderDataTypesMapToClickHouse[dataType]))
		}

	}
	return strings.Join(arr[:], ",")
}

func (ch *HandleT) CrashRecover(config warehouseutils.ConfigT) (err error) {
	return
}

// FetchSchema queries postgres and returns the schema associated with provided namespace
func (ch *HandleT) FetchSchema(warehouse warehouseutils.WarehouseT, namespace string) (schema map[string]map[string]string, err error) {
	ch.Warehouse = warehouse
	ch.Db, err = connect(ch.getConnectionCredentials())
	if err != nil {
		return
	}

	schema = make(map[string]map[string]string)
	sqlStatement := fmt.Sprintf(`select table, name, type
									from system.columns
									where database = '%s'`, namespace)

	rows, err := ch.Db.Query(sqlStatement)
	if err != nil && err != sql.ErrNoRows {
		logger.Errorf("ch: Error in fetching schema from postgres destination:%v, query: %v", ch.Warehouse.Destination.ID, sqlStatement)
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
			logger.Errorf("ch: Error in processing fetched schema from clickhouse destination:%v", ch.Warehouse.Destination.ID)
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

func (ch *HandleT) Export() (err error) {
	logger.Infof("ch: Starting export to clickhouse for source:%s and wh_upload:%v", ch.Warehouse.Source.ID, ch.Upload.ID)
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

func (ch *HandleT) DownloadLoadFiles(tableName string) ([]string, error) {
	objectLocations, _ := warehouseutils.GetLoadFileLocations(ch.DbHandle, ch.Warehouse.Source.ID, ch.Warehouse.Destination.ID, tableName, ch.Upload.StartLoadFileID, ch.Upload.EndLoadFileID)
	var fileNames []string
	for _, objectLocation := range objectLocations {
		object, err := warehouseutils.GetObjectName(ch.Warehouse.Destination.Config, objectLocation)
		if err != nil {
			logger.Errorf("ch: Error in converting object location to object key for table:%s: %s,%v", tableName, objectLocation, err)
			return nil, err
		}
		dirName := "/rudder-warehouse-load-uploads-tmp/"
		tmpDirPath, err := misc.CreateTMPDIR()
		if err != nil {
			logger.Errorf("ch: Error in creating tmp directory for downloading load file for table:%s: %s, %v", tableName, objectLocation, err)
			return nil, err
		}
		ObjectPath := tmpDirPath + dirName + fmt.Sprintf(`%s_%s_%d/`, ch.Warehouse.Destination.DestinationDefinition.Name, ch.Warehouse.Destination.ID, time.Now().Unix()) + object
		err = os.MkdirAll(filepath.Dir(ObjectPath), os.ModePerm)
		if err != nil {
			logger.Errorf("ch: Error in making tmp directory for downloading load file for table:%s: %s, %s %v", tableName, objectLocation, err)
			return nil, err
		}
		objectFile, err := os.Create(ObjectPath)
		if err != nil {
			logger.Errorf("ch: Error in creating file in tmp directory for downloading load file for table:%s: %s, %v", tableName, objectLocation, err)
			return nil, err
		}
		downloader, err := filemanager.New(&filemanager.SettingsT{
			Provider: warehouseutils.ObjectStorageType(ch.Warehouse.Destination.DestinationDefinition.Name, ch.Warehouse.Destination.Config),
			Config:   ch.Warehouse.Destination.Config,
		})
		err = downloader.Download(objectFile, object)
		if err != nil {
			logger.Errorf("ch: Error in downloading file in tmp directory for downloading load file for table:%s: %s, %v", tableName, objectLocation, err)
			return nil, err
		}
		fileName := objectFile.Name()
		if err = objectFile.Close(); err != nil {
			logger.Errorf("ch: Error in closing downloaded file in tmp directory for downloading load file for table:%s: %s, %v", tableName, objectLocation, err)
			return nil, err
		}
		fileNames = append(fileNames, fileName)
	}
	return fileNames, nil

}
func generateArgumentString(arg string, length int) string {
	var argString string
	for i := 0; i < length; i++ {
		if i == length-1 {
			argString += arg
		} else {
			argString += arg + ","
		}

	}
	return argString
}

func getDataFromType(data string, datatype string) interface{} {
	switch datatype {
	case "int":
		i, err := strconv.Atoi(data)
		if err != nil {
			return ""
		}
		return i
	case "float":
		f, err := strconv.ParseFloat(data, 64)
		if err != nil {
			return ""
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
			return ""
		}
		if b {
			return 1
		}
		return 0
	}
	return data
}

func (ch *HandleT) loadTable(tableName string, columnMap map[string]string, forceLoad bool) (stagingTableName string, err error) {
	if !forceLoad {
		status, _ := warehouseutils.GetTableUploadStatus(ch.Upload.ID, tableName, ch.DbHandle)
		if status == warehouseutils.ExportedDataState {
			logger.Infof("ch: Skipping load for table:%s as it has been successfully loaded earlier", tableName)
			return
		}
	}
	if !warehouseutils.HasLoadFiles(ch.DbHandle, ch.Warehouse.Source.ID, ch.Warehouse.Destination.ID, tableName, ch.Upload.StartLoadFileID, ch.Upload.EndLoadFileID) {
		warehouseutils.SetTableUploadStatus(warehouseutils.ExportedDataState, ch.Upload.ID, tableName, ch.DbHandle)
		return
	}
	logger.Infof("ch: Starting load for table:%s", tableName)
	warehouseutils.SetTableUploadStatus(warehouseutils.ExecutingState, ch.Upload.ID, tableName, ch.DbHandle)
	timer := warehouseutils.DestStat(stats.TimerType, "single_table_upload_time", ch.Warehouse.Destination.ID)
	timer.Start()
	defer timer.End()

	// sort column names
	sortedColumnKeys := warehouseutils.SortColumnKeysFromColumnMap(columnMap)
	sortedColumnString := strings.Join(sortedColumnKeys, ", ")

	fileNames, err := ch.DownloadLoadFiles(tableName)
	defer misc.RemoveFilePaths(fileNames...)
	if err != nil {
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, ch.Upload.ID, tableName, err, ch.DbHandle)
		return
	}

	txn, err := ch.Db.Begin()
	if err != nil {
		logger.Errorf("ch: Error while beginning a transaction in db for loading in table:%s: %v", tableName, err)
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, ch.Upload.ID, tableName, err, ch.DbHandle)
		return
	}

	stmt, err := txn.Prepare(fmt.Sprintf(`INSERT INTO "%s"."%s" (%v) VALUES (%s) `, ch.Namespace, tableName, sortedColumnString, generateArgumentString("?", len(sortedColumnKeys))))
	if err != nil {
		logger.Errorf("ch: Error while preparing statement for  transaction in db for loading in staging table:%s: %v", stagingTableName, err)
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, ch.Upload.ID, tableName, err, ch.DbHandle)
		return
	}
	for _, objectFileName := range fileNames {
		var gzipFile *os.File
		gzipFile, err = os.Open(objectFileName)
		if err != nil {
			logger.Errorf("ch: Error opening file using os.Open for file:%s while loading to table %s", objectFileName, tableName)
			warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, ch.Upload.ID, tableName, err, ch.DbHandle)
			return
		}

		var gzipReader *gzip.Reader
		gzipReader, err = gzip.NewReader(gzipFile)
		if err != nil {
			logger.Errorf("CH: Error reading file using gzip.NewReader for file:%s while loading to table %s", gzipFile, tableName)
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
					logger.Infof("CH: File reading completed while reading csv file for loading in staging table:%s: %s", stagingTableName, objectFileName)
				}
				break
			}
			var recordInterface []interface{}
			for key, value := range record {
				columnName := sortedColumnKeys[key]
				columnDataType := columnMap[columnName]
				data := getDataFromType(value, columnDataType)
				recordInterface = append(recordInterface, data)

			}
			_, err = stmt.Exec(recordInterface...)
			if err != nil {
				break
			}

		}
		gzipReader.Close()
		gzipFile.Close()
	}
	if err != nil && err != io.EOF {
		txn.Rollback()
		logger.Errorf("CH: Rollback transaction as there was error while loading staging table:%s: %v", stagingTableName, err)
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, ch.Upload.ID, tableName, err, ch.DbHandle)
		return

	}

	if err = txn.Commit(); err != nil {
		logger.Errorf("CH: Error while committing transaction as there was error while loading staging table:%s: %v", stagingTableName, err)
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, ch.Upload.ID, tableName, err, ch.DbHandle)
		return
	}

	warehouseutils.SetTableUploadStatus(warehouseutils.ExportedDataState, ch.Upload.ID, tableName, ch.DbHandle)
	logger.Infof("CH: Complete load for table:%s", tableName)
	return
}

func (ch *HandleT) load() (errList []error) {
	logger.Infof("ch: Starting load for all %v tables\n", len(ch.Upload.Schema))
	var wg sync.WaitGroup
	loadChan := make(chan struct{}, maxParallelLoads)
	wg.Add(len(ch.Upload.Schema))
	for tableName, columnMap := range ch.Upload.Schema {
		if tableName == warehouseutils.UsersTable || tableName == warehouseutils.IdentifiesTable {
			wg.Done()
			continue
		}
		tName := tableName
		cMap := columnMap
		loadChan <- struct{}{}
		rruntime.Go(func() {
			_, loadError := ch.loadTable(tName, cMap, false)
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

func checkAndIgnoreAlreadyExistError(err error) bool {
	if err != nil {
		// TODO: throw error if column already exists but of different type
		return false
	}
	return true
}

func (ch *HandleT) createSchema() (err error) {
	sqlStatement := fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS "%s"`, ch.Namespace)
	logger.Infof("CH: Creating database in clickhouse for ch:%s : %v", ch.Warehouse.Destination.ID, sqlStatement)
	_, err = ch.Db.Exec(sqlStatement)
	return
}

func (ch *HandleT) createUsersTable(name string, columns map[string]string) (err error) {
	sortKeyField := "received_at"
	if _, ok := columns["received_at"]; !ok {
		sortKeyField = "uuid_ts"
		if _, ok = columns["uuid_ts"]; !ok {
			sortKeyField = "id"
		}
	}
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s"."%s" ( %v ) engine = AggregatingMergeTree() order by %s `, ch.Namespace, name, columnsWithDataTypesForUsersTable(columns, "", sortKeyField), sortKeyField)
	logger.Infof("CH: Creating table in clickhouse for ch:%s : %v", ch.Warehouse.Destination.ID, sqlStatement)
	_, err = ch.Db.Exec(sqlStatement)
	return
}

// createTable create a table in the database provided in control plane
func (ch *HandleT) createTable(name string, columns map[string]string) (err error) {
	sortKeyField := "received_at"
	if _, ok := columns["received_at"]; !ok {
		sortKeyField = "uuid_ts"
		if _, ok = columns["uuid_ts"]; !ok {
			sortKeyField = "id"
		}
	}

	var sqlStatement string
	if name == warehouseutils.UsersTable {
		return ch.createUsersTable(name, columns)
	}
	sqlStatement = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s"."%s" ( %v ) engine = ReplacingMergeTree() order by %s `, ch.Namespace, name, columnsWithDataTypes(columns, "", sortKeyField), sortKeyField)

	logger.Infof("CH: Creating table in clickhouse for ch:%s : %v", ch.Warehouse.Destination.ID, sqlStatement)
	_, err = ch.Db.Exec(sqlStatement)
	return
}

func (ch *HandleT) tableExists(tableName string) (exists bool, err error) {
	sqlStatement := fmt.Sprintf(` EXISTS TABLE "%s"."%s"`, ch.Namespace, tableName)
	err = ch.Db.QueryRow(sqlStatement).Scan(&exists)
	return
}

func (ch *HandleT) addColumn(tableName string, columnName string, columnType string) (err error) {
	sqlStatement := fmt.Sprintf(`ALTER TABLE "%s"."%s" ADD COLUMN IF NOT EXISTS %s %s`, ch.Namespace, tableName, columnName, rudderDataTypesMapToClickHouse[columnType])
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

func (ch *HandleT) MigrateSchema() (err error) {
	timer := warehouseutils.DestStat(stats.TimerType, "migrate_schema_time", ch.Warehouse.Destination.ID)
	timer.Start()
	warehouseutils.SetUploadStatus(ch.Upload, warehouseutils.UpdatingSchemaState, ch.DbHandle)
	logger.Infof("CH: Updating schema for postgres schema name: %s", ch.Namespace)
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
