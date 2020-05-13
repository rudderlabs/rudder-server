package postgres

import (
	"compress/gzip"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var (
	warehouseUploadsTable string
	stagingTablePrefix    string
	maxParallelLoads      int
)

const (
	host     = "host"
	dbName   = "database"
	user     = "user"
	password = "password"
	port     = "port"
	sslMode  = "sslMode"
)

var dataTypesMap = map[string]string{
	"int":      "bigint",
	"float":    "numeric",
	"string":   "text",
	"datetime": "timestamp",
	"boolean":  "boolean",
}

type HandleT struct {
	DbHandle      *sql.DB
	Db            *sql.DB
	Namespace     string
	CurrentSchema map[string]map[string]string
	Warehouse     warehouseutils.WarehouseT
	Upload        warehouseutils.UploadT
	CloudProvider string
	ObjectStorage string
}

type credentialsT struct {
	host       string
	dbName     string
	user       string
	password   string
	schemaName string
	port       string
	sslMode    string
}

type optionalCredsT struct {
	schemaName string
}

func connect(cred credentialsT) (*sql.DB, error) {
	url := fmt.Sprintf("user=%v password=%v host=%v port=%v dbname=%v sslmode=%v",
		cred.user,
		cred.password,
		cred.host,
		cred.port,
		cred.dbName,
		cred.sslMode)

	var err error
	var db *sql.DB
	if db, err = sql.Open("postgres", url); err != nil {
		return nil, fmt.Errorf("postgres connection error : (%v)", err)
	}
	return db, nil
}

func init() {
	config.Initialize()
	loadConfig()
}
func loadConfig() {
	warehouseUploadsTable = config.GetString("Warehouse.uploadsTable", "wh_uploads")
	stagingTablePrefix = "rudder_staging_"
	maxParallelLoads = config.GetInt("Warehouse.postgres.maxParallelLoads", 1)
}

func (pg *HandleT) getConnectionCredentials(opts optionalCredsT) credentialsT {
	return credentialsT{
		host:       warehouseutils.GetConfigValue(host, pg.Warehouse),
		dbName:     warehouseutils.GetConfigValue(dbName, pg.Warehouse),
		user:       warehouseutils.GetConfigValue(user, pg.Warehouse),
		password:   warehouseutils.GetConfigValue(password, pg.Warehouse),
		port:       warehouseutils.GetConfigValue(port, pg.Warehouse),
		sslMode:    warehouseutils.GetConfigValue(sslMode, pg.Warehouse),
		schemaName: opts.schemaName,
	}
}

func columnsWithDataTypes(columns map[string]string, prefix string) string {
	var arr []string
	for name, dataType := range columns {
		arr = append(arr, fmt.Sprintf(`%s%s %s`, prefix, name, dataTypesMap[dataType]))
	}
	return strings.Join(arr[:], ",")
}

func (pg *HandleT) CrashRecover(config warehouseutils.ConfigT) (err error) {
	return
}

func (pg *HandleT) Export() (err error) {
	logger.Infof("PG: Starting export to postgres for source:%s and wh_upload:%v", pg.Warehouse.Source.ID, pg.Upload.ID)
	err = warehouseutils.SetUploadStatus(pg.Upload, warehouseutils.ExportingDataState, pg.DbHandle)
	if err != nil {
		panic(err)
	}
	timer := warehouseutils.DestStat(stats.TimerType, "upload_time", pg.Warehouse.Destination.ID)
	timer.Start()
	errList := pg.load()
	timer.End()
	if len(errList) > 0 {
		errStr := ""
		for idx, err := range errList {
			errStr += err.Error()
			if idx < len(errList)-1 {
				errStr += ", "
			}
		}
		warehouseutils.SetUploadError(pg.Upload, errors.New(errStr), warehouseutils.ExportingDataFailedState, pg.DbHandle)
		return errors.New(errStr)
	}
	err = warehouseutils.SetUploadStatus(pg.Upload, warehouseutils.ExportedDataState, pg.DbHandle)
	if err != nil {
		panic(err)
	}
	return
}

func (pg *HandleT) DownloadLoadFile(tableName string) (*os.File, error) {
	objectLocation, _ := warehouseutils.GetLoadFileLocation(pg.DbHandle, pg.Warehouse.Source.ID, pg.Warehouse.Destination.ID, tableName, pg.Upload.StartLoadFileID, pg.Upload.EndLoadFileID)
	object, err := warehouseutils.GetObjectName(pg.Warehouse.Destination.Config, objectLocation)
	if err != nil {
		logger.Errorf("PG: Error in converting object location to object key for table:%s: %s,%v", tableName, objectLocation, err)
		return nil, err
	}
	dirName := "/rudder-warehouse-load-uploads-tmp/" //TODO: have a config a variable
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		logger.Errorf("PG: Error in creating tmp directory for downloading load file for table:%s: %s, %v", tableName, objectLocation, err)
		return nil, err
	}
	ObjectPath := tmpDirPath + dirName + fmt.Sprintf(`%s_%s_%s/`, pg.Warehouse.Destination.DestinationDefinition.Name, pg.Warehouse.Destination.ID, pg.Upload.Status) + object
	err = os.MkdirAll(filepath.Dir(ObjectPath), os.ModePerm)
	if err != nil {
		logger.Errorf("PG: Error in making tmp directory for downloading load file for table:%s: %s, %s %v", tableName, objectLocation, err)
		return nil, err
	}
	objectFile, err := os.Create(ObjectPath)
	if err != nil {
		logger.Errorf("PG: Error in creating file in tmp directory for downloading load file for table:%s: %s, %v", tableName, objectLocation, err)
		return nil, err
	}
	downloader, err := filemanager.New(&filemanager.SettingsT{
		Provider: warehouseutils.ObjectStorageType(pg.Warehouse.Destination.DestinationDefinition.Name, pg.Warehouse.Destination.Config),
		Config:   pg.Warehouse.Destination.Config.(map[string]interface{}),
	})
	err = downloader.Download(objectFile, object)
	if err != nil {
		logger.Errorf("PG: Error in downloading file in tmp directory for downloading load file for table:%s: %s, %v", tableName, objectLocation, err)
		return nil, err
	}
	return objectFile, nil
}

func (pg *HandleT) loadTable(tableName string, columnMap map[string]string) (err error) {
	status, err := warehouseutils.GetTableUploadStatus(pg.Upload.ID, tableName, pg.DbHandle)
	if status == warehouseutils.ExportedDataState {
		logger.Infof("PG: Skipping load for table:%s as it has been successfully loaded earlier", tableName)
		return
	}
	if !warehouseutils.HasLoadFiles(pg.DbHandle, pg.Warehouse.Source.ID, pg.Warehouse.Destination.ID, tableName, pg.Upload.StartLoadFileID, pg.Upload.EndLoadFileID) {
		warehouseutils.SetTableUploadStatus(warehouseutils.ExportedDataState, pg.Upload.ID, tableName, pg.DbHandle)
		return
	}
	logger.Infof("PG: Starting load for table:%s", tableName)
	warehouseutils.SetTableUploadStatus(warehouseutils.ExecutingState, pg.Upload.ID, tableName, pg.DbHandle)
	timer := warehouseutils.DestStat(stats.TimerType, "single_table_upload_time", pg.Warehouse.Destination.ID)
	timer.Start()
	defer timer.End()

	dbHandle, err := connect(pg.getConnectionCredentials(optionalCredsT{schemaName: pg.Namespace}))
	if err != nil {
		logger.Errorf("PG: Error establishing connection for copying table:%s: %v", tableName, err)
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, pg.Upload.ID, tableName, err, pg.DbHandle)
		return
	}
	defer dbHandle.Close()
	objectFile, err := pg.DownloadLoadFile(tableName)
	if err != nil {
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, pg.Upload.ID, tableName, err, pg.DbHandle)
		return
	}
	defer objectFile.Close()
	//pg.ExtractDownloadFile

	//
	//csvString := tmpDirPath + dirName + fmt.Sprintf(`%s_%s/`, pg.Warehouse.Destination.DestinationDefinition.Name, pg.Warehouse.Destination.ID) + strings.Replace(csvObject, ".gz", "", 1)
	//err = os.MkdirAll(filepath.Dir(csvString), os.ModePerm)
	//if err != nil { //TODO: Handle error, log error
	//	panic(err)
	//}
	//defer os.Remove(csvString)
	//csvFile, err := os.Create(csvString)
	//if err != nil {
	//	panic(err)
	//}
	//defer csvFile.Close()
	//_, err = io.Copy(csvFile, gzipReader)
	//if err != nil {
	//	panic(err)
	//}

	gzipFile, err := os.Open(objectFile.Name())
	if err != nil {
		logger.Errorf("WH: Error opening file using os.Open for file:%s while loading to table %s", objectFile.Name(), tableName)
		panic(err)
	}
	defer gzipFile.Close()
	gzipReader, err := gzip.NewReader(gzipFile)
	if err != nil {
		if err.Error() == "EOF" {
			logger.Infof("WH: EOF while reading file using gzip.NewReader for file:%s while loading to table %s", gzipFile, tableName)
		} else {
			logger.Errorf("WH: Error reading file using gzip.NewReader for file:%s while loading to table %s", gzipFile, tableName)
			warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, pg.Upload.ID, tableName, err, pg.DbHandle)
			return

		}

	}

	// sort column names
	sortedColumnKeys := warehouseutils.SortColumnKeysFromColumnMap(columnMap, pg.Warehouse.Destination.DestinationDefinition.Name)
	//sortedColumnsString := strings.Join(sortedColumnKeys, ", ")
	//sqlStatement := fmt.Sprintf(`copy %s (%s) from '%s' with delimiter ',' csv`, tableName, sortedColumnsString, csvString)
	//_, err = pg.Db.Query(sqlStatement)
	//if err != nil {
	//	logger.Error(err)
	//}

	txn, err := pg.Db.Begin()
	if err != nil {
		logger.Errorf("PG: Error while beginning a transaction in db for loading in table:%s: %v", tableName, err)
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, pg.Upload.ID, tableName, err, pg.DbHandle)
		return
	}
	stmt, err := txn.Prepare(pq.CopyInSchema(pg.Namespace, tableName, sortedColumnKeys...))
	if err != nil {
		logger.Errorf("PG: Error while preparing statement for  transaction in db for loading in table:%s: %v", tableName, err)
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, pg.Upload.ID, tableName, err, pg.DbHandle)
		return
	}
	csvReader := csv.NewReader(gzipReader)
	for {
		record, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				logger.Infof("PG: File reading completed while reading csv file for loading in table:%s: %s", tableName, objectFile.Name())
				break
			} else {
				logger.Errorf("PG: Error while reading csv file for loading in table:%s: %v", tableName, err)
				warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, pg.Upload.ID, tableName, err, pg.DbHandle)
				return err
			}

		}
		var recordInterface []interface{}
		for _, value := range record {
			recordInterface = append(recordInterface, value)
		}
		_, err = stmt.Exec(recordInterface...)
	}
	_, err = stmt.Exec()
	if err != nil {
		txn.Rollback()
		logger.Errorf("PG: Rollback transaction as there was error while loading table:%s: %v", tableName, err)
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, pg.Upload.ID, tableName, err, pg.DbHandle)
		return

	}
	if err = txn.Commit(); err != nil {
		logger.Errorf("PG: Error while committing transaction as there was error while loading table:%s: %v", tableName, err)
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, pg.Upload.ID, tableName, err, pg.DbHandle)
		return
	}
	warehouseutils.SetTableUploadStatus(warehouseutils.ExportedDataState, pg.Upload.ID, tableName, pg.DbHandle)
	logger.Infof("PG: Complete load for table:%s", tableName)
	return err
}

func (pg *HandleT) load() (errList []error) {
	logger.Infof("PG: Starting load for all %v tables\n", len(pg.Upload.Schema))
	var wg sync.WaitGroup
	loadChan := make(chan struct{}, maxParallelLoads)
	wg.Add(len(pg.Upload.Schema))
	for tableName, columnMap := range pg.Upload.Schema {
		tName := tableName
		cMap := columnMap
		loadChan <- struct{}{}
		rruntime.Go(func() {
			loadError := pg.loadTable(tName, cMap)
			if loadError != nil {
				errList = append(errList, loadError)
			}
			wg.Done()
			<-loadChan
		})
	}
	wg.Wait()
	logger.Infof("PG: Completed load for all tables\n")
	return
}

func checkAndIgnoreAlreadyExistError(err error) bool {
	if err != nil {
		// TODO: throw error if column already exists but of different type
		return false
	}
	return true
}

func (pg *HandleT) createSchema() (err error) {
	sqlStatement := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s"`, pg.Namespace)
	logger.Infof("Creating schema name in postgres for PG:%s : %v", pg.Warehouse.Destination.ID, sqlStatement)
	_, err = pg.Db.Exec(sqlStatement)
	return
}

func (pg *HandleT) createTable(name string, columns map[string]string) (err error) {
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s ( %v )`, name, columnsWithDataTypes(columns, ""))
	logger.Infof("Creating table in postgres for PG:%s : %v", pg.Warehouse.Destination.ID, sqlStatement)
	_, err = pg.Db.Exec(sqlStatement)
	return
}

func (pg *HandleT) tableExists(tableName string) (exists bool, err error) {
	sqlStatement := fmt.Sprintf(`SELECT EXISTS ( SELECT 1
   								 FROM   information_schema.tables
   								 WHERE  table_schema = '%s'
   								 AND    table_name = '%s'
								   )`, pg.Namespace, tableName)
	err = pg.Db.QueryRow(sqlStatement).Scan(&exists)
	return
}

func (pg *HandleT) addColumn(tableName string, columnName string, columnType string) (err error) {
	sqlStatement := fmt.Sprintf(`ALTER TABLE %s ADD COLUMN %s %s`, tableName, columnName, dataTypesMap[columnType])
	logger.Infof("Adding column in postgres for PG:%s : %v", pg.Warehouse.Destination.ID, sqlStatement)
	_, err = pg.Db.Exec(sqlStatement)
	return
}

func (pg *HandleT) updateSchema() (updatedSchema map[string]map[string]string, err error) {
	diff := warehouseutils.GetSchemaDiff(pg.CurrentSchema, pg.Upload.Schema)
	updatedSchema = diff.UpdatedSchema
	if len(pg.CurrentSchema) == 0 {
		err = pg.createSchema()
		if err != nil {
			return nil, err
		}
	}
	// set the schema in search path. so that we can query table with unqualified name which is just the table name rather than using schema.table in queries
	sqlStatement := fmt.Sprintf(`SET search_path to "%s"`, pg.Namespace)
	_, err = pg.Db.Exec(sqlStatement)
	if err != nil {
		return nil, err
	}

	processedTables := make(map[string]bool)
	for _, tableName := range diff.Tables {
		tableExists, err := pg.tableExists(tableName)
		if err != nil {
			return nil, err
		}
		if !tableExists {
			err = pg.createTable(fmt.Sprintf(`%s`, tableName), diff.ColumnMaps[tableName])
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
				err := pg.addColumn(tableName, columnName, columnType)
				if err != nil {
					if checkAndIgnoreAlreadyExistError(err) {
						logger.Infof("SF: Column %s already exists on %s.%s \nResponse: %v", columnName, pg.Namespace, tableName, err)
					} else {
						return nil, err
					}
				}
			}
		}
	}
	return
}

func (pg *HandleT) MigrateSchema() (err error) {
	timer := warehouseutils.DestStat(stats.TimerType, "migrate_schema_time", pg.Warehouse.Destination.ID)
	timer.Start()
	warehouseutils.SetUploadStatus(pg.Upload, warehouseutils.UpdatingSchemaState, pg.DbHandle)
	logger.Infof("PG: Updating schema for postgres schema name: %s", pg.Namespace)
	updatedSchema, err := pg.updateSchema()
	if err != nil {
		warehouseutils.SetUploadError(pg.Upload, err, warehouseutils.UpdatingSchemaFailedState, pg.DbHandle)
		return
	}
	err = warehouseutils.SetUploadStatus(pg.Upload, warehouseutils.UpdatedSchemaState, pg.DbHandle)
	if err != nil {
		panic(err)
	}
	err = warehouseutils.UpdateCurrentSchema(pg.Namespace, pg.Warehouse, pg.Upload.ID, pg.CurrentSchema, updatedSchema, pg.DbHandle)
	timer.End()
	if err != nil {
		warehouseutils.SetUploadError(pg.Upload, err, warehouseutils.UpdatingSchemaFailedState, pg.DbHandle)
		return
	}
	return
}

func (pg *HandleT) Process(config warehouseutils.ConfigT) (err error) {
	pg.DbHandle = config.DbHandle
	pg.Warehouse = config.Warehouse
	pg.Upload = config.Upload
	pg.CloudProvider = warehouseutils.PostgresCloudProvider(config.Warehouse.Destination.Config)
	pg.ObjectStorage = warehouseutils.ObjectStorageType("POSTGRES", config.Warehouse.Destination.Config)

	currSchema, err := warehouseutils.GetCurrentSchema(pg.DbHandle, pg.Warehouse)
	if err != nil {
		panic(err)
	}
	pg.CurrentSchema = currSchema.Schema
	pg.Namespace = strings.ToLower(currSchema.Namespace)
	if pg.Namespace == "" {
		logger.Infof("pg: Namespace not found in current schema for pg:%s, setting from upload: %s", pg.Warehouse.Destination.ID, pg.Upload.Namespace)
		pg.Namespace = strings.ToLower(pg.Upload.Namespace)
	}

	pg.Db, err = connect(pg.getConnectionCredentials(optionalCredsT{}))
	if err != nil {
		warehouseutils.SetUploadError(pg.Upload, err, warehouseutils.UpdatingSchemaFailedState, pg.DbHandle)
		return err
	}
	if err := pg.MigrateSchema(); err == nil {
		pg.Export()
	}
	pg.Db.Close()
	return
}
