package postgres

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"strings"
	"sync"
)

var (
	warehouseUploadsTable string
	stagingTablePrefix    string
	maxParallelLoads      int
)

const (
	AWSAccessKey       = "accessKey"
	AWSAccessSecret    = "accessKeyID"
	StorageIntegration = "storageIntegration"
	host               = "host"
	dbName     = "database"
	user   = "user"
	password   = "password"
	port       ="port"
)

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
	host    string
	dbName     string
	user   string
	password   string
	schemaName string
	port       string
}

type optionalCredsT struct {
	schemaName string
}

func connect(cred credentialsT) (*sql.DB, error) {
	url := fmt.Sprintf("sslmode=require user=%v password=%v host=%v port=%v dbname=%v",
		cred.user,
		cred.password,
		cred.host,
		cred.port,
		cred.dbName)

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
		host:    warehouseutils.GetConfigValue(host, pg.Warehouse),
		dbName:     warehouseutils.GetConfigValue(dbName, pg.Warehouse),
		user:   warehouseutils.GetConfigValue(user, pg.Warehouse),
		password:   warehouseutils.GetConfigValue(password, pg.Warehouse),
		port: warehouseutils.GetConfigValue(port, pg.Warehouse),
		schemaName: opts.schemaName,
	}
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
	logger.Infof("PG: Starting load for table:%s\n", tableName)
	warehouseutils.SetTableUploadStatus(warehouseutils.ExecutingState, pg.Upload.ID, tableName, pg.DbHandle)
	timer := warehouseutils.DestStat(stats.TimerType, "single_table_upload_time", pg.Warehouse.Destination.ID)
	timer.Start()

	dbHandle, err := connect(pg.getConnectionCredentials(optionalCredsT{schemaName: pg.Namespace}))
	if err != nil {
		logger.Errorf("SF: Error establishing connection for copying table:%s: %v\n", tableName, err)
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, pg.Upload.ID, tableName, err, pg.DbHandle)
		return
	}
	defer dbHandle.Close()

	csvObjectLocation, err := warehouseutils.GetLoadFileLocation(pg.DbHandle, pg.Warehouse.Source.ID, pg.Warehouse.Destination.ID, tableName, pg.Upload.StartLoadFileID, pg.Upload.EndLoadFileID)
	if err != nil {
		panic(err)
	}

	return err
}

func (pg *HandleT) load() (errList []error) {
	logger.Infof("PG: Starting load for all %v tables\n", len(pg.Upload.Schema))
	var wg sync.WaitGroup
	loadChan := make(chan struct{}, maxParallelLoads)
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
	logger.Infof("SF: Completed load for all tables\n")
	return
}

//func (pg *HandleT) MigrateSchema() (err error) {
//	timer := warehouseutils.DestStat(stats.TimerType, "migrate_schema_time", pg.Warehouse.Destination.ID)
//	timer.Start()
//	warehouseutils.SetUploadStatus(pg.Upload, warehouseutils.UpdatingSchemaState, pg.DbHandle)
//	logger.Infof("PG: Updating schema for postgres schema name: %s", pg.Namespace)
//	updatedSchema, err := pg.updateSchema()
//	if err != nil {
//		warehouseutils.SetUploadError(pg.Upload, err, warehouseutils.UpdatingSchemaFailedState, pg.DbHandle)
//		return
//	}
//	err = warehouseutils.SetUploadStatus(pg.Upload, warehouseutils.UpdatedSchemaState, pg.DbHandle)
//	if err != nil {
//		panic(err)
//	}
//	err = warehouseutils.UpdateCurrentSchema(pg.Namespace, pg.Warehouse, pg.Upload.ID, pg.CurrentSchema, updatedSchema, pg.DbHandle)
//	timer.End()
//	if err != nil {
//		warehouseutils.SetUploadError(pg.Upload, err, warehouseutils.UpdatingSchemaFailedState, pg.DbHandle)
//		return
//	}
//	return
//}

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
	pg.Namespace = strings.ToUpper(currSchema.Namespace)
	if pg.Namespace == "" {
		logger.Infof("pg: Namespace not found in currentschema for pg:%s, setting from upload: %s", pg.Warehouse.Destination.ID, pg.Upload.Namespace)
		pg.Namespace = strings.ToUpper(pg.Upload.Namespace)
	}

	pg.Db, err = connect(pg.getConnectionCredentials(optionalCredsT{}))
	if err != nil {
		warehouseutils.SetUploadError(pg.Upload, err, warehouseutils.UpdatingSchemaFailedState, pg.DbHandle)
		return err
	}
	pg.Export()
	//if config.Stage == "ExportData" {
	//	err = pg.Export()
	//} else {
	//	err = pg.MigrateSchema()
	//	if err == nil {
	//		err = pg.Export()
	//	}
	//}
	pg.Db.Close()
	return
}
