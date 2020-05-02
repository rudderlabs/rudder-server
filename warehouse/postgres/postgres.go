package postgres

import (
	"database/sql"
	"fmt"
	"github.com/rudderlabs/rudder-server/utils/logger"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"strings"
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

func (pg *HandleT) Process(config warehouseutils.ConfigT) (err error) {
	pg.DbHandle = config.DbHandle
	pg.Warehouse = config.Warehouse
	pg.Upload = config.Upload
	pg.CloudProvider = warehouseutils.SnowflakeCloudProvider(config.Warehouse.Destination.Config)
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

	if config.Stage == "ExportData" {
		err = pg.Export()
	} else {
		err = pg.MigrateSchema()
		if err == nil {
			err = pg.Export()
		}
	}
	pg.Db.Close()
	return
}
