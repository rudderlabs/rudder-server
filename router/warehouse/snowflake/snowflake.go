package snowflake

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/rudderlabs/rudder-server/config"
	warehouseutils "github.com/rudderlabs/rudder-server/router/warehouse/utils"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	uuid "github.com/satori/go.uuid"
	snowflake "github.com/snowflakedb/gosnowflake" //blank comment
)

var (
	warehouseUploadsTable string
	stagingTablePrefix    string
)

type HandleT struct {
	DbHandle      *sql.DB
	Db            *sql.DB
	Namespace     string
	CurrentSchema map[string]map[string]string
	Warehouse     warehouseutils.WarehouseT
	Upload        warehouseutils.UploadT
}

var dataTypesMap = map[string]string{
	"boolean":  "boolean",
	"int":      "number",
	"bigint":   "number",
	"float":    "double precision",
	"string":   "varchar",
	"datetime": "timestamp",
}

var primaryKeyMap = map[string]string{
	"users":      "id",
	"identifies": "id",
}

func columnsWithDataTypes(columns map[string]string, prefix string) string {
	arr := []string{}
	for name, dataType := range columns {
		arr = append(arr, fmt.Sprintf(`%s%s %s`, prefix, name, dataTypesMap[dataType]))
	}
	return strings.Join(arr[:], ",")
}

func (sf *HandleT) createTable(name string, columns map[string]string) (err error) {
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s ( %v )`, name, columnsWithDataTypes(columns, ""))
	logger.Infof("Creating table in snowflake for SF:%s : %v", sf.Warehouse.Destination.ID, sqlStatement)
	_, err = sf.Db.Exec(sqlStatement)
	return
}

func (sf *HandleT) tableExists(tableName string) (exists bool, err error) {
	sqlStatement := fmt.Sprintf(`SELECT EXISTS ( SELECT 1
   								 FROM   information_schema.tables
   								 WHERE  table_schema = '%s'
   								 AND    table_name = '%s'
								   )`, sf.Namespace, strings.ToUpper(tableName))
	err = sf.Db.QueryRow(sqlStatement).Scan(&exists)
	return
}

func (sf *HandleT) addColumn(tableName string, columnName string, columnType string) (err error) {
	sqlStatement := fmt.Sprintf(`ALTER TABLE %s ADD COLUMN %s %s`, tableName, columnName, dataTypesMap[columnType])
	logger.Infof("Adding column in snowflake for SF:%s : %v", sf.Warehouse.Destination.ID, sqlStatement)
	_, err = sf.Db.Exec(sqlStatement)
	return
}

func (sf *HandleT) createSchema() (err error) {
	sqlStatement := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, sf.Namespace)
	logger.Infof("Creating schemaname in snowflake for SF:%s : %v", sf.Warehouse.Destination.ID, sqlStatement)
	_, err = sf.Db.Exec(sqlStatement)
	return
}

func (sf *HandleT) updateSchema() (updatedSchema map[string]map[string]string, err error) {
	diff := warehouseutils.GetSchemaDiff(sf.CurrentSchema, sf.Upload.Schema)
	updatedSchema = diff.UpdatedSchema
	if len(sf.CurrentSchema) == 0 {
		err = sf.createSchema()
		if err != nil {
			return nil, err
		}
	}

	sqlStatement := fmt.Sprintf(`USE SCHEMA %s`, sf.Namespace)
	_, err = sf.Db.Exec(sqlStatement)
	if err != nil {
		return nil, err
	}

	processedTables := make(map[string]bool)
	for _, tableName := range diff.Tables {
		tableExists, err := sf.tableExists(tableName)
		if err != nil {
			return nil, err
		}
		if !tableExists {
			err = sf.createTable(fmt.Sprintf(`%s`, tableName), diff.ColumnMaps[tableName])
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
				err := sf.addColumn(tableName, columnName, columnType)
				if !checkAndIgnoreAlreadyExistError(err) {
					return nil, err
				}
			}
		}
	}
	return
}

func checkAndIgnoreAlreadyExistError(err error) bool {
	if err != nil {
		if e, ok := err.(*snowflake.SnowflakeError); ok {
			if e.SQLState == "42601" {
				return true
			}
		}
		return false
	}
	return true
}

func (sf *HandleT) load() (errList []error) {
	var accessKeyID, accessKey string
	config := sf.Warehouse.Destination.Config.(map[string]interface{})
	if config["accessKeyID"] != nil {
		accessKeyID = config["accessKeyID"].(string)
	}
	if config["accessKey"] != nil {
		accessKey = config["accessKey"].(string)
	}

	sqlStatement := fmt.Sprintf(`USE SCHEMA %s`, sf.Namespace)
	_, err := sf.Db.Exec(sqlStatement)
	if err != nil {
		return []error{err}
	}

	logger.Infof("SF: Starting load for all %v tables\n", len(sf.Upload.Schema))
	counter := 0
	for tableName, columnMap := range sf.Upload.Schema {
		counter++
		logger.Infof("SF: Starting load for %v table:%s\n", counter, tableName)
		timer := warehouseutils.DestStat(stats.TimerType, "single_table_upload_time", sf.Warehouse.Destination.ID)
		timer.Start()
		// sort columnnames
		keys := reflect.ValueOf(columnMap).MapKeys()
		strkeys := make([]string, len(keys))
		for i := 0; i < len(keys); i++ {
			strkeys[i] = keys[i].String()
		}
		sort.Strings(strkeys)
		sortedColumnNames := strings.Join(strkeys, ",")

		stagingTableName := fmt.Sprintf(`%s%s-%s`, stagingTablePrefix, tableName, uuid.NewV4().String())
		sqlStatement := fmt.Sprintf(`CREATE TEMPORARY TABLE "%s"."%s" LIKE "%s"."%s"`, sf.Namespace, stagingTableName, sf.Namespace, strings.ToUpper(tableName))

		logger.Infof("SF: Creating temporary table for table:%s at %s\n", tableName, sqlStatement)
		_, err := sf.Db.Exec(sqlStatement)
		if err != nil {
			logger.Errorf("SF: Error creating temporary table: %v\n", err)
			errList = append(errList, err)
			continue
		}

		csvObjectLocation, err := warehouseutils.GetLoadFileLocation(sf.DbHandle, sf.Warehouse.Source.ID, sf.Warehouse.Destination.ID, tableName, sf.Upload.StartLoadFileID, sf.Upload.EndLoadFileID)
		if err != nil {
			panic(err)
		}
		loadFolder := warehouseutils.GetS3LocationFolder(csvObjectLocation)

		sqlStatement = fmt.Sprintf(`COPY INTO %v(%v) FROM '%v' CREDENTIALS = (AWS_KEY_ID='%s' AWS_SECRET_KEY='%s') PATTERN = '.*\.csv\.gz'
		FILE_FORMAT = ( TYPE = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE )`, fmt.Sprintf(`%s."%s"`, sf.Namespace, stagingTableName), sortedColumnNames, loadFolder, accessKeyID, accessKey)

		logger.Infof("SF: Running COPY command for table:%s at %s\n", tableName, sqlStatement)
		_, err = sf.Db.Exec(sqlStatement)
		if err != nil {
			logger.Errorf("SF: Error running COPY command: %v\n", err)
			errList = append(errList, err)
			continue
		}

		primaryKey := "id"
		if column, ok := primaryKeyMap[tableName]; ok {
			primaryKey = column
		}

		var columnNames, stagingColumnNames string
		for idx, str := range strkeys {
			columnNames += str
			stagingColumnNames += fmt.Sprintf(`staging.%s`, str)
			if idx != len(strkeys)-1 {
				columnNames += ","
				stagingColumnNames += ","
			}
		}

		sqlStatement = fmt.Sprintf(`MERGE INTO "%[1]s"."%[2]s" AS original
									USING (
										SELECT * FROM (
											SELECT *, row_number() OVER (PARTITION BY %[4]s ORDER BY received_at ASC) AS _rudder_staging_row_number FROM "%[1]s"."%[3]s"
										) AS q WHERE _rudder_staging_row_number = 1
									) AS staging
									ON original.%[4]s = staging.%[4]s
									WHEN NOT MATCHED THEN
									INSERT (%[5]s) VALUES (%[6]s)`, sf.Namespace, strings.ToUpper(tableName), stagingTableName, primaryKey, columnNames, stagingColumnNames)
		logger.Infof("SF: Dedup records for table:%s using staging table: %s\n", tableName, sqlStatement)
		_, err = sf.Db.Exec(sqlStatement)
		if err != nil {
			logger.Errorf("SF: Error running MERGE for dedup: %v\n", err)
			errList = append(errList, err)
			continue
		}

		timer.End()
		logger.Infof("SF: Complete load for table:%s\n", tableName)
	}
	logger.Infof("SF: Complete load for all tables\n")
	return
}

type SnowflakeCredentialsT struct {
	account  string
	whName   string
	dbName   string
	username string
	password string
}

func connect(cred SnowflakeCredentialsT) (*sql.DB, error) {
	url := fmt.Sprintf("%s:%s@%s/%s?warehouse=%s",
		cred.username,
		cred.password,
		cred.account,
		cred.dbName,
		cred.whName)

	var err error
	var db *sql.DB
	if db, err = sql.Open("snowflake", url); err != nil {
		return nil, fmt.Errorf("SF: snowflake connect error : (%v)", err)
	}

	alterStatement := fmt.Sprintf(`ALTER SESSION SET ABORT_DETACHED_QUERY=TRUE`)
	logger.Infof("SF: Altering session with abort_detached_query for snowflake: %v", alterStatement)
	_, err = db.Exec(alterStatement)
	if err != nil {
		return nil, fmt.Errorf("SF: snowflake alter session error : (%v)", err)
	}
	return db, nil
}

func loadConfig() {
	warehouseUploadsTable = config.GetString("Warehouse.uploadsTable", "wh_uploads")
	stagingTablePrefix = "rudder-staging-"
}

func init() {
	config.Initialize()
	loadConfig()
}

func (sf *HandleT) MigrateSchema() (err error) {
	timer := warehouseutils.DestStat(stats.TimerType, "migrate_schema_time", sf.Warehouse.Destination.ID)
	timer.Start()
	warehouseutils.SetUploadStatus(sf.Upload, warehouseutils.UpdatingSchemaState, sf.DbHandle)
	logger.Infof("SF: Updaing schema for snowflake schemaname: %s", sf.Namespace)
	updatedSchema, err := sf.updateSchema()
	if err != nil {
		warehouseutils.SetUploadError(sf.Upload, err, warehouseutils.UpdatingSchemaFailedState, sf.DbHandle)
		return
	}
	err = warehouseutils.SetUploadStatus(sf.Upload, warehouseutils.UpdatedSchemaState, sf.DbHandle)
	if err != nil {
		panic(err)
	}
	err = warehouseutils.UpdateCurrentSchema(sf.Namespace, sf.Warehouse, sf.Upload.ID, sf.CurrentSchema, updatedSchema, sf.DbHandle)
	timer.End()
	if err != nil {
		warehouseutils.SetUploadError(sf.Upload, err, warehouseutils.UpdatingSchemaFailedState, sf.DbHandle)
		return
	}
	return
}

func (sf *HandleT) Export() (err error) {
	logger.Infof("SF: Starting export to snowflake for source:%s and wh_upload:%v", sf.Warehouse.Source.ID, sf.Upload.ID)
	err = warehouseutils.SetUploadStatus(sf.Upload, warehouseutils.ExportingDataState, sf.DbHandle)
	if err != nil {
		panic(err)
	}
	timer := warehouseutils.DestStat(stats.TimerType, "upload_time", sf.Warehouse.Destination.ID)
	timer.Start()
	errList := sf.load()
	timer.End()
	if len(errList) > 0 {
		errStr := ""
		for idx, err := range errList {
			errStr += err.Error()
			if idx < len(errList)-1 {
				errStr += ", "
			}
		}
		warehouseutils.SetUploadError(sf.Upload, errors.New(errStr), warehouseutils.ExportingDataFailedState, sf.DbHandle)
		return errors.New(errStr)
	}
	err = warehouseutils.SetUploadStatus(sf.Upload, warehouseutils.ExportedDataState, sf.DbHandle)
	if err != nil {
		panic(err)
	}
	return
}

func (sf *HandleT) CrashRecover(config warehouseutils.ConfigT) (err error) {
	return
}

func (sf *HandleT) Process(config warehouseutils.ConfigT) (err error) {
	sf.DbHandle = config.DbHandle
	sf.Warehouse = config.Warehouse
	sf.Upload = config.Upload

	currSchema, err := warehouseutils.GetCurrentSchema(sf.DbHandle, sf.Warehouse)
	if err != nil {
		panic(err)
	}
	sf.CurrentSchema = currSchema.Schema
	sf.Namespace = strings.ToUpper(currSchema.Namespace)
	if sf.Namespace == "" {
		logger.Infof("SF: Namespace not found in currentschema for SF:%s, setting from upload: %s", sf.Warehouse.Destination.ID, sf.Upload.Namespace)
		sf.Namespace = strings.ToUpper(sf.Upload.Namespace)
	}

	sf.Db, err = connect(SnowflakeCredentialsT{
		account:  sf.Warehouse.Destination.Config.(map[string]interface{})["account"].(string),
		whName:   sf.Warehouse.Destination.Config.(map[string]interface{})["warehouse"].(string),
		dbName:   sf.Warehouse.Destination.Config.(map[string]interface{})["database"].(string),
		username: sf.Warehouse.Destination.Config.(map[string]interface{})["user"].(string),
		password: sf.Warehouse.Destination.Config.(map[string]interface{})["password"].(string),
	})
	if err != nil {
		warehouseutils.SetUploadError(sf.Upload, err, warehouseutils.UpdatingSchemaFailedState, sf.DbHandle)
		return err
	}

	if config.Stage == "ExportData" {
		err = sf.Export()
	} else {
		err = sf.MigrateSchema()
		if err == nil {
			err = sf.Export()
		}
	}
	sf.Db.Close()
	return
}
