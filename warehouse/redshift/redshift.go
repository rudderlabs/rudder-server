package redshift

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	uuid "github.com/satori/go.uuid"
)

var (
	warehouseUploadsTable string
	stagingTablePrefix    string
	maxParallelLoads      int
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
	"int":      "bigint",
	"bigint":   "bigint",
	"float":    "double precision",
	"string":   "varchar(512)",
	"datetime": "timestamp",
}

var primaryKeyMap = map[string]string{
	"users":      "id",
	"identifies": "id",
}

func columnsWithDataTypes(columns map[string]string, prefix string) string {
	arr := []string{}
	for name, dataType := range columns {
		arr = append(arr, fmt.Sprintf(`"%s%s" %s`, prefix, name, dataTypesMap[dataType]))
	}
	return strings.Join(arr[:], ",")
}

func (rs *HandleT) createTable(name string, columns map[string]string) (err error) {
	sortKeyField := "received_at"
	if _, ok := columns["received_at"]; !ok {
		sortKeyField = "uuid_ts"
		if _, ok = columns["uuid_ts"]; !ok {
			sortKeyField = "id"
		}
	}
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s ( %v ) SORTKEY("%s")`, name, columnsWithDataTypes(columns, ""), sortKeyField)
	logger.Infof("Creating table in redshift for RS:%s : %v", rs.Warehouse.Destination.ID, sqlStatement)
	_, err = rs.Db.Exec(sqlStatement)
	return
}

func (rs *HandleT) tableExists(tableName string) (exists bool, err error) {
	sqlStatement := fmt.Sprintf(`SELECT EXISTS ( SELECT 1
   								 FROM   information_schema.tables
   								 WHERE  table_schema = '%s'
   								 AND    table_name = '%s'
								   )`, rs.Namespace, tableName)
	err = rs.Db.QueryRow(sqlStatement).Scan(&exists)
	return
}

func (rs *HandleT) addColumn(tableName string, columnName string, columnType string) (err error) {
	sqlStatement := fmt.Sprintf(`ALTER TABLE %v ADD COLUMN "%s" %s`, tableName, columnName, dataTypesMap[columnType])
	logger.Infof("Adding column in redshift for RS:%s : %v", rs.Warehouse.Destination.ID, sqlStatement)
	_, err = rs.Db.Exec(sqlStatement)
	return
}

func (rs *HandleT) createSchema() (err error) {
	sqlStatement := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s"`, rs.Namespace)
	logger.Infof("Creating schemaname in redshift for RS:%s : %v", rs.Warehouse.Destination.ID, sqlStatement)
	_, err = rs.Db.Exec(sqlStatement)
	return
}

func (rs *HandleT) updateSchema() (updatedSchema map[string]map[string]string, err error) {
	diff := warehouseutils.GetSchemaDiff(rs.CurrentSchema, rs.Upload.Schema)
	updatedSchema = diff.UpdatedSchema
	if len(rs.CurrentSchema) == 0 {
		err = rs.createSchema()
		if err != nil {
			return nil, err
		}
	}
	processedTables := make(map[string]bool)
	for _, tableName := range diff.Tables {
		tableExists, err := rs.tableExists(tableName)
		if err != nil {
			return nil, err
		}
		if !tableExists {
			err = rs.createTable(fmt.Sprintf(`"%s"."%s"`, rs.Namespace, tableName), diff.ColumnMaps[tableName])
			if err != nil {
				return nil, err
			}
			processedTables[tableName] = true
		}
	}
	for tableName, columnMap := range diff.ColumnMaps {
		// skip adding columns when table didn't exist previously and was created in the prev statement
		// this to make sure all columns in the the columnMap exists in the table in redshift
		if _, ok := processedTables[tableName]; ok {
			continue
		}
		if len(columnMap) > 0 {
			for columnName, columnType := range columnMap {
				err := rs.addColumn(fmt.Sprintf(`"%s"."%s"`, rs.Namespace, tableName), columnName, columnType)
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
		if e, ok := err.(*pq.Error); ok {
			if e.Code == "42701" {
				return true
			}
		}
		return false
	}
	return true
}

type S3ManifestEntryT struct {
	Url       string `json:"url"`
	Mandatory bool   `json:"mandatory"`
}

type S3ManifestT struct {
	Entries []S3ManifestEntryT `json:"entries"`
}

func (rs *HandleT) generateManifest(bucketName, tableName string, columnMap map[string]string, accessKeyID, accessKey string) (string, error) {
	csvObjectLocations, err := warehouseutils.GetLoadFileLocations(rs.DbHandle, rs.Warehouse.Source.ID, rs.Warehouse.Destination.ID, tableName, rs.Upload.StartLoadFileID, rs.Upload.EndLoadFileID)
	if err != nil {
		panic(err)
	}
	csvS3Locations, err := warehouseutils.GetS3Locations(csvObjectLocations)
	var manifest S3ManifestT
	for _, location := range csvS3Locations {
		manifest.Entries = append(manifest.Entries, S3ManifestEntryT{Url: location, Mandatory: true})
	}
	logger.Infof("RS: Generated manifest for table:%s", tableName)
	manifestJSON, err := json.Marshal(&manifest)

	manifestFolder := "rudder-redshift-manifests"
	dirName := "/" + manifestFolder + "/"
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		panic(err)
	}
	localManifestPath := fmt.Sprintf("%v%v", tmpDirPath+dirName, uuid.NewV4().String())
	err = os.MkdirAll(filepath.Dir(localManifestPath), os.ModePerm)
	if err != nil {
		panic(err)
	}
	_ = ioutil.WriteFile(localManifestPath, manifestJSON, 0644)

	file, err := os.Open(localManifestPath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	uploader, err := filemanager.New(&filemanager.SettingsT{
		Provider: "S3",
		Config:   map[string]interface{}{"bucketName": bucketName, "accessKeyID": accessKeyID, "accessKey": accessKey},
	})

	uploadOutput, err := uploader.Upload(file, manifestFolder, rs.Warehouse.Source.ID, rs.Warehouse.Destination.ID, time.Now().Format("01-02-2006"), tableName, uuid.NewV4().String())

	if err != nil {
		return "", err
	}

	return uploadOutput.Location, nil
}

func (rs *HandleT) dropStagingTables(stagingTableNames []string) {
	for _, stagingTableName := range stagingTableNames {
		logger.Infof("WH: dropping table %+v\n", stagingTableName)
		_, err := rs.Db.Exec(fmt.Sprintf(`DROP TABLE "%[1]s"."%[2]s"`, rs.Namespace, stagingTableName))
		if err != nil {
			logger.Errorf("WH: RS:  Error dropping staging tables in redshift: %v", err)
		}
	}
}

func (rs *HandleT) loadTable(tableName string, columnMap map[string]string, bucketName, accessKeyID, accessKey string) (err error) {
	status, err := warehouseutils.GetTableUploadStatus(rs.Upload.ID, tableName, rs.DbHandle)
	if status == warehouseutils.ExportedDataState {
		logger.Infof("RS: Skipping load for table:%s as it has been succesfully loaded earlier", tableName)
		return
	}

	logger.Infof("RS: Starting load for table:%s\n", tableName)
	warehouseutils.SetTableUploadStatus(warehouseutils.ExecutingState, rs.Upload.ID, tableName, rs.DbHandle)

	timer := warehouseutils.DestStat(stats.TimerType, "generate_manifest_time", rs.Warehouse.Destination.ID)
	timer.Start()
	manifestLocation, err := rs.generateManifest(bucketName, tableName, columnMap, accessKeyID, accessKey)
	timer.End()
	if err != nil {
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, rs.Upload.ID, tableName, err, rs.DbHandle)
		return
	}
	logger.Infof("RS: Generated and stored manifest for table:%s at %s\n", tableName, manifestLocation)

	// sort columnnames
	keys := reflect.ValueOf(columnMap).MapKeys()
	strkeys := make([]string, len(keys))
	for i := 0; i < len(keys); i++ {
		strkeys[i] = keys[i].String()
	}
	sort.Strings(strkeys)
	var sortedColumnNames string
	for index, key := range strkeys {
		if index > 0 {
			sortedColumnNames += fmt.Sprintf(`, `)
		}
		sortedColumnNames += fmt.Sprintf(`"%s"`, key)
	}

	stagingTableName := fmt.Sprintf(`%s%s-%s`, stagingTablePrefix, tableName, uuid.NewV4().String())
	err = rs.createTable(fmt.Sprintf(`"%s"."%s"`, rs.Namespace, stagingTableName), rs.Upload.Schema[tableName])
	if err != nil {
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, rs.Upload.ID, tableName, err, rs.DbHandle)
		return
	}
	defer rs.dropStagingTables([]string{stagingTableName})

	region, manifestS3Location := warehouseutils.GetS3Location(manifestLocation)
	if region == "" {
		region = "us-east-1"
	}

	// BEGIN TRANSACTION
	tx, err := rs.Db.Begin()
	if err != nil {
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, rs.Upload.ID, tableName, err, rs.DbHandle)
		return
	}

	sqlStatement := fmt.Sprintf(`COPY %v(%v) FROM '%v' CSV GZIP ACCESS_KEY_ID '%s' SECRET_ACCESS_KEY '%s' REGION '%s'  DATEFORMAT 'auto' TIMEFORMAT 'auto' MANIFEST TRUNCATECOLUMNS EMPTYASNULL BLANKSASNULL FILLRECORD ACCEPTANYDATE TRIMBLANKS ACCEPTINVCHARS COMPUPDATE OFF STATUPDATE OFF`, fmt.Sprintf(`"%s"."%s"`, rs.Namespace, stagingTableName), sortedColumnNames, manifestS3Location, accessKeyID, accessKey, region)

	sanitisedSQLStmt, regexErr := misc.ReplaceMultiRegex(sqlStatement, map[string]string{
		"ACCESS_KEY_ID '[^']*'":     "ACCESS_KEY_ID '***'",
		"SECRET_ACCESS_KEY '[^']*'": "SECRET_ACCESS_KEY '***'",
	})
	if regexErr == nil {
		logger.Infof("RS: Running COPY command for table:%s at %s\n", tableName, sanitisedSQLStmt)
	}

	_, err = tx.Exec(sqlStatement)
	if err != nil {
		logger.Errorf("RS: Error running COPY command: %v\n", err)
		tx.Rollback()
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, rs.Upload.ID, tableName, err, rs.DbHandle)
		return
	}

	primaryKey := "id"
	if column, ok := primaryKeyMap[tableName]; ok {
		primaryKey = column
	}

	sqlStatement = fmt.Sprintf(`DELETE FROM %[1]s."%[2]s" using %[1]s."%[3]s" _source where _source.%[4]s = %[1]s.%[2]s.%[4]s`, rs.Namespace, tableName, stagingTableName, primaryKey)
	logger.Infof("RS: Dedup records for table:%s using staging table: %s\n", tableName, sqlStatement)
	_, err = tx.Exec(sqlStatement)
	if err != nil {
		logger.Errorf("RS: Error deleting from original table for dedup: %v\n", err)
		tx.Rollback()
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, rs.Upload.ID, tableName, err, rs.DbHandle)
		return
	}

	var quotedColumnNames string
	for idx, str := range strkeys {
		quotedColumnNames += "\"" + str + "\""
		if idx != len(strkeys)-1 {
			quotedColumnNames += ","
		}
	}

	sqlStatement = fmt.Sprintf(`INSERT INTO "%[1]s"."%[2]s" (%[3]s) SELECT %[3]s FROM ( SELECT *, row_number() OVER (PARTITION BY %[5]s ORDER BY received_at ASC) AS _rudder_staging_row_number FROM "%[1]s"."%[4]s" ) AS _ where _rudder_staging_row_number = 1`, rs.Namespace, tableName, quotedColumnNames, stagingTableName, primaryKey)
	logger.Infof("RS: Inserting records for table:%s using staging table: %s\n", tableName, sqlStatement)
	_, err = tx.Exec(sqlStatement)

	if err != nil {
		logger.Errorf("RS: Error inserting into original table: %v\n", err)
		tx.Rollback()
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, rs.Upload.ID, tableName, err, rs.DbHandle)
		return
	}

	err = tx.Commit()
	if err != nil {
		logger.Errorf("RS: Error in transaction commit: %v\n", err)
		tx.Rollback()
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, rs.Upload.ID, tableName, err, rs.DbHandle)
		return
	}
	warehouseutils.SetTableUploadStatus(warehouseutils.ExportedDataState, rs.Upload.ID, tableName, rs.DbHandle)
	logger.Infof("RS: Complete load for table:%s\n", tableName)
	return
}

func (rs *HandleT) load() (errList []error) {
	var accessKeyID, accessKey, bucketName string
	config := rs.Warehouse.Destination.Config.(map[string]interface{})
	if config["accessKeyID"] != nil {
		accessKeyID = config["accessKeyID"].(string)
	}
	if config["accessKey"] != nil {
		accessKey = config["accessKey"].(string)
	}
	if config["bucketName"] != nil {
		bucketName = config["bucketName"].(string)
	}

	logger.Infof("RS: Starting load for all %v tables\n", len(rs.Upload.Schema))
	var wg sync.WaitGroup
	wg.Add(len(rs.Upload.Schema))
	loadChan := make(chan struct{}, maxParallelLoads)
	for tableName, columnMap := range rs.Upload.Schema {
		tName := tableName
		cMap := columnMap
		loadChan <- struct{}{}
		rruntime.Go(func() {
			err := rs.loadTable(tName, cMap, bucketName, accessKeyID, accessKey)
			if err != nil {
				errList = append(errList, err)
			}
			wg.Done()
			<-loadChan
		})
	}
	wg.Wait()
	logger.Infof("RS: Completed load for all tables\n")
	return
}

// RedshiftCredentialsT ...
type RedshiftCredentialsT struct {
	host     string
	port     string
	dbName   string
	username string
	password string
}

func connect(cred RedshiftCredentialsT) (*sql.DB, error) {
	url := fmt.Sprintf("sslmode=require user=%v password=%v host=%v port=%v dbname=%v",
		cred.username,
		cred.password,
		cred.host,
		cred.port,
		cred.dbName)

	var err error
	var db *sql.DB
	if db, err = sql.Open("postgres", url); err != nil {
		return nil, fmt.Errorf("redshift connect error : (%v)", err)
	}
	return db, nil
}

func loadConfig() {
	warehouseUploadsTable = config.GetString("Warehouse.uploadsTable", "wh_uploads")
	stagingTablePrefix = "rudder-staging-"
	maxParallelLoads = config.GetInt("Warehose.redshift.maxParallelLoads", 3)
}

func init() {
	config.Initialize()
	loadConfig()
}

func (rs *HandleT) MigrateSchema() (err error) {
	timer := warehouseutils.DestStat(stats.TimerType, "migrate_schema_time", rs.Warehouse.Destination.ID)
	timer.Start()
	warehouseutils.SetUploadStatus(rs.Upload, warehouseutils.UpdatingSchemaState, rs.DbHandle)
	logger.Infof("RS: Updaing schema for redshfit schemaname: %s", rs.Namespace)
	updatedSchema, err := rs.updateSchema()
	if err != nil {
		warehouseutils.SetUploadError(rs.Upload, err, warehouseutils.UpdatingSchemaFailedState, rs.DbHandle)
		return
	}
	err = warehouseutils.SetUploadStatus(rs.Upload, warehouseutils.UpdatedSchemaState, rs.DbHandle)
	if err != nil {
		panic(err)
	}
	err = warehouseutils.UpdateCurrentSchema(rs.Namespace, rs.Warehouse, rs.Upload.ID, rs.CurrentSchema, updatedSchema, rs.DbHandle)
	timer.End()
	if err != nil {
		warehouseutils.SetUploadError(rs.Upload, err, warehouseutils.UpdatingSchemaFailedState, rs.DbHandle)
		return
	}
	return
}

func (rs *HandleT) Export() (err error) {
	logger.Infof("RS: Starting export to redshift for source:%s and wh_upload:%v", rs.Warehouse.Source.ID, rs.Upload.ID)
	err = warehouseutils.SetUploadStatus(rs.Upload, warehouseutils.ExportingDataState, rs.DbHandle)
	if err != nil {
		panic(err)
	}
	timer := warehouseutils.DestStat(stats.TimerType, "upload_time", rs.Warehouse.Destination.ID)
	timer.Start()
	errList := rs.load()
	timer.End()
	if len(errList) > 0 {
		rs.dropDanglingStagingTables()
		errStr := ""
		for idx, err := range errList {
			errStr += err.Error()
			if idx < len(errList)-1 {
				errStr += ", "
			}
		}
		warehouseutils.SetUploadError(rs.Upload, errors.New(errStr), warehouseutils.ExportingDataFailedState, rs.DbHandle)
		return errors.New(errStr)
	}
	err = warehouseutils.SetUploadStatus(rs.Upload, warehouseutils.ExportedDataState, rs.DbHandle)
	if err != nil {
		panic(err)
	}
	return
}

func (rs *HandleT) dropDanglingStagingTables() bool {

	sqlStatement := fmt.Sprintf(`select table_name
								 from information_schema.tables
								 where table_schema = '%s' AND table_name like '%s';`, rs.Namespace, fmt.Sprintf("%s%s", stagingTablePrefix, "%"))
	rows, err := rs.Db.Query(sqlStatement)
	if err != nil {
		logger.Errorf("WH: RS:  Error dropping dangling staging tables in redshift: %v\n", err)
		return false
	}
	defer rows.Close()

	var stagingTableNames []string
	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)
		if err != nil {
			panic(err)
		}
		stagingTableNames = append(stagingTableNames, tableName)
	}
	logger.Infof("WH: RS: Dropping dangling staging tables: %+v  %+v\n", len(stagingTableNames), stagingTableNames)
	delSuccess := true
	for _, stagingTableName := range stagingTableNames {
		_, err := rs.Db.Exec(fmt.Sprintf(`DROP TABLE "%[1]s"."%[2]s"`, rs.Namespace, stagingTableName))
		if err != nil {
			logger.Errorf("WH: RS:  Error dropping dangling staging table: %s in redshift: %v\n", stagingTableName, err)
			delSuccess = false
		}
	}
	return delSuccess
}

func (rs *HandleT) CrashRecover(config warehouseutils.ConfigT) (err error) {
	rs.DbHandle = config.DbHandle
	rs.Warehouse = config.Warehouse
	rs.Db, err = connect(RedshiftCredentialsT{
		host:     rs.Warehouse.Destination.Config.(map[string]interface{})["host"].(string),
		port:     rs.Warehouse.Destination.Config.(map[string]interface{})["port"].(string),
		dbName:   rs.Warehouse.Destination.Config.(map[string]interface{})["database"].(string),
		username: rs.Warehouse.Destination.Config.(map[string]interface{})["user"].(string),
		password: rs.Warehouse.Destination.Config.(map[string]interface{})["password"].(string),
	})
	if err != nil {
		return err
	}
	currSchema, err := warehouseutils.GetCurrentSchema(rs.DbHandle, rs.Warehouse)
	if err != nil {
		panic(err)
	}
	rs.CurrentSchema = currSchema.Schema
	rs.Namespace = currSchema.Namespace
	rs.dropDanglingStagingTables()
	return
}

func (rs *HandleT) Process(config warehouseutils.ConfigT) (err error) {
	rs.DbHandle = config.DbHandle
	rs.Warehouse = config.Warehouse
	rs.Upload = config.Upload
	rs.Db, err = connect(RedshiftCredentialsT{
		host:     rs.Warehouse.Destination.Config.(map[string]interface{})["host"].(string),
		port:     rs.Warehouse.Destination.Config.(map[string]interface{})["port"].(string),
		dbName:   rs.Warehouse.Destination.Config.(map[string]interface{})["database"].(string),
		username: rs.Warehouse.Destination.Config.(map[string]interface{})["user"].(string),
		password: rs.Warehouse.Destination.Config.(map[string]interface{})["password"].(string),
	})
	if err != nil {
		warehouseutils.SetUploadError(rs.Upload, err, warehouseutils.UpdatingSchemaFailedState, rs.DbHandle)
		return err
	}
	currSchema, err := warehouseutils.GetCurrentSchema(rs.DbHandle, rs.Warehouse)
	if err != nil {
		panic(err)
	}
	rs.CurrentSchema = currSchema.Schema
	rs.Namespace = currSchema.Namespace
	if rs.Namespace == "" {
		logger.Infof("Namespace not found in currentschema for RS:%s, setting from upload: %s", rs.Warehouse.Destination.ID, rs.Upload.Namespace)
		rs.Namespace = rs.Upload.Namespace
	}

	if config.Stage == "ExportData" {
		err = rs.Export()
	} else {
		err = rs.MigrateSchema()
		if err == nil {
			err = rs.Export()
		}
	}
	return
}
