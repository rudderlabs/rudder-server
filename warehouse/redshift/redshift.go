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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sts"

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

// String constants for redshift destination config
const (
	AWSAccessKey        = "accessKey"
	AWSAccessKeyID      = "accessKeyID"
	AWSBucketNameConfig = "bucketName"
	RSHost              = "host"
	RSPort              = "port"
	RSDbName            = "database"
	RSUserName          = "user"
	RSPassword          = "password"
)

var dataTypesMap = map[string]string{
	"boolean":  "boolean",
	"int":      "bigint",
	"bigint":   "bigint",
	"float":    "double precision",
	"string":   "varchar(512)",
	"datetime": "timestamp",
}

var dataTypesMapToRudder = map[string]string{
	"int":                         "int",
	"int2":                        "int",
	"int4":                        "int",
	"int8":                        "int",
	"bigint":                      "int",
	"float":                       "float",
	"float4":                      "float",
	"float8":                      "float",
	"numeric":                     "float",
	"double precision":            "float",
	"boolean":                     "boolean",
	"bool":                        "boolean",
	"text":                        "string",
	"character varying":           "string",
	"nchar":                       "string",
	"bpchar":                      "string",
	"character":                   "string",
	"nvarchar":                    "string",
	"string":                      "string",
	"date":                        "datetime",
	"timestamp without time zone": "datetime",
	"timestamp with time zone":    "datetime",
}

var primaryKeyMap = map[string]string{
	"users":                      "id",
	"identifies":                 "id",
	warehouseutils.DiscardsTable: "row_id",
}

var partitionKeyMap = map[string]string{
	"users":                      "id",
	"identifies":                 "id",
	warehouseutils.DiscardsTable: "row_id, column_name, table_name",
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
				if err != nil {
					if checkAndIgnoreAlreadyExistError(err) {
						logger.Infof("RS: Column %s already exists on %s.%s \nResponse: %v", columnName, rs.Namespace, tableName, err)
					} else {
						return nil, err
					}
				}
			}
		}
	}
	return
}

// FetchSchema queries redshift and returns the schema assoiciated with provided namespace
func (rs *HandleT) FetchSchema(warehouse warehouseutils.WarehouseT, namespace string) (schema map[string]map[string]string, err error) {
	rs.Warehouse = warehouse
	rs.Db, err = connect(RedshiftCredentialsT{
		host:     warehouseutils.GetConfigValue(RSHost, rs.Warehouse),
		port:     warehouseutils.GetConfigValue(RSPort, rs.Warehouse),
		dbName:   warehouseutils.GetConfigValue(RSDbName, rs.Warehouse),
		username: warehouseutils.GetConfigValue(RSUserName, rs.Warehouse),
		password: warehouseutils.GetConfigValue(RSPassword, rs.Warehouse),
	})
	if err != nil {
		return
	}

	schema = make(map[string]map[string]string)
	sqlStatement := fmt.Sprintf(`SELECT table_name, column_name, data_type
									FROM INFORMATION_SCHEMA.COLUMNS
									WHERE table_schema = '%s' and table_name not like '%s%s'`, namespace, stagingTablePrefix, "%")

	rows, err := rs.Db.Query(sqlStatement)
	if err != nil && err != sql.ErrNoRows {
		logger.Errorf("RS: Error in fetching schema from redshift destination:%v, query: %v", rs.Warehouse.Destination.ID, sqlStatement)
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
			logger.Errorf("RS: Error in processing fetched schema from redshift destination:%v", rs.Warehouse.Destination.ID)
			return
		}
		if _, ok := schema[tName]; !ok {
			schema[tName] = make(map[string]string)
		}
		if datatype, ok := dataTypesMapToRudder[cType]; ok {
			schema[tName][cName] = datatype
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

func (rs *HandleT) generateManifest(tableName string, columnMap map[string]string) (string, error) {
	csvObjectLocations, err := warehouseutils.GetLoadFileLocations(rs.DbHandle, rs.Warehouse.Source.ID, rs.Warehouse.Destination.ID, tableName, rs.Upload.StartLoadFileID, rs.Upload.EndLoadFileID)
	if err != nil {
		panic(err)
	}
	csvS3Locations := warehouseutils.GetS3Locations(csvObjectLocations)
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
	var accessKeyID, accessKey string
	if misc.HasAWSKeysInConfig(rs.Warehouse.Destination.Config) {
		accessKeyID = warehouseutils.GetConfigValue(AWSAccessKeyID, rs.Warehouse)
		accessKey = warehouseutils.GetConfigValue(AWSAccessKey, rs.Warehouse)
	} else {
		accessKeyID = config.GetEnv("RUDDER_AWS_S3_COPY_USER_ACCESS_KEY_ID", "")
		accessKey = config.GetEnv("RUDDER_AWS_S3_COPY_USER_ACCESS_KEY", "")
	}
	uploader, err := filemanager.New(&filemanager.SettingsT{
		Provider: "S3",
		Config: map[string]interface{}{
			"bucketName":  warehouseutils.GetConfigValue(AWSBucketNameConfig, rs.Warehouse),
			"accessKeyID": accessKeyID,
			"accessKey":   accessKey,
		},
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

func (rs *HandleT) loadTable(tableName string, columnMap map[string]string, skipTempTableDelete bool, forceLoad bool) (stagingTableName string, err error) {
	if !forceLoad {
		status, _ := warehouseutils.GetTableUploadStatus(rs.Upload.ID, tableName, rs.DbHandle)
		if status == warehouseutils.ExportedDataState {
			logger.Infof("RS: Skipping load for table:%s as it has been succesfully loaded earlier", tableName)
			return
		}
	}
	if !warehouseutils.HasLoadFiles(rs.DbHandle, rs.Warehouse.Source.ID, rs.Warehouse.Destination.ID, tableName, rs.Upload.StartLoadFileID, rs.Upload.EndLoadFileID) {
		warehouseutils.SetTableUploadStatus(warehouseutils.ExportedDataState, rs.Upload.ID, tableName, rs.DbHandle)
		return
	}

	logger.Infof("RS: Starting load for table:%s\n", tableName)
	warehouseutils.SetTableUploadStatus(warehouseutils.ExecutingState, rs.Upload.ID, tableName, rs.DbHandle)

	timer := warehouseutils.DestStat(stats.TimerType, "generate_manifest_time", rs.Warehouse.Destination.ID)
	timer.Start()
	manifestLocation, err := rs.generateManifest(tableName, columnMap)
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

	stagingTableName = misc.TruncateStr(fmt.Sprintf(`%s%s_%s`, stagingTablePrefix, strings.Replace(uuid.NewV4().String(), "-", "", -1), tableName), 127)
	err = rs.createTable(fmt.Sprintf(`"%s"."%s"`, rs.Namespace, stagingTableName), rs.CurrentSchema[tableName])
	if err != nil {
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, rs.Upload.ID, tableName, err, rs.DbHandle)
		return
	}
	if !skipTempTableDelete {
		defer rs.dropStagingTables([]string{stagingTableName})
	}

	manifestS3Location, region := warehouseutils.GetS3Location(manifestLocation)
	if region == "" {
		region = "us-east-1"
	}

	// BEGIN TRANSACTION
	tx, err := rs.Db.Begin()
	if err != nil {
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, rs.Upload.ID, tableName, err, rs.DbHandle)
		return
	}
	// create session token and temporary credentials
	tempAccessKeyId, tempSecretAccessKey, token, err := rs.getTemporaryCredForCopy()
	if err != nil {
		logger.Errorf("RS: Failed to create temp credentials before copying, while create load for table %v, err%v", tableName, err)
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, rs.Upload.ID, tableName, err, rs.DbHandle)
		return
	}

	sqlStatement := fmt.Sprintf(`COPY %v(%v) FROM '%v' CSV GZIP ACCESS_KEY_ID '%s' SECRET_ACCESS_KEY '%s' SESSION_TOKEN '%s' REGION '%s'  DATEFORMAT 'auto' TIMEFORMAT 'auto' MANIFEST TRUNCATECOLUMNS EMPTYASNULL BLANKSASNULL FILLRECORD ACCEPTANYDATE TRIMBLANKS ACCEPTINVCHARS COMPUPDATE OFF STATUPDATE OFF`, fmt.Sprintf(`"%s"."%s"`, rs.Namespace, stagingTableName), sortedColumnNames, manifestS3Location, tempAccessKeyId, tempSecretAccessKey, token, region)
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

	partitionKey := "id"
	if column, ok := partitionKeyMap[tableName]; ok {
		partitionKey = column
	}

	var additionalJoinClause string
	if tableName == warehouseutils.DiscardsTable {
		additionalJoinClause = fmt.Sprintf(`AND _source.%[3]s = %[1]s.%[2]s.%[3]s`, rs.Namespace, tableName, "table_name")
	}

	sqlStatement = fmt.Sprintf(`DELETE FROM %[1]s."%[2]s" using %[1]s."%[3]s" _source where (_source.%[4]s = %[1]s.%[2]s.%[4]s %[5]s)`, rs.Namespace, tableName, stagingTableName, primaryKey, additionalJoinClause)
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

	sqlStatement = fmt.Sprintf(`INSERT INTO "%[1]s"."%[2]s" (%[3]s) SELECT %[3]s FROM ( SELECT *, row_number() OVER (PARTITION BY %[5]s ORDER BY received_at ASC) AS _rudder_staging_row_number FROM "%[1]s"."%[4]s" ) AS _ where _rudder_staging_row_number = 1`, rs.Namespace, tableName, quotedColumnNames, stagingTableName, partitionKey)
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

func (rs *HandleT) loadUserTables() (err error) {
	logger.Infof("RS: Starting load for identifies and users tables\n")
	identifyStagingTable, err := rs.loadTable(warehouseutils.IdentifiesTable, rs.Upload.Schema[warehouseutils.IdentifiesTable], true, true)
	if err != nil {
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, rs.Upload.ID, warehouseutils.IdentifiesTable, err, rs.DbHandle)
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, rs.Upload.ID, warehouseutils.UsersTable, errors.New("Failed to upload identifies table"), rs.DbHandle)
		return
	}
	defer rs.dropStagingTables([]string{identifyStagingTable})

	if _, ok := rs.Upload.Schema["users"]; !ok {
		return
	}

	userColMap := rs.CurrentSchema["users"]
	var userColNames, firstValProps []string
	firstValPropsForIdentifies := []string{fmt.Sprintf(`FIRST_VALUE(%[1]s IGNORE NULLS) OVER (PARTITION BY anonymous_id ORDER BY received_at DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS %[1]s`, "user_id")}
	for colName := range userColMap {
		if colName == "id" {
			continue
		}
		userColNames = append(userColNames, colName)
		firstValProps = append(firstValProps, fmt.Sprintf(`FIRST_VALUE(%[1]s IGNORE NULLS) OVER (PARTITION BY id ORDER BY received_at DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS %[1]s`, colName))
		firstValPropsForIdentifies = append(firstValPropsForIdentifies, fmt.Sprintf(`FIRST_VALUE(%[1]s IGNORE NULLS) OVER (PARTITION BY anonymous_id ORDER BY received_at DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS %[1]s`, colName))
	}
	stagingTableName := misc.TruncateStr(fmt.Sprintf(`%s%s_%s`, stagingTablePrefix, strings.Replace(uuid.NewV4().String(), "-", "", -1), "users"), 127)
	sqlStatement := fmt.Sprintf(`CREATE TABLE "%[1]s"."%[2]s" AS (SELECT DISTINCT * FROM
										(
											SELECT
											id, %[3]s
											FROM (
												(
													SELECT id, %[6]s FROM "%[1]s"."%[4]s" WHERE id in (SELECT user_id FROM "%[1]s"."%[5]s" WHERE user_id IS NOT NULL)
												) UNION
												(
													SELECT user_id, %[6]s FROM (SELECT %[7]s FROM "%[1]s"."%[8]s") WHERE user_id IS NOT NULL
												)
											)
										)
									)`,
		rs.Namespace,
		stagingTableName,
		strings.Join(firstValProps, ","),
		warehouseutils.UsersTable,
		identifyStagingTable,
		strings.Join(userColNames, ","),
		strings.Join(firstValPropsForIdentifies, ","),
		warehouseutils.IdentifiesTable,
	)

	// BEGIN TRANSACTION
	tx, err := rs.Db.Begin()
	if err != nil {
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, rs.Upload.ID, warehouseutils.UsersTable, err, rs.DbHandle)
		return
	}

	logger.Infof("RS: Creating staging table for users: %s\n", sqlStatement)
	_, err = tx.Exec(sqlStatement)
	if err != nil {
		logger.Errorf("RS: Error creating users staging table from original table and identifies staging table: %v\n", err)
		tx.Rollback()
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, rs.Upload.ID, warehouseutils.UsersTable, err, rs.DbHandle)
		return
	}
	defer rs.dropStagingTables([]string{stagingTableName})

	primaryKey := "id"
	sqlStatement = fmt.Sprintf(`DELETE FROM %[1]s."%[2]s" using %[1]s."%[3]s" _source where (_source.%[4]s = %[1]s.%[2]s.%[4]s)`, rs.Namespace, warehouseutils.UsersTable, stagingTableName, primaryKey)
	logger.Infof("RS: Dedup records for table:%s using staging table: %s\n", warehouseutils.UsersTable, sqlStatement)
	_, err = tx.Exec(sqlStatement)
	if err != nil {
		logger.Errorf("RS: Error deleting from original table for dedup: %v\n", err)
		tx.Rollback()
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, rs.Upload.ID, warehouseutils.UsersTable, err, rs.DbHandle)
		return
	}

	sqlStatement = fmt.Sprintf(`INSERT INTO "%[1]s"."%[2]s" (%[4]s) SELECT %[4]s FROM  "%[1]s"."%[3]s"`, rs.Namespace, warehouseutils.UsersTable, stagingTableName, strings.Join(append([]string{"id"}, userColNames...), ","))
	logger.Infof("RS: Inserting records for table:%s using staging table: %s\n", warehouseutils.UsersTable, sqlStatement)
	_, err = tx.Exec(sqlStatement)

	if err != nil {
		logger.Errorf("RS: Error inserting into users table from staging table: %v\n", err)
		tx.Rollback()
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, rs.Upload.ID, warehouseutils.UsersTable, err, rs.DbHandle)
		return
	}

	err = tx.Commit()
	if err != nil {
		logger.Errorf("RS: Error in transaction commit for users table: %v\n", err)
		tx.Rollback()
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, rs.Upload.ID, warehouseutils.UsersTable, err, rs.DbHandle)
		return
	}
	warehouseutils.SetTableUploadStatus(warehouseutils.ExportedDataState, rs.Upload.ID, warehouseutils.UsersTable, rs.DbHandle)
	return
}

func (rs *HandleT) load() (errList []error) {
	logger.Infof("RS: Starting load for all %v tables\n", len(rs.Upload.Schema))
	if _, ok := rs.Upload.Schema["identifies"]; ok {
		err := rs.loadUserTables()
		if err != nil {
			errList = append(errList, err)
		}
	}
	var wg sync.WaitGroup
	wg.Add(len(rs.Upload.Schema))
	loadChan := make(chan struct{}, maxParallelLoads)
	for tableName, columnMap := range rs.Upload.Schema {
		if tableName == "users" || tableName == "identifies" {
			wg.Done()
			continue
		}
		tName := tableName
		cMap := columnMap
		loadChan <- struct{}{}
		rruntime.Go(func() {
			_, err := rs.loadTable(tName, cMap, false, false)
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

func (rs *HandleT) getTemporaryCredForCopy() (string, string, string, error) {

	var accessKey, accessKeyID string
	if misc.HasAWSKeysInConfig(rs.Warehouse.Destination.Config) {
		accessKey = warehouseutils.GetConfigValue(AWSAccessKey, rs.Warehouse)
		accessKeyID = warehouseutils.GetConfigValue(AWSAccessKeyID, rs.Warehouse)
	} else {
		accessKeyID = config.GetEnv("RUDDER_AWS_S3_COPY_USER_ACCESS_KEY_ID", "")
		accessKey = config.GetEnv("RUDDER_AWS_S3_COPY_USER_ACCESS_KEY", "")
	}
	mySession := session.Must(session.NewSession())
	// Create a STS client from just a session.
	svc := sts.New(mySession, aws.NewConfig().WithCredentials(credentials.NewStaticCredentials(accessKeyID, accessKey, "")))

	//sts.New(mySession, aws.NewConfig().WithRegion("us-west-2"))
	SessionTokenOutput, err := svc.GetSessionToken(&sts.GetSessionTokenInput{})
	if err != nil {
		return "", "", "", err
	}
	return *SessionTokenOutput.Credentials.AccessKeyId, *SessionTokenOutput.Credentials.SecretAccessKey, *SessionTokenOutput.Credentials.SessionToken, err
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
	stagingTablePrefix = "rudder_staging_"
	maxParallelLoads = config.GetInt("Warehouse.redshift.maxParallelLoads", 3)
}

func init() {
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
	rs.CurrentSchema = updatedSchema
	err = warehouseutils.SetUploadStatus(rs.Upload, warehouseutils.UpdatedSchemaState, rs.DbHandle)
	if err != nil {
		panic(err)
	}
	err = warehouseutils.UpdateCurrentSchema(rs.Namespace, rs.Warehouse, rs.Upload.ID, updatedSchema, rs.DbHandle)
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
		host:     warehouseutils.GetConfigValue(RSHost, rs.Warehouse),
		port:     warehouseutils.GetConfigValue(RSPort, rs.Warehouse),
		dbName:   warehouseutils.GetConfigValue(RSDbName, rs.Warehouse),
		username: warehouseutils.GetConfigValue(RSUserName, rs.Warehouse),
		password: warehouseutils.GetConfigValue(RSPassword, rs.Warehouse),
	})
	if err != nil {
		return err
	}
	currSchema, err := warehouseutils.GetCurrentSchema(rs.DbHandle, rs.Warehouse)
	if err != nil {
		panic(err)
	}
	rs.CurrentSchema = currSchema
	rs.Namespace = rs.Upload.Namespace
	rs.dropDanglingStagingTables()
	return
}

func (rs *HandleT) Process(config warehouseutils.ConfigT) (err error) {
	rs.DbHandle = config.DbHandle
	rs.Warehouse = config.Warehouse
	rs.Upload = config.Upload
	rs.Db, err = connect(RedshiftCredentialsT{
		host:     warehouseutils.GetConfigValue(RSHost, rs.Warehouse),
		port:     warehouseutils.GetConfigValue(RSPort, rs.Warehouse),
		dbName:   warehouseutils.GetConfigValue(RSDbName, rs.Warehouse),
		username: warehouseutils.GetConfigValue(RSUserName, rs.Warehouse),
		password: warehouseutils.GetConfigValue(RSPassword, rs.Warehouse),
	})
	if err != nil {
		warehouseutils.SetUploadError(rs.Upload, err, warehouseutils.UpdatingSchemaFailedState, rs.DbHandle)
		return err
	}
	defer rs.Db.Close()
	currSchema, err := warehouseutils.GetCurrentSchema(rs.DbHandle, rs.Warehouse)
	if err != nil {
		panic(err)
	}
	rs.CurrentSchema = currSchema
	rs.Namespace = rs.Upload.Namespace
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
