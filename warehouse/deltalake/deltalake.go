package deltalake

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/alexbrainman/odbc"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	uuid "github.com/satori/go.uuid"
	"github.com/tidwall/gjson"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"
)

var (
	stagingTablePrefix string
	pkgLogger          logger.LoggerI
)

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("warehouse").Child("deltalake")
}

func loadConfig() {
	stagingTablePrefix = "rudder_staging_"
}

type HandleT struct {
	Db        *sql.DB
	Namespace string
	Warehouse warehouseutils.WarehouseT
	Uploader  warehouseutils.UploaderI
}

// String constants for deltalake destination config
const (
	AWSAccessKey        = "accessKey"
	AWSAccessKeyID      = "accessKeyID"
	AWSBucketNameConfig = "bucketName"
	DeltaLakeHost       = "host"
	DeltaLakePort       = "port"
	DeltaLakePath       = "path"
	DeltaLakeToken      = "token"
)

const PROVIDER = "DELTALAKE"

var dataTypesMap = map[string]string{
	"boolean":  "boolean encode runlength",
	"int":      "bigint",
	"bigint":   "bigint",
	"float":    "double precision",
	"string":   "varchar(512)",
	"text":     "varchar(max)",
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

// getRSDataType gets datatype for deltalake which is mapped with rudderstack datatype
func getRSDataType(columnType string) string {
	return dataTypesMap[columnType]
}

func columnsWithDataTypes(columns map[string]string, prefix string) string {
	// TODO: do we need sorted order here?
	keys := []string{}
	for colName := range columns {
		keys = append(keys, colName)
	}
	sort.Strings(keys)

	arr := []string{}
	for _, name := range keys {
		arr = append(arr, fmt.Sprintf(`"%s%s" %s`, prefix, name, getRSDataType(columns[name])))
	}
	return strings.Join(arr[:], ",")
}

func (dl *HandleT) CreateTable(tableName string, columns map[string]string) (err error) {
	name := fmt.Sprintf(`"%s"."%s"`, dl.Namespace, tableName)
	sortKeyField := "received_at"
	if _, ok := columns["received_at"]; !ok {
		sortKeyField = "uuid_ts"
		if _, ok = columns["uuid_ts"]; !ok {
			sortKeyField = "id"
		}
	}
	var distKeySql string
	if _, ok := columns["id"]; ok {
		distKeySql = `DISTSTYLE KEY DISTKEY("id")`
	}
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s ( %v ) %s SORTKEY("%s") `, name, columnsWithDataTypes(columns, ""), distKeySql, sortKeyField)
	pkgLogger.Infof("Creating table in deltalake for DELTALAKE:%s : %v", dl.Warehouse.Destination.ID, sqlStatement)
	_, err = dl.Db.Exec(sqlStatement)
	return
}

func (dl *HandleT) schemaExists(schemaname string) (exists bool, err error) {
	sqlStatement := fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = '%s');`, dl.Namespace)
	err = dl.Db.QueryRow(sqlStatement).Scan(&exists)
	return
}

func (dl *HandleT) AddColumn(name string, columnName string, columnType string) (err error) {
	tableName := fmt.Sprintf(`"%s"."%s"`, dl.Namespace, name)
	sqlStatement := fmt.Sprintf(`ALTER TABLE %v ADD COLUMN "%s" %s`, tableName, columnName, getRSDataType(columnType))
	pkgLogger.Infof("Adding column in deltalake for DELTALAKE:%s : %v", dl.Warehouse.Destination.ID, sqlStatement)
	_, err = dl.Db.Exec(sqlStatement)
	return
}

func (dl *HandleT) createSchema() (err error) {
	sqlStatement := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s"`, dl.Namespace)
	pkgLogger.Infof("Creating schemaname in deltalake for DELTALAKE:%s : %v", dl.Warehouse.Destination.ID, sqlStatement)
	_, err = dl.Db.Exec(sqlStatement)
	return
}

type S3ManifestEntryMetadataT struct {
	ContentLength int64 `json:"content_length"`
}

type S3ManifestEntryT struct {
	Url       string                   `json:"url"`
	Mandatory bool                     `json:"mandatory"`
	Metadata  S3ManifestEntryMetadataT `json:"meta"`
}

type S3ManifestT struct {
	Entries []S3ManifestEntryT `json:"entries"`
}

func (dl *HandleT) generateManifest(tableName string, columnMap map[string]string) (string, error) {
	loadFiles := dl.Uploader.GetLoadFilesMetadata(warehouseutils.GetLoadFilesOptionsT{Table: tableName})
	loadFiles = warehouseutils.GetS3Locations(loadFiles)
	var manifest S3ManifestT
	for idx, loadFile := range loadFiles {
		manifestEntry := S3ManifestEntryT{Url: loadFile.Location, Mandatory: true}
		// add contentLength to manifest entry if it exists
		contentLength := gjson.Get(string(loadFiles[idx].Metadata), "content_length")
		if contentLength.Exists() {
			manifestEntry.Metadata.ContentLength = contentLength.Int()
		}
		manifest.Entries = append(manifest.Entries, manifestEntry)
	}
	pkgLogger.Infof("DELTALAKE: Generated manifest for table:%s", tableName)
	manifestJSON, _ := json.Marshal(&manifest)

	manifestFolder := "rudder-deltalake-manifests"
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
	defer misc.RemoveFilePaths(localManifestPath)
	_ = os.WriteFile(localManifestPath, manifestJSON, 0644)

	file, err := os.Open(localManifestPath)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	uploader, _ := filemanager.New(&filemanager.SettingsT{
		Provider: "S3",
		Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
			Provider:         "S3",
			Config:           dl.Warehouse.Destination.Config,
			UseRudderStorage: dl.Uploader.UseRudderStorage(),
		}),
	})

	uploadOutput, err := uploader.Upload(file, manifestFolder, dl.Warehouse.Source.ID, dl.Warehouse.Destination.ID, time.Now().Format("01-02-2006"), tableName, uuid.NewV4().String())

	if err != nil {
		return "", err
	}

	return uploadOutput.Location, nil
}

func (dl *HandleT) dropStagingTables(stagingTableNames []string) {
	for _, stagingTableName := range stagingTableNames {
		pkgLogger.Infof("WH: dropping table %+v\n", stagingTableName)
		_, err := dl.Db.Exec(fmt.Sprintf(`DROP TABLE "%[1]s"."%[2]s"`, dl.Namespace, stagingTableName))
		if err != nil {
			pkgLogger.Errorf("WH: DELTALAKE:  Error dropping staging tables in deltalake: %v", err)
		}
	}
}

func (dl *HandleT) loadTable(tableName string, tableSchemaInUpload warehouseutils.TableSchemaT, tableSchemaAfterUpload warehouseutils.TableSchemaT, skipTempTableDelete bool) (stagingTableName string, err error) {
	manifestLocation, err := dl.generateManifest(tableName, tableSchemaInUpload)
	if err != nil {
		return
	}
	pkgLogger.Infof("DELTALAKE: Generated and stored manifest for table:%s at %s\n", tableName, manifestLocation)

	// sort columnnames
	keys := reflect.ValueOf(tableSchemaInUpload).MapKeys()
	strkeys := make([]string, len(keys))
	for i := 0; i < len(keys); i++ {
		strkeys[i] = keys[i].String()
	}
	sort.Strings(strkeys)
	var sortedColumnNames string
	//TODO: use strings.Join() instead
	for index, key := range strkeys {
		if index > 0 {
			sortedColumnNames += `, `
		}
		sortedColumnNames += fmt.Sprintf(`"%s"`, key)
	}

	stagingTableName = misc.TruncateStr(fmt.Sprintf(`%s%s_%s`, stagingTablePrefix, strings.Replace(uuid.NewV4().String(), "-", "", -1), tableName), 127)
	err = dl.CreateTable(stagingTableName, tableSchemaAfterUpload)
	if err != nil {
		return
	}
	if !skipTempTableDelete {
		defer dl.dropStagingTables([]string{stagingTableName})
	}

	manifestS3Location, region := warehouseutils.GetS3Location(manifestLocation)
	if region == "" {
		region = "us-east-1"
	}

	// BEGIN TRANSACTION
	tx, err := dl.Db.Begin()
	if err != nil {
		return
	}
	// create session token and temporary credentials
	tempAccessKeyId, tempSecretAccessKey, token, err := dl.getTemporaryCredForCopy()
	if err != nil {
		pkgLogger.Errorf("DELTALAKE: Failed to create temp credentials before copying, while create load for table %v, err%v", tableName, err)
		tx.Rollback()
		return
	}

	var sqlStatement string
	if dl.Uploader.GetLoadFileType() == warehouseutils.LOAD_FILE_TYPE_PARQUET {
		// copy statement for parquet load files
		sqlStatement = fmt.Sprintf(`COPY %v FROM '%s' ACCESS_KEY_ID '%s' SECRET_ACCESS_KEY '%s' SESSION_TOKEN '%s' MANIFEST FORMAT PARQUET`, fmt.Sprintf(`"%s"."%s"`, dl.Namespace, stagingTableName), manifestS3Location, tempAccessKeyId, tempSecretAccessKey, token)
	} else {
		// copy statement for csv load files
		sqlStatement = fmt.Sprintf(`COPY %v(%v) FROM '%v' CSV GZIP ACCESS_KEY_ID '%s' SECRET_ACCESS_KEY '%s' SESSION_TOKEN '%s' REGION '%s'  DATEFORMAT 'auto' TIMEFORMAT 'auto' MANIFEST TRUNCATECOLUMNS EMPTYASNULL BLANKSASNULL FILLRECORD ACCEPTANYDATE TRIMBLANKS ACCEPTINVCHARS COMPUPDATE OFF STATUPDATE OFF`,
			fmt.Sprintf(`"%s"."%s"`, dl.Namespace, stagingTableName), sortedColumnNames, manifestS3Location, tempAccessKeyId, tempSecretAccessKey, token, region)
	}

	sanitisedSQLStmt, regexErr := misc.ReplaceMultiRegex(sqlStatement, map[string]string{
		"ACCESS_KEY_ID '[^']*'":     "ACCESS_KEY_ID '***'",
		"SECRET_ACCESS_KEY '[^']*'": "SECRET_ACCESS_KEY '***'",
	})
	if regexErr == nil {
		pkgLogger.Infof("DELTALAKE: Running COPY command for table:%s at %s\n", tableName, sanitisedSQLStmt)
	}

	_, err = tx.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("DELTALAKE: Error running COPY command: %v\n", err)
		tx.Rollback()
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
		additionalJoinClause = fmt.Sprintf(`AND _source.%[3]s = %[1]s.%[2]s.%[3]s AND _source.%[4]s = %[1]s.%[2]s.%[4]s`, dl.Namespace, tableName, "table_name", "column_name")
	}

	sqlStatement = fmt.Sprintf(`DELETE FROM %[1]s."%[2]s" using %[1]s."%[3]s" _source where (_source.%[4]s = %[1]s.%[2]s.%[4]s %[5]s)`, dl.Namespace, tableName, stagingTableName, primaryKey, additionalJoinClause)
	pkgLogger.Infof("DELTALAKE: Dedup records for table:%s using staging table: %s\n", tableName, sqlStatement)
	_, err = tx.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("DELTALAKE: Error deleting from original table for dedup: %v\n", err)
		tx.Rollback()
		return
	}

	quotedColumnNames := warehouseutils.DoubleQuoteAndJoinByComma(strkeys)

	sqlStatement = fmt.Sprintf(`INSERT INTO "%[1]s"."%[2]s" (%[3]s) SELECT %[3]s FROM ( SELECT *, row_number() OVER (PARTITION BY %[5]s ORDER BY received_at ASC) AS _rudder_staging_row_number FROM "%[1]s"."%[4]s" ) AS _ where _rudder_staging_row_number = 1`, dl.Namespace, tableName, quotedColumnNames, stagingTableName, partitionKey)
	pkgLogger.Infof("DELTALAKE: Inserting records for table:%s using staging table: %s\n", tableName, sqlStatement)
	_, err = tx.Exec(sqlStatement)

	if err != nil {
		pkgLogger.Errorf("DELTALAKE: Error inserting into original table: %v\n", err)
		tx.Rollback()
		return
	}

	err = tx.Commit()
	if err != nil {
		pkgLogger.Errorf("DELTALAKE: Error in transaction commit: %v\n", err)
		tx.Rollback()
		return
	}
	pkgLogger.Infof("DELTALAKE: Complete load for table:%s\n", tableName)
	return
}

func (dl *HandleT) loadUserTables() (errorMap map[string]error) {
	errorMap = map[string]error{warehouseutils.IdentifiesTable: nil}
	pkgLogger.Infof("DELTALAKE: Starting load for identifies and users tables\n")

	identifyStagingTable, err := dl.loadTable(warehouseutils.IdentifiesTable, dl.Uploader.GetTableSchemaInUpload(warehouseutils.IdentifiesTable), dl.Uploader.GetTableSchemaInWarehouse(warehouseutils.IdentifiesTable), true)
	if err != nil {
		errorMap[warehouseutils.IdentifiesTable] = err
		return
	}
	defer dl.dropStagingTables([]string{identifyStagingTable})

	if len(dl.Uploader.GetTableSchemaInUpload(warehouseutils.UsersTable)) == 0 {
		return
	}
	errorMap[warehouseutils.UsersTable] = nil

	userColMap := dl.Uploader.GetTableSchemaInWarehouse(warehouseutils.UsersTable)
	var userColNames, firstValProps []string
	for colName := range userColMap {
		// do not reference uuid in queries as it can be an autoincrementing field set by segment compatible tables
		if colName == "id" || colName == "user_id" || colName == "uuid" {
			continue
		}
		userColNames = append(userColNames, colName)
		firstValProps = append(firstValProps, fmt.Sprintf(`FIRST_VALUE("%[1]s" IGNORE NULLS) OVER (PARTITION BY id ORDER BY received_at DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "%[1]s"`, colName))
	}
	quotedUserColNames := warehouseutils.DoubleQuoteAndJoinByComma(userColNames)
	stagingTableName := misc.TruncateStr(fmt.Sprintf(`%s%s_%s`, stagingTablePrefix, strings.Replace(uuid.NewV4().String(), "-", "", -1), warehouseutils.UsersTable), 127)

	sqlStatement := fmt.Sprintf(`CREATE TABLE "%[1]s"."%[2]s" AS (SELECT DISTINCT * FROM
										(
											SELECT
											id, %[3]s
											FROM (
												(
													SELECT id, %[6]s FROM "%[1]s"."%[4]s" WHERE id in (SELECT DISTINCT(user_id) FROM "%[1]s"."%[5]s" WHERE user_id IS NOT NULL)
												) UNION
												(
													SELECT user_id, %[6]s FROM "%[1]s"."%[5]s" WHERE user_id IS NOT NULL
												)
											)
										)
									)`,
		dl.Namespace,                     // 1
		stagingTableName,                 // 2
		strings.Join(firstValProps, ","), // 3
		warehouseutils.UsersTable,        // 4
		identifyStagingTable,             // 5
		quotedUserColNames,               // 6
	)

	// BEGIN TRANSACTION
	tx, err := dl.Db.Begin()
	if err != nil {
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	_, err = tx.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("DELTALAKE: Creating staging table for users failed: %s\n", sqlStatement)
		pkgLogger.Errorf("DELTALAKE: Error creating users staging table from original table and identifies staging table: %v\n", err)
		tx.Rollback()
		errorMap[warehouseutils.UsersTable] = err
		return
	}
	defer dl.dropStagingTables([]string{stagingTableName})

	primaryKey := "id"
	sqlStatement = fmt.Sprintf(`DELETE FROM %[1]s."%[2]s" using %[1]s."%[3]s" _source where (_source.%[4]s = %[1]s.%[2]s.%[4]s)`, dl.Namespace, warehouseutils.UsersTable, stagingTableName, primaryKey)

	_, err = tx.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("DELTALAKE: Dedup records for table:%s using staging table: %s\n", warehouseutils.UsersTable, sqlStatement)
		pkgLogger.Errorf("DELTALAKE: Error deleting from original table for dedup: %v\n", err)
		tx.Rollback()
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	sqlStatement = fmt.Sprintf(`INSERT INTO "%[1]s"."%[2]s" (%[4]s) SELECT %[4]s FROM  "%[1]s"."%[3]s"`, dl.Namespace, warehouseutils.UsersTable, stagingTableName, warehouseutils.DoubleQuoteAndJoinByComma(append([]string{"id"}, userColNames...)))
	pkgLogger.Infof("DELTALAKE: Inserting records for table:%s using staging table: %s\n", warehouseutils.UsersTable, sqlStatement)
	_, err = tx.Exec(sqlStatement)

	if err != nil {
		pkgLogger.Errorf("DELTALAKE: Error inserting into users table from staging table: %v\n", err)
		tx.Rollback()
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	err = tx.Commit()
	if err != nil {
		pkgLogger.Errorf("DELTALAKE: Error in transaction commit for users table: %v\n", err)
		tx.Rollback()
		errorMap[warehouseutils.UsersTable] = err
		return
	}
	return
}

func (dl *HandleT) getTemporaryCredForCopy() (string, string, string, error) {
	var accessKey, accessKeyID string
	if misc.HasAWSKeysInConfig(dl.Warehouse.Destination.Config) && !misc.IsConfiguredToUseRudderObjectStorage(dl.Warehouse.Destination.Config) {
		accessKey = warehouseutils.GetConfigValue(AWSAccessKey, dl.Warehouse)
		accessKeyID = warehouseutils.GetConfigValue(AWSAccessKeyID, dl.Warehouse)
	} else {
		accessKeyID = config.GetEnv("RUDDER_AWS_S3_COPY_USER_ACCESS_KEY_ID", "")
		accessKey = config.GetEnv("RUDDER_AWS_S3_COPY_USER_ACCESS_KEY", "")
	}
	mySession := session.Must(session.NewSession())
	// Create a STS client from just a session.
	svc := sts.New(mySession, aws.NewConfig().WithCredentials(credentials.NewStaticCredentials(accessKeyID, accessKey, "")))

	//sts.New(mySession, aws.NewConfig().WithRegion("us-west-2"))
	SessionTokenOutput, err := svc.GetSessionToken(&sts.GetSessionTokenInput{DurationSeconds: &warehouseutils.AWSCredsExpiryInS})
	if err != nil {
		return "", "", "", err
	}
	return *SessionTokenOutput.Credentials.AccessKeyId, *SessionTokenOutput.Credentials.SecretAccessKey, *SessionTokenOutput.Credentials.SessionToken, err
}

// DeltaLakeCredentialsT ...
type DeltaLakeCredentialsT struct {
	host  string
	port  string
	path  string
	token string
}

func GetDriver() string {
	return "/Library/simba/spark/lib/libsparkodbc_sbu.dylib"
}

func connect(cred DeltaLakeCredentialsT) (*sql.DB, error) {
	dsn := fmt.Sprintf("Driver=%v;HOST=%v;PORT=%v;Schema=default;SparkServerType=3;AuthMech=3;UID=token;PWD=%v;ThriftTransport=2;SSL=1;HTTPPath=%v",
		GetDriver(),
		cred.host,
		cred.port,
		cred.token,
		cred.path)

	var err error
	var db *sql.DB
	if db, err = sql.Open("odbc", dsn); err != nil {
		return nil, fmt.Errorf("deltalake connect error : (%v)", err)
	}
	return db, nil
}

func (dl *HandleT) dropDanglingStagingTables() bool {
	sqlStatement := fmt.Sprintf(`select table_name
								 from information_schema.tables
								 where table_schema = '%s' AND table_name like '%s';`, dl.Namespace, fmt.Sprintf("%s%s", stagingTablePrefix, "%"))
	rows, err := dl.Db.Query(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("WH: DELTALAKE: Error dropping dangling staging tables in deltalake: %v\nQuery: %s\n", err, sqlStatement)
		return false
	}
	defer rows.Close()

	var stagingTableNames []string
	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)
		if err != nil {
			panic(fmt.Errorf("Failed to scan result from query: %s\nwith Error : %w", sqlStatement, err))
		}
		stagingTableNames = append(stagingTableNames, tableName)
	}
	pkgLogger.Infof("WH: DELTALAKE: Dropping dangling staging tables: %+v  %+v\n", len(stagingTableNames), stagingTableNames)
	delSuccess := true
	for _, stagingTableName := range stagingTableNames {
		_, err := dl.Db.Exec(fmt.Sprintf(`DROP TABLE "%[1]s"."%[2]s"`, dl.Namespace, stagingTableName))
		if err != nil {
			pkgLogger.Errorf("WH: DELTALAKE:  Error dropping dangling staging table: %s in deltalake: %v\n", stagingTableName, err)
			delSuccess = false
		}
	}
	return delSuccess
}

func (dl *HandleT) connectToWarehouse() (*sql.DB, error) {
	return connect(DeltaLakeCredentialsT{
		host:  warehouseutils.GetConfigValue(DeltaLakeHost, dl.Warehouse),
		port:  warehouseutils.GetConfigValue(DeltaLakePort, dl.Warehouse),
		path:  warehouseutils.GetConfigValue(DeltaLakePath, dl.Warehouse),
		token: warehouseutils.GetConfigValue(DeltaLakeToken, dl.Warehouse),
	})
}

func (dl *HandleT) CreateSchema() (err error) {
	var schemaExists bool
	schemaExists, err = dl.schemaExists(dl.Namespace)
	if err != nil {
		pkgLogger.Errorf("DELTALAKE: Error checking if schema: %s exists: %v", dl.Namespace, err)
		return err
	}
	if schemaExists {
		pkgLogger.Infof("DELTALAKE: Skipping creating schema: %s since it already exists", dl.Namespace)
		return
	}
	return dl.createSchema()
}

func (dl *HandleT) AlterColumn(tableName string, columnName string, columnType string) (err error) {
	return
}

// FetchSchema queries deltalake and returns the schema assoiciated with provided namespace
func (dl *HandleT) FetchSchema(warehouse warehouseutils.WarehouseT) (schema warehouseutils.SchemaT, err error) {
	dl.Warehouse = warehouse
	dl.Namespace = warehouse.Namespace
	dbHandle, err := dl.connectToWarehouse()
	if err != nil {
		return
	}
	defer dbHandle.Close()

	schema = make(warehouseutils.SchemaT)
	sqlStatement := fmt.Sprintf(`SELECT table_name, column_name, data_type, character_maximum_length
									FROM INFORMATION_SCHEMA.COLUMNS
									WHERE table_schema = '%s' and table_name not like '%s%s'`, dl.Namespace, stagingTablePrefix, "%")

	rows, err := dbHandle.Query(sqlStatement)
	if err != nil && err != sql.ErrNoRows {
		pkgLogger.Errorf("DELTALAKE: Error in fetching schema from deltalake destination:%v, query: %v", dl.Warehouse.Destination.ID, sqlStatement)
		return
	}
	if err == sql.ErrNoRows {
		pkgLogger.Infof("DELTALAKE: No rows, while fetching schema from  destination:%v, query: %v", dl.Warehouse.Identifier, sqlStatement)
		return schema, nil
	}
	defer rows.Close()
	for rows.Next() {
		var tName, cName, cType string
		var charLength sql.NullInt64
		err = rows.Scan(&tName, &cName, &cType, &charLength)
		if err != nil {
			pkgLogger.Errorf("DELTALAKE: Error in processing fetched schema from deltalake destination:%v", dl.Warehouse.Destination.ID)
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

func (dl *HandleT) Setup(warehouse warehouseutils.WarehouseT, uploader warehouseutils.UploaderI) (err error) {
	dl.Warehouse = warehouse
	dl.Namespace = warehouse.Namespace
	dl.Uploader = uploader

	dl.Db, err = dl.connectToWarehouse()
	return err
}

func (dl *HandleT) TestConnection(warehouse warehouseutils.WarehouseT) (err error) {
	dl.Warehouse = warehouse
	dl.Db, err = dl.connectToWarehouse()
	if err != nil {
		return
	}
	defer dl.Db.Close()
	timeOut := 5 * time.Second

	ctx, cancel := context.WithTimeout(context.TODO(), timeOut)
	defer cancel()

	err = dl.Db.PingContext(ctx)
	if err == context.DeadlineExceeded {
		return fmt.Errorf("connection testing timed out after %d sec", timeOut/time.Second)
	}
	if err != nil {
		return err
	}

	return
}

func (dl *HandleT) Cleanup() {
	if dl.Db != nil {
		dl.dropDanglingStagingTables()
		dl.Db.Close()
	}
}

func (dl *HandleT) CrashRecover(warehouse warehouseutils.WarehouseT) (err error) {
	dl.Warehouse = warehouse
	dl.Namespace = warehouse.Namespace
	dl.Db, err = dl.connectToWarehouse()
	if err != nil {
		return err
	}
	defer dl.Db.Close()
	dl.dropDanglingStagingTables()
	return
}

func (dl *HandleT) IsEmpty(warehouse warehouseutils.WarehouseT) (empty bool, err error) {
	return
}

func (dl *HandleT) LoadUserTables() map[string]error {
	return dl.loadUserTables()
}

func (dl *HandleT) LoadTable(tableName string) error {
	_, err := dl.loadTable(tableName, dl.Uploader.GetTableSchemaInUpload(tableName), dl.Uploader.GetTableSchemaInWarehouse(tableName), false)
	return err
}

func (dl *HandleT) LoadIdentityMergeRulesTable() (err error) {
	return
}

func (dl *HandleT) LoadIdentityMappingsTable() (err error) {
	return
}

func (dl *HandleT) DownloadIdentityRules(*misc.GZipWriter) (err error) {
	return
}

func (dl *HandleT) GetTotalCountInTable(tableName string) (total int64, err error) {
	sqlStatement := fmt.Sprintf(`SELECT count(*) FROM "%[1]s"."%[2]s"`, dl.Namespace, tableName)
	err = dl.Db.QueryRow(sqlStatement).Scan(&total)
	if err != nil {
		pkgLogger.Errorf(`DELTALAKE: Error getting total count in table %s:%s`, dl.Namespace, tableName)
	}
	return
}

func (dl *HandleT) Connect(warehouse warehouseutils.WarehouseT) (client.Client, error) {
	dl.Warehouse = warehouse
	dl.Namespace = warehouse.Namespace
	dbHandle, err := dl.connectToWarehouse()

	if err != nil {
		return client.Client{}, err
	}

	return client.Client{Type: client.SQLClient, SQL: dbHandle}, err
}
