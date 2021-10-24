package deltalake

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/alexbrainman/odbc"
	_ "github.com/alexbrainman/odbc"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	uuid "github.com/satori/go.uuid"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"time"
)

// Done
var (
	stagingTablePrefix string
	pkgLogger          logger.LoggerI
)

// Done
func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("warehouse").Child("deltalake")
}

// Done
func loadConfig() {
	stagingTablePrefix = "rudder_staging_"
}

// Done
type HandleT struct {
	Db            *sql.DB
	Namespace     string
	ObjectStorage string
	Warehouse     warehouseutils.WarehouseT
	Uploader      warehouseutils.UploaderI
}

// Done
// String constants for deltalake destination config
const (
	AWSAccessKey   = "accessKey"
	AWSAccessKeyID = "accessKeyID"
	DeltaLakeHost  = "host"
	DeltaLakePort  = "port"
	DeltaLakePath  = "path"
	DeltaLakeToken = "token"
)

// Done
var dataTypesMap = map[string]string{
	"boolean":  "BOOLEAN",
	"int":      "BIGINT",
	"float":    "DOUBLE",
	"string":   "STRING",
	"datetime": "TIMESTAMP",
}

// Done
// Reference: https://docs.databricks.com/sql/language-manual/sql-ref-datatype-rules.html
var dataTypesMapToRudder = map[string]string{
	"TINYINT":   "int",
	"SMALLINT":  "int",
	"INT":       "int",
	"BIGINT":    "int",
	"DECIMAL":   "float",
	"FLOAT":     "float",
	"DOUBLE":    "float",
	"BOOLEAN":   "boolean",
	"STRING":    "string",
	"DATE":      "datetime",
	"TIMESTAMP": "datetime",
}

// Done
var primaryKeyMap = map[string]string{
	"users":                      "id",
	"identifies":                 "id",
	warehouseutils.DiscardsTable: "row_id",
}

// Done
var partitionKeyMap = map[string]string{
	"users":                      "id",
	"identifies":                 "id",
	warehouseutils.DiscardsTable: "row_id, column_name, table_name",
}

// Done
// getDeltaLakeDataType gets datatype for deltalake which is mapped with rudderstack datatype
func getDeltaLakeDataType(columnType string) string {
	return dataTypesMap[columnType]
}

// Done
func columnsWithDataTypes(columns map[string]string, prefix string) string {
	keys := []string{}
	for colName := range columns {
		keys = append(keys, colName)
	}
	sort.Strings(keys)

	arr := []string{}
	for _, name := range keys {
		arr = append(arr, fmt.Sprintf(`"%s%s" %s`, prefix, name, getDeltaLakeDataType(columns[name])))
	}
	return strings.Join(arr[:], ",")
}

// Done
func (dl *HandleT) CreateTable(tableName string, columns map[string]string) (err error) {
	name := fmt.Sprintf(`"%s"."%s"`, dl.Namespace, tableName)
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s ( %v )`, name, columnsWithDataTypes(columns, ""))
	pkgLogger.Infof("Creating table in deltalake for DL:%s : %v", dl.Warehouse.Destination.ID, sqlStatement)
	_, err = dl.Db.Exec(sqlStatement)
	return
}

// Done
func (dl *HandleT) schemaExists(schemaname string) (exists bool, err error) {
	var count int
	sqlStatement := fmt.Sprintf(`SHOW SCHEMAS LIKE '%s'`, schemaname)
	err = dl.Db.QueryRow(sqlStatement).Scan(&count)
	if err == sql.ErrNoRows {
		err = nil
	}
	exists = count > 0
	return
}

// Done
func (dl *HandleT) AddColumn(name string, columnName string, columnType string) (err error) {
	tableName := fmt.Sprintf(`"%s"."%s"`, dl.Namespace, name)
	sqlStatement := fmt.Sprintf(`ALTER TABLE %v ADD COLUMNS ( '%s' %s )`, tableName, columnName, getDeltaLakeDataType(columnType))
	pkgLogger.Infof("Adding column in deltalake for DL:%s : %v", dl.Warehouse.Destination.ID, sqlStatement)
	_, err = dl.Db.Exec(sqlStatement)
	return
}

// Done
func (dl *HandleT) createSchema() (err error) {
	sqlStatement := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s"`, dl.Namespace)
	pkgLogger.Infof("Creating schemaname in deltalake for DL:%s : %v", dl.Warehouse.Destination.ID, sqlStatement)
	_, err = dl.Db.Exec(sqlStatement)
	return
}

// Done
func (dl *HandleT) dropStagingTables(stagingTableNames []string) {
	for _, stagingTableName := range stagingTableNames {
		pkgLogger.Infof("WH: dropping table %+v\n", stagingTableName)
		_, err := dl.Db.Exec(fmt.Sprintf(`DROP TABLE "%[1]s"."%[2]s"`, dl.Namespace, stagingTableName))
		if err != nil {
			pkgLogger.Errorf("WH: DL:  Error dropping staging tables in deltalake: %v", err)
		}
	}
}

// Done
func (dl *HandleT) loadTable(tableName string, tableSchemaInUpload warehouseutils.TableSchemaT, tableSchemaAfterUpload warehouseutils.TableSchemaT, skipTempTableDelete bool) (stagingTableName string, err error) {
	keys := reflect.ValueOf(tableSchemaInUpload).MapKeys()
	strkeys := make([]string, len(keys))
	for i := 0; i < len(keys); i++ {
		strkeys[i] = fmt.Sprintf(`"%s"`, keys[i].String())
	}
	sort.Strings(strkeys)
	sortedColumnNames := strings.Join(strkeys[:], ",")

	stagingTableName = misc.TruncateStr(fmt.Sprintf(`%s%s_%s`, stagingTablePrefix, strings.Replace(uuid.NewV4().String(), "-", "", -1), tableName), 127)
	err = dl.CreateTable(stagingTableName, tableSchemaAfterUpload)
	if err != nil {
		return
	}
	if !skipTempTableDelete {
		defer dl.dropStagingTables([]string{stagingTableName})
	}

	// BEGIN TRANSACTION
	tx, err := dl.Db.Begin()
	if err != nil {
		return
	}
	// create session token and temporary credentials
	tempAccessKeyId, tempSecretAccessKey, token, err := dl.getTemporaryCredForCopy()
	if err != nil {
		pkgLogger.Errorf("DL: Failed to create temp credentials before copying, while create load for table %v, err%v", tableName, err)
		tx.Rollback()
		return
	}

	loadFiles := dl.Uploader.GetLoadFilesMetadata(warehouseutils.GetLoadFilesOptionsT{Table: tableName})
	loadFiles = warehouseutils.GetS3Locations(loadFiles)

	csvObjectLocation, err := dl.Uploader.GetSampleLoadFileLocation(tableName)
	if err != nil {
		return
	}
	loadFolder := warehouseutils.GetObjectFolder(dl.ObjectStorage, csvObjectLocation)

	sqlStatement := fmt.Sprintf("COPY INTO %v` FROM ( SELECT %v FROM '%v' )  FILEFORMAT = CSV PATTERN = ( '*.gz' ) FORMAT_OPTIONS ( 'compression' = 'gzip' ) CREDENTIALS ( 'awsKeyId' = '%v', 'awsSecretKey' = '+%v', 'awsSessionToken' = '%v' ) COPY_OPTIONS ('force' = 'false')",
		fmt.Sprintf(`"%s"."%s"`, dl.Namespace, stagingTableName), sortedColumnNames, loadFolder, tempAccessKeyId, tempSecretAccessKey, token)

	sanitisedSQLStmt, regexErr := misc.ReplaceMultiRegex(sqlStatement, map[string]string{
		"ACCESS_KEY_ID '[^']*'":     "ACCESS_KEY_ID '***'",
		"SECRET_ACCESS_KEY '[^']*'": "SECRET_ACCESS_KEY '***'",
	})
	if regexErr == nil {
		pkgLogger.Infof("DL: Running COPY command for table:%s at %s\n", tableName, sanitisedSQLStmt)
	}

	_, err = tx.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("DL: Error running COPY command: %v\n", err)
		tx.Rollback()
		return
	}

	sqlStatement = fmt.Sprintf(`MERGE INTO "%[1]s"."%[2]s" AS MAIN USING "%[1]s"."%[3]s" AS STAGING ON MAIN.row = _fivetran_staging_.row WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *`, dl.Namespace, tableName, stagingTableName)
	pkgLogger.Infof("DL: Inserting records for table:%s using staging table: %s\n", tableName, sqlStatement)
	_, err = tx.Exec(sqlStatement)

	if err != nil {
		pkgLogger.Errorf("DL: Error inserting into original table: %v\n", err)
		tx.Rollback()
		return
	}

	err = tx.Commit()
	if err != nil {
		pkgLogger.Errorf("DL: Error in transaction commit: %v\n", err)
		tx.Rollback()
		return
	}
	pkgLogger.Infof("DL: Complete load for table:%s\n", tableName)
	return
}

// Done
func (dl *HandleT) loadUserTables() (errorMap map[string]error) {
	errorMap = map[string]error{warehouseutils.IdentifiesTable: nil}
	pkgLogger.Infof("DL: Starting load for identifies and users tables\n")

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
		pkgLogger.Errorf("DL: Creating staging table for users failed: %s\n", sqlStatement)
		pkgLogger.Errorf("DL: Error creating users staging table from original table and identifies staging table: %v\n", err)
		tx.Rollback()
		errorMap[warehouseutils.UsersTable] = err
		return
	}
	defer dl.dropStagingTables([]string{stagingTableName})

	primaryKey := "id"
	sqlStatement = fmt.Sprintf(`DELETE FROM %[1]s."%[2]s" using %[1]s."%[3]s" _source where (_source.%[4]s = %[1]s.%[2]s.%[4]s)`, dl.Namespace, warehouseutils.UsersTable, stagingTableName, primaryKey)

	_, err = tx.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("DL: Dedup records for table:%s using staging table: %s\n", warehouseutils.UsersTable, sqlStatement)
		pkgLogger.Errorf("DL: Error deleting from original table for dedup: %v\n", err)
		tx.Rollback()
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	sqlStatement = fmt.Sprintf(`INSERT INTO "%[1]s"."%[2]s" (%[4]s) SELECT %[4]s FROM  "%[1]s"."%[3]s"`, dl.Namespace, warehouseutils.UsersTable, stagingTableName, warehouseutils.DoubleQuoteAndJoinByComma(append([]string{"id"}, userColNames...)))
	pkgLogger.Infof("DL: Inserting records for table:%s using staging table: %s\n", warehouseutils.UsersTable, sqlStatement)
	_, err = tx.Exec(sqlStatement)

	if err != nil {
		pkgLogger.Errorf("DL: Error inserting into users table from staging table: %v\n", err)
		tx.Rollback()
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	err = tx.Commit()
	if err != nil {
		pkgLogger.Errorf("DL: Error in transaction commit for users table: %v\n", err)
		tx.Rollback()
		errorMap[warehouseutils.UsersTable] = err
		return
	}
	return
}

// Done
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

// CredentialsT ...
type CredentialsT struct {
	host  string
	port  string
	path  string
	token string
}

// Done
// GetDriver
// macOS: /Library/simba/spark/lib/libsparkodbc_sbu.dylib
// Linux 64-bit: /opt/simba/spark/lib/64/libsparkodbc_sb64.so
// Linux 32-bit: /opt/simba/spark/lib/32/libsparkodbc_sb32.so
func GetDriver() string {
	pkgLogger.Infof("Running in platform %v", runtime.GOOS)
	return "/Library/simba/spark/lib/libsparkodbc_sbu.dylib"
}

// Done
func connect(cred CredentialsT) (*sql.DB, error) {
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

// Done
func (dl *HandleT) dropDanglingStagingTables() bool {
	sqlStatement := fmt.Sprintf(`SHOW TABLES FROM '%s' LIKE '%s';`, dl.Namespace, fmt.Sprintf("%s%s", stagingTablePrefix, "%"))
	rows, err := dl.Db.Query(sqlStatement)
	if err != nil && err != sql.ErrNoRows {
		pkgLogger.Errorf("WH: DL: Error dropping dangling staging tables in deltalake: %v\nQuery: %s\n", err, sqlStatement)
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
	pkgLogger.Infof("WH: DL: Dropping dangling staging tables: %+v  %+v\n", len(stagingTableNames), stagingTableNames)
	delSuccess := true
	for _, stagingTableName := range stagingTableNames {
		_, err := dl.Db.Exec(fmt.Sprintf(`DROP TABLE "%[1]s"."%[2]s"`, dl.Namespace, stagingTableName))
		if err != nil {
			pkgLogger.Errorf("WH: DL:  Error dropping dangling staging table: %s in deltalake: %v\n", stagingTableName, err)
			delSuccess = false
		}
	}
	return delSuccess
}

// Done
func (dl *HandleT) connectToWarehouse() (*sql.DB, error) {
	return connect(CredentialsT{
		host:  warehouseutils.GetConfigValue(DeltaLakeHost, dl.Warehouse),
		port:  warehouseutils.GetConfigValue(DeltaLakePort, dl.Warehouse),
		path:  warehouseutils.GetConfigValue(DeltaLakePath, dl.Warehouse),
		token: warehouseutils.GetConfigValue(DeltaLakeToken, dl.Warehouse),
	})
}

// Done
func (dl *HandleT) CreateSchema() (err error) {
	var schemaExists bool
	schemaExists, err = dl.schemaExists(dl.Namespace)
	if err != nil {
		pkgLogger.Errorf("DL: Error checking if schema: %s exists: %v", dl.Namespace, err)
		return err
	}
	if schemaExists {
		pkgLogger.Infof("DL: Skipping creating schema: %s since it already exists", dl.Namespace)
		return
	}
	return dl.createSchema()
}

// Done
func (dl *HandleT) AlterColumn(tableName string, columnName string, columnType string) (err error) {
	return
}

// Done
func checkAndIgnoreAlreadyExistError(err error) bool {
	if err != nil {
		if odbcErrs, ok := err.(*odbc.Error); ok {
			for _, odbcErr := range odbcErrs.Diag {
				// Checking for Table or view not found
				if odbcErr.NativeError == 31740 {
					return true
				}
			}
		}
		return false
	}
	return true
}

// Done
// FetchSchema queries delta lake and returns the schema associated with provided namespace
func (dl *HandleT) FetchSchema(warehouse warehouseutils.WarehouseT) (schema warehouseutils.SchemaT, err error) {
	dl.Warehouse = warehouse
	dl.Namespace = warehouse.Namespace
	dbHandle, err := dl.connectToWarehouse()
	if err != nil {
		return
	}
	defer dbHandle.Close()

	schema = make(warehouseutils.SchemaT)

	tSqlStatement := fmt.Sprintf(`SHOW TABLES FROM '%s'`, dl.Namespace)
	tRows, err := dbHandle.Query(tSqlStatement)
	if err != nil && err != sql.ErrNoRows {
		if checkAndIgnoreAlreadyExistError(err) {
			return schema, nil
		}
		pkgLogger.Errorf("DL: Error in fetching tables schema from deltalake destination:%v, query: %v", dl.Warehouse.Destination.ID, tSqlStatement)
		return
	}
	defer tRows.Close()

	var tableNames []string
	for tRows.Next() {
		var tableName string
		err = tRows.Scan(&tableName)
		if err != nil {
			pkgLogger.Errorf("DL: Error in processing fetched tables schema from deltalake destination:%v, tableName:%v", dl.Warehouse.Destination.ID, tableName)
			return
		}
		if strings.HasPrefix(tableName, stagingTablePrefix) {
			continue
		}
		tableNames = append(tableNames, tableName)
	}

	for _, tableName := range tableNames {
		ttSqlStatement := fmt.Sprintf(`DESCRIBE TABLE '%s'.'%s'`, dl.Namespace, tableName)
		ttRows, err := dbHandle.Query(ttSqlStatement)
		if err != nil && err != sql.ErrNoRows {
			if checkAndIgnoreAlreadyExistError(err) {
				return schema, nil
			}
			pkgLogger.Errorf("DL: Error in fetching describe table schema from deltalake destination:%v, query: %v", dl.Warehouse.Destination.ID, ttSqlStatement)
			break
		}
		if err == sql.ErrNoRows {
			pkgLogger.Infof("DL: No rows, while fetched describe table schema from destination:%v, query: %v", dl.Warehouse.Identifier, ttSqlStatement)
			return schema, nil
		}

		defer ttRows.Close()
		for ttRows.Next() {
			var col_name, data_type string
			err := ttRows.Scan(&col_name, &data_type)
			if err != nil {
				pkgLogger.Errorf("DL: Error in processing fetched describe table schema from deltalake destination:%v", dl.Warehouse.Destination.ID)
				break
			}
			if _, ok := schema[tableName]; !ok {
				schema[tableName] = make(map[string]string)
			}
			if datatype, ok := dataTypesMapToRudder[col_name]; ok {
				schema[tableName][col_name] = datatype
			}
		}
	}
	return
}

// Done
func (dl *HandleT) Setup(warehouse warehouseutils.WarehouseT, uploader warehouseutils.UploaderI) (err error) {
	dl.Warehouse = warehouse
	dl.Namespace = warehouse.Namespace
	dl.Uploader = uploader
	dl.ObjectStorage = warehouseutils.ObjectStorageType("SNOWFLAKE", warehouse.Destination.Config, dl.Uploader.UseRudderStorage())

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

// Done
func (dl *HandleT) Cleanup() {
	if dl.Db != nil {
		dl.dropDanglingStagingTables()
		dl.Db.Close()
	}
}

// Done
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

// Done
func (dl *HandleT) IsEmpty(warehouse warehouseutils.WarehouseT) (empty bool, err error) {
	return
}

// Done
func (dl *HandleT) LoadUserTables() map[string]error {
	return dl.loadUserTables()
}

// Done
func (dl *HandleT) LoadTable(tableName string) error {
	_, err := dl.loadTable(tableName, dl.Uploader.GetTableSchemaInUpload(tableName), dl.Uploader.GetTableSchemaInWarehouse(tableName), false)
	return err
}

// Done
func (dl *HandleT) LoadIdentityMergeRulesTable() (err error) {
	return
}

// Done
func (dl *HandleT) LoadIdentityMappingsTable() (err error) {
	return
}

// Done
func (dl *HandleT) DownloadIdentityRules(*misc.GZipWriter) (err error) {
	return
}

// Done
func (dl *HandleT) GetTotalCountInTable(tableName string) (total int64, err error) {
	sqlStatement := fmt.Sprintf(`SELECT COUNT(*) FROM "%[1]s"."%[2]s"`, dl.Namespace, tableName)
	err = dl.Db.QueryRow(sqlStatement).Scan(&total)
	if err != nil {
		pkgLogger.Errorf(`DL: Error getting total count in table %s:%s`, dl.Namespace, tableName)
	}
	return
}

// Done
func (dl *HandleT) Connect(warehouse warehouseutils.WarehouseT) (client.Client, error) {
	dl.Warehouse = warehouse
	dl.Namespace = warehouse.Namespace
	dbHandle, err := dl.connectToWarehouse()

	if err != nil {
		return client.Client{}, err
	}

	return client.Client{Type: client.SQLClient, SQL: dbHandle}, err
}
