package deltalake

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/alexbrainman/odbc"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/satori/go.uuid"
	"strings"
	"time"
)

// ODBC NATIVE ERRORS
const (
	odbcTableOrViewNotFound = 31740
	odbcDBNotFound          = 80
)

// Database configuration
const (
	DLHost  = "host"
	DLPort  = "port"
	DLPath  = "path"
	DLToken = "token"
)

var (
	stagingTablePrefix string
	driverPath         string
	pkgLogger          logger.LoggerI
)

// Rudder data type mapping with Delta lake mappings.
var dataTypesMap = map[string]string{
	"boolean":  "BOOLEAN",
	"int":      "BIGINT",
	"float":    "DOUBLE",
	"string":   "STRING",
	"datetime": "TIMESTAMP",
}

// Delta Lake mapping with rudder data types mappings.
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
	"tinyint":   "int",
	"smallint":  "int",
	"int":       "int",
	"bigint":    "int",
	"decimal":   "float",
	"float":     "float",
	"double":    "float",
	"boolean":   "boolean",
	"string":    "string",
	"date":      "datetime",
	"timestamp": "datetime",
}

// Primary Key mappings for tables
var primaryKeyMap = map[string]string{
	warehouseutils.UsersTable:      "id",
	warehouseutils.IdentifiesTable: "id",
	warehouseutils.DiscardsTable:   "row_id",
}

type HandleT struct {
	Db            *sql.DB
	Namespace     string
	ObjectStorage string
	Warehouse     warehouseutils.WarehouseT
	Uploader      warehouseutils.UploaderI
}

type CredentialsT struct {
	host  string
	port  string
	path  string
	token string
}

// Init initializes the delta lake warehouse
func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("warehouse").Child("deltalake")
}

// loadConfig loads config
func loadConfig() {
	stagingTablePrefix = "rudder_staging_"
	config.RegisterStringConfigVariable("/opt/simba/spark/lib/64/libsparkodbc_sb64.so", &driverPath, false, "Warehouse.deltalake.driverPath") // Reference: https://docs.databricks.com/integrations/bi/jdbc-odbc-bi.html
}

// getDeltaLakeDataType returns datatype for delta lake which is mapped with rudder stack datatype
func getDeltaLakeDataType(columnType string) string {
	return dataTypesMap[columnType]
}

// columnsWithDataTypes returns columns with specified prefix and data type
func columnsWithDataTypes(columns map[string]string, prefix string) string {
	keys := warehouseutils.SortColumnKeysFromColumnMap(columns)
	format := func(idx int, name string) string {
		return fmt.Sprintf(`%s%s %s`, prefix, name, getDeltaLakeDataType(columns[name]))
	}
	return warehouseutils.JoinWithFormatting(keys, format, ",")
}

// columnNames returns joined column with comma separated
func columnNames(keys []string) string {
	return strings.Join(keys[:], ",")
}

// stagingColumnNames returns staging column names
func stagingColumnNames(keys []string) string {
	format := func(idx int, str string) string {
		return fmt.Sprintf(`STAGING.%s`, str)
	}
	return warehouseutils.JoinWithFormatting(keys, format, ",")
}

// columnsWithValues returns main and staging column values.
func columnsWithValues(keys []string) string {
	format := func(idx int, str string) string {
		return fmt.Sprintf(`MAIN.%[1]s = STAGING.%[1]s`, str)
	}
	return warehouseutils.JoinWithFormatting(keys, format, ",")
}

// checkAndIgnoreAlreadyExistError checks and ignores native errors.
func checkAndIgnoreAlreadyExistError(err error, ignoreNativeError int) bool {
	if err != nil {
		if odbcErrs, ok := err.(*odbc.Error); ok {
			for _, odbcErr := range odbcErrs.Diag {
				if odbcErr.NativeError == ignoreNativeError {
					return true
				}
			}
		}
		return false
	}
	return true
}

// connect creates database connection with CredentialsT
func (dl *HandleT) connect(cred CredentialsT) (*sql.DB, error) {
	dsn := fmt.Sprintf("Driver=%v; HOST=%v; PORT=%v; Schema=default; SparkServerType=3; AuthMech=3; UID=token; PWD=%v; ThriftTransport=2; SSL=1; HTTPPath=%v; UserAgentEntry=RudderStack",
		driverPath,
		cred.host,
		cred.port,
		cred.token,
		cred.path)

	var err error
	var db *sql.DB
	if db, err = sql.Open("odbc", dsn); err != nil {
		return nil, fmt.Errorf("%s Error connection to Delta lake: %v", dl.GetLogIdentifier(), err)
	}
	return db, nil
}

// fetchTables fetch tables with tableNames
func (dl *HandleT) fetchTables(db *sql.DB, sqlStatement string) (tableNames []string, err error) {
	// Executing the table sql statement
	rows, err := db.Query(sqlStatement)
	if err != nil && err != sql.ErrNoRows {
		if checkAndIgnoreAlreadyExistError(err, odbcDBNotFound) {
			err = nil
			return
		}
		pkgLogger.Errorf("%s Error in fetching tables schema from delta lake with SQL: %v, error: %v", dl.GetLogIdentifier(), sqlStatement, err)
		return
	}
	defer rows.Close()

	if err == sql.ErrNoRows {
		err = nil
		return
	}

	// Populating tablesNames
	for rows.Next() {
		var database, tableName sql.NullString
		var isTemporary bool
		err = rows.Scan(&database, &tableName, &isTemporary)
		if err != nil {
			pkgLogger.Errorf("%s Error in processing fetched tables schema from delta lake tableName: %v, error: %v", dl.GetLogIdentifier(), tableName, err)
			return
		}
		if !tableName.Valid {
			pkgLogger.Errorf("%s Error in processing fetched tables schema from delta lake tableName: %v ValidTableName: %v, error: %v", dl.GetLogIdentifier(), tableName, tableName.String, err)
			return
		}
		tableNames = append(tableNames, tableName.String)
	}
	return
}

// CreateTable creates tables with table name and columns
func (dl *HandleT) CreateTable(tableName string, columns map[string]string) (err error) {
	name := fmt.Sprintf(`%s.%s`, dl.Namespace, tableName)
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s ( %v ) USING DELTA`, name, columnsWithDataTypes(columns, ""))
	pkgLogger.Infof("%s Creating table in delta lake with SQL: %v", dl.GetLogIdentifier(tableName), sqlStatement)
	_, err = dl.Db.Exec(sqlStatement)
	return
}

// schemaExists checks it schema exists or not.
func (dl *HandleT) schemaExists(schemaName string) (exists bool, err error) {
	var databaseName string
	sqlStatement := fmt.Sprintf(`SHOW SCHEMAS LIKE '%s'`, schemaName)
	err = dl.Db.QueryRow(sqlStatement).Scan(&databaseName)
	if err != nil && err != sql.ErrNoRows {
		if checkAndIgnoreAlreadyExistError(err, odbcDBNotFound) {
			err = nil
			return
		}
		return
	}
	if err == sql.ErrNoRows {
		err = nil
		return
	}
	exists = strings.Compare(databaseName, schemaName) == 0
	return
}

// AddColumn adds column for column name and type
func (dl *HandleT) AddColumn(name string, columnName string, columnType string) (err error) {
	tableName := fmt.Sprintf(`%s.%s`, dl.Namespace, name)
	sqlStatement := fmt.Sprintf(`ALTER TABLE %v ADD COLUMNS ( %s %s )`, tableName, columnName, getDeltaLakeDataType(columnType))
	pkgLogger.Infof("%s Adding column in delta lake with SQL:%v", dl.GetLogIdentifier(tableName, columnName), sqlStatement)
	_, err = dl.Db.Exec(sqlStatement)
	return
}

// createSchema creates schema
func (dl *HandleT) createSchema() (err error) {
	sqlStatement := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, dl.Namespace)
	pkgLogger.Infof("%s Creating schema in delta lake with SQL:%v", dl.GetLogIdentifier(), sqlStatement)
	_, err = dl.Db.Exec(sqlStatement)
	return
}

// dropStagingTables drops staging tables
func (dl *HandleT) dropStagingTables(tableNames []string) {
	for _, stagingTableName := range tableNames {
		pkgLogger.Infof("%s Dropping table %+v\n", dl.GetLogIdentifier(), stagingTableName)
		_, err := dl.Db.Exec(fmt.Sprintf(`DROP TABLE %[1]s.%[2]s`, dl.Namespace, stagingTableName))
		if err != nil {
			if checkAndIgnoreAlreadyExistError(err, odbcTableOrViewNotFound) {
				continue
			}
			pkgLogger.Errorf("%s Error dropping staging tables in delta lake: %v", dl.GetLogIdentifier(), err)
		}
	}
}

// sortedColumnNames returns sorted column names for
// 1. LOAD_FILE_TYPE_PARQUET
// 2. LOAD_FILE_TYPE_CSV
func (dl *HandleT) sortedColumnNames(tableSchemaInUpload warehouseutils.TableSchemaT, sortedColumnKeys []string) (sortedColumnNames string) {
	if dl.Uploader.GetLoadFileType() == warehouseutils.LOAD_FILE_TYPE_PARQUET {
		sortedColumnNames = strings.Join(sortedColumnKeys[:], ",")
	} else {
		format := func(index int, value string) string {
			csvColumnIndex := fmt.Sprintf(`%s%d`, "_c", index)
			columnName := value
			columnType := getDeltaLakeDataType(tableSchemaInUpload[columnName])
			return fmt.Sprintf(`CAST ( %s AS %s ) AS %s`, csvColumnIndex, columnType, columnName)
		}
		return warehouseutils.JoinWithFormatting(sortedColumnKeys, format, ",")
	}
	return
}

// credentialsStr return authentication for AWS STS and SSE-C encryption
/*
CREDENTIALS ('awsKeyId' = '$key', 'awsSecretKey' = '$secret', 'awsSessionToken' = '$token)
*/
func (sf *HandleT) credentialsStr() string {
	var auth string
	// TODO: Required when we are going to use hosted data plane.
	//if sf.Uploader.UseRudderStorage() {
	//	tempAccessKeyId, tempSecretAccessKey, token, _ := warehouseutils.GetTemporaryS3Cred(misc.GetRudderObjectStorageAccessKeys())
	//	auth = fmt.Sprintf(`CREDENTIALS ( 'awsKeyId' = '%s', 'awsSecretKey' = '%s', awsSessionToken = '%s' )`, tempAccessKeyId, tempSecretAccessKey, token)
	//}
	return auth
}

func (dl *HandleT) loadTable(tableName string, tableSchemaInUpload warehouseutils.TableSchemaT, tableSchemaAfterUpload warehouseutils.TableSchemaT, skipTempTableDelete bool) (stagingTableName string, err error) {
	// Getting sorted column keys from tableSchemaInUpload
	sortedColumnKeys := warehouseutils.SortColumnKeysFromColumnMap(tableSchemaInUpload)

	// Creating staging table
	stagingTableName = misc.TruncateStr(fmt.Sprintf(`%s%s_%s`, stagingTablePrefix, strings.Replace(uuid.NewV4().String(), "-", "", -1), tableName), 127)
	err = dl.CreateTable(stagingTableName, tableSchemaAfterUpload)
	if err != nil {
		return
	}

	// Dropping staging tables if required
	if !skipTempTableDelete {
		defer dl.dropStagingTables([]string{stagingTableName})
	}

	// Getting the load folder where the load files are present
	csvObjectLocation, err := dl.Uploader.GetSampleLoadFileLocation(tableName)
	if err != nil {
		return
	}
	loadFolder := warehouseutils.GetObjectFolder(dl.ObjectStorage, csvObjectLocation)

	// Get the credentials string to copy from the stagling locstion to table
	credentialsStr := dl.credentialsStr()

	// Creating copy sql statement to copy from load folder to the staging table
	var sortedColumnNames = dl.sortedColumnNames(tableSchemaInUpload, sortedColumnKeys)
	var sqlStatement string
	if dl.Uploader.GetLoadFileType() == warehouseutils.LOAD_FILE_TYPE_PARQUET {
		sqlStatement = fmt.Sprintf("COPY INTO %v FROM ( SELECT %v FROM '%v' ) "+
			"FILEFORMAT = PARQUET "+
			"PATTERN = '*.parquet' "+
			"COPY_OPTIONS ('force' = 'true') "+
			"%s",
			fmt.Sprintf(`%s.%s`, dl.Namespace, stagingTableName), sortedColumnNames, loadFolder, credentialsStr)
	} else {
		sqlStatement = fmt.Sprintf("COPY INTO %v FROM ( SELECT %v FROM '%v' ) "+
			"FILEFORMAT = CSV "+
			"PATTERN = '*.gz' "+
			"FORMAT_OPTIONS ( 'compression' = 'gzip' ) "+
			"COPY_OPTIONS ('force' = 'true') "+
			"%s",
			fmt.Sprintf(`%s.%s`, dl.Namespace, stagingTableName), sortedColumnNames, loadFolder, credentialsStr)
	}

	// Sanitising copy sql statement for logging
	sanitisedSQLStmt, regexErr := misc.ReplaceMultiRegex(sqlStatement, map[string]string{
		"ACCESS_KEY_ID '[^']*'":     "ACCESS_KEY_ID '***'",
		"SECRET_ACCESS_KEY '[^']*'": "SECRET_ACCESS_KEY '***'",
	})
	if regexErr == nil {
		pkgLogger.Infof("%s Running COPY command with SQL: %s\n", dl.GetLogIdentifier(tableName), sanitisedSQLStmt)
	}

	// Executing copy sql statement
	_, err = dl.Db.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("%s Error running COPY command with SQL: %s\n error: %v", dl.GetLogIdentifier(tableName), sqlStatement, err)
		return
	}

	// Getting the primary key for the merge sql statement
	primaryKey := "id"
	if column, ok := primaryKeyMap[tableName]; ok {
		primaryKey = column
	}

	// Creating merge sql statement to copy from staging table to the main table
	sqlStatement = fmt.Sprintf(`MERGE INTO %[1]s.%[2]s AS MAIN
                                       USING ( SELECT * FROM ( SELECT *, row_number() OVER (PARTITION BY %[4]s ORDER BY RECEIVED_AT ASC) AS _rudder_staging_row_number FROM %[1]s.%[3]s ) AS q WHERE _rudder_staging_row_number = 1) AS STAGING
									   ON MAIN.%[4]s = STAGING.%[4]s
									   WHEN MATCHED THEN UPDATE SET %[5]s
									   WHEN NOT MATCHED THEN INSERT (%[6]s) VALUES (%[7]s)`,
		dl.Namespace,
		tableName,
		stagingTableName,
		primaryKey,
		columnsWithValues(sortedColumnKeys),
		columnNames(sortedColumnKeys),
		stagingColumnNames(sortedColumnKeys),
	)
	pkgLogger.Infof("%v Inserting records using staging table with SQL: %s\n", dl.GetLogIdentifier(tableName), sqlStatement)

	// Executing merge sql statement
	_, err = dl.Db.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("%v Error inserting into original table: %v\n", dl.GetLogIdentifier(tableName), err)
		return
	}

	pkgLogger.Infof("%v Complete load for table\n", dl.GetLogIdentifier(tableName))
	return
}

func (dl *HandleT) loadUserTables() (errorMap map[string]error) {
	// Creating errorMap
	errorMap = map[string]error{warehouseutils.IdentifiesTable: nil}
	pkgLogger.Infof("%s Starting load for identifies and users tables\n", dl.GetLogIdentifier())

	// Loading identifies tables
	identifyStagingTable, err := dl.loadTable(warehouseutils.IdentifiesTable, dl.Uploader.GetTableSchemaInUpload(warehouseutils.IdentifiesTable), dl.Uploader.GetTableSchemaInWarehouse(warehouseutils.IdentifiesTable), true)
	if err != nil {
		errorMap[warehouseutils.IdentifiesTable] = err
		return
	}

	// dropping identifies staging table
	defer dl.dropStagingTables([]string{identifyStagingTable})

	// Checking if users schema is present in GetTableSchemaInUpload
	if len(dl.Uploader.GetTableSchemaInUpload(warehouseutils.UsersTable)) == 0 {
		return
	}
	errorMap[warehouseutils.UsersTable] = nil

	// Creating userColNames and firstValProps for create table sql statement
	userColMap := dl.Uploader.GetTableSchemaInWarehouse(warehouseutils.UsersTable)
	var userColNames, firstValProps []string
	for colName := range userColMap {
		// do not reference uuid in queries as it can be an autoincrementing field set by segment compatible tables
		if colName == "id" || colName == "user_id" || colName == "uuid" {
			continue
		}
		userColNames = append(userColNames, colName)
		firstValProps = append(firstValProps, fmt.Sprintf(`FIRST_VALUE(%[1]s , TRUE) OVER (PARTITION BY id ORDER BY received_at DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS %[1]s`, colName))
	}
	quotedUserColNames := warehouseutils.DoubleQuoteAndJoinByComma(userColNames)
	stagingTableName := misc.TruncateStr(fmt.Sprintf(`%s%s_%s`, stagingTablePrefix, strings.Replace(uuid.NewV4().String(), "-", "", -1), warehouseutils.UsersTable), 127)

	// Creating create table sql statement for staging users table
	sqlStatement := fmt.Sprintf(`CREATE TABLE %[1]s.%[2]s USING DELTA AS (SELECT DISTINCT * FROM
										(
											SELECT
											id, %[3]s
											FROM (
												(
													SELECT id, %[6]s FROM %[1]s.%[4]s WHERE id in (SELECT DISTINCT(user_id) FROM %[1]s.%[5]s WHERE user_id IS NOT NULL)
												) UNION
												(
													SELECT user_id, %[6]s FROM %[1]s.%[5]s WHERE user_id IS NOT NULL
												)
											)
										)
									) `,
		dl.Namespace,
		stagingTableName,
		strings.Join(firstValProps, ","),
		warehouseutils.UsersTable,
		identifyStagingTable,
		quotedUserColNames,
	)

	// Executing create sql statement
	_, err = dl.Db.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("%s Creating staging table for users failed with SQL: %s\n", dl.GetLogIdentifier(), sqlStatement)
		pkgLogger.Errorf("%s Error creating users staging table from original table and identifies staging table: %v\n", dl.GetLogIdentifier(), err)
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	// Dropping staging users table
	defer dl.dropStagingTables([]string{stagingTableName})

	// Creating the Primary Key
	primaryKey := "id"

	// Creating the column Keys
	columnKeys := append([]string{`id`}, userColNames...)

	// Creating the merge sql statement to copy from staging users table to the main users table
	sqlStatement = fmt.Sprintf(`MERGE INTO %[1]s.%[2]s AS MAIN
									   USING ( SELECT %[5]s FROM %[1]s.%[3]s ) AS STAGING
									   ON MAIN.%[4]s = STAGING.%[4]s
									   WHEN MATCHED THEN UPDATE SET %[6]s
									   WHEN NOT MATCHED THEN INSERT (%[5]s) VALUES (%[7]s)`,
		dl.Namespace,
		warehouseutils.UsersTable,
		stagingTableName,
		primaryKey,
		columnNames(columnKeys),
		columnsWithValues(columnKeys),
		stagingColumnNames(columnKeys),
	)
	pkgLogger.Infof("%s Inserting records using staging table with SQL: %s\n", dl.GetLogIdentifier(warehouseutils.UsersTable), sqlStatement)

	// Executing the merge sql statement
	_, err = dl.Db.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("%s Error inserting into users table from staging table: %v\n", err)
		errorMap[warehouseutils.UsersTable] = err
		return
	}
	return
}

// dropDanglingStagingTables drop dandling staging tables.
func (dl *HandleT) dropDanglingStagingTables() {
	// Creating show tables sql statement to get the staging tables associated with the namespace
	sqlStatement := fmt.Sprintf(`SHOW TABLES FROM %s LIKE '%s';`, dl.Namespace, fmt.Sprintf("%s%s", stagingTablePrefix, "*"))

	// Fetching the staging tables
	stagingTableNames, _ := dl.fetchTables(dl.Db, sqlStatement)

	// Drop staging tables
	dl.dropStagingTables(stagingTableNames)
	return
}

// connectToWarehouse returns the database connection configured with CredentialsT
func (dl *HandleT) connectToWarehouse() (*sql.DB, error) {
	return dl.connect(CredentialsT{
		host:  warehouseutils.GetConfigValue(DLHost, dl.Warehouse),
		port:  warehouseutils.GetConfigValue(DLPort, dl.Warehouse),
		path:  warehouseutils.GetConfigValue(DLPath, dl.Warehouse),
		token: warehouseutils.GetConfigValue(DLToken, dl.Warehouse),
	})
}

// CreateSchema checks if schema exists or not. If it does not exists, it creates the schema.
func (dl *HandleT) CreateSchema() (err error) {
	// Checking if schema exists or not
	var schemaExists bool
	schemaExists, err = dl.schemaExists(dl.Namespace)
	if err != nil {
		pkgLogger.Errorf("%s Error checking if schema exists: %s, error: %v", dl.GetLogIdentifier(), dl.Namespace, err)
		return err
	}
	if schemaExists {
		pkgLogger.Infof("%s Skipping creating schema: %s since it already exists", dl.GetLogIdentifier(), dl.Namespace)
		return
	}

	// Creating schema
	return dl.createSchema()
}

// AlterColumn alter table with column name and type
func (dl *HandleT) AlterColumn(tableName string, columnName string, columnType string) (err error) {
	return
}

// FetchSchema queries delta lake and returns the schema associated with provided namespace
/*
DESCRIBE TABLE salesdb.customer;
+-----------------------+---------+----------+
|               col_name|data_type|   comment|
+-----------------------+---------+----------+
|                cust_id|      int|      null|
|                   name|   string|Short name|
|                  state|   string|      null|
|                       |         |          |
|# Partition Information|         |          |
|             # col_name|data_type|   comment|
|                  state|   string|      null|
+-----------------------+---------+----------+
*/
func (dl *HandleT) FetchSchema(warehouse warehouseutils.WarehouseT) (schema warehouseutils.SchemaT, err error) {
	dl.Warehouse = warehouse
	dl.Namespace = warehouse.Namespace
	dbHandle, err := dl.connectToWarehouse()
	if err != nil {
		return
	}
	defer dbHandle.Close()

	// Schema Initialization
	schema = make(warehouseutils.SchemaT)

	// Creating show tables sql statement to get the tables associated with the namespace
	sqlStatement := fmt.Sprintf(`SHOW TABLES FROM %s`, dl.Namespace)

	// Fetching the tables
	tableNames, err := dl.fetchTables(dl.Db, sqlStatement)
	if err != nil {
		return
	}

	// Filtering tables based on not part of staging tables
	var filteredTablesNames []string
	for _, tableName := range tableNames {
		// Ignoring the staging tables
		if strings.HasPrefix(tableName, stagingTablePrefix) {
			continue
		}
		filteredTablesNames = append(filteredTablesNames, tableName)
	}

	// For each table we are generating schema
	for _, tableName := range filteredTablesNames {
		// Creating describe sql statement for table
		ttSqlStatement := fmt.Sprintf(`DESCRIBE TABLE %s.%s`, dl.Namespace, tableName)

		// Executing describe sql statement
		var ttRows *sql.Rows
		ttRows, err = dbHandle.Query(ttSqlStatement)
		if err != nil && err != sql.ErrNoRows {
			if checkAndIgnoreAlreadyExistError(err, odbcTableOrViewNotFound) {
				err = nil
				return
			}
			pkgLogger.Errorf("%s Error in fetching describe table schema from delta lake with SQL: %v", dl.GetLogIdentifier(), ttSqlStatement)
			break
		}
		if err == sql.ErrNoRows {
			pkgLogger.Infof("%s No rows, while fetched describe table schema from delta lake with SQL: %v, error: %v", dl.GetLogIdentifier(), ttSqlStatement, err)
			err = nil
			return
		}
		defer ttRows.Close()

		// Populating the schema for the table
		for ttRows.Next() {
			var col_name, data_type, comment sql.NullString
			err := ttRows.Scan(&col_name, &data_type, &comment)
			if err != nil {
				pkgLogger.Errorf("%s Error in processing fetched describe table schema from delta lake with SQL: %v, error: %v", dl.GetLogIdentifier(), ttSqlStatement, err)
				break
			}
			if !col_name.Valid || !data_type.Valid {
				continue
			}
			// Here the breaking condition is when we are finding empty row.
			if len(col_name.String) == 0 && len(data_type.String) == 0 {
				break
			}
			if _, ok := schema[tableName]; !ok {
				schema[tableName] = make(map[string]string)
			}
			if datatype, ok := dataTypesMapToRudder[data_type.String]; ok {
				schema[tableName][col_name.String] = datatype
			}
		}
	}
	return
}

// Setup populate the HandleT
func (dl *HandleT) Setup(warehouse warehouseutils.WarehouseT, uploader warehouseutils.UploaderI) (err error) {
	dl.Warehouse = warehouse
	dl.Namespace = warehouse.Namespace
	dl.Uploader = uploader
	dl.ObjectStorage = warehouseutils.ObjectStorageType(warehouseutils.DELTALAKE, warehouse.Destination.Config, dl.Uploader.UseRudderStorage())

	dl.Db, err = dl.connectToWarehouse()
	return err
}

// TestConnection test the connection for the warehouse
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

// Cleanup handle cleanup when upload is done.
func (dl *HandleT) Cleanup() {
	if dl.Db != nil {
		dl.dropDanglingStagingTables()
		dl.Db.Close()
	}
}

// CrashRecover handle crash recover scenarios
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

// IsEmpty checks if the warehouse is empty or not
func (dl *HandleT) IsEmpty(warehouse warehouseutils.WarehouseT) (empty bool, err error) {
	return
}

// LoadUserTables loads user tables
func (dl *HandleT) LoadUserTables() map[string]error {
	return dl.loadUserTables()
}

// LoadTable loads table for table name
func (dl *HandleT) LoadTable(tableName string) error {
	_, err := dl.loadTable(tableName, dl.Uploader.GetTableSchemaInUpload(tableName), dl.Uploader.GetTableSchemaInWarehouse(tableName), false)
	return err
}

// LoadIdentityMergeRulesTable loads identifies merge rules tables
func (dl *HandleT) LoadIdentityMergeRulesTable() (err error) {
	return
}

// LoadIdentityMappingsTable loads identifies mappings table
func (dl *HandleT) LoadIdentityMappingsTable() (err error) {
	return
}

// DownloadIdentityRules download identity tules
func (dl *HandleT) DownloadIdentityRules(*misc.GZipWriter) (err error) {
	return
}

// GetTotalCountInTable returns total count in tables.
func (dl *HandleT) GetTotalCountInTable(tableName string) (total int64, err error) {
	sqlStatement := fmt.Sprintf(`SELECT COUNT(*) FROM %[1]s.%[2]s`, dl.Namespace, tableName)
	err = dl.Db.QueryRow(sqlStatement).Scan(&total)
	if err != nil {
		pkgLogger.Errorf(`%s Error getting total count: %v`, dl.GetLogIdentifier(tableName), err)
	}
	return
}

// Connect returns Client
func (dl *HandleT) Connect(warehouse warehouseutils.WarehouseT) (client.Client, error) {
	dl.Warehouse = warehouse
	dl.Namespace = warehouse.Namespace
	dbHandle, err := dl.connectToWarehouse()

	if err != nil {
		return client.Client{}, err
	}

	return client.Client{Type: client.SQLClient, SQL: dbHandle}, err
}

// GetLogIdentifier returns log identifier
func (dl *HandleT) GetLogIdentifier(args ...string) string {
	if len(args) == 0 {
		return fmt.Sprintf("[%s][%s][%s][%s]", dl.Warehouse.Type, dl.Warehouse.Source.ID, dl.Warehouse.Destination.ID, dl.Warehouse.Namespace)
	}
	return fmt.Sprintf("[%s][%s][%s][%s][%s]", dl.Warehouse.Type, dl.Warehouse.Source.ID, dl.Warehouse.Destination.ID, dl.Warehouse.Namespace, strings.Join(args, "]["))
}
