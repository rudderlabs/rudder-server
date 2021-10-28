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
	"reflect"
	"strings"
	"time"
)

// ODBC NATIVE ERRORS
const (
	odbcTableOrViewNotFound = 31740
	odbcDBNotFound          = 80
)

// String constants for delta lake destination config
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

var dataTypesMap = map[string]string{
	"boolean":  "BOOLEAN",
	"int":      "BIGINT",
	"float":    "DOUBLE",
	"string":   "STRING",
	"datetime": "TIMESTAMP",
}

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

var primaryKeyMap = map[string]string{
	"users":                      "id",
	"identifies":                 "id",
	warehouseutils.DiscardsTable: "row_id",
}

type CredentialsT struct {
	host  string
	port  string
	path  string
	token string
}

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("warehouse").Child("deltalake")
}

func loadConfig() {
	stagingTablePrefix = "rudder_staging_"
	config.RegisterStringConfigVariable("/opt/simba/spark/lib/64/libsparkodbc_sb64.so", &driverPath, false, "Warehouse.deltalake.driverPath") // Reference: https://docs.databricks.com/integrations/bi/jdbc-odbc-bi.html
}

type HandleT struct {
	Db            *sql.DB
	Namespace     string
	ObjectStorage string
	Warehouse     warehouseutils.WarehouseT
	Uploader      warehouseutils.UploaderI
}

// getDeltaLakeDataType gets datatype for delta lake which is mapped with rudderstack datatype
func getDeltaLakeDataType(columnType string) string {
	return dataTypesMap[columnType]
}

func columnsWithDataTypes(columns map[string]string, prefix string) string {
	keys := warehouseutils.SortColumnKeysFromColumnMap(columns)
	var arr []string
	for _, name := range keys {
		arr = append(arr, fmt.Sprintf(`%s%s %s`, prefix, name, getDeltaLakeDataType(columns[name])))
	}
	return strings.Join(arr[:], ",")
}

func (dl *HandleT) CreateTable(tableName string, columns map[string]string) (err error) {
	name := fmt.Sprintf(`%s.%s`, dl.Namespace, tableName)
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s ( %v )`, name, columnsWithDataTypes(columns, ""))
	pkgLogger.Infof("%s Creating table in delta lake with SQL: %v", dl.GetLogIdentifier(tableName), sqlStatement)
	_, err = dl.Db.Exec(sqlStatement)
	return
}

func (dl *HandleT) schemaExists(schemaName string) (exists bool, err error) {
	var databaseName string
	sqlStatement := fmt.Sprintf(`SHOW SCHEMAS LIKE '%s'`, schemaName)
	err = dl.Db.QueryRow(sqlStatement).Scan(&databaseName)
	if err != nil && err != sql.ErrNoRows {
		if checkAndIgnoreAlreadyExistError(err, odbcDBNotFound) {
			return false, nil
		}
		return
	}
	if err == sql.ErrNoRows {
		err = nil
	}
	exists = strings.Compare(databaseName, schemaName) == 0
	return
}

func (dl *HandleT) AddColumn(name string, columnName string, columnType string) (err error) {
	tableName := fmt.Sprintf(`%s.%s`, dl.Namespace, name)
	sqlStatement := fmt.Sprintf(`ALTER TABLE %v ADD COLUMNS ( %s %s )`, tableName, columnName, getDeltaLakeDataType(columnType))
	pkgLogger.Infof("%s Adding column in delta lake with SQL:%v", dl.GetLogIdentifier(tableName, columnName), sqlStatement)
	_, err = dl.Db.Exec(sqlStatement)
	return
}

func (dl *HandleT) createSchema() (err error) {
	sqlStatement := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, dl.Namespace)
	pkgLogger.Infof("%s Creating schema in delta lake with SQL:%v", dl.GetLogIdentifier(), sqlStatement)
	_, err = dl.Db.Exec(sqlStatement)
	return
}

func (dl *HandleT) dropStagingTables(stagingTableNames []string) {
	for _, stagingTableName := range stagingTableNames {
		pkgLogger.Infof("%s Dropping table %+v\n", dl.GetLogIdentifier(), stagingTableName)
		_, err := dl.Db.Exec(fmt.Sprintf(`DROP TABLE %[1]s.%[2]s`, dl.Namespace, stagingTableName))
		if err != nil {
			pkgLogger.Errorf("%s Error dropping staging tables in delta lake: %v", dl.GetLogIdentifier(), err)
		}
	}
}

func (dl *HandleT) getSortedColumnNames(tableSchemaInUpload warehouseutils.TableSchemaT, sortedColumnKeys []string) (sortedColumnNames string) {
	if dl.Uploader.GetLoadFileType() == warehouseutils.LOAD_FILE_TYPE_PARQUET {
		sortedColumnNames = strings.Join(sortedColumnKeys[:], ",")
	} else {
		keys := reflect.ValueOf(tableSchemaInUpload).MapKeys()
		strKeys := make([]string, len(keys))

		for index, value := range sortedColumnKeys {
			csvColumnIndex := fmt.Sprintf(`%s%d`, "_c", index)
			columnName := value
			columnType := getDeltaLakeDataType(tableSchemaInUpload[columnName])
			strKeys[index] = fmt.Sprintf(`CAST ( %s AS %s ) AS %s`, csvColumnIndex, columnType, columnName)
		}
		sortedColumnNames = strings.Join(strKeys[:], ",")
	}
	return
}

func (dl *HandleT) loadTable(tableName string, tableSchemaInUpload warehouseutils.TableSchemaT, tableSchemaAfterUpload warehouseutils.TableSchemaT, skipTempTableDelete bool) (stagingTableName string, err error) {
	sortedColumnKeys := warehouseutils.SortColumnKeysFromColumnMap(tableSchemaInUpload)

	sortedColumnNames := dl.getSortedColumnNames(tableSchemaInUpload, sortedColumnKeys)
	stagingTableName = misc.TruncateStr(fmt.Sprintf(`%s%s_%s`, stagingTablePrefix, strings.Replace(uuid.NewV4().String(), "-", "", -1), tableName), 127)
	err = dl.CreateTable(stagingTableName, tableSchemaAfterUpload)
	if err != nil {
		return
	}
	if !skipTempTableDelete {
		defer dl.dropStagingTables([]string{stagingTableName})
	}

	csvObjectLocation, err := dl.Uploader.GetSampleLoadFileLocation(tableName)
	if err != nil {
		return
	}
	loadFolder := warehouseutils.GetObjectFolder(dl.ObjectStorage, csvObjectLocation)

	var sqlStatement string
	if dl.Uploader.GetLoadFileType() == warehouseutils.LOAD_FILE_TYPE_PARQUET {
		// copy statement for parquet load files
		sqlStatement = fmt.Sprintf("COPY INTO %v FROM ( SELECT %v FROM '%v' ) "+
			"FILEFORMAT = PARQUET "+
			"PATTERN = '*.parquet' "+
			"COPY_OPTIONS ('force' = 'true')",
			fmt.Sprintf(`%s.%s`, dl.Namespace, stagingTableName), sortedColumnNames, loadFolder)
	} else {
		// copy statement for csv load files
		sqlStatement = fmt.Sprintf("COPY INTO %v FROM ( SELECT %v FROM '%v' ) "+
			"FILEFORMAT = CSV "+
			"PATTERN = '*.gz' "+
			"FORMAT_OPTIONS ( 'compression' = 'gzip' ) "+
			"COPY_OPTIONS ('force' = 'true')",
			fmt.Sprintf(`%s.%s`, dl.Namespace, stagingTableName), sortedColumnNames, loadFolder)
	}

	sanitisedSQLStmt, regexErr := misc.ReplaceMultiRegex(sqlStatement, map[string]string{
		"ACCESS_KEY_ID '[^']*'":     "ACCESS_KEY_ID '***'",
		"SECRET_ACCESS_KEY '[^']*'": "SECRET_ACCESS_KEY '***'",
	})
	if regexErr == nil {
		pkgLogger.Infof("%s Running COPY command with SQL: %s\n", dl.GetLogIdentifier(tableName), sanitisedSQLStmt)
	}

	_, err = dl.Db.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("%s Error running COPY command: %v\n", dl.GetLogIdentifier(tableName), err)
		return
	}

	primaryKey := "id"
	if column, ok := primaryKeyMap[tableName]; ok {
		primaryKey = column
	}

	var columnNames, stagingColumnNames, columnsWithValues string
	for idx, str := range sortedColumnKeys {
		columnNames += str
		stagingColumnNames += fmt.Sprintf(`STAGING.%s`, str)
		columnsWithValues += fmt.Sprintf(`MAIN.%[1]s = STAGING.%[1]s`, str)
		if idx != len(sortedColumnKeys)-1 {
			columnNames += `,`
			stagingColumnNames += `,`
			columnsWithValues += `,`
		}
	}

	sqlStatement = fmt.Sprintf(`MERGE INTO %[1]s.%[2]s AS MAIN USING ( SELECT * FROM ( SELECT *, row_number() OVER (PARTITION BY %[4]s ORDER BY RECEIVED_AT ASC) AS _rudder_staging_row_number FROM %[1]s.%[3]s ) AS q WHERE _rudder_staging_row_number = 1) AS STAGING ON MAIN.%[4]s = STAGING.%[4]s WHEN MATCHED THEN UPDATE SET %[5]s WHEN NOT MATCHED THEN INSERT (%[6]s) VALUES (%[7]s)`, dl.Namespace, tableName, stagingTableName, primaryKey, columnsWithValues, columnNames, stagingColumnNames)
	pkgLogger.Infof("%v Inserting records using staging table with SQL: %s\n", dl.GetLogIdentifier(tableName), sqlStatement)
	_, err = dl.Db.Exec(sqlStatement)

	if err != nil {
		pkgLogger.Errorf("%v Error inserting into original table: %v\n", dl.GetLogIdentifier(tableName), err)
		return
	}

	pkgLogger.Infof("%v Complete load for table\n", dl.GetLogIdentifier(tableName))
	return
}

func (dl *HandleT) loadUserTables() (errorMap map[string]error) {
	errorMap = map[string]error{warehouseutils.IdentifiesTable: nil}
	pkgLogger.Infof("%s Starting load for identifies and users tables\n", dl.GetLogIdentifier())

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
		firstValProps = append(firstValProps, fmt.Sprintf(`FIRST_VALUE(%[1]s , TRUE) OVER (PARTITION BY id ORDER BY received_at DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS %[1]s`, colName))
	}
	quotedUserColNames := warehouseutils.DoubleQuoteAndJoinByComma(userColNames)
	stagingTableName := misc.TruncateStr(fmt.Sprintf(`%s%s_%s`, stagingTablePrefix, strings.Replace(uuid.NewV4().String(), "-", "", -1), warehouseutils.UsersTable), 127)

	sqlStatement := fmt.Sprintf(`CREATE TABLE %[1]s.%[2]s AS (SELECT DISTINCT * FROM
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
									)`,
		dl.Namespace,                     // 1
		stagingTableName,                 // 2
		strings.Join(firstValProps, ","), // 3
		warehouseutils.UsersTable,        // 4
		identifyStagingTable,             // 5
		quotedUserColNames,               // 6
	)

	_, err = dl.Db.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("%s Creating staging table for users failed with SQL: %s\n", dl.GetLogIdentifier(), sqlStatement)
		pkgLogger.Errorf("%s Error creating users staging table from original table and identifies staging table: %v\n", dl.GetLogIdentifier(), err)
		errorMap[warehouseutils.UsersTable] = err
		return
	}
	defer dl.dropStagingTables([]string{stagingTableName})

	primaryKey := "id"

	columnNames := append([]string{`id`}, userColNames...)
	columnNamesStr := strings.Join(columnNames, ",")
	var columnsWithValues, stagingColumnValues string
	for idx, colName := range columnNames {
		columnsWithValues += fmt.Sprintf(`MAIN.%[1]s = STAGING.%[1]s`, colName)
		stagingColumnValues += fmt.Sprintf(`STAGING.%s`, colName)
		if idx != len(columnNames)-1 {
			columnsWithValues += `,`
			stagingColumnValues += `,`
		}
	}

	sqlStatement = fmt.Sprintf(`MERGE INTO %[1]s.%[2]s AS MAIN USING ( SELECT %[5]s FROM %[1]s.%[3]s ) AS STAGING ON MAIN.%[4]s = STAGING.%[4]s WHEN MATCHED THEN UPDATE SET %[6]s WHEN NOT MATCHED THEN INSERT (%[5]s) VALUES (%[7]s)`, dl.Namespace, warehouseutils.UsersTable, stagingTableName, primaryKey, columnNamesStr, columnsWithValues, stagingColumnValues)
	pkgLogger.Infof("%s Inserting records using staging table with SQL: %s\n", dl.GetLogIdentifier(warehouseutils.UsersTable), sqlStatement)
	_, err = dl.Db.Exec(sqlStatement)

	if err != nil {
		pkgLogger.Errorf("%s Error inserting into users table from staging table: %v\n", err)
		errorMap[warehouseutils.UsersTable] = err
		return
	}
	return
}

func connect(cred CredentialsT) (*sql.DB, error) {
	dsn := fmt.Sprintf("Driver=%v; HOST=%v; PORT=%v; Schema=default; SparkServerType=3; AuthMech=3; UID=token; PWD=%v; ThriftTransport=2; SSL=1; HTTPPath=%v; UserAgentEntry=RudderStack",
		driverPath,
		cred.host,
		cred.port,
		cred.token,
		cred.path)

	var err error
	var db *sql.DB
	if db, err = sql.Open("odbc", dsn); err != nil {
		return nil, fmt.Errorf("%s Delta lake connect error : (%v)", err)
	}
	return db, nil
}

func (dl *HandleT) dropDanglingStagingTables() bool {
	sqlStatement := fmt.Sprintf(`SHOW TABLES FROM %s LIKE '%s';`, dl.Namespace, fmt.Sprintf("%s%s", stagingTablePrefix, "*"))
	rows, err := dl.Db.Query(sqlStatement)
	if err != nil && err != sql.ErrNoRows {
		if checkAndIgnoreAlreadyExistError(err, odbcDBNotFound) {
			return true
		}
		pkgLogger.Errorf("%s Error dropping dangling staging tables in delta lake with SQL: %s\nError: %v\n", dl.GetLogIdentifier(), sqlStatement, err)
		return false
	}
	defer rows.Close()

	var stagingTableNames []string
	for rows.Next() {
		var database, tableName string
		var isTemporary bool
		err = rows.Scan(&database, &tableName, &isTemporary)
		if err != nil {
			panic(fmt.Errorf("%s Failed to scan result with SQL: %s\nError: %w", dl.GetLogIdentifier(), sqlStatement, err))
		}
		stagingTableNames = append(stagingTableNames, tableName)
	}
	pkgLogger.Infof("%s Dropping dangling staging tables: %+v %+v\n", dl.GetLogIdentifier(), len(stagingTableNames), stagingTableNames)
	delSuccess := true
	for _, stagingTableName := range stagingTableNames {
		_, err := dl.Db.Exec(fmt.Sprintf(`DROP TABLE %[1]s.%[2]s`, dl.Namespace, stagingTableName))
		if err != nil {
			pkgLogger.Errorf("%s Error dropping dangling staging table: %s in delta lake: %v\n", dl.GetLogIdentifier(), stagingTableName, err)
			delSuccess = false
		}
	}
	return delSuccess
}

func (dl *HandleT) connectToWarehouse() (*sql.DB, error) {
	return connect(CredentialsT{
		host:  warehouseutils.GetConfigValue(DLHost, dl.Warehouse),
		port:  warehouseutils.GetConfigValue(DLPort, dl.Warehouse),
		path:  warehouseutils.GetConfigValue(DLPath, dl.Warehouse),
		token: warehouseutils.GetConfigValue(DLToken, dl.Warehouse),
	})
}

func (dl *HandleT) CreateSchema() (err error) {
	var schemaExists bool
	schemaExists, err = dl.schemaExists(dl.Namespace)
	if err != nil {
		pkgLogger.Errorf("%s Error checking if schema: %s exists: %v", dl.GetLogIdentifier(), dl.Namespace, err)
		return err
	}
	if schemaExists {
		pkgLogger.Infof("%s Skipping creating schema: %s since it already exists", dl.GetLogIdentifier(), dl.Namespace)
		return
	}
	return dl.createSchema()
}

func (dl *HandleT) AlterColumn(tableName string, columnName string, columnType string) (err error) {
	return
}

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

	schema = make(warehouseutils.SchemaT)

	tSqlStatement := fmt.Sprintf(`SHOW TABLES FROM %s`, dl.Namespace)
	tRows, err := dbHandle.Query(tSqlStatement)
	if err != nil && err != sql.ErrNoRows {
		if checkAndIgnoreAlreadyExistError(err, odbcDBNotFound) {
			return schema, nil
		}
		pkgLogger.Errorf("%s Error in fetching tables schema from delta lake with SQL: %v", dl.GetLogIdentifier(), tSqlStatement)
		return
	}
	defer tRows.Close()

	var tableNames []string
	for tRows.Next() {
		var database, tableName string
		var isTemporary bool
		err = tRows.Scan(&database, &tableName, &isTemporary)
		if err != nil {
			pkgLogger.Errorf("%s Error in processing fetched tables schema from delta lake tableName: %v", dl.GetLogIdentifier(), tableName)
			return
		}
		// Ignoring the staging tables
		if strings.HasPrefix(tableName, stagingTablePrefix) {
			continue
		}
		tableNames = append(tableNames, tableName)
	}

	for _, tableName := range tableNames {
		ttSqlStatement := fmt.Sprintf(`DESCRIBE TABLE %s.%s`, dl.Namespace, tableName)
		ttRows, err := dbHandle.Query(ttSqlStatement)
		if err != nil && err != sql.ErrNoRows {
			if checkAndIgnoreAlreadyExistError(err, odbcTableOrViewNotFound) {
				return schema, nil
			}
			pkgLogger.Errorf("%s Error in fetching describe table schema from delta lake with SQL: %v", dl.GetLogIdentifier(), ttSqlStatement)
			break
		}
		if err == sql.ErrNoRows {
			pkgLogger.Infof("%s No rows, while fetched describe table schema from delta lake with SQL: %v", dl.GetLogIdentifier(), ttSqlStatement)
			return schema, nil
		}

		defer ttRows.Close()
		for ttRows.Next() {
			var col_name, data_type, comment string
			err := ttRows.Scan(&col_name, &data_type, &comment)
			if err != nil {
				pkgLogger.Errorf("%s Error in processing fetched describe table schema from delta lake", dl.GetLogIdentifier())
				break
			}
			// Here the breaking condition is when we are finding empty row.
			if len(col_name) == 0 && len(data_type) == 0 {
				break
			}
			if _, ok := schema[tableName]; !ok {
				schema[tableName] = make(map[string]string)
			}
			if datatype, ok := dataTypesMapToRudder[data_type]; ok {
				schema[tableName][col_name] = datatype
			}
		}
	}
	return
}

func (dl *HandleT) Setup(warehouse warehouseutils.WarehouseT, uploader warehouseutils.UploaderI) (err error) {
	dl.Warehouse = warehouse
	dl.Namespace = warehouse.Namespace
	dl.Uploader = uploader
	dl.ObjectStorage = warehouseutils.ObjectStorageType(warehouseutils.DELTALAKE, warehouse.Destination.Config, dl.Uploader.UseRudderStorage())

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
	sqlStatement := fmt.Sprintf(`SELECT COUNT(*) FROM %[1]s.%[2]s`, dl.Namespace, tableName)
	err = dl.Db.QueryRow(sqlStatement).Scan(&total)
	if err != nil {
		pkgLogger.Errorf(`%s Error getting total count`, dl.GetLogIdentifier(tableName))
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

func (dl *HandleT) GetLogIdentifier(args ...string) string {
	if len(args) == 0 {
		return fmt.Sprintf("[%s][%s][%s][%s]", dl.Warehouse.Type, dl.Warehouse.Source.ID, dl.Warehouse.Destination.ID, dl.Warehouse.Namespace)
	}
	return fmt.Sprintf("[%s][%s][%s][%s][%s]", dl.Warehouse.Type, dl.Warehouse.Source.ID, dl.Warehouse.Destination.ID, dl.Warehouse.Namespace, strings.Join(args, "]["))
}
