package snowflake

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/gofrs/uuid"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	snowflake "github.com/snowflakedb/gosnowflake" // blank comment
)

var (
	stagingTablePrefix string
	pkgLogger          logger.LoggerI
)

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("warehouse").Child("snowflake")
}

func loadConfig() {
	stagingTablePrefix = "RUDDER_STAGING_"
}

type HandleT struct {
	Db             *sql.DB
	Namespace      string
	CloudProvider  string
	ObjectStorage  string
	Warehouse      warehouseutils.WarehouseT
	Uploader       warehouseutils.UploaderI
	ConnectTimeout time.Duration
}

// String constants for snowflake destination config
const (
	AWSAccessKey       = "accessKey"
	AWSAccessSecret    = "accessKeyID"
	StorageIntegration = "storageIntegration"
	SFAccount          = "account"
	SFWarehouse        = "warehouse"
	SFDbName           = "database"
	SFUserName         = "user"
	SFPassword         = "password"
)

var dataTypesMap = map[string]string{
	"boolean":  "boolean",
	"int":      "number",
	"bigint":   "number",
	"float":    "double precision",
	"string":   "varchar",
	"datetime": "timestamp",
	"json":     "variant",
}

var dataTypesMapToRudder = map[string]string{
	"NUMBER":           "int",
	"DECIMAL":          "int",
	"NUMERIC":          "int",
	"INT":              "int",
	"INTEGER":          "int",
	"BIGINT":           "int",
	"SMALLINT":         "int",
	"FLOAT":            "float",
	"FLOAT4":           "float",
	"FLOAT8":           "float",
	"DOUBLE":           "float",
	"REAL":             "float",
	"DOUBLE PRECISION": "float",
	"BOOLEAN":          "boolean",
	"TEXT":             "string",
	"VARCHAR":          "string",
	"CHAR":             "string",
	"CHARACTER":        "string",
	"STRING":           "string",
	"BINARY":           "string",
	"VARBINARY":        "string",
	"TIMESTAMP_NTZ":    "datetime",
	"DATE":             "datetime",
	"DATETIME":         "datetime",
	"TIME":             "datetime",
	"TIMESTAMP":        "datetime",
	"TIMESTAMP_LTZ":    "datetime",
	"TIMESTAMP_TZ":     "datetime",
	"VARIANT":          "json",
}

var primaryKeyMap = map[string]string{
	usersTable:      "ID",
	identifiesTable: "ID",
	discardsTable:   "ROW_ID",
}

var partitionKeyMap = map[string]string{
	usersTable:      `"ID"`,
	identifiesTable: `"ID"`,
	discardsTable:   `"ROW_ID", "COLUMN_NAME", "TABLE_NAME"`,
}

var (
	usersTable              = warehouseutils.ToProviderCase(warehouseutils.SNOWFLAKE, warehouseutils.UsersTable)
	identifiesTable         = warehouseutils.ToProviderCase(warehouseutils.SNOWFLAKE, warehouseutils.IdentifiesTable)
	discardsTable           = warehouseutils.ToProviderCase(warehouseutils.SNOWFLAKE, warehouseutils.DiscardsTable)
	identityMergeRulesTable = warehouseutils.ToProviderCase(warehouseutils.SNOWFLAKE, warehouseutils.IdentityMergeRulesTable)
	identityMappingsTable   = warehouseutils.ToProviderCase(warehouseutils.SNOWFLAKE, warehouseutils.IdentityMappingsTable)
)

type tableLoadRespT struct {
	dbHandle     *sql.DB
	stagingTable string
}

func ColumnsWithDataTypes(columns map[string]string, prefix string) string {
	var arr []string
	for name, dataType := range columns {
		arr = append(arr, fmt.Sprintf(`"%s%s" %s`, prefix, name, dataTypesMap[dataType]))
	}
	return strings.Join(arr, ",")
}

// schemaIdentifier returns [DATABASE_NAME].[NAMESPACE] format to access the schema directly.
func (sf *HandleT) schemaIdentifier() string {
	DatabaseName := warehouseutils.GetConfigValue(SFDbName, sf.Warehouse)
	return fmt.Sprintf(`"%s"."%s"`,
		DatabaseName,
		sf.Namespace,
	)
}

func (sf *HandleT) createTable(tableName string, columns map[string]string) (err error) {
	schemaIdentifier := sf.schemaIdentifier()
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s."%s" ( %v )`, schemaIdentifier, tableName, ColumnsWithDataTypes(columns, ""))
	pkgLogger.Infof("Creating table in snowflake for SF:%s : %v", sf.Warehouse.Destination.ID, sqlStatement)
	_, err = sf.Db.Exec(sqlStatement)
	return
}

func (sf *HandleT) tableExists(tableName string) (exists bool, err error) {
	sqlStatement := fmt.Sprintf(`SELECT EXISTS ( SELECT 1
   								 FROM   information_schema.tables
   								 WHERE  table_schema = '%s'
   								 AND    table_name = '%s'
								   )`, sf.Namespace, tableName)
	err = sf.Db.QueryRow(sqlStatement).Scan(&exists)
	return
}

func (sf *HandleT) columnExists(columnName, tableName string) (exists bool, err error) {
	sqlStatement := fmt.Sprintf(`SELECT EXISTS ( SELECT 1
   								 FROM   information_schema.columns
   								 WHERE  table_schema = '%s'
									AND table_name = '%s'
									AND column_name = '%s'
								   )`, sf.Namespace, tableName, columnName)
	err = sf.Db.QueryRow(sqlStatement).Scan(&exists)
	return
}

func (sf *HandleT) schemaExists() (exists bool, err error) {
	sqlStatement := fmt.Sprintf("SELECT EXISTS ( SELECT 1 FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '%s' )", sf.Namespace)
	r := sf.Db.QueryRow(sqlStatement)
	err = r.Scan(&exists)
	// ignore err if no results for query
	if err == sql.ErrNoRows {
		err = nil
	}
	return
}

func (sf *HandleT) addColumn(tableName, columnName, columnType string) (err error) {
	schemaIdentifier := sf.schemaIdentifier()
	sqlStatement := fmt.Sprintf(`ALTER TABLE %s."%s" ADD COLUMN "%s" %s`, schemaIdentifier, tableName, columnName, dataTypesMap[columnType])
	pkgLogger.Infof("SF: Adding column in snowflake for %s:%s : %v", sf.Warehouse.Namespace, sf.Warehouse.Destination.ID, sqlStatement)
	_, err = sf.Db.Exec(sqlStatement)
	return
}

func (sf *HandleT) createSchema() (err error) {
	schemaIdentifier := sf.schemaIdentifier()
	sqlStatement := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, schemaIdentifier)
	pkgLogger.Infof("SF: Creating schemaname in snowflake for %s:%s : %v", sf.Warehouse.Namespace, sf.Warehouse.Destination.ID, sqlStatement)
	_, err = sf.Db.Exec(sqlStatement)
	return
}

func checkAndIgnoreAlreadyExistError(err error) bool {
	if err != nil {
		// TODO: throw error if column already exists but of different type
		if e, ok := err.(*snowflake.SnowflakeError); ok {
			if e.SQLState == "42601" {
				return true
			}
		}
		return false
	}
	return true
}

func (sf *HandleT) authString() string {
	var auth string
	if misc.IsConfiguredToUseRudderObjectStorage(sf.Warehouse.Destination.Config) {
		tempAccessKeyId, tempSecretAccessKey, token, _ := warehouseutils.GetTemporaryS3Cred(misc.GetRudderObjectStorageAccessKeys())
		auth = fmt.Sprintf(`CREDENTIALS = (AWS_KEY_ID='%s' AWS_SECRET_KEY='%s' AWS_TOKEN='%s')`, tempAccessKeyId, tempSecretAccessKey, token)
	} else if sf.CloudProvider == "AWS" && warehouseutils.GetConfigValue(StorageIntegration, sf.Warehouse) == "" {
		tempAccessKeyId, tempSecretAccessKey, token, _ := warehouseutils.GetTemporaryS3Cred(warehouseutils.GetConfigValue(AWSAccessSecret, sf.Warehouse), warehouseutils.GetConfigValue(AWSAccessKey, sf.Warehouse))
		auth = fmt.Sprintf(`CREDENTIALS = (AWS_KEY_ID='%s' AWS_SECRET_KEY='%s' AWS_TOKEN='%s')`, tempAccessKeyId, tempSecretAccessKey, token)
	} else {
		auth = fmt.Sprintf(`STORAGE_INTEGRATION = %s`, warehouseutils.GetConfigValue(StorageIntegration, sf.Warehouse))
	}
	return auth
}

func (sf *HandleT) loadTable(tableName string, tableSchemaInUpload warehouseutils.TableSchemaT, dbHandle *sql.DB, skipClosingDBSession bool) (tableLoadResp tableLoadRespT, err error) {
	pkgLogger.Infof("SF: Starting load for table:%s\n", tableName)

	if dbHandle == nil {
		dbHandle, err = Connect(sf.getConnectionCredentials(OptionalCredsT{schemaName: sf.Namespace}))
		if err != nil {
			pkgLogger.Errorf("SF: Error establishing connection for copying table:%s: %v\n", tableName, err)
			return
		}
	}
	tableLoadResp.dbHandle = dbHandle
	if !skipClosingDBSession {
		defer dbHandle.Close()
	}

	// sort column names
	keys := reflect.ValueOf(tableSchemaInUpload).MapKeys()
	strkeys := make([]string, len(keys))
	for i := 0; i < len(keys); i++ {
		strkeys[i] = keys[i].String()
	}
	sort.Strings(strkeys)
	var sortedColumnNames string
	// TODO: use strings.Join() instead
	for index, key := range strkeys {
		if index > 0 {
			sortedColumnNames += `, `
		}
		sortedColumnNames += fmt.Sprintf(`"%s"`, key)
	}

	schemaIdentifier := sf.schemaIdentifier()
	stagingTableName := misc.TruncateStr(fmt.Sprintf(`%s%s_%s`, stagingTablePrefix, strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", ""), tableName), 127)
	sqlStatement := fmt.Sprintf(`CREATE TEMPORARY TABLE %[1]s."%[2]s" LIKE %[1]s."%[3]s"`, schemaIdentifier, stagingTableName, tableName)

	pkgLogger.Debugf("SF: Creating temporary table for table:%s at %s\n", tableName, sqlStatement)
	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("SF: Error creating temporary table for table:%s: %v\n", tableName, err)
		return
	}
	tableLoadResp.stagingTable = stagingTableName

	csvObjectLocation, err := sf.Uploader.GetSampleLoadFileLocation(tableName)
	if err != nil {
		return
	}
	loadFolder := warehouseutils.GetObjectFolder(sf.ObjectStorage, csvObjectLocation)
	// Truncating the columns by default so as to avoid size limitation errors
	// https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#copy-options-copyoptions
	sqlStatement = fmt.Sprintf(`COPY INTO %v(%v) FROM '%v' %s PATTERN = '.*\.csv\.gz'
		FILE_FORMAT = ( TYPE = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE ) TRUNCATECOLUMNS = TRUE`, fmt.Sprintf(`%s."%s"`, schemaIdentifier, stagingTableName), sortedColumnNames, loadFolder, sf.authString())

	sanitisedSQLStmt, regexErr := misc.ReplaceMultiRegex(sqlStatement, map[string]string{
		"AWS_KEY_ID='[^']*'":     "AWS_KEY_ID='***'",
		"AWS_SECRET_KEY='[^']*'": "AWS_SECRET_KEY='***'",
		"AWS_TOKEN='[^']*'":      "AWS_TOKEN='***'",
	})
	if regexErr == nil {
		pkgLogger.Infof("SF: Running COPY command for table:%s at %s\n", tableName, sanitisedSQLStmt)
	}

	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("SF: Error running COPY command: %v\n", err)
		return
	}

	primaryKey := "ID"
	if column, ok := primaryKeyMap[tableName]; ok {
		primaryKey = column
	}

	partitionKey := `"ID"`
	if column, ok := partitionKeyMap[tableName]; ok {
		partitionKey = column
	}

	var columnNames, stagingColumnNames, columnsWithValues string
	// TODO: use strings.Join() instead
	for idx, str := range strkeys {
		columnNames += fmt.Sprintf(`"%s"`, str)
		stagingColumnNames += fmt.Sprintf(`staging."%s"`, str)
		columnsWithValues += fmt.Sprintf(`original."%[1]s" = staging."%[1]s"`, str)
		if idx != len(strkeys)-1 {
			columnNames += `,`
			stagingColumnNames += `,`
			columnsWithValues += `,`
		}
	}

	var additionalJoinClause string
	if tableName == discardsTable {
		additionalJoinClause = fmt.Sprintf(`AND original."%[1]s" = staging."%[1]s" AND original."%[2]s" = staging."%[2]s"`, "TABLE_NAME", "COLUMN_NAME")
	}

	keepLatestRecordOnDedup := sf.Uploader.ShouldOnDedupUseNewRecord()

	if keepLatestRecordOnDedup {
		sqlStatement = fmt.Sprintf(`MERGE INTO %[9]s."%[1]s" AS original
									USING (
										SELECT * FROM (
											SELECT *, row_number() OVER (PARTITION BY %[8]s ORDER BY RECEIVED_AT DESC) AS _rudder_staging_row_number FROM %[9]s."%[2]s"
										) AS q WHERE _rudder_staging_row_number = 1
									) AS staging
									ON (original."%[3]s" = staging."%[3]s" %[7]s)
									WHEN MATCHED THEN
									UPDATE SET %[6]s
									WHEN NOT MATCHED THEN
									INSERT (%[4]s) VALUES (%[5]s)`, tableName, stagingTableName, primaryKey, columnNames, stagingColumnNames, columnsWithValues, additionalJoinClause, partitionKey, schemaIdentifier)
	} else {
		sqlStatement = fmt.Sprintf(`MERGE INTO %[8]s."%[1]s" AS original
										USING (
											SELECT * FROM (
												SELECT *, row_number() OVER (PARTITION BY %[7]s ORDER BY RECEIVED_AT DESC) AS _rudder_staging_row_number FROM %[8]s."%[2]s"
											) AS q WHERE _rudder_staging_row_number = 1
										) AS staging
										ON (original."%[3]s" = staging."%[3]s" %[6]s)
										WHEN NOT MATCHED THEN
										INSERT (%[4]s) VALUES (%[5]s)`, tableName, stagingTableName, primaryKey, columnNames, stagingColumnNames, additionalJoinClause, partitionKey, schemaIdentifier)
	}

	pkgLogger.Infof("SF: Dedup records for table:%s using staging table: %s\n", tableName, sqlStatement)
	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("SF: Error running MERGE for dedup: %v\n", err)
		return
	}

	pkgLogger.Infof("SF: Complete load for table:%s\n", tableName)
	return
}

func (sf *HandleT) LoadIdentityMergeRulesTable() (err error) {
	pkgLogger.Infof("SF: Starting load for table:%s\n", identityMergeRulesTable)

	pkgLogger.Infof("SF: Fetching load file location for %s", identityMergeRulesTable)
	var loadfile warehouseutils.LoadFileT
	loadfile, err = sf.Uploader.GetSingleLoadFile(identityMergeRulesTable)
	if err != nil {
		return err
	}

	dbHandle, err := Connect(sf.getConnectionCredentials(OptionalCredsT{schemaName: sf.Namespace}))
	if err != nil {
		pkgLogger.Errorf("SF: Error establishing connection for copying table:%s: %v\n", identityMergeRulesTable, err)
		return
	}

	sortedColumnNames := strings.Join([]string{"MERGE_PROPERTY_1_TYPE", "MERGE_PROPERTY_1_VALUE", "MERGE_PROPERTY_2_TYPE", "MERGE_PROPERTY_2_VALUE"}, ",")
	loadLocation := warehouseutils.GetObjectLocation(sf.ObjectStorage, loadfile.Location)
	schemaIdentifier := sf.schemaIdentifier()
	sqlStatement := fmt.Sprintf(`COPY INTO %v(%v) FROM '%v' %s PATTERN = '.*\.csv\.gz'
		FILE_FORMAT = ( TYPE = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE ) TRUNCATECOLUMNS = TRUE`, fmt.Sprintf(`%s."%s"`, schemaIdentifier, identityMergeRulesTable), sortedColumnNames, loadLocation, sf.authString())

	sanitisedSQLStmt, regexErr := misc.ReplaceMultiRegex(sqlStatement, map[string]string{
		"AWS_KEY_ID='[^']*'":     "AWS_KEY_ID='***'",
		"AWS_SECRET_KEY='[^']*'": "AWS_SECRET_KEY='***'",
	})
	if regexErr == nil {
		pkgLogger.Infof("SF: Dedup records for table:%s using staging table: %s\n", identityMergeRulesTable, sanitisedSQLStmt)
	}

	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("SF: Error running MERGE for dedup: %v\n", err)
		return
	}
	pkgLogger.Infof("SF: Complete load for table:%s\n", identityMergeRulesTable)
	return
}

func (sf *HandleT) LoadIdentityMappingsTable() (err error) {
	pkgLogger.Infof("SF: Starting load for table:%s\n", identityMappingsTable)
	pkgLogger.Infof("SF: Fetching load file location for %s", identityMappingsTable)
	var loadfile warehouseutils.LoadFileT

	loadfile, err = sf.Uploader.GetSingleLoadFile(identityMappingsTable)
	if err != nil {
		return err
	}

	dbHandle, err := Connect(sf.getConnectionCredentials(OptionalCredsT{schemaName: sf.Namespace}))
	if err != nil {
		pkgLogger.Errorf("SF: Error establishing connection for copying table:%s: %v\n", identityMappingsTable, err)
		return
	}

	schemaIdentifier := sf.schemaIdentifier()
	stagingTableName := misc.TruncateStr(fmt.Sprintf(`%s%s_%s`, stagingTablePrefix, strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", ""), identityMappingsTable), 127)
	sqlStatement := fmt.Sprintf(`CREATE TEMPORARY TABLE %[1]s."%[2]s" LIKE %[1]s."%[3]s"`, schemaIdentifier, stagingTableName, identityMappingsTable)

	pkgLogger.Infof("SF: Creating temporary table for table:%s at %s\n", identityMappingsTable, sqlStatement)
	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("SF: Error creating temporary table for table:%s: %v\n", identityMappingsTable, err)
		return
	}

	sqlStatement = fmt.Sprintf(`ALTER TABLE %s."%s" ADD COLUMN "ID" int AUTOINCREMENT start 1 increment 1`, schemaIdentifier, stagingTableName)
	pkgLogger.Infof("SF: Adding autoincrement column for table:%s at %s\n", stagingTableName, sqlStatement)
	_, err = dbHandle.Exec(sqlStatement)
	if err != nil && !checkAndIgnoreAlreadyExistError(err) {
		pkgLogger.Errorf("SF: Error adding autoincrement column for table:%s: %v\n", stagingTableName, err)
		return
	}

	loadLocation := warehouseutils.GetObjectLocation(sf.ObjectStorage, loadfile.Location)
	sqlStatement = fmt.Sprintf(`COPY INTO %v("MERGE_PROPERTY_TYPE", "MERGE_PROPERTY_VALUE", "RUDDER_ID", "UPDATED_AT") FROM '%v' %s PATTERN = '.*\.csv\.gz'
		FILE_FORMAT = ( TYPE = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE ) TRUNCATECOLUMNS = TRUE`, fmt.Sprintf(`%s."%s"`, schemaIdentifier, stagingTableName), loadLocation, sf.authString())

	pkgLogger.Infof("SF: Dedup records for table:%s using staging table: %s\n", identityMappingsTable, sqlStatement)
	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("SF: Error running MERGE for dedup: %v\n", err)
		return
	}

	sqlStatement = fmt.Sprintf(`MERGE INTO %[3]s."%[1]s" AS original
									USING (
										SELECT * FROM (
											SELECT *, row_number() OVER (PARTITION BY "MERGE_PROPERTY_TYPE", "MERGE_PROPERTY_VALUE" ORDER BY "ID" DESC) AS _rudder_staging_row_number FROM %[3]s."%[2]s"
										) AS q WHERE _rudder_staging_row_number = 1
									) AS staging
									ON (original."MERGE_PROPERTY_TYPE" = staging."MERGE_PROPERTY_TYPE" AND original."MERGE_PROPERTY_VALUE" = staging."MERGE_PROPERTY_VALUE")
									WHEN MATCHED THEN
									UPDATE SET original."RUDDER_ID" = staging."RUDDER_ID", original."UPDATED_AT" =  staging."UPDATED_AT"
									WHEN NOT MATCHED THEN
									INSERT ("MERGE_PROPERTY_TYPE", "MERGE_PROPERTY_VALUE", "RUDDER_ID", "UPDATED_AT") VALUES (staging."MERGE_PROPERTY_TYPE", staging."MERGE_PROPERTY_VALUE", staging."RUDDER_ID", staging."UPDATED_AT")`, identityMappingsTable, stagingTableName, schemaIdentifier)
	pkgLogger.Infof("SF: Dedup records for table:%s using staging table: %s\n", identityMappingsTable, sqlStatement)
	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("SF: Error running MERGE for dedup: %v\n", err)
		return
	}
	pkgLogger.Infof("SF: Complete load for table:%s\n", identityMappingsTable)
	return
}

func (sf *HandleT) loadUserTables() (errorMap map[string]error) {
	identifyColMap := sf.Uploader.GetTableSchemaInUpload(identifiesTable)
	if len(identifyColMap) == 0 {
		return errorMap
	}
	errorMap = map[string]error{identifiesTable: nil}
	pkgLogger.Infof("SF: Starting load for identifies and users tables\n")

	resp, err := sf.loadTable(identifiesTable, sf.Uploader.GetTableSchemaInUpload(identifiesTable), nil, true)
	if err != nil {
		errorMap[identifiesTable] = err
		return errorMap
	}
	defer resp.dbHandle.Close()

	if len(sf.Uploader.GetTableSchemaInUpload(usersTable)) == 0 {
		return errorMap
	}
	errorMap[usersTable] = nil

	userColMap := sf.Uploader.GetTableSchemaInWarehouse(usersTable)
	var userColNames, firstValProps, identifyColNames []string
	for colName := range userColMap {
		if colName == "ID" {
			continue
		}
		userColNames = append(userColNames, fmt.Sprintf(`"%s"`, colName))
		if _, ok := identifyColMap[colName]; ok {
			identifyColNames = append(identifyColNames, fmt.Sprintf(`"%s"`, colName))
		} else {
			// This is to handle cases when column in users table not present in inditifies table
			identifyColNames = append(identifyColNames, fmt.Sprintf(`NULL as "%s"`, colName))
		}
		firstValProps = append(firstValProps, fmt.Sprintf(`FIRST_VALUE("%[1]s" IGNORE NULLS) OVER (PARTITION BY ID ORDER BY RECEIVED_AT DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "%[1]s"`, colName))
	}
	schemaIdentifier := sf.schemaIdentifier()
	stagingTableName := misc.TruncateStr(fmt.Sprintf(`%s%s_%s`, stagingTablePrefix, strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", ""), usersTable), 127)
	sqlStatement := fmt.Sprintf(`CREATE TEMPORARY TABLE %[1]s."%[2]s" AS (SELECT DISTINCT * FROM
										(
											SELECT
											"ID", %[3]s
											FROM (
												(
													SELECT "ID", %[6]s FROM %[1]s."%[4]s" WHERE "ID" in (SELECT "USER_ID" FROM %[1]s."%[5]s" WHERE "USER_ID" IS NOT NULL)
												) UNION
												(
													SELECT "USER_ID", %[7]s FROM %[1]s."%[5]s" WHERE "USER_ID" IS NOT NULL
												)
											)
										)
									)`,
		schemaIdentifier,                    // 1
		stagingTableName,                    // 2
		strings.Join(firstValProps, ","),    // 3
		usersTable,                          // 4
		resp.stagingTable,                   // 5
		strings.Join(userColNames, ","),     // 6
		strings.Join(identifyColNames, ","), // 7
	)
	pkgLogger.Infof("SF: Creating staging table for users: %s\n", sqlStatement)
	_, err = resp.dbHandle.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("SF: Error creating temporary table for table:%s: %v\n", usersTable, err)
		errorMap[usersTable] = err
		return errorMap
	}

	primaryKey := `"ID"`
	columnNames := append([]string{`"ID"`}, userColNames...)
	columnNamesStr := strings.Join(columnNames, ",")
	var columnsWithValues, stagingColumnValues string
	for idx, colName := range columnNames {
		columnsWithValues += fmt.Sprintf(`original.%[1]s = staging.%[1]s`, colName)
		stagingColumnValues += fmt.Sprintf(`staging.%s`, colName)
		if idx != len(columnNames)-1 {
			columnsWithValues += `,`
			stagingColumnValues += `,`
		}
	}

	sqlStatement = fmt.Sprintf(`MERGE INTO %[7]s."%[1]s" AS original
									USING (
										SELECT %[3]s FROM %[7]s."%[2]s"
									) AS staging
									ON (original.%[4]s = staging.%[4]s)
									WHEN MATCHED THEN
									UPDATE SET %[5]s
									WHEN NOT MATCHED THEN
									INSERT (%[3]s) VALUES (%[6]s)`, usersTable, stagingTableName, columnNamesStr, primaryKey, columnsWithValues, stagingColumnValues, schemaIdentifier)
	pkgLogger.Infof("SF: Dedup records for table:%s using staging table: %s\n", usersTable, sqlStatement)
	_, err = resp.dbHandle.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("SF: Error running MERGE for dedup: %v\n", err)
		errorMap[usersTable] = err
		return errorMap
	}
	pkgLogger.Infof("SF: Complete load for table:%s", usersTable)
	return errorMap
}

type SnowflakeCredentialsT struct {
	Account    string
	WHName     string
	DBName     string
	Username   string
	Password   string
	schemaName string
	timeout    time.Duration
}

func Connect(cred SnowflakeCredentialsT) (*sql.DB, error) {
	urlConfig := snowflake.Config{
		Account:     cred.Account,
		User:        cred.Username,
		Password:    cred.Password,
		Database:    cred.DBName,
		Schema:      cred.schemaName,
		Warehouse:   cred.WHName,
		Application: "Rudderstack",
	}

	if cred.timeout > 0 {
		urlConfig.LoginTimeout = cred.timeout
	}

	var err error
	dsn, err := snowflake.DSN(&urlConfig)
	if err != nil {
		return nil, fmt.Errorf("SF: Error costructing DSN to connect : (%v)", err)
	}

	var db *sql.DB
	if db, err = sql.Open("snowflake", dsn); err != nil {
		return nil, fmt.Errorf("SF: snowflake connect error : (%v)", err)
	}

	alterStatement := `ALTER SESSION SET ABORT_DETACHED_QUERY=TRUE`
	pkgLogger.Infof("SF: Altering session with abort_detached_query for snowflake: %v", alterStatement)
	_, err = db.Exec(alterStatement)
	if err != nil {
		return nil, fmt.Errorf("SF: snowflake alter session error : (%v)", err)
	}
	return db, nil
}

func (sf *HandleT) CreateSchema() (err error) {
	var schemaExists bool
	schemaIdentifier := sf.schemaIdentifier()
	schemaExists, err = sf.schemaExists()
	if err != nil {
		pkgLogger.Errorf("SF: Error checking if schema: %s exists: %v", schemaIdentifier, err)
		return err
	}
	if schemaExists {
		pkgLogger.Infof("SF: Skipping creating schema: %s since it already exists", schemaIdentifier)
		return
	}
	return sf.createSchema()
}

func (sf *HandleT) CreateTable(tableName string, columnMap map[string]string) (err error) {
	return sf.createTable(tableName, columnMap)
}

func (sf *HandleT) DropTable(tableName string) (err error) {
	schemaIdentifier := sf.schemaIdentifier()
	sqlStatement := fmt.Sprintf(`DROP TABLE %[1]s."%[2]s"`, schemaIdentifier, tableName)
	pkgLogger.Infof("SF: Dropping table in snowflake for SF:%s : %v", sf.Warehouse.Destination.ID, sqlStatement)
	_, err = sf.Db.Exec(sqlStatement)
	return
}

func (sf *HandleT) AddColumn(tableName, columnName, columnType string) (err error) {
	err = sf.addColumn(tableName, columnName, columnType)
	schemaIdentifier := sf.schemaIdentifier()
	if err != nil {
		if checkAndIgnoreAlreadyExistError(err) {
			pkgLogger.Infof("SF: Column %s already exists on %s.%s \nResponse: %v", columnName, schemaIdentifier, tableName, err)
			err = nil
		}
	}
	return err
}

func (sf *HandleT) AlterColumn(_, _, _ string) (err error) {
	return
}

// DownloadIdentityRules gets distinct combinations of anonymous_id, user_id from tables in warehouse
func (sf *HandleT) DownloadIdentityRules(gzWriter *misc.GZipWriter) (err error) {
	getFromTable := func(tableName string) (err error) {
		var exists bool
		exists, err = sf.tableExists(tableName)
		if err != nil || !exists {
			return
		}

		schemaIdentifier := sf.schemaIdentifier()
		sqlStatement := fmt.Sprintf(`SELECT count(*) FROM %s."%s"`, schemaIdentifier, tableName)
		var totalRows int64
		err = sf.Db.QueryRow(sqlStatement).Scan(&totalRows)
		if err != nil {
			return
		}

		// check if table in warehouse has anonymous_id and user_id and construct accordingly
		hasAnonymousID, err := sf.columnExists("ANONYMOUS_ID", tableName)
		if err != nil {
			return
		}
		hasUserID, err := sf.columnExists("USER_ID", tableName)
		if err != nil {
			return
		}

		var toSelectFields string
		if hasAnonymousID && hasUserID {
			toSelectFields = `"ANONYMOUS_ID", "USER_ID"`
		} else if hasAnonymousID {
			toSelectFields = `"ANONYMOUS_ID", NULL AS "USER_ID"`
		} else if hasUserID {
			toSelectFields = `NULL AS "ANONYMOUS_ID", "USER_ID"`
		} else {
			pkgLogger.Infof("SF: ANONYMOUS_ID, USER_ID columns not present in table: %s", tableName)
			return nil
		}

		batchSize := int64(10000)
		var offset int64
		for {
			// TODO: Handle case for missing anonymous_id, user_id columns
			sqlStatement = fmt.Sprintf(`SELECT DISTINCT %s FROM %s."%s" LIMIT %d OFFSET %d`, toSelectFields, schemaIdentifier, tableName, batchSize, offset)
			pkgLogger.Infof("SF: Downloading distinct combinations of anonymous_id, user_id: %s, totalRows: %d", sqlStatement, totalRows)
			var rows *sql.Rows
			rows, err = sf.Db.Query(sqlStatement)
			if err != nil {
				return
			}

			for rows.Next() {
				var buff bytes.Buffer
				csvWriter := csv.NewWriter(&buff)
				var csvRow []string

				var anonymousID, userID sql.NullString
				err = rows.Scan(&anonymousID, &userID)
				if err != nil {
					return
				}

				if !anonymousID.Valid && !userID.Valid {
					continue
				}

				// avoid setting null merge_property_1 to avoid not null constraint in local postgres
				if anonymousID.Valid {
					csvRow = append(csvRow, "anonymous_id", anonymousID.String, "user_id", userID.String)
				} else {
					csvRow = append(csvRow, "user_id", userID.String, "anonymous_id", anonymousID.String)
				}
				csvWriter.Write(csvRow)
				csvWriter.Flush()
				gzWriter.WriteGZ(buff.String())
			}

			offset += batchSize
			if offset >= totalRows {
				break
			}
		}
		return
	}

	tables := []string{"TRACKS", "PAGES", "SCREENS", "IDENTIFIES", "ALIASES"}
	for _, table := range tables {
		err = getFromTable(table)
		if err != nil {
			return
		}
	}
	return nil
}

func (sf *HandleT) CrashRecover(_ warehouseutils.WarehouseT) (err error) {
	return
}

func (sf *HandleT) IsEmpty(warehouse warehouseutils.WarehouseT) (empty bool, err error) {
	empty = true

	sf.Warehouse = warehouse
	sf.Namespace = warehouse.Namespace
	sf.Db, err = Connect(sf.getConnectionCredentials(OptionalCredsT{}))
	if err != nil {
		return
	}
	defer sf.Db.Close()

	tables := []string{"TRACKS", "PAGES", "SCREENS", "IDENTIFIES", "ALIASES"}
	for _, tableName := range tables {
		var exists bool
		exists, err = sf.tableExists(tableName)
		if err != nil {
			return
		}
		if !exists {
			continue
		}
		schemaIdentifier := sf.schemaIdentifier()
		sqlStatement := fmt.Sprintf(`SELECT COUNT(*) FROM %s."%s"`, schemaIdentifier, tableName)
		var count int64
		err = sf.Db.QueryRow(sqlStatement).Scan(&count)
		if err != nil {
			return
		}
		if count > 0 {
			empty = false
			return
		}
	}
	return
}

type OptionalCredsT struct {
	schemaName string
}

func (sf *HandleT) getConnectionCredentials(opts OptionalCredsT) SnowflakeCredentialsT {
	return SnowflakeCredentialsT{
		Account:    warehouseutils.GetConfigValue(SFAccount, sf.Warehouse),
		WHName:     warehouseutils.GetConfigValue(SFWarehouse, sf.Warehouse),
		DBName:     warehouseutils.GetConfigValue(SFDbName, sf.Warehouse),
		Username:   warehouseutils.GetConfigValue(SFUserName, sf.Warehouse),
		Password:   warehouseutils.GetConfigValue(SFPassword, sf.Warehouse),
		schemaName: opts.schemaName,
		timeout:    sf.ConnectTimeout,
	}
}

func (sf *HandleT) Setup(warehouse warehouseutils.WarehouseT, uploader warehouseutils.UploaderI) (err error) {
	sf.Warehouse = warehouse
	sf.Namespace = warehouse.Namespace
	sf.CloudProvider = warehouseutils.SnowflakeCloudProvider(warehouse.Destination.Config)
	sf.Uploader = uploader
	sf.ObjectStorage = warehouseutils.ObjectStorageType(warehouseutils.SNOWFLAKE, warehouse.Destination.Config, sf.Uploader.UseRudderStorage())

	sf.Db, err = Connect(sf.getConnectionCredentials(OptionalCredsT{}))
	return err
}

func (sf *HandleT) TestConnection(warehouse warehouseutils.WarehouseT) (err error) {
	sf.Warehouse = warehouse
	sf.Db, err = Connect(sf.getConnectionCredentials(OptionalCredsT{}))
	if err != nil {
		return
	}
	defer sf.Db.Close()

	ctx, cancel := context.WithTimeout(context.TODO(), sf.ConnectTimeout)
	defer cancel()

	err = sf.Db.PingContext(ctx)
	if err == context.DeadlineExceeded {
		return fmt.Errorf("connection testing timed out after %d sec", sf.ConnectTimeout/time.Second)
	}
	if err != nil {
		return err
	}

	return nil
}

// FetchSchema queries snowflake and returns the schema assoiciated with provided namespace
func (sf *HandleT) FetchSchema(warehouse warehouseutils.WarehouseT) (schema warehouseutils.SchemaT, err error) {
	sf.Warehouse = warehouse
	sf.Namespace = warehouse.Namespace
	dbHandle, err := Connect(sf.getConnectionCredentials(OptionalCredsT{}))
	if err != nil {
		return
	}
	defer dbHandle.Close()

	schema = make(warehouseutils.SchemaT)
	sqlStatement := fmt.Sprintf(`SELECT t.table_name, c.column_name, c.data_type
									FROM INFORMATION_SCHEMA.TABLES as t
									JOIN INFORMATION_SCHEMA.COLUMNS as c
									ON t.table_schema = c.table_schema and t.table_name = c.table_name
									WHERE t.table_schema = '%s'`, sf.Namespace)

	rows, err := dbHandle.Query(sqlStatement)
	if err != nil && err != sql.ErrNoRows {
		pkgLogger.Errorf("SF: Error in fetching schema from snowflake destination:%v, query: %v", sf.Warehouse.Destination.ID, sqlStatement)
		return
	}
	if err == sql.ErrNoRows {
		pkgLogger.Infof("SF: No rows, while fetching schema from  destination:%v, query: %v", sf.Warehouse.Identifier, sqlStatement)
		return schema, nil
	}
	defer rows.Close()
	for rows.Next() {
		var tName, cName, cType string
		err = rows.Scan(&tName, &cName, &cType)
		if err != nil {
			pkgLogger.Errorf("SF: Error in processing fetched schema from snowflake destination:%v", sf.Warehouse.Destination.ID)
			return
		}
		if _, ok := schema[tName]; !ok {
			schema[tName] = make(map[string]string)
		}
		if datatype, ok := dataTypesMapToRudder[cType]; ok {
			schema[tName][cName] = datatype
		} else {
			warehouseutils.WHCounterStat(warehouseutils.RUDDER_MISSING_DATATYPE, &sf.Warehouse, warehouseutils.Tag{Name: "datatype", Value: cType}).Count(1)
		}
	}
	return
}

func (sf *HandleT) Cleanup() {
	if sf.Db != nil {
		sf.Db.Close()
	}
}

func (sf *HandleT) LoadUserTables() map[string]error {
	return sf.loadUserTables()
}

func (sf *HandleT) LoadTable(tableName string) error {
	_, err := sf.loadTable(tableName, sf.Uploader.GetTableSchemaInUpload(tableName), nil, false)
	return err
}

func (sf *HandleT) GetTotalCountInTable(tableName string) (total int64, err error) {
	schemaIdentifier := sf.schemaIdentifier()
	sqlStatement := fmt.Sprintf(`SELECT count(*) FROM %[1]s."%[2]s"`, schemaIdentifier, tableName)
	err = sf.Db.QueryRow(sqlStatement).Scan(&total)
	if err != nil {
		pkgLogger.Errorf(`SF: Error getting total count in table %s:%s`, schemaIdentifier, tableName)
	}
	return
}

func (sf *HandleT) Connect(warehouse warehouseutils.WarehouseT) (client.Client, error) {
	sf.Warehouse = warehouse
	sf.Namespace = warehouse.Namespace
	sf.CloudProvider = warehouseutils.SnowflakeCloudProvider(warehouse.Destination.Config)
	sf.ObjectStorage = warehouseutils.ObjectStorageType(
		warehouseutils.SNOWFLAKE,
		warehouse.Destination.Config,
		misc.IsConfiguredToUseRudderObjectStorage(sf.Warehouse.Destination.Config),
	)
	dbHandle, err := Connect(sf.getConnectionCredentials(OptionalCredsT{}))
	if err != nil {
		return client.Client{}, err
	}

	return client.Client{Type: client.SQLClient, SQL: dbHandle}, err
}

func (sf *HandleT) LoadTestTable(location, tableName string, _ map[string]interface{}, _ string) (err error) {
	loadFolder := warehouseutils.GetObjectFolder(sf.ObjectStorage, location)
	schemaIdentifier := sf.schemaIdentifier()
	sqlStatement := fmt.Sprintf(`COPY INTO %v(%v) FROM '%v' %s PATTERN = '.*\.csv\.gz'
		FILE_FORMAT = ( TYPE = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE ) TRUNCATECOLUMNS = TRUE`,
		fmt.Sprintf(`%s."%s"`, schemaIdentifier, tableName),
		fmt.Sprintf(`"%s", "%s"`, "id", "val"),
		loadFolder,
		sf.authString(),
	)

	_, err = sf.Db.Exec(sqlStatement)
	return
}

func (sf *HandleT) SetConnectionTimeout(timeout time.Duration) {
	sf.ConnectTimeout = timeout
}
