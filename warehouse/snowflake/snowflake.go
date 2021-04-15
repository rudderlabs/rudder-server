package snowflake

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	uuid "github.com/satori/go.uuid"
	snowflake "github.com/snowflakedb/gosnowflake" //blank comment
)

var (
	stagingTablePrefix string
	pkgLogger          logger.LoggerI
)

func init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("warehouse").Child("snowflake")
}

func loadConfig() {
	stagingTablePrefix = "RUDDER_STAGING_"
}

type HandleT struct {
	Db            *sql.DB
	Namespace     string
	CloudProvider string
	ObjectStorage string
	Stage         string
	Warehouse     warehouseutils.WarehouseT
	Uploader      warehouseutils.UploaderI
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

const PROVIDER = "SNOWFLAKE"

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
	usersTable              = warehouseutils.ToProviderCase(PROVIDER, warehouseutils.UsersTable)
	identifiesTable         = warehouseutils.ToProviderCase(PROVIDER, warehouseutils.IdentifiesTable)
	discardsTable           = warehouseutils.ToProviderCase(PROVIDER, warehouseutils.DiscardsTable)
	identityMergeRulesTable = warehouseutils.ToProviderCase(PROVIDER, warehouseutils.IdentityMergeRulesTable)
	identityMappingsTable   = warehouseutils.ToProviderCase(PROVIDER, warehouseutils.IdentityMappingsTable)
)

type tableLoadRespT struct {
	dbHandle     *sql.DB
	stagingTable string
}

func columnsWithDataTypes(columns map[string]string, prefix string) string {
	arr := []string{}
	for name, dataType := range columns {
		arr = append(arr, fmt.Sprintf(`"%s%s" %s`, prefix, name, dataTypesMap[dataType]))
	}
	return strings.Join(arr[:], ",")
}

func (sf *HandleT) createTable(name string, columns map[string]string) (err error) {
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" ( %v )`, name, columnsWithDataTypes(columns, ""))
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

func (sf *HandleT) columnExists(columnName string, tableName string) (exists bool, err error) {
	sqlStatement := fmt.Sprintf(`SELECT EXISTS ( SELECT 1
   								 FROM   information_schema.columns
   								 WHERE  table_schema = '%s'
									AND table_name = '%s'
									AND column_name = '%s'
								   )`, sf.Namespace, tableName, columnName)
	err = sf.Db.QueryRow(sqlStatement).Scan(&exists)
	return
}

func (sf *HandleT) schemaExists(schemaname string) (exists bool, err error) {
	var count int
	sqlStatement := fmt.Sprintf(`SELECT count(*) FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '%s'`, sf.Namespace)
	err = sf.Db.QueryRow(sqlStatement).Scan(&count)
	// ignore err if no results for query
	if err == sql.ErrNoRows {
		err = nil
	}
	exists = count > 0
	return
}

func (sf *HandleT) addColumn(tableName string, columnName string, columnType string) (err error) {
	sqlStatement := fmt.Sprintf(`ALTER TABLE "%s" ADD COLUMN "%s" %s`, tableName, columnName, dataTypesMap[columnType])
	pkgLogger.Infof("SF: Adding column in snowflake for %s:%s : %v", sf.Warehouse.Namespace, sf.Warehouse.Destination.ID, sqlStatement)
	_, err = sf.Db.Exec(sqlStatement)
	return
}

func (sf *HandleT) createSchema() (err error) {
	sqlStatement := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s"`, sf.Namespace)
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
	if sf.CloudProvider == "AWS" && warehouseutils.GetConfigValue(StorageIntegration, sf.Warehouse) == "" {
		auth = fmt.Sprintf(`CREDENTIALS = (AWS_KEY_ID='%s' AWS_SECRET_KEY='%s')`, warehouseutils.GetConfigValue(AWSAccessSecret, sf.Warehouse), warehouseutils.GetConfigValue(AWSAccessKey, sf.Warehouse))
	} else {
		auth = fmt.Sprintf(`STORAGE_INTEGRATION = %s`, warehouseutils.GetConfigValue(StorageIntegration, sf.Warehouse))
	}
	return auth
}

func (sf *HandleT) loadTable(tableName string, tableSchemaInUpload warehouseutils.TableSchemaT, dbHandle *sql.DB, skipClosingDBSession bool) (tableLoadResp tableLoadRespT, err error) {
	pkgLogger.Infof("SF: Starting load for table:%s\n", tableName)

	if dbHandle == nil {
		dbHandle, err = connect(sf.getConnectionCredentials(OptionalCredsT{schemaName: sf.Namespace}))
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
	//TODO: use strings.Join() instead
	for index, key := range strkeys {
		if index > 0 {
			sortedColumnNames += `, `
		}
		sortedColumnNames += fmt.Sprintf(`"%s"`, key)
	}

	stagingTableName := misc.TruncateStr(fmt.Sprintf(`%s%s_%s`, stagingTablePrefix, strings.Replace(uuid.NewV4().String(), "-", "", -1), tableName), 127)
	sqlStatement := fmt.Sprintf(`CREATE TEMPORARY TABLE "%s" LIKE "%s"`, stagingTableName, tableName)

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

	sqlStatement = fmt.Sprintf(`COPY INTO %v(%v) FROM '%v' %s PATTERN = '.*\.csv\.gz'
		FILE_FORMAT = ( TYPE = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE )`, fmt.Sprintf(`"%s"."%s"`, sf.Namespace, stagingTableName), sortedColumnNames, loadFolder, sf.authString())

	sanitisedSQLStmt, regexErr := misc.ReplaceMultiRegex(sqlStatement, map[string]string{
		"AWS_KEY_ID='[^']*'":     "AWS_KEY_ID='***'",
		"AWS_SECRET_KEY='[^']*'": "AWS_SECRET_KEY='***'",
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
	//TODO: use strings.Join() instead
	for idx, str := range strkeys {
		columnNames += str
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

	sqlStatement = fmt.Sprintf(`MERGE INTO "%[1]s" AS original
									USING (
										SELECT * FROM (
											SELECT *, row_number() OVER (PARTITION BY %[7]s ORDER BY RECEIVED_AT ASC) AS _rudder_staging_row_number FROM "%[2]s"
										) AS q WHERE _rudder_staging_row_number = 1
									) AS staging
									ON (original."%[3]s" = staging."%[3]s" %[6]s)
									WHEN NOT MATCHED THEN
									INSERT (%[4]s) VALUES (%[5]s)`, tableName, stagingTableName, primaryKey, columnNames, stagingColumnNames, additionalJoinClause, partitionKey)
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
	var location string
	location, err = sf.Uploader.GetSingleLoadFileLocation(identityMergeRulesTable)
	if err != nil {
		return err
	}

	dbHandle, err := connect(sf.getConnectionCredentials(OptionalCredsT{schemaName: sf.Namespace}))
	if err != nil {
		pkgLogger.Errorf("SF: Error establishing connection for copying table:%s: %v\n", identityMergeRulesTable, err)
		return
	}

	sortedColumnNames := strings.Join([]string{"MERGE_PROPERTY_1_TYPE", "MERGE_PROPERTY_1_VALUE", "MERGE_PROPERTY_2_TYPE", "MERGE_PROPERTY_2_VALUE"}, ",")
	loadLocation := warehouseutils.GetObjectLocation(sf.ObjectStorage, location)
	sqlStatement := fmt.Sprintf(`COPY INTO %v(%v) FROM '%v' %s PATTERN = '.*\.csv\.gz'
		FILE_FORMAT = ( TYPE = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE )`, fmt.Sprintf(`"%s"."%s"`, sf.Namespace, identityMergeRulesTable), sortedColumnNames, loadLocation, sf.authString())

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
	var location string

	location, err = sf.Uploader.GetSingleLoadFileLocation(identityMappingsTable)
	if err != nil {
		return err
	}

	dbHandle, err := connect(sf.getConnectionCredentials(OptionalCredsT{schemaName: sf.Namespace}))
	if err != nil {
		pkgLogger.Errorf("SF: Error establishing connection for copying table:%s: %v\n", identityMappingsTable, err)
		return
	}

	stagingTableName := misc.TruncateStr(fmt.Sprintf(`%s%s_%s`, stagingTablePrefix, strings.Replace(uuid.NewV4().String(), "-", "", -1), identityMappingsTable), 127)
	sqlStatement := fmt.Sprintf(`CREATE TEMPORARY TABLE "%s" LIKE "%s"`, stagingTableName, identityMappingsTable)

	pkgLogger.Infof("SF: Creating temporary table for table:%s at %s\n", identityMappingsTable, sqlStatement)
	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("SF: Error creating temporary table for table:%s: %v\n", identityMappingsTable, err)
		return
	}

	loadLocation := warehouseutils.GetObjectLocation(sf.ObjectStorage, location)
	sqlStatement = fmt.Sprintf(`COPY INTO %v("MERGE_PROPERTY_TYPE", "MERGE_PROPERTY_VALUE", "RUDDER_ID", "UPDATED_AT") FROM '%v' %s PATTERN = '.*\.csv\.gz'
		FILE_FORMAT = ( TYPE = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE )`, fmt.Sprintf(`"%s"."%s"`, sf.Namespace, stagingTableName), loadLocation, sf.authString())

	pkgLogger.Infof("SF: Dedup records for table:%s using staging table: %s\n", identityMappingsTable, sqlStatement)
	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("SF: Error running MERGE for dedup: %v\n", err)
		return
	}

	sqlStatement = fmt.Sprintf(`MERGE INTO "%[1]s" AS original
									USING (
										SELECT * FROM (
											SELECT *, row_number() OVER (PARTITION BY "MERGE_PROPERTY_TYPE", "MERGE_PROPERTY_VALUE" ORDER BY "UPDATED_AT" DESC) AS _rudder_staging_row_number FROM "%[2]s"
										) AS q WHERE _rudder_staging_row_number = 1
									) AS staging
									ON (original."MERGE_PROPERTY_TYPE" = staging."MERGE_PROPERTY_TYPE" AND original."MERGE_PROPERTY_VALUE" = staging."MERGE_PROPERTY_VALUE")
									WHEN MATCHED THEN
									UPDATE SET original."RUDDER_ID" = staging."RUDDER_ID", original."UPDATED_AT" =  staging."UPDATED_AT"
									WHEN NOT MATCHED THEN
									INSERT ("MERGE_PROPERTY_TYPE", "MERGE_PROPERTY_VALUE", "RUDDER_ID", "UPDATED_AT") VALUES (staging."MERGE_PROPERTY_TYPE", staging."MERGE_PROPERTY_VALUE", staging."RUDDER_ID", staging."UPDATED_AT")`, identityMappingsTable, stagingTableName)
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
	if len(sf.Uploader.GetTableSchemaInUpload(identifiesTable)) == 0 {
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
	var userColNames, firstValProps []string
	for colName := range userColMap {
		if colName == "ID" {
			continue
		}
		userColNames = append(userColNames, fmt.Sprintf(`"%s"`, colName))
		firstValProps = append(firstValProps, fmt.Sprintf(`FIRST_VALUE("%[1]s" IGNORE NULLS) OVER (PARTITION BY ID ORDER BY RECEIVED_AT DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "%[1]s"`, colName))
	}
	stagingTableName := misc.TruncateStr(fmt.Sprintf(`%s%s_%s`, stagingTablePrefix, strings.Replace(uuid.NewV4().String(), "-", "", -1), usersTable), 127)
	sqlStatement := fmt.Sprintf(`CREATE TEMPORARY TABLE "%[2]s" AS (SELECT DISTINCT * FROM
										(
											SELECT
											"ID", %[3]s
											FROM (
												(
													SELECT "ID", %[6]s FROM "%[1]s"."%[4]s" WHERE "ID" in (SELECT "USER_ID" FROM "%[5]s" WHERE "USER_ID" IS NOT NULL)
												) UNION
												(
													SELECT "USER_ID", %[6]s FROM "%[5]s" WHERE "USER_ID" IS NOT NULL
												)
											)
										)
									)`,
		sf.Namespace,                     // 1
		stagingTableName,                 // 2
		strings.Join(firstValProps, ","), // 3
		usersTable,                       // 4
		resp.stagingTable,                // 5
		strings.Join(userColNames, ","),  // 6
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

	sqlStatement = fmt.Sprintf(`MERGE INTO "%[1]s" AS original
									USING (
										SELECT %[3]s FROM "%[2]s"
									) AS staging
									ON (original.%[4]s = staging.%[4]s)
									WHEN MATCHED THEN
									UPDATE SET %[5]s
									WHEN NOT MATCHED THEN
									INSERT (%[3]s) VALUES (%[6]s)`, usersTable, stagingTableName, columnNamesStr, primaryKey, columnsWithValues, stagingColumnValues)
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
	account    string
	whName     string
	dbName     string
	username   string
	password   string
	schemaName string
}

func connect(cred SnowflakeCredentialsT) (*sql.DB, error) {
	urlConfig := snowflake.Config{
		Account:   cred.account,
		User:      cred.username,
		Password:  cred.password,
		Database:  cred.dbName,
		Schema:    cred.schemaName,
		Warehouse: cred.whName,
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
	schemaExists, err = sf.schemaExists(sf.Namespace)
	if err != nil {
		pkgLogger.Errorf("SF: Error checking if schema: %s exists: %v", sf.Namespace, err)
		return err
	}
	if schemaExists {
		pkgLogger.Infof("SF: Skipping creating schema: %s since it already exists", sf.Namespace)
		return
	}
	return sf.createSchema()
}

func (sf *HandleT) CreateTable(tableName string, columnMap map[string]string) (err error) {
	sqlStatement := fmt.Sprintf(`USE SCHEMA "%s"`, sf.Namespace)
	_, err = sf.Db.Exec(sqlStatement)
	if err != nil {
		return err
	}

	return sf.createTable(tableName, columnMap)
}

func (sf *HandleT) AddColumn(tableName string, columnName string, columnType string) (err error) {
	sqlStatement := fmt.Sprintf(`USE SCHEMA "%s"`, sf.Namespace)
	_, err = sf.Db.Exec(sqlStatement)
	if err != nil {
		return err
	}

	err = sf.addColumn(tableName, columnName, columnType)
	if err != nil {
		if checkAndIgnoreAlreadyExistError(err) {
			pkgLogger.Infof("SF: Column %s already exists on %s.%s \nResponse: %v", columnName, sf.Namespace, tableName, err)
			err = nil
		}
	}
	return err
}

func (sf *HandleT) AlterColumn(tableName string, columnName string, columnType string) (err error) {
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

		sqlStatement := fmt.Sprintf(`SELECT count(*) FROM "%s"."%s"`, sf.Namespace, tableName)
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
			sqlStatement = fmt.Sprintf(`SELECT DISTINCT %s FROM "%s"."%s" LIMIT %d OFFSET %d`, toSelectFields, sf.Namespace, tableName, batchSize, offset)
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

func (sf *HandleT) CrashRecover(warehouse warehouseutils.WarehouseT) (err error) {
	return
}

func (sf *HandleT) IsEmpty(warehouse warehouseutils.WarehouseT) (empty bool, err error) {
	empty = true

	sf.Warehouse = warehouse
	sf.Namespace = warehouse.Namespace
	sf.Db, err = connect(sf.getConnectionCredentials(OptionalCredsT{}))
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
		sqlStatement := fmt.Sprintf(`SELECT COUNT(*) FROM "%s"."%s"`, sf.Namespace, tableName)
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
		account:    warehouseutils.GetConfigValue(SFAccount, sf.Warehouse),
		whName:     warehouseutils.GetConfigValue(SFWarehouse, sf.Warehouse),
		dbName:     warehouseutils.GetConfigValue(SFDbName, sf.Warehouse),
		username:   warehouseutils.GetConfigValue(SFUserName, sf.Warehouse),
		password:   warehouseutils.GetConfigValue(SFPassword, sf.Warehouse),
		schemaName: opts.schemaName,
	}
}

func (sf *HandleT) Setup(warehouse warehouseutils.WarehouseT, uploader warehouseutils.UploaderI) (err error) {
	sf.Warehouse = warehouse
	sf.Namespace = warehouse.Namespace
	sf.CloudProvider = warehouseutils.SnowflakeCloudProvider(warehouse.Destination.Config)
	sf.ObjectStorage = warehouseutils.ObjectStorageType("SNOWFLAKE", warehouse.Destination.Config)
	sf.Uploader = uploader

	sf.Db, err = connect(sf.getConnectionCredentials(OptionalCredsT{}))
	return err
}

func (sf *HandleT) TestConnection(warehouse warehouseutils.WarehouseT) (err error) {
	sf.Warehouse = warehouse
	sf.Db, err = connect(sf.getConnectionCredentials(OptionalCredsT{}))
	if err != nil {
		return
	}
	defer sf.Db.Close()
	pingResultChannel := make(chan error, 1)
	rruntime.Go(func() {
		pingResultChannel <- sf.Db.Ping()
	})
	var timeOut time.Duration = 5
	select {
	case err = <-pingResultChannel:
	case <-time.After(5 * time.Second):
		err = fmt.Errorf("connection testing timed out after %d sec", timeOut)
	}
	return
}

// FetchSchema queries snowflake and returns the schema assoiciated with provided namespace
func (sf *HandleT) FetchSchema(warehouse warehouseutils.WarehouseT) (schema warehouseutils.SchemaT, err error) {
	sf.Warehouse = warehouse
	sf.Namespace = warehouse.Namespace
	dbHandle, err := connect(sf.getConnectionCredentials(OptionalCredsT{}))
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
	sqlStatement := fmt.Sprintf(`SELECT count(*) FROM "%[1]s"."%[2]s"`, sf.Namespace, tableName)
	err = sf.Db.QueryRow(sqlStatement).Scan(&total)
	if err != nil {
		pkgLogger.Errorf(`SF: Error getting total count in table %s:%s`, sf.Namespace, tableName)
	}
	return
}

func (sf *HandleT) Connect(warehouse warehouseutils.WarehouseT) (client.Client, error) {
	sf.Warehouse = warehouse
	sf.Namespace = warehouse.Namespace
	dbHandle, err := connect(sf.getConnectionCredentials(OptionalCredsT{schemaName: sf.Namespace}))
	if err != nil {
		return client.Client{}, err
	}

	return client.Client{Type: client.SQLClient, SQL: dbHandle}, err
}
