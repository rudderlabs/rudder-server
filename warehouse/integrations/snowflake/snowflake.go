package snowflake

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	snowflake "github.com/snowflakedb/gosnowflake" // blank comment
)

const (
	provider       = warehouseutils.SNOWFLAKE
	tableNameLimit = 127
)

var (
	pkgLogger          logger.Logger
	enableDeleteByJobs bool
)

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("warehouse").Child("snowflake")
}

func loadConfig() {
	config.RegisterBoolConfigVariable(false, &enableDeleteByJobs, true, "Warehouse.snowflake.enableDeleteByJobs")
}

type HandleT struct {
	DB             *sql.DB
	Namespace      string
	CloudProvider  string
	ObjectStorage  string
	Warehouse      warehouseutils.Warehouse
	Uploader       warehouseutils.UploaderI
	ConnectTimeout time.Duration
}

// String constants for snowflake destination config
const (
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

var errorsMappings = []model.JobError{
	{
		Type:   model.PermissionError,
		Format: regexp.MustCompile(`The requested warehouse does not exist or not authorized`),
	},
	{
		Type:   model.PermissionError,
		Format: regexp.MustCompile(`The requested database does not exist or not authorized`),
	},
	{
		Type:   model.PermissionError,
		Format: regexp.MustCompile(`failed to connect to db. verify account name is correct`),
	},
	{
		Type:   model.PermissionError,
		Format: regexp.MustCompile(`Incorrect username or password was specified`),
	},
	{
		Type:   model.PermissionError,
		Format: regexp.MustCompile(`Insufficient privileges to operate on table`),
	},
	{
		Type:   model.PermissionError,
		Format: regexp.MustCompile(`IP .* is not allowed to access Snowflake. Contact your local security administrator or please create a case with Snowflake Support or reach us on our support line`),
	},
	{
		Type:   model.PermissionError,
		Format: regexp.MustCompile(`User temporarily locked`),
	},
	{
		Type:   model.PermissionError,
		Format: regexp.MustCompile(`Schema .* already exists, but current role has no privileges on it`),
	},
	{
		Type:   model.PermissionError,
		Format: regexp.MustCompile(`The AWS Access Key Id you provided is not valid`),
	},
	{
		Type:   model.PermissionError,
		Format: regexp.MustCompile(`Location .* is not allowed by integration .*. Please use DESC INTEGRATION to check out allowed and blocked locations.`),
	},
	{
		Type:   model.InsufficientResourceError,
		Format: regexp.MustCompile(`Warehouse .* cannot be resumed because resource monitor .* has exceeded its quota`),
	},
	{
		Type:   model.InsufficientResourceError,
		Format: regexp.MustCompile(`Your free trial has ended and all of your virtual warehouses have been suspended. Add billing information in the Snowflake web UI to continue using the full set of Snowflake features.`),
	},
	{
		Type:   model.ResourceNotFoundError,
		Format: regexp.MustCompile(`Table .* does not exist`),
	},
}

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
	return fmt.Sprintf(`"%s"`,
		sf.Namespace,
	)
}

func (sf *HandleT) createTable(tableName string, columns map[string]string) (err error) {
	schemaIdentifier := sf.schemaIdentifier()
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s."%s" ( %v )`, schemaIdentifier, tableName, ColumnsWithDataTypes(columns, ""))
	pkgLogger.Infof("Creating table in snowflake for SF:%s : %v", sf.Warehouse.Destination.ID, sqlStatement)
	_, err = sf.DB.Exec(sqlStatement)
	return
}

func (sf *HandleT) tableExists(tableName string) (exists bool, err error) {
	sqlStatement := fmt.Sprintf(`SELECT EXISTS ( SELECT 1
   								 FROM   information_schema.tables
   								 WHERE  table_schema = '%s'
   								 AND    table_name = '%s'
								   )`, sf.Namespace, tableName)
	err = sf.DB.QueryRow(sqlStatement).Scan(&exists)
	return
}

func (sf *HandleT) columnExists(columnName, tableName string) (exists bool, err error) {
	sqlStatement := fmt.Sprintf(`SELECT EXISTS ( SELECT 1
   								 FROM   information_schema.columns
   								 WHERE  table_schema = '%s'
									AND table_name = '%s'
									AND column_name = '%s'
								   )`, sf.Namespace, tableName, columnName)
	err = sf.DB.QueryRow(sqlStatement).Scan(&exists)
	return
}

func (sf *HandleT) schemaExists() (exists bool, err error) {
	sqlStatement := fmt.Sprintf("SELECT EXISTS ( SELECT 1 FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '%s' )", sf.Namespace)
	r := sf.DB.QueryRow(sqlStatement)
	err = r.Scan(&exists)
	// ignore err if no results for query
	if err == sql.ErrNoRows {
		err = nil
	}
	return
}

func (sf *HandleT) createSchema() (err error) {
	schemaIdentifier := sf.schemaIdentifier()
	sqlStatement := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, schemaIdentifier)
	pkgLogger.Infof("SF: Creating schema name in snowflake for %s:%s : %v", sf.Warehouse.Namespace, sf.Warehouse.Destination.ID, sqlStatement)
	_, err = sf.DB.Exec(sqlStatement)
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
	if misc.IsConfiguredToUseRudderObjectStorage(sf.Warehouse.Destination.Config) || (sf.CloudProvider == "AWS" && warehouseutils.GetConfigValue(StorageIntegration, sf.Warehouse) == "") {
		tempAccessKeyId, tempSecretAccessKey, token, _ := warehouseutils.GetTemporaryS3Cred(&sf.Warehouse.Destination)
		auth = fmt.Sprintf(`CREDENTIALS = (AWS_KEY_ID='%s' AWS_SECRET_KEY='%s' AWS_TOKEN='%s')`, tempAccessKeyId, tempSecretAccessKey, token)
	} else {
		auth = fmt.Sprintf(`STORAGE_INTEGRATION = %s`, warehouseutils.GetConfigValue(StorageIntegration, sf.Warehouse))
	}
	return auth
}

func (sf *HandleT) DeleteBy(tableNames []string, params warehouseutils.DeleteByParams) (err error) {
	pkgLogger.Infof("SF: Cleaning up the following tables in snowflake for SF:%s : %v", tableNames)
	for _, tb := range tableNames {
		sqlStatement := fmt.Sprintf(`
			DELETE FROM
				%[1]q.%[2]q
			WHERE
				context_sources_job_run_id <> '%[3]s' AND
				context_sources_task_run_id <> '%[4]s' AND
				context_source_id = '%[5]s' AND
				received_at < '%[6]s';
		`,
			sf.Namespace,
			tb,
			params.JobRunId,
			params.TaskRunId,
			params.SourceId,
			params.StartTime,
		)

		pkgLogger.Infof("SF: Deleting rows in table in snowflake for SF:%s", sf.Warehouse.Destination.ID)
		pkgLogger.Debugf("SF: Executing the sql statement %v", sqlStatement)

		if enableDeleteByJobs {
			_, err = sf.DB.Exec(sqlStatement)
			if err != nil {
				pkgLogger.Errorf("Error %s", err)
				return err
			}
		}
	}
	return nil
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

	strKeys := warehouseutils.GetColumnsFromTableSchema(tableSchemaInUpload)
	sort.Strings(strKeys)
	sortedColumnNames := warehouseutils.JoinWithFormatting(strKeys, func(idx int, name string) string {
		return fmt.Sprintf(`%q`, name)
	}, ",")

	schemaIdentifier := sf.schemaIdentifier()
	stagingTableName := warehouseutils.StagingTableName(provider, tableName, tableNameLimit)
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
	// Truncating the columns by default to avoid size limitation errors
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

	stagingColumnNames := warehouseutils.JoinWithFormatting(strKeys, func(_ int, name string) string {
		return fmt.Sprintf(`staging."%s"`, name)
	}, ",")
	columnsWithValues := warehouseutils.JoinWithFormatting(strKeys, func(_ int, name string) string {
		return fmt.Sprintf(`original."%[1]s" = staging."%[1]s"`, name)
	}, ",")

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
									INSERT (%[4]s) VALUES (%[5]s)`, tableName, stagingTableName, primaryKey, sortedColumnNames, stagingColumnNames, columnsWithValues, additionalJoinClause, partitionKey, schemaIdentifier)
	} else {
		sqlStatement = fmt.Sprintf(`MERGE INTO %[8]s."%[1]s" AS original
										USING (
											SELECT * FROM (
												SELECT *, row_number() OVER (PARTITION BY %[7]s ORDER BY RECEIVED_AT DESC) AS _rudder_staging_row_number FROM %[8]s."%[2]s"
											) AS q WHERE _rudder_staging_row_number = 1
										) AS staging
										ON (original."%[3]s" = staging."%[3]s" %[6]s)
										WHEN NOT MATCHED THEN
										INSERT (%[4]s) VALUES (%[5]s)`, tableName, stagingTableName, primaryKey, sortedColumnNames, stagingColumnNames, additionalJoinClause, partitionKey, schemaIdentifier)
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
	var loadFile warehouseutils.LoadFileT
	loadFile, err = sf.Uploader.GetSingleLoadFile(identityMergeRulesTable)
	if err != nil {
		return err
	}

	dbHandle, err := Connect(sf.getConnectionCredentials(OptionalCredsT{schemaName: sf.Namespace}))
	if err != nil {
		pkgLogger.Errorf("SF: Error establishing connection for copying table:%s: %v\n", identityMergeRulesTable, err)
		return
	}

	sortedColumnNames := strings.Join([]string{"MERGE_PROPERTY_1_TYPE", "MERGE_PROPERTY_1_VALUE", "MERGE_PROPERTY_2_TYPE", "MERGE_PROPERTY_2_VALUE"}, ",")
	loadLocation := warehouseutils.GetObjectLocation(sf.ObjectStorage, loadFile.Location)
	schemaIdentifier := sf.schemaIdentifier()
	sqlStatement := fmt.Sprintf(`COPY INTO %v(%v) FROM '%v' %s PATTERN = '.*\.csv\.gz'
		FILE_FORMAT = ( TYPE = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE ) TRUNCATECOLUMNS = TRUE`, fmt.Sprintf(`%s."%s"`, schemaIdentifier, identityMergeRulesTable), sortedColumnNames, loadLocation, sf.authString())

	sanitisedSQLStmt, regexErr := misc.ReplaceMultiRegex(sqlStatement, map[string]string{
		"AWS_KEY_ID='[^']*'":     "AWS_KEY_ID='***'",
		"AWS_SECRET_KEY='[^']*'": "AWS_SECRET_KEY='***'",
		"AWS_TOKEN='[^']*'":      "AWS_TOKEN='***'",
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
	var loadFile warehouseutils.LoadFileT

	loadFile, err = sf.Uploader.GetSingleLoadFile(identityMappingsTable)
	if err != nil {
		return err
	}

	dbHandle, err := Connect(sf.getConnectionCredentials(OptionalCredsT{schemaName: sf.Namespace}))
	if err != nil {
		pkgLogger.Errorf("SF: Error establishing connection for copying table:%s: %v\n", identityMappingsTable, err)
		return
	}

	schemaIdentifier := sf.schemaIdentifier()
	stagingTableName := warehouseutils.StagingTableName(provider, identityMappingsTable, tableNameLimit)
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

	loadLocation := warehouseutils.GetObjectLocation(sf.ObjectStorage, loadFile.Location)
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
			// This is to handle cases when column in users table not present in identities table
			identifyColNames = append(identifyColNames, fmt.Sprintf(`NULL as "%s"`, colName))
		}
		firstValProps = append(firstValProps, fmt.Sprintf(`FIRST_VALUE("%[1]s" IGNORE NULLS) OVER (PARTITION BY ID ORDER BY RECEIVED_AT DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "%[1]s"`, colName))
	}
	schemaIdentifier := sf.schemaIdentifier()

	stagingTableName := warehouseutils.StagingTableName(provider, usersTable, tableNameLimit)
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
	_, err = sf.DB.Exec(sqlStatement)
	return
}

func (sf *HandleT) AddColumns(tableName string, columnsInfo []warehouseutils.ColumnInfo) (err error) {
	var (
		query            string
		queryBuilder     strings.Builder
		schemaIdentifier string
	)

	schemaIdentifier = sf.schemaIdentifier()

	queryBuilder.WriteString(fmt.Sprintf(`
		ALTER TABLE
		  %s.%q
		ADD COLUMN`,
		schemaIdentifier,
		tableName,
	))

	for _, columnInfo := range columnsInfo {
		queryBuilder.WriteString(fmt.Sprintf(` %q %s,`, columnInfo.Name, dataTypesMap[columnInfo.Type]))
	}

	query = strings.TrimSuffix(queryBuilder.String(), ",")
	query += ";"

	pkgLogger.Infof("SF: Adding columns for destinationID: %s, tableName: %s with query: %v", sf.Warehouse.Destination.ID, tableName, query)
	_, err = sf.DB.Exec(query)

	// Handle error in case of single column
	if len(columnsInfo) == 1 {
		if err != nil {
			if checkAndIgnoreAlreadyExistError(err) {
				pkgLogger.Infof("SF: Column %s already exists on %s.%s \nResponse: %v", columnsInfo[0].Name, schemaIdentifier, tableName, err)
				err = nil
			}
		}
	}
	return
}

func (*HandleT) AlterColumn(_, _, _ string) (model.AlterTableResponse, error) {
	return model.AlterTableResponse{}, nil
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
		err = sf.DB.QueryRow(sqlStatement).Scan(&totalRows)
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
			rows, err = sf.DB.Query(sqlStatement)
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

func (*HandleT) CrashRecover(_ warehouseutils.Warehouse) (err error) {
	return
}

func (sf *HandleT) IsEmpty(warehouse warehouseutils.Warehouse) (empty bool, err error) {
	empty = true

	sf.Warehouse = warehouse
	sf.Namespace = warehouse.Namespace
	sf.DB, err = Connect(sf.getConnectionCredentials(OptionalCredsT{}))
	if err != nil {
		return
	}
	defer sf.DB.Close()

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
		err = sf.DB.QueryRow(sqlStatement).Scan(&count)
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

func (sf *HandleT) Setup(warehouse warehouseutils.Warehouse, uploader warehouseutils.UploaderI) (err error) {
	sf.Warehouse = warehouse
	sf.Namespace = warehouse.Namespace
	sf.CloudProvider = warehouseutils.SnowflakeCloudProvider(warehouse.Destination.Config)
	sf.Uploader = uploader
	sf.ObjectStorage = warehouseutils.ObjectStorageType(warehouseutils.SNOWFLAKE, warehouse.Destination.Config, sf.Uploader.UseRudderStorage())

	sf.DB, err = Connect(sf.getConnectionCredentials(OptionalCredsT{}))
	return err
}

func (sf *HandleT) TestConnection(warehouse warehouseutils.Warehouse) (err error) {
	sf.Warehouse = warehouse
	sf.DB, err = Connect(sf.getConnectionCredentials(OptionalCredsT{}))
	if err != nil {
		return
	}
	defer sf.DB.Close()

	ctx, cancel := context.WithTimeout(context.TODO(), sf.ConnectTimeout)
	defer cancel()

	err = sf.DB.PingContext(ctx)
	if err == context.DeadlineExceeded {
		return fmt.Errorf("connection testing timed out after %d sec", sf.ConnectTimeout/time.Second)
	}
	if err != nil {
		return err
	}

	return nil
}

// FetchSchema queries snowflake and returns the schema associated with provided namespace
func (sf *HandleT) FetchSchema(warehouse warehouseutils.Warehouse) (schema, unrecognizedSchema warehouseutils.SchemaT, err error) {
	sf.Warehouse = warehouse
	sf.Namespace = warehouse.Namespace
	dbHandle, err := Connect(sf.getConnectionCredentials(OptionalCredsT{}))
	if err != nil {
		return
	}
	defer dbHandle.Close()

	schema = make(warehouseutils.SchemaT)
	unrecognizedSchema = make(warehouseutils.SchemaT)

	sqlStatement := fmt.Sprintf(`
		SELECT
		  table_name,
		  column_name,
		  data_type
		FROM
		  INFORMATION_SCHEMA.COLUMNS
		WHERE
		  table_schema = '%s'
	`,
		sf.Namespace,
	)

	rows, err := dbHandle.Query(sqlStatement)
	if err != nil && err != sql.ErrNoRows {
		pkgLogger.Errorf("SF: Error in fetching schema from snowflake destination:%v, query: %v", sf.Warehouse.Destination.ID, sqlStatement)
		return
	}
	if err == sql.ErrNoRows {
		pkgLogger.Infof("SF: No rows, while fetching schema from  destination:%v, query: %v", sf.Warehouse.Identifier, sqlStatement)
		return schema, unrecognizedSchema, nil
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
			if _, ok := unrecognizedSchema[tName]; !ok {
				unrecognizedSchema[tName] = make(map[string]string)
			}
			unrecognizedSchema[tName][cName] = warehouseutils.MISSING_DATATYPE

			warehouseutils.WHCounterStat(warehouseutils.RUDDER_MISSING_DATATYPE, &sf.Warehouse, warehouseutils.Tag{Name: "datatype", Value: cType}).Count(1)
		}
	}
	return
}

func (sf *HandleT) Cleanup() {
	if sf.DB != nil {
		sf.DB.Close()
	}
}

func (sf *HandleT) LoadUserTables() map[string]error {
	return sf.loadUserTables()
}

func (sf *HandleT) LoadTable(tableName string) error {
	_, err := sf.loadTable(tableName, sf.Uploader.GetTableSchemaInUpload(tableName), nil, false)
	return err
}

func (sf *HandleT) GetTotalCountInTable(ctx context.Context, tableName string) (int64, error) {
	var (
		total        int64
		err          error
		sqlStatement string
	)
	sqlStatement = fmt.Sprintf(`
		SELECT count(*) FROM %[1]s."%[2]s";
	`,
		sf.schemaIdentifier(),
		tableName,
	)
	err = sf.DB.QueryRowContext(ctx, sqlStatement).Scan(&total)
	return total, err
}

func (sf *HandleT) Connect(warehouse warehouseutils.Warehouse) (client.Client, error) {
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

	_, err = sf.DB.Exec(sqlStatement)
	return
}

func (sf *HandleT) SetConnectionTimeout(timeout time.Duration) {
	sf.ConnectTimeout = timeout
}

func (sf *HandleT) ErrorMappings() []model.JobError {
	return errorsMappings
}
