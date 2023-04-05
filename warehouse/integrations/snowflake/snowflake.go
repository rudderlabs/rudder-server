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

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	snowflake "github.com/snowflakedb/gosnowflake" // blank comment
)

const (
	provider       = warehouseutils.SNOWFLAKE
	tableNameLimit = 127
)

// String constants for snowflake destination config
const (
	StorageIntegration = "storageIntegration"
	Account            = "account"
	Warehouse          = "warehouse"
	Database           = "database"
	User               = "user"
	Role               = "role"
	Password           = "password"
	Application        = "Rudderstack_Warehouse"
)

var pkgLogger logger.Logger

var dataTypesMap = map[string]string{
	"boolean":  "boolean",
	"int":      "number",
	"bigint":   "number",
	"float":    "double precision",
	"string":   "varchar",
	"datetime": "timestamp_tz",
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
	{
		Type:   model.ColumnCountError,
		Format: regexp.MustCompile(`Operation failed because soft limit on objects of type 'Column' per table was exceeded. Please reduce number of 'Column's or contact Snowflake support about raising the limit.`),
	},
}

type Credentials struct {
	Account    string
	Warehouse  string
	Database   string
	User       string
	Role       string
	Password   string
	schemaName string
	timeout    time.Duration
}

type tableLoadResp struct {
	db           *sql.DB
	stagingTable string
}

type optionalCreds struct {
	schemaName string
}

type Snowflake struct {
	DB             *sql.DB
	Namespace      string
	CloudProvider  string
	ObjectStorage  string
	Warehouse      model.Warehouse
	Uploader       warehouseutils.Uploader
	ConnectTimeout time.Duration
	Logger         logger.Logger
	stats          stats.Stats

	EnableDeleteByJobs bool
	SlowQueryInSec     time.Duration
}

func Init() {
	pkgLogger = logger.NewLogger().Child("warehouse").Child("snowflake")
}

func NewSnowflake() *Snowflake {
	return &Snowflake{
		Logger: pkgLogger,
		stats:  stats.Default,
	}
}

func WithConfig(h *Snowflake, config *config.Config) {
	h.EnableDeleteByJobs = config.GetBool("Warehouse.snowflake.enableDeleteByJobs", false)
	h.SlowQueryInSec = config.GetDuration("Warehouse.snowflake.slowQueryInSec", 300, time.Second)
}

func ColumnsWithDataTypes(columns model.TableSchema, prefix string) string {
	var arr []string
	for name, dataType := range columns {
		arr = append(arr, fmt.Sprintf(`"%s%s" %s`, prefix, name, dataTypesMap[dataType]))
	}
	return strings.Join(arr, ",")
}

// schemaIdentifier returns [DATABASE_NAME].[NAMESPACE] format to access the schema directly.
func (sf *Snowflake) schemaIdentifier() string {
	return fmt.Sprintf(`%q`,
		sf.Namespace,
	)
}

func (sf *Snowflake) createTable(tableName string, columns model.TableSchema) error {
	schemaIdentifier := sf.schemaIdentifier()
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%q ( %v )`, schemaIdentifier, tableName, ColumnsWithDataTypes(columns, ""))

	startedAt := time.Now()

	_, err := sf.DB.Exec(sqlStatement)

	sf.slowQueryLog(model.CreateTable, time.Since(startedAt),
		logfield.TableName, tableName,
		logfield.Query, sqlStatement,
	)

	if err != nil {
		return fmt.Errorf("creating table %s: %w", tableName, err)
	}

	return nil
}

func (sf *Snowflake) tableExists(tableName string) (exists bool, err error) {
	sqlStatement := fmt.Sprintf(`SELECT EXISTS ( SELECT 1
   								 FROM   information_schema.tables
   								 WHERE  table_schema = '%s'
   								 AND    table_name = '%s'
								   )`, sf.Namespace, tableName)

	startedAt := time.Now()

	err = sf.DB.QueryRow(sqlStatement).Scan(&exists)

	sf.slowQueryLog(model.TableExists, time.Since(startedAt),
		logfield.TableName, tableName,
		logfield.Query, sqlStatement,
		"exists", exists,
	)
	return
}

func (sf *Snowflake) columnExists(columnName, tableName string) (exists bool, err error) {
	sqlStatement := fmt.Sprintf(`SELECT EXISTS ( SELECT 1
   								 FROM   information_schema.columns
   								 WHERE  table_schema = '%s'
									AND table_name = '%s'
									AND column_name = '%s'
								   )`, sf.Namespace, tableName, columnName)

	startedAt := time.Now()

	err = sf.DB.QueryRow(sqlStatement).Scan(&exists)

	sf.slowQueryLog(model.ColumnExists, time.Since(startedAt),
		logfield.TableName, tableName,
		logfield.ColumnName, columnName,
		logfield.Query, sqlStatement,
		"exists", exists,
	)
	return
}

func (sf *Snowflake) schemaExists() (exists bool, err error) {
	sqlStatement := fmt.Sprintf("SELECT EXISTS ( SELECT 1 FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '%s' )", sf.Namespace)

	startedAt := time.Now()

	r := sf.DB.QueryRow(sqlStatement)
	err = r.Scan(&exists)
	// ignore err if no results for query
	if err == sql.ErrNoRows {
		err = nil
	}

	sf.slowQueryLog(model.SchemaExists, time.Since(startedAt),
		logfield.Query, sqlStatement,
		"exists", exists,
	)
	return
}

func (sf *Snowflake) createSchema()  error {
	schemaIdentifier := sf.schemaIdentifier()
	sqlStatement := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, schemaIdentifier)

	startedAt := time.Now()

	_, err := sf.DB.Exec(sqlStatement)

	sf.slowQueryLog(model.CreateSchema, time.Since(startedAt),
		logfield.Query, sqlStatement,
	)

	if err != nil {
		return fmt.Errorf("creating schema: %w", err)
	}

	return nil
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

func (sf *Snowflake) authString() string {
	var auth string
	if misc.IsConfiguredToUseRudderObjectStorage(sf.Warehouse.Destination.Config) || (sf.CloudProvider == "AWS" && warehouseutils.GetConfigValue(StorageIntegration, sf.Warehouse) == "") {
		tempAccessKeyId, tempSecretAccessKey, token, _ := warehouseutils.GetTemporaryS3Cred(&sf.Warehouse.Destination)
		auth = fmt.Sprintf(`CREDENTIALS = (AWS_KEY_ID='%s' AWS_SECRET_KEY='%s' AWS_TOKEN='%s')`, tempAccessKeyId, tempSecretAccessKey, token)
	} else {
		auth = fmt.Sprintf(`STORAGE_INTEGRATION = %s`, warehouseutils.GetConfigValue(StorageIntegration, sf.Warehouse))
	}
	return auth
}

func (sf *Snowflake) DeleteBy(tableNames []string, params warehouseutils.DeleteByParams) (err error) {
	sf.Logger.Infof("SF: Cleaning up the following tables in snowflake for SF:%s : %v", tableNames)
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

		sf.Logger.Infof("SF: Deleting rows in table in snowflake for SF:%s", sf.Warehouse.Destination.ID)
		sf.Logger.Debugf("SF: Executing the sql statement %v", sqlStatement)

		if sf.EnableDeleteByJobs {
			_, err = sf.DB.Exec(sqlStatement)
			if err != nil {
				sf.Logger.Errorf("Error %s", err)
				return err
			}
		}
	}
	return nil
}

func (sf *Snowflake) loadTable(tableName string, tableSchemaInUpload model.TableSchema, skipClosingDBSession bool) (tableLoadResp, error) {
	var (
		csvObjectLocation string
		db                *sql.DB
		err               error
	)

	sf.Logger.Infow("started loading",
		logfield.SourceID, sf.Warehouse.Source.ID,
		logfield.SourceType, sf.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, sf.Warehouse.Destination.ID,
		logfield.DestinationType, sf.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, sf.Warehouse.WorkspaceID,
		logfield.Namespace, sf.Namespace,
		logfield.TableName, tableName,
	)

	credentials := sf.getConnectionCredentials(optionalCreds{schemaName: sf.Namespace})
	if db, err = Connect(credentials); err != nil {
		return tableLoadResp{}, fmt.Errorf("connect: %w", err)
	}

	if !skipClosingDBSession {
		defer func() { _ = db.Close() }()
	}

	strKeys := warehouseutils.GetColumnsFromTableSchema(tableSchemaInUpload)
	sort.Strings(strKeys)
	sortedColumnNames := warehouseutils.JoinWithFormatting(strKeys, func(idx int, name string) string {
		return fmt.Sprintf(`%q`, name)
	}, ",")

	schemaIdentifier := sf.schemaIdentifier()
	stagingTableName := warehouseutils.StagingTableName(provider, tableName, tableNameLimit)
	sqlStatement := fmt.Sprintf(`
		CREATE TEMPORARY TABLE %[1]s.%[2]q LIKE %[1]s.%[3]q;
`,
		schemaIdentifier,
		stagingTableName,
		tableName,
	)

	startedAt := time.Now()

	_, err = db.Exec(sqlStatement)

	sf.slowQueryLog(model.CreateTable, time.Since(startedAt),
		logfield.Query, sqlStatement,
		logfield.TableName, tableName,
		logfield.StagingTableName, stagingTableName,
	)

	if err != nil {
		return tableLoadResp{}, fmt.Errorf("create temporary table: %w", err)
	}

	csvObjectLocation, err = sf.Uploader.GetSampleLoadFileLocation(tableName)
	if err != nil {
		return tableLoadResp{}, fmt.Errorf("getting sample load file location: %w", err)
	}
	loadFolder := warehouseutils.GetObjectFolder(sf.ObjectStorage, csvObjectLocation)

	// Truncating the columns by default to avoid size limitation errors
	// https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#copy-options-copyoptions
	sqlStatement = fmt.Sprintf(`
		COPY INTO
			%v(%v)
		FROM
		  '%v' %s
		PATTERN = '.*\.csv\.gz'
		FILE_FORMAT = ( TYPE = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE)
		TRUNCATECOLUMNS = TRUE;
`,
		fmt.Sprintf(`%s.%q`, schemaIdentifier, stagingTableName),
		sortedColumnNames,
		loadFolder,
		sf.authString(),
	)

	sanitisedQuery, regexErr := misc.ReplaceMultiRegex(sqlStatement, map[string]string{
		"AWS_KEY_ID='[^']*'":     "AWS_KEY_ID='***'",
		"AWS_SECRET_KEY='[^']*'": "AWS_SECRET_KEY='***'",
		"AWS_TOKEN='[^']*'":      "AWS_TOKEN='***'",
	})
	if regexErr != nil {
		sanitisedQuery = ""
	}

	startedAt = time.Now()

	_, err = db.Exec(sqlStatement)

	sf.slowQueryLog(model.Copy, time.Since(startedAt),
		logfield.Query, sanitisedQuery,
		logfield.TableName, tableName,
	)

	if err != nil {
		return tableLoadResp{}, fmt.Errorf("copy into table: %w", err)
	}

	var (
		primaryKey              = "ID"
		partitionKey            = `"ID"`
		keepLatestRecordOnDedup = sf.Uploader.ShouldOnDedupUseNewRecord()

		additionalJoinClause string
		inserted             int64
		updated              int64
	)

	if column, ok := primaryKeyMap[tableName]; ok {
		primaryKey = column
	}
	if column, ok := partitionKeyMap[tableName]; ok {
		partitionKey = column
	}

	stagingColumnNames := warehouseutils.JoinWithFormatting(strKeys, func(_ int, name string) string {
		return fmt.Sprintf(`staging.%q`, name)
	}, ",")
	columnsWithValues := warehouseutils.JoinWithFormatting(strKeys, func(_ int, name string) string {
		return fmt.Sprintf(`original.%[1]q = staging.%[1]q`, name)
	}, ",")

	if tableName == discardsTable {
		additionalJoinClause = fmt.Sprintf(`AND original.%[1]q = staging.%[1]q AND original.%[2]q = staging.%[2]q`, "TABLE_NAME", "COLUMN_NAME")
	}

	if keepLatestRecordOnDedup {
		sqlStatement = fmt.Sprintf(`
			MERGE INTO %[9]s.%[1]q AS original USING (
			  SELECT
				*
			  FROM
				(
				  SELECT
					*,
					row_number() OVER (
					  PARTITION BY %[8]s
					  ORDER BY
						RECEIVED_AT DESC
					) AS _rudder_staging_row_number
				  FROM
					%[9]s.%[2]q
				) AS q
			  WHERE
				_rudder_staging_row_number = 1
			) AS staging ON (
			  original.%[3]q = staging.%[3]q %[7]s
			)
			WHEN NOT MATCHED THEN
			  INSERT (%[4]s) VALUES (%[5]s)
			WHEN MATCHED THEN
			  UPDATE SET %[6]s;
`,
			tableName,
			stagingTableName,
			primaryKey,
			sortedColumnNames,
			stagingColumnNames,
			columnsWithValues,
			additionalJoinClause,
			partitionKey,
			schemaIdentifier,
		)
	} else {
		// This is being added in order to get the updates count
		dummyColumnWithValues := fmt.Sprintf(`original.%[1]q = original.%[1]q`, strKeys[0])

		sqlStatement = fmt.Sprintf(`
			MERGE INTO %[8]s.%[1]q AS original USING (
			  SELECT
				*
			  FROM
				(
				  SELECT
					*,
					row_number() OVER (
					  PARTITION BY %[7]s
					  ORDER BY
						RECEIVED_AT DESC
					) AS _rudder_staging_row_number
				  FROM
					%[8]s.%[2]q
				) AS q
			  WHERE
				_rudder_staging_row_number = 1
			) AS staging ON (
			  original.%[3]q = staging.%[3]q %[6]s
			)
			WHEN NOT MATCHED THEN
			  INSERT (%[4]s) VALUES (%[5]s)
			WHEN MATCHED THEN
			  UPDATE SET %[9]s;
`,
			tableName,
			stagingTableName,
			primaryKey,
			sortedColumnNames,
			stagingColumnNames,
			additionalJoinClause,
			partitionKey,
			schemaIdentifier,
			dummyColumnWithValues,
		)
	}

	startedAt = time.Now()

	row := db.QueryRow(sqlStatement)

	sf.slowQueryLog(model.Merge, time.Since(startedAt),
		logfield.Query, sqlStatement,
		logfield.TableName, tableName,
	)

	if row.Err() != nil {
		return tableLoadResp{}, fmt.Errorf("merge into table: %w", row.Err())
	}

	if err = row.Scan(&inserted, &updated); err != nil {
		return tableLoadResp{}, fmt.Errorf("getting rows affected for dedup: %w", err)
	}

	sf.stats.NewTaggedStat("dedup_rows", stats.CountType, stats.Tags{
		"sourceID":    sf.Warehouse.Source.ID,
		"sourceType":  sf.Warehouse.Source.SourceDefinition.Name,
		"destID":      sf.Warehouse.Destination.ID,
		"destType":    sf.Warehouse.Destination.DestinationDefinition.Name,
		"workspaceId": sf.Warehouse.WorkspaceID,
		"namespace":   sf.Namespace,
		"tableName":   tableName,
	}).Count(int(updated))

	sf.Logger.Infow("completed loading",
		logfield.SourceID, sf.Warehouse.Source.ID,
		logfield.SourceType, sf.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, sf.Warehouse.Destination.ID,
		logfield.DestinationType, sf.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, sf.Warehouse.WorkspaceID,
		logfield.Namespace, sf.Namespace,
		logfield.TableName, tableName,
	)

	res := tableLoadResp{
		db:           db,
		stagingTable: stagingTableName,
	}

	return res, nil
}

func (sf *Snowflake) LoadIdentityMergeRulesTable() (err error) {
	sf.Logger.Infof("SF: Starting load for table:%s\n", identityMergeRulesTable)

	sf.Logger.Infof("SF: Fetching load file location for %s", identityMergeRulesTable)
	var loadFile warehouseutils.LoadFile
	loadFile, err = sf.Uploader.GetSingleLoadFile(identityMergeRulesTable)
	if err != nil {
		return err
	}

	dbHandle, err := Connect(sf.getConnectionCredentials(optionalCreds{schemaName: sf.Namespace}))
	if err != nil {
		sf.Logger.Errorf("SF: Error establishing connection for copying table:%s: %v\n", identityMergeRulesTable, err)
		return
	}

	sortedColumnNames := strings.Join([]string{"MERGE_PROPERTY_1_TYPE", "MERGE_PROPERTY_1_VALUE", "MERGE_PROPERTY_2_TYPE", "MERGE_PROPERTY_2_VALUE"}, ",")
	loadLocation := warehouseutils.GetObjectLocation(sf.ObjectStorage, loadFile.Location)
	schemaIdentifier := sf.schemaIdentifier()
	sqlStatement := fmt.Sprintf(`COPY INTO %v(%v) FROM '%v' %s PATTERN = '.*\.csv\.gz'
		FILE_FORMAT = ( TYPE = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE ) TRUNCATECOLUMNS = TRUE`, fmt.Sprintf(`%s.%q`, schemaIdentifier, identityMergeRulesTable), sortedColumnNames, loadLocation, sf.authString())

	sanitisedSQLStmt, regexErr := misc.ReplaceMultiRegex(sqlStatement, map[string]string{
		"AWS_KEY_ID='[^']*'":     "AWS_KEY_ID='***'",
		"AWS_SECRET_KEY='[^']*'": "AWS_SECRET_KEY='***'",
		"AWS_TOKEN='[^']*'":      "AWS_TOKEN='***'",
	})
	if regexErr == nil {
		sf.Logger.Infof("SF: Dedup records for table:%s using staging table: %s\n", identityMergeRulesTable, sanitisedSQLStmt)
	}

	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		sf.Logger.Errorf("SF: Error running MERGE for dedup: %v\n", err)
		return
	}
	sf.Logger.Infof("SF: Complete load for table:%s\n", identityMergeRulesTable)
	return
}

func (sf *Snowflake) LoadIdentityMappingsTable() (err error) {
	sf.Logger.Infof("SF: Starting load for table:%s\n", identityMappingsTable)
	sf.Logger.Infof("SF: Fetching load file location for %s", identityMappingsTable)
	var loadFile warehouseutils.LoadFile

	loadFile, err = sf.Uploader.GetSingleLoadFile(identityMappingsTable)
	if err != nil {
		return err
	}

	dbHandle, err := Connect(sf.getConnectionCredentials(optionalCreds{schemaName: sf.Namespace}))
	if err != nil {
		sf.Logger.Errorf("SF: Error establishing connection for copying table:%s: %v\n", identityMappingsTable, err)
		return
	}

	schemaIdentifier := sf.schemaIdentifier()
	stagingTableName := warehouseutils.StagingTableName(provider, identityMappingsTable, tableNameLimit)
	sqlStatement := fmt.Sprintf(`CREATE TEMPORARY TABLE %[1]s.%[2]q LIKE %[1]s.%[3]q`, schemaIdentifier, stagingTableName, identityMappingsTable)

	sf.Logger.Infof("SF: Creating temporary table for table:%s at %s\n", identityMappingsTable, sqlStatement)
	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		sf.Logger.Errorf("SF: Error creating temporary table for table:%s: %v\n", identityMappingsTable, err)
		return
	}

	sqlStatement = fmt.Sprintf(`ALTER TABLE %s.%q ADD COLUMN "ID" int AUTOINCREMENT start 1 increment 1`, schemaIdentifier, stagingTableName)
	sf.Logger.Infof("SF: Adding autoincrement column for table:%s at %s\n", stagingTableName, sqlStatement)
	_, err = dbHandle.Exec(sqlStatement)
	if err != nil && !checkAndIgnoreAlreadyExistError(err) {
		sf.Logger.Errorf("SF: Error adding autoincrement column for table:%s: %v\n", stagingTableName, err)
		return
	}

	loadLocation := warehouseutils.GetObjectLocation(sf.ObjectStorage, loadFile.Location)
	sqlStatement = fmt.Sprintf(`COPY INTO %v("MERGE_PROPERTY_TYPE", "MERGE_PROPERTY_VALUE", "RUDDER_ID", "UPDATED_AT") FROM '%v' %s PATTERN = '.*\.csv\.gz'
		FILE_FORMAT = ( TYPE = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE ) TRUNCATECOLUMNS = TRUE`, fmt.Sprintf(`%s.%q`, schemaIdentifier, stagingTableName), loadLocation, sf.authString())

	sf.Logger.Infof("SF: Dedup records for table:%s using staging table: %s\n", identityMappingsTable, sqlStatement)
	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		sf.Logger.Errorf("SF: Error running MERGE for dedup: %v\n", err)
		return
	}

	sqlStatement = fmt.Sprintf(`MERGE INTO %[3]s.%[1]q AS original
									USING (
										SELECT * FROM (
											SELECT *, row_number() OVER (PARTITION BY "MERGE_PROPERTY_TYPE", "MERGE_PROPERTY_VALUE" ORDER BY "ID" DESC) AS _rudder_staging_row_number FROM %[3]s.%[2]q
										) AS q WHERE _rudder_staging_row_number = 1
									) AS staging
									ON (original."MERGE_PROPERTY_TYPE" = staging."MERGE_PROPERTY_TYPE" AND original."MERGE_PROPERTY_VALUE" = staging."MERGE_PROPERTY_VALUE")
									WHEN MATCHED THEN
									UPDATE SET original."RUDDER_ID" = staging."RUDDER_ID", original."UPDATED_AT" =  staging."UPDATED_AT"
									WHEN NOT MATCHED THEN
									INSERT ("MERGE_PROPERTY_TYPE", "MERGE_PROPERTY_VALUE", "RUDDER_ID", "UPDATED_AT") VALUES (staging."MERGE_PROPERTY_TYPE", staging."MERGE_PROPERTY_VALUE", staging."RUDDER_ID", staging."UPDATED_AT")`, identityMappingsTable, stagingTableName, schemaIdentifier)
	sf.Logger.Infof("SF: Dedup records for table:%s using staging table: %s\n", identityMappingsTable, sqlStatement)
	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		sf.Logger.Errorf("SF: Error running MERGE for dedup: %v\n", err)
		return
	}
	sf.Logger.Infof("SF: Complete load for table:%s\n", identityMappingsTable)
	return
}

func (sf *Snowflake) loadUserTables() map[string]error {
	var (
		identifiesSchema = sf.Uploader.GetTableSchemaInUpload(identifiesTable)
		usersSchema      = sf.Uploader.GetTableSchemaInUpload(usersTable)

		userColNames        []string
		firstValProps       []string
		identifyColNames    []string
		columnsWithValues   string
		stagingColumnValues string
		inserted            int64
		updated             int64
	)

	sf.Logger.Infow("started loading for identifies and users tables",
		logfield.SourceID, sf.Warehouse.Source.ID,
		logfield.SourceType, sf.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, sf.Warehouse.Destination.ID,
		logfield.DestinationType, sf.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, sf.Warehouse.WorkspaceID,
		logfield.Namespace, sf.Namespace,
	)

	resp, err := sf.loadTable(identifiesTable, identifiesSchema, true)
	if err != nil {
		return map[string]error{
			identifiesTable: fmt.Errorf("loading table %s: %w", identifiesTable, err),
		}
	}
	defer func() { _ = resp.db.Close() }()

	if len(usersSchema) == 0 {
		return map[string]error{
			identifiesTable: nil,
		}
	}

	userColMap := sf.Uploader.GetTableSchemaInWarehouse(usersTable)
	for colName := range userColMap {
		if colName == "ID" {
			continue
		}
		userColNames = append(userColNames, fmt.Sprintf(`%q`, colName))
		if _, ok := identifiesSchema[colName]; ok {
			identifyColNames = append(identifyColNames, fmt.Sprintf(`%q`, colName))
		} else {
			// This is to handle cases when column in users table not present in identities table
			identifyColNames = append(identifyColNames, fmt.Sprintf(`NULL as %q`, colName))
		}
		firstValPropsQuery := fmt.Sprintf(`
			FIRST_VALUE(%[1]q IGNORE NULLS) OVER (
			  PARTITION BY ID
			  ORDER BY
				RECEIVED_AT DESC ROWS BETWEEN UNBOUNDED PRECEDING
				AND UNBOUNDED FOLLOWING
			) AS %[1]q
`,
			colName,
		)
		firstValProps = append(firstValProps, firstValPropsQuery)
	}

	schemaIdentifier := sf.schemaIdentifier()
	stagingTableName := warehouseutils.StagingTableName(provider, usersTable, tableNameLimit)
	sqlStatement := fmt.Sprintf(`
		CREATE TEMPORARY TABLE %[1]s.%[2]q AS (
		  SELECT
			DISTINCT *
		  FROM
			(
			  SELECT
				"ID",
				%[3]s
			  FROM
				(
				  (
					SELECT
					  "ID",
					  %[6]s
					FROM
					  %[1]s.%[4]q
					WHERE
					  "ID" in (
						SELECT
						  "USER_ID"
						FROM
						  %[1]s.%[5]q
						WHERE
						  "USER_ID" IS NOT NULL
					  )
				  )
				  UNION
					(
					  SELECT
						"USER_ID",
						%[7]s
					  FROM
						%[1]s.%[5]q
					  WHERE
						"USER_ID" IS NOT NULL
					)
				)
			)
		);
`,
		schemaIdentifier,
		stagingTableName,
		strings.Join(firstValProps, ","),
		usersTable,
		resp.stagingTable,
		strings.Join(userColNames, ","),
		strings.Join(identifyColNames, ","),
	)

	startedAt := time.Now()

	_, err = resp.db.Exec(sqlStatement)

	sf.slowQueryLog(model.CreateTable, time.Since(startedAt),
		logfield.TableName, warehouseutils.UsersTable,
		logfield.StagingTableName, stagingTableName,
		logfield.Query, sqlStatement,
	)

	if err != nil {
		return map[string]error{
			identifiesTable: nil,
			usersTable:      fmt.Errorf("creating staging table for users: %w", err),
		}
	}

	var (
		primaryKey     = `"ID"`
		columnNames    = append([]string{`"ID"`}, userColNames...)
		columnNamesStr = strings.Join(columnNames, ",")
	)

	for idx, colName := range columnNames {
		columnsWithValues += fmt.Sprintf(`original.%[1]s = staging.%[1]s`, colName)
		stagingColumnValues += fmt.Sprintf(`staging.%s`, colName)
		if idx != len(columnNames)-1 {
			columnsWithValues += `,`
			stagingColumnValues += `,`
		}
	}

	sqlStatement = fmt.Sprintf(`
		MERGE INTO %[7]s.%[1]q AS original USING (
		  SELECT
			%[3]s
		  FROM
			%[7]s.%[2]q
		) AS staging ON (original.%[4]s = staging.%[4]s)
		WHEN NOT MATCHED THEN
			INSERT (%[3]s) VALUES (%[6]s)
		WHEN MATCHED THEN
			UPDATE SET %[5]s;
;
`,
		usersTable,
		stagingTableName,
		columnNamesStr,
		primaryKey,
		columnsWithValues,
		stagingColumnValues,
		schemaIdentifier,
	)

	startedAt = time.Now()

	row := resp.db.QueryRow(sqlStatement)

	sf.slowQueryLog(model.Merge, time.Since(startedAt),
		logfield.TableName, warehouseutils.UsersTable,
		logfield.Query, sqlStatement,
	)

	if row.Err() != nil {
		return map[string]error{
			identifiesTable: nil,
			usersTable:      fmt.Errorf("running deduplication: %w", row.Err()),
		}
	}
	if err = row.Scan(&inserted, &updated); err != nil {
		return map[string]error{
			identifiesTable: nil,
			usersTable:      fmt.Errorf("getting rows affected for dedup: %w", err),
		}
	}

	sf.stats.NewTaggedStat("dedup_rows", stats.CountType, stats.Tags{
		"sourceID":    sf.Warehouse.Source.ID,
		"sourceType":  sf.Warehouse.Source.SourceDefinition.Name,
		"destID":      sf.Warehouse.Destination.ID,
		"destType":    sf.Warehouse.Destination.DestinationDefinition.Name,
		"workspaceId": sf.Warehouse.WorkspaceID,
		"namespace":   sf.Namespace,
		"tableName":   warehouseutils.UsersTable,
	}).Count(int(updated))

	sf.Logger.Infow("completed loading for users and identifies tables",
		logfield.SourceID, sf.Warehouse.Source.ID,
		logfield.SourceType, sf.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, sf.Warehouse.Destination.ID,
		logfield.DestinationType, sf.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, sf.Warehouse.WorkspaceID,
		logfield.Namespace, sf.Namespace,
	)

	return map[string]error{
		identifiesTable: nil,
		usersTable:      nil,
	}
}

func Connect(cred Credentials) (*sql.DB, error) {
	urlConfig := snowflake.Config{
		Account:     cred.Account,
		User:        cred.User,
		Role:        cred.Role,
		Password:    cred.Password,
		Database:    cred.Database,
		Schema:      cred.schemaName,
		Warehouse:   cred.Warehouse,
		Application: Application,
	}

	if cred.timeout > 0 {
		urlConfig.LoginTimeout = cred.timeout
	}

	var (
		err error
		dsn string
		db  *sql.DB
	)

	if dsn, err = snowflake.DSN(&urlConfig); err != nil {
		return nil, fmt.Errorf("snowflake dsn error: %w", err)
	}

	if db, err = sql.Open("snowflake", dsn); err != nil {
		return nil, fmt.Errorf("snowflake connect error: %w", err)
	}

	alterStatement := `ALTER SESSION SET ABORT_DETACHED_QUERY=TRUE`

	startedAt := time.Now()

	_, err = db.Exec(alterStatement)

	pkgLogger.Infow("executing query",
		logfield.QueryType, model.AlterSession,
		logfield.QueryExecutionTime, time.Since(startedAt),
	)

	if err != nil {
		return nil, fmt.Errorf("SF: snowflake alter session error : (%v)", err)
	}
	return db, nil
}

func (sf *Snowflake) CreateSchema() error {
	var (
		exists bool
		err    error
	)

	if exists, err = sf.schemaExists(); err != nil {
		return fmt.Errorf("checking if schema exists: %w", err)
	}
	if exists {
		return nil
	}

	return sf.createSchema()
}

func (sf *Snowflake) CreateTable(tableName string, columnMap model.TableSchema) error {
	return sf.createTable(tableName, columnMap)
}

func (sf *Snowflake) DropTable(tableName string) error {
	schemaIdentifier := sf.schemaIdentifier()
	sqlStatement := fmt.Sprintf(`DROP TABLE %[1]s.%[2]q`, schemaIdentifier, tableName)

	startedAt := time.Now()

	_, err := sf.DB.Exec(sqlStatement)

	sf.slowQueryLog(model.DropTable, time.Since(startedAt),
		logfield.TableName, tableName,
		logfield.Query, sqlStatement,
	)

	if err != nil {
		return fmt.Errorf("dropping table: %w", err)
	}

	return nil
}

func (sf *Snowflake) AddColumns(tableName string, columnsInfo []warehouseutils.ColumnInfo) error {
	var (
		query            string
		queryBuilder     strings.Builder
		schemaIdentifier string
		err error
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

	startedAt := time.Now()

	_, err = sf.DB.Exec(query)

	sf.slowQueryLog(model.AddColumns, time.Since(startedAt),
		logfield.TableName, tableName,
		logfield.Query, query,
	)

	// Handle error in case of single column
	if len(columnsInfo) == 1 {
		if err != nil {
			if checkAndIgnoreAlreadyExistError(err) {
				sf.Logger.Infof("SF: Column %s already exists on %s.%s \nResponse: %v", columnsInfo[0].Name, schemaIdentifier, tableName, err)
				return nil
			}
		}
	}

	if err != nil {
		return fmt.Errorf("adding columns to table: %w", err)
	}

	return nil
}

func (*Snowflake) AlterColumn(_, _, _ string) (model.AlterTableResponse, error) {
	return model.AlterTableResponse{}, nil
}

// DownloadIdentityRules gets distinct combinations of anonymous_id, user_id from tables in warehouse
func (sf *Snowflake) DownloadIdentityRules(gzWriter *misc.GZipWriter) (err error) {
	getFromTable := func(tableName string) (err error) {
		var exists bool
		exists, err = sf.tableExists(tableName)
		if err != nil || !exists {
			return
		}

		schemaIdentifier := sf.schemaIdentifier()
		sqlStatement := fmt.Sprintf(`SELECT count(*) FROM %s.%q`, schemaIdentifier, tableName)
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
			sf.Logger.Infof("SF: ANONYMOUS_ID, USER_ID columns not present in table: %s", tableName)
			return nil
		}

		batchSize := int64(10000)
		var offset int64
		for {
			// TODO: Handle case for missing anonymous_id, user_id columns
			sqlStatement = fmt.Sprintf(`SELECT DISTINCT %s FROM %s.%q LIMIT %d OFFSET %d`, toSelectFields, schemaIdentifier, tableName, batchSize, offset)
			sf.Logger.Infof("SF: Downloading distinct combinations of anonymous_id, user_id: %s, totalRows: %d", sqlStatement, totalRows)
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
				_ = csvWriter.Write(csvRow)
				csvWriter.Flush()
				_ = gzWriter.WriteGZ(buff.String())
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

func (*Snowflake) CrashRecover(_ model.Warehouse) (err error) {
	return
}

func (sf *Snowflake) IsEmpty(warehouse model.Warehouse) (empty bool, err error) {
	empty = true

	sf.Warehouse = warehouse
	sf.Namespace = warehouse.Namespace
	sf.DB, err = Connect(sf.getConnectionCredentials(optionalCreds{}))
	if err != nil {
		return
	}
	defer func() { _ = sf.DB.Close() }()

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
		sqlStatement := fmt.Sprintf(`SELECT COUNT(*) FROM %s.%q`, schemaIdentifier, tableName)
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

func (sf *Snowflake) getConnectionCredentials(opts optionalCreds) Credentials {
	return Credentials{
		Account:    warehouseutils.GetConfigValue(Account, sf.Warehouse),
		Warehouse:  warehouseutils.GetConfigValue(Warehouse, sf.Warehouse),
		Database:   warehouseutils.GetConfigValue(Database, sf.Warehouse),
		User:       warehouseutils.GetConfigValue(User, sf.Warehouse),
		Role:       warehouseutils.GetConfigValue(Role, sf.Warehouse),
		Password:   warehouseutils.GetConfigValue(Password, sf.Warehouse),
		schemaName: opts.schemaName,
		timeout:    sf.ConnectTimeout,
	}
}

func (sf *Snowflake) Setup(warehouse model.Warehouse, uploader warehouseutils.Uploader) error {
	sf.Warehouse = warehouse
	sf.Namespace = warehouse.Namespace
	sf.CloudProvider = warehouseutils.SnowflakeCloudProvider(warehouse.Destination.Config)
	sf.Uploader = uploader
	sf.ObjectStorage = warehouseutils.ObjectStorageType(warehouseutils.SNOWFLAKE, warehouse.Destination.Config, sf.Uploader.UseRudderStorage())

	var (
		db *sql.DB
		err error
	)
	if db, err = Connect(sf.getConnectionCredentials(optionalCreds{})); err != nil {
		return fmt.Errorf("connect: %w", err)
	}

	sf.DB = db

	return nil
}

func (sf *Snowflake) TestConnection(warehouse model.Warehouse) error {
	var (
		db  *sql.DB
		err error
	)

	sf.Warehouse = warehouse

	if db, err = Connect(sf.getConnectionCredentials(optionalCreds{})); err != nil {
		return fmt.Errorf("connect: %w", err)
	}

	sf.DB = db
	defer func() { _ = sf.DB.Close() }()

	ctx, cancel := context.WithTimeout(context.TODO(), sf.ConnectTimeout)
	defer cancel()

	if err = sf.DB.PingContext(ctx); err == context.DeadlineExceeded {
		return fmt.Errorf("connection testing timed out after %d sec", sf.ConnectTimeout/time.Second)
	}

	return nil
}

// FetchSchema queries snowflake and returns the schema associated with provided namespace
func (sf *Snowflake) FetchSchema(warehouse model.Warehouse) (model.Schema, model.Schema, error) {
	sf.Warehouse = warehouse
	sf.Namespace = warehouse.Namespace
	dbHandle, err := Connect(sf.getConnectionCredentials(optionalCreds{}))
	if err != nil {
		return model.Schema{}, model.Schema{}, fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = dbHandle.Close() }()

	schema := make(model.Schema)
	unrecognizedSchema := make(model.Schema)

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

	startedAt := time.Now()

	rows, err := dbHandle.Query(sqlStatement)

	sf.slowQueryLog(model.FetchSchema, time.Since(startedAt),
		logfield.Query, sqlStatement,
	)

	if err != nil && err != sql.ErrNoRows {
		return model.Schema{}, model.Schema{}, fmt.Errorf("fetching schema: %w", err)
	}
	if err == sql.ErrNoRows {
		return model.Schema{}, model.Schema{}, nil
	}

	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var tName, cName, cType string
		err = rows.Scan(&tName, &cName, &cType)
		if err != nil {
			return model.Schema{}, model.Schema{}, fmt.Errorf("processing fetched schema: %w", err)
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

	return schema, unrecognizedSchema, nil
}

func (sf *Snowflake) Cleanup() {
	if sf.DB != nil {
		_ = sf.DB.Close()
	}
}

func (sf *Snowflake) LoadUserTables() map[string]error {
	return sf.loadUserTables()
}

func (sf *Snowflake) LoadTable(tableName string) error {
	_, err := sf.loadTable(tableName, sf.Uploader.GetTableSchemaInUpload(tableName), false)
	return err
}

func (sf *Snowflake) GetTotalCountInTable(ctx context.Context, tableName string) (int64, error) {
	var (
		total        int64
		err          error
		sqlStatement string
	)
	sqlStatement = fmt.Sprintf(`
		SELECT count(*) FROM %[1]s.%[2]q;
	`,
		sf.schemaIdentifier(),
		tableName,
	)

	startedAt := time.Now()

	err = sf.DB.QueryRowContext(ctx, sqlStatement).Scan(&total)

	sf.slowQueryLog(model.TableCount, time.Since(startedAt),
		logfield.TableName, tableName,
		logfield.Query, sqlStatement,
	)

	return total, err
}

func (sf *Snowflake) Connect(warehouse model.Warehouse) (client.Client, error) {
	sf.Warehouse = warehouse
	sf.Namespace = warehouse.Namespace
	sf.CloudProvider = warehouseutils.SnowflakeCloudProvider(warehouse.Destination.Config)
	sf.ObjectStorage = warehouseutils.ObjectStorageType(
		warehouseutils.SNOWFLAKE,
		warehouse.Destination.Config,
		misc.IsConfiguredToUseRudderObjectStorage(sf.Warehouse.Destination.Config),
	)
	dbHandle, err := Connect(sf.getConnectionCredentials(optionalCreds{}))
	if err != nil {
		return client.Client{}, err
	}

	return client.Client{Type: client.SQLClient, SQL: dbHandle}, err
}

func (sf *Snowflake) LoadTestTable(location, tableName string, _ map[string]interface{}, _ string) (err error) {
	loadFolder := warehouseutils.GetObjectFolder(sf.ObjectStorage, location)
	schemaIdentifier := sf.schemaIdentifier()
	sqlStatement := fmt.Sprintf(`COPY INTO %v(%v) FROM '%v' %s PATTERN = '.*\.csv\.gz'
		FILE_FORMAT = ( TYPE = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE ) TRUNCATECOLUMNS = TRUE`,
		fmt.Sprintf(`%s.%q`, schemaIdentifier, tableName),
		fmt.Sprintf(`%q, %q`, "id", "val"),
		loadFolder,
		sf.authString(),
	)

	_, err = sf.DB.Exec(sqlStatement)
	return
}

func (sf *Snowflake) SetConnectionTimeout(timeout time.Duration) {
	sf.ConnectTimeout = timeout
}

func (sf *Snowflake) ErrorMappings() []model.JobError {
	return errorsMappings
}

func (sf *Snowflake) slowQueryLog(queryType model.QueryType, executionTime time.Duration, keysAndValues ...interface{}) {
	if executionTime > sf.SlowQueryInSec {
		sf.Logger.Infow("executing query",
			logfield.QueryType, queryType,
			logfield.QueryExecutionTime, executionTime,
			logfield.SourceID, sf.Warehouse.Source.ID,
			logfield.SourceType, sf.Warehouse.Source.SourceDefinition.Name,
			logfield.DestinationID, sf.Warehouse.Destination.ID,
			logfield.DestinationType, sf.Warehouse.Destination.DestinationDefinition.Name,
			logfield.WorkspaceID, sf.Warehouse.WorkspaceID,
			logfield.Namespace, sf.Namespace,
			keysAndValues,
		)
	}
}
