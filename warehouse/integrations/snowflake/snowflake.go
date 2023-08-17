package snowflake

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/samber/lo"

	snowflake "github.com/snowflakedb/gosnowflake" // blank comment

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	sqlmw "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	lf "github.com/rudderlabs/rudder-server/warehouse/logfield"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const (
	provider       = whutils.SNOWFLAKE
	tableNameLimit = 127
)

// String constants for snowflake destination config
const (
	storageIntegration = "storageIntegration"
	account            = "account"
	warehouse          = "warehouse"
	database           = "database"
	user               = "user"
	role               = "role"
	password           = "password"
	application        = "Rudderstack_Warehouse"

	loadTableStrategyMergeMode  = "MERGE"
	loadTableStrategyAppendMode = "APPEND"
)

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
	usersTable              = whutils.ToProviderCase(whutils.SNOWFLAKE, whutils.UsersTable)
	identifiesTable         = whutils.ToProviderCase(whutils.SNOWFLAKE, whutils.IdentifiesTable)
	discardsTable           = whutils.ToProviderCase(whutils.SNOWFLAKE, whutils.DiscardsTable)
	identityMergeRulesTable = whutils.ToProviderCase(whutils.SNOWFLAKE, whutils.IdentityMergeRulesTable)
	identityMappingsTable   = whutils.ToProviderCase(whutils.SNOWFLAKE, whutils.IdentityMappingsTable)
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

type duplicateMessage struct {
	id         string
	receivedAt time.Time
}

func (m *duplicateMessage) String() string {
	return fmt.Sprintf("%s@%s", m.id, m.receivedAt.Format("2006-01-02 15:04:05"))
}

type credentials struct {
	account    string
	warehouse  string
	database   string
	user       string
	role       string
	password   string
	schemaName string
	timeout    time.Duration
}

type tableLoadResp struct {
	db           *sqlmw.DB
	stagingTable string
}

type optionalCreds struct {
	schemaName string
}

type Snowflake struct {
	DB             *sqlmw.DB
	Namespace      string
	CloudProvider  string
	ObjectStorage  string
	Warehouse      model.Warehouse
	Uploader       whutils.Uploader
	connectTimeout time.Duration
	logger         logger.Logger
	stats          stats.Stats

	config struct {
		loadTableStrategy  string
		slowQueryThreshold time.Duration
		enableDeleteByJobs bool

		debugDuplicateWorkspaceIDs   []string
		debugDuplicateTables         []string
		debugDuplicateIntervalInDays int
		debugDuplicateLimit          int
	}
}

func New(conf *config.Config, log logger.Logger, stat stats.Stats) *Snowflake {
	sf := &Snowflake{}

	sf.logger = log.Child("integrations").Child("snowflake")
	sf.stats = stat

	loadTableStrategy := conf.GetString("Warehouse.snowflake.loadTableStrategy", loadTableStrategyMergeMode)
	switch loadTableStrategy {
	case loadTableStrategyMergeMode, loadTableStrategyAppendMode:
		sf.config.loadTableStrategy = loadTableStrategy
	default:
		loadTableStrategy = loadTableStrategyMergeMode
		sf.logger.Errorw(
			"Received invalid loadTableStrategy, defaulting to MERGE mode",
			lf.LoadTableStrategy, loadTableStrategy,
		)
	}

	sf.config.enableDeleteByJobs = conf.GetBool("Warehouse.snowflake.enableDeleteByJobs", false)
	sf.config.slowQueryThreshold = conf.GetDuration("Warehouse.snowflake.slowQueryThreshold", 5, time.Minute)
	sf.config.debugDuplicateWorkspaceIDs = conf.GetStringSlice("Warehouse.snowflake.debugDuplicateWorkspaceIDs", nil)
	sf.config.debugDuplicateIntervalInDays = conf.GetInt("Warehouse.snowflake.debugDuplicateIntervalInDays", 30)
	sf.config.debugDuplicateLimit = conf.GetInt("Warehouse.snowflake.debugDuplicateLimit", 100)
	sf.config.debugDuplicateTables = lo.Map(conf.GetStringSlice("Warehouse.snowflake.debugDuplicateTables", nil), func(item string, index int) string {
		return strings.ToUpper(item)
	})

	return sf
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

func (sf *Snowflake) createTable(ctx context.Context, tableName string, columns model.TableSchema) (err error) {
	schemaIdentifier := sf.schemaIdentifier()
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%q ( %v )`, schemaIdentifier, tableName, ColumnsWithDataTypes(columns, ""))
	sf.logger.Infof("Creating table in snowflake for SF:%s : %v", sf.Warehouse.Destination.ID, sqlStatement)
	_, err = sf.DB.ExecContext(ctx, sqlStatement)
	return
}

func (sf *Snowflake) tableExists(ctx context.Context, tableName string) (exists bool, err error) {
	sqlStatement := fmt.Sprintf(`SELECT EXISTS ( SELECT 1
   								 FROM   information_schema.tables
   								 WHERE  table_schema = '%s'
   								 AND    table_name = '%s'
								   )`, sf.Namespace, tableName)
	err = sf.DB.QueryRowContext(ctx, sqlStatement).Scan(&exists)
	return
}

func (sf *Snowflake) columnExists(ctx context.Context, columnName, tableName string) (exists bool, err error) {
	sqlStatement := fmt.Sprintf(`SELECT EXISTS ( SELECT 1
   								 FROM   information_schema.columns
   								 WHERE  table_schema = '%s'
									AND table_name = '%s'
									AND column_name = '%s'
								   )`, sf.Namespace, tableName, columnName)
	err = sf.DB.QueryRowContext(ctx, sqlStatement).Scan(&exists)
	return
}

func (sf *Snowflake) schemaExists(ctx context.Context) (exists bool, err error) {
	sqlStatement := fmt.Sprintf("SELECT EXISTS ( SELECT 1 FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '%s' )", sf.Namespace)
	r := sf.DB.QueryRowContext(ctx, sqlStatement)
	err = r.Scan(&exists)
	// ignore err if no results for query
	if err == sql.ErrNoRows {
		err = nil
	}
	return
}

func (sf *Snowflake) createSchema(ctx context.Context) (err error) {
	schemaIdentifier := sf.schemaIdentifier()
	sqlStatement := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, schemaIdentifier)
	sf.logger.Infof("SF: Creating schema name in snowflake for %s:%s : %v", sf.Warehouse.Namespace, sf.Warehouse.Destination.ID, sqlStatement)
	_, err = sf.DB.ExecContext(ctx, sqlStatement)
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

func (sf *Snowflake) authString() string {
	var auth string
	if misc.IsConfiguredToUseRudderObjectStorage(sf.Warehouse.Destination.Config) || (sf.CloudProvider == "AWS" && whutils.GetConfigValue(storageIntegration, sf.Warehouse) == "") {
		tempAccessKeyId, tempSecretAccessKey, token, _ := whutils.GetTemporaryS3Cred(&sf.Warehouse.Destination)
		auth = fmt.Sprintf(`CREDENTIALS = (AWS_KEY_ID='%s' AWS_SECRET_KEY='%s' AWS_TOKEN='%s')`, tempAccessKeyId, tempSecretAccessKey, token)
	} else {
		auth = fmt.Sprintf(`STORAGE_INTEGRATION = %s`, whutils.GetConfigValue(storageIntegration, sf.Warehouse))
	}
	return auth
}

func (sf *Snowflake) DeleteBy(ctx context.Context, tableNames []string, params whutils.DeleteByParams) (err error) {
	for _, tb := range tableNames {
		sf.logger.Infof("SF: Cleaning up the following tables in snowflake for SF:%s", tb)
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

		sf.logger.Infof("SF: Deleting rows in table in snowflake for SF:%s", sf.Warehouse.Destination.ID)
		sf.logger.Debugf("SF: Executing the sql statement %v", sqlStatement)

		if sf.config.enableDeleteByJobs {
			_, err = sf.DB.ExecContext(ctx, sqlStatement)
			if err != nil {
				sf.logger.Errorf("Error %s", err)
				return err
			}
		}
	}
	return nil
}

func (sf *Snowflake) loadTable(ctx context.Context, tableName string, tableSchemaInUpload model.TableSchema, skipClosingDBSession bool) (tableLoadResp, error) {
	var (
		csvObjectLocation string
		db                *sqlmw.DB
		err               error
	)

	logFields := []interface{}{
		lf.SourceID, sf.Warehouse.Source.ID,
		lf.SourceType, sf.Warehouse.Source.SourceDefinition.Name,
		lf.DestinationID, sf.Warehouse.Destination.ID,
		lf.DestinationType, sf.Warehouse.Destination.DestinationDefinition.Name,
		lf.WorkspaceID, sf.Warehouse.WorkspaceID,
		lf.Namespace, sf.Namespace,
		lf.TableName, tableName,
		lf.LoadTableStrategy, sf.config.loadTableStrategy,
	}
	sf.logger.Infow("started loading", logFields...)

	if db, err = sf.connect(ctx, optionalCreds{schemaName: sf.Namespace}); err != nil {
		return tableLoadResp{}, fmt.Errorf("connect: %w", err)
	}

	if !skipClosingDBSession {
		defer func() { _ = db.Close() }()
	}

	strKeys := whutils.GetColumnsFromTableSchema(tableSchemaInUpload)
	sort.Strings(strKeys)
	sortedColumnNames := whutils.JoinWithFormatting(strKeys, func(idx int, name string) string {
		return fmt.Sprintf(`%q`, name)
	}, ",")

	schemaIdentifier := sf.schemaIdentifier()
	stagingTableName := whutils.StagingTableName(provider, tableName, tableNameLimit)
	sqlStatement := fmt.Sprintf(`
		CREATE TEMPORARY TABLE %[1]s.%[2]q LIKE %[1]s.%[3]q;
`,
		schemaIdentifier,
		stagingTableName,
		tableName,
	)

	sf.logger.Debugw("creating temporary table",
		append(logFields, lf.StagingTableName, stagingTableName)...,
	)
	if _, err = db.ExecContext(ctx, sqlStatement); err != nil {
		sf.logger.Warnw("failure creating temporary table",
			append(logFields, lf.StagingTableName, stagingTableName, lf.Error, err.Error())...,
		)
		return tableLoadResp{}, fmt.Errorf("create temporary table: %w", err)
	}

	csvObjectLocation, err = sf.Uploader.GetSampleLoadFileLocation(ctx, tableName)
	if err != nil {
		return tableLoadResp{}, fmt.Errorf("getting sample load file location: %w", err)
	}
	loadFolder := whutils.GetObjectFolder(sf.ObjectStorage, csvObjectLocation)

	// Truncating the columns by default to avoid size limitation errors
	// https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#copy-options-copyoptions
	copyTargetTable := stagingTableName
	if sf.config.loadTableStrategy == loadTableStrategyAppendMode {
		copyTargetTable = tableName
	}
	sqlStatement = fmt.Sprintf(
		`COPY INTO
			%v(%v)
		FROM
		  '%v' %s
		PATTERN = '.*\.csv\.gz'
		FILE_FORMAT = ( TYPE = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE)
		TRUNCATECOLUMNS = TRUE;`,
		fmt.Sprintf(`%s.%q`, schemaIdentifier, copyTargetTable),
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
	sf.logger.Infow("copy command",
		append(logFields, lf.Query, sanitisedQuery)...,
	)

	if _, err = db.ExecContext(ctx, sqlStatement); err != nil {
		sf.logger.Warnw("failure running COPY command",
			append(logFields, lf.Query, sanitisedQuery, lf.Error, err.Error()),
		)
		return tableLoadResp{}, fmt.Errorf("copy into table: %w", err)
	}

	if sf.config.loadTableStrategy == loadTableStrategyAppendMode {
		sf.logger.Infow("completed loading", logFields...)

		return tableLoadResp{
			db:           db,
			stagingTable: stagingTableName,
		}, nil
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

	stagingColumnNames := whutils.JoinWithFormatting(strKeys, func(_ int, name string) string {
		return fmt.Sprintf(`staging.%q`, name)
	}, ",")
	columnsWithValues := whutils.JoinWithFormatting(strKeys, func(_ int, name string) string {
		return fmt.Sprintf(`original.%[1]q = staging.%[1]q`, name)
	}, ",")

	if tableName == discardsTable {
		additionalJoinClause = fmt.Sprintf(`AND original.%[1]q = staging.%[1]q AND original.%[2]q = staging.%[2]q`, "TABLE_NAME", "COLUMN_NAME")
	}

	updateSet := columnsWithValues
	if !keepLatestRecordOnDedup {
		// This is being added in order to get the updates count
		updateSet = fmt.Sprintf(`original.%[1]q = original.%[1]q`, strKeys[0])
	}

	sqlStatement = sf.mergeIntoStmt(
		schemaIdentifier,
		tableName,
		stagingTableName,
		partitionKey,
		primaryKey,
		additionalJoinClause,
		sortedColumnNames,
		stagingColumnNames,
		updateSet,
	)

	sf.logger.Infow("deduplication",
		append(logFields, lf.Query, sqlStatement)...,
	)

	row := db.QueryRowContext(ctx, sqlStatement)
	if row.Err() != nil {
		sf.logger.Warnw("failure running deduplication",
			append(logFields, lf.Query, sqlStatement, lf.Error, row.Err().Error())...,
		)
		return tableLoadResp{}, fmt.Errorf("merge into table: %w", row.Err())
	}

	if err = row.Scan(&inserted, &updated); err != nil {
		sf.logger.Warnw("getting rows affected for dedup",
			append(logFields, lf.Query, sqlStatement, lf.Error, err.Error())...,
		)
		return tableLoadResp{}, fmt.Errorf("getting rows affected for dedup: %w", err)
	}

	sf.stats.NewTaggedStat("dedup_rows", stats.CountType, stats.Tags{
		"sourceID":       sf.Warehouse.Source.ID,
		"sourceType":     sf.Warehouse.Source.SourceDefinition.Name,
		"sourceCategory": sf.Warehouse.Source.SourceDefinition.Category,
		"destID":         sf.Warehouse.Destination.ID,
		"destType":       sf.Warehouse.Destination.DestinationDefinition.Name,
		"workspaceId":    sf.Warehouse.WorkspaceID,
		"tableName":      tableName,
	}).Count(int(updated))

	if updated > 0 {
		sampleDuplicateMessages, err := sf.sampleDuplicateMessages(ctx, db, tableName, stagingTableName)
		if err != nil {
			sf.logger.Warnw("failed to sample duplicate rows",
				append(logFields, lf.Error, err)...,
			)
		} else {
			sf.logger.Infow("sample duplicate rows",
				append(logFields, lf.UploadJobID, ctx.Value("uploadID"), lf.SampleDuplicateMessages, lo.Map(sampleDuplicateMessages, func(item duplicateMessage, index int) string {
					return item.String()
				}))...,
			)
		}
	}

	sf.logger.Infow("completed loading", logFields...)

	return tableLoadResp{
		db:           db,
		stagingTable: stagingTableName,
	}, nil
}

func (sf *Snowflake) mergeIntoStmt(
	schemaIdentifier, tableName, stagingTableName,
	partitionKey,
	primaryKey, additionalJoinClause,
	sortedColumnNames, stagingColumnNames,
	updateSet string,
) string {
	return fmt.Sprintf(`MERGE INTO %[1]s.%[2]q AS original USING (
	  SELECT *
	  FROM
		(
		  SELECT *,
			row_number() OVER (
			  PARTITION BY %[4]s
			  ORDER BY
				RECEIVED_AT DESC
			) AS _rudder_staging_row_number
		  FROM
			%[1]s.%[3]q
		) AS q
	  WHERE
		_rudder_staging_row_number = 1
	) AS staging ON (
	  original.%[5]q = staging.%[5]q %[6]s
	)
	WHEN NOT MATCHED THEN
	  INSERT (%[7]s) VALUES (%[8]s)
	WHEN MATCHED THEN
	  UPDATE SET %[9]s;`,
		schemaIdentifier, tableName, stagingTableName,
		partitionKey,
		primaryKey, additionalJoinClause,
		sortedColumnNames, stagingColumnNames,
		updateSet,
	)
}

func (sf *Snowflake) sampleDuplicateMessages(ctx context.Context, db *sqlmw.DB, mainTableName, stagingTableName string) ([]duplicateMessage, error) {
	if !lo.Contains(sf.config.debugDuplicateWorkspaceIDs, sf.Warehouse.WorkspaceID) {
		return nil, nil
	}
	if !lo.Contains(sf.config.debugDuplicateTables, mainTableName) {
		return nil, nil
	}

	identifier := sf.schemaIdentifier()
	mainTable := fmt.Sprintf("%s.%q", identifier, mainTableName)
	stagingTable := fmt.Sprintf("%s.%q", identifier, stagingTableName)

	rows, err := db.QueryContext(ctx, `
		SELECT
			ID,
			RECEIVED_AT
		FROM
			`+mainTable+`
		WHERE
		  	RECEIVED_AT > (SELECT DATEADD(day,-?,current_date())) AND
		  	ID IN (
				SELECT
			  		ID
				FROM
			  		`+stagingTable+`
		  	)
		LIMIT
		  ?;
`,
		sf.config.debugDuplicateIntervalInDays,
		sf.config.debugDuplicateLimit,
	)
	if err != nil {
		return nil, fmt.Errorf("querying for duplicate rows: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	var duplicateMessagesIDs []duplicateMessage
	for rows.Next() {
		var id string
		var receivedAt time.Time

		if err := rows.Scan(&id, &receivedAt); err != nil {
			return nil, fmt.Errorf("scanning duplicate row: %w", err)
		}

		duplicateMessagesIDs = append(duplicateMessagesIDs, duplicateMessage{
			id:         id,
			receivedAt: receivedAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating over duplicate rows: %w", err)
	}

	return duplicateMessagesIDs, nil
}

func (sf *Snowflake) LoadIdentityMergeRulesTable(ctx context.Context) error {
	sf.logger.Infow("Fetching load file location", lf.TableName, identityMergeRulesTable)

	loadFile, err := sf.Uploader.GetSingleLoadFile(ctx, identityMergeRulesTable)
	if err != nil {
		return fmt.Errorf("cannot get load file location for %q: %w", identityMergeRulesTable, err)
	}

	dbHandle, err := sf.connect(ctx, optionalCreds{schemaName: sf.Namespace})
	if err != nil {
		sf.logger.Errorw("Error establishing connection for copying table",
			lf.TableName, identityMergeRulesTable, lf.Error, err.Error(),
		)
		return fmt.Errorf("cannot connect to snowflake with namespace %q: %w", sf.Namespace, err)
	}

	sortedColumnNames := strings.Join([]string{
		"MERGE_PROPERTY_1_TYPE", "MERGE_PROPERTY_1_VALUE", "MERGE_PROPERTY_2_TYPE", "MERGE_PROPERTY_2_VALUE",
	}, ",")
	loadLocation := whutils.GetObjectLocation(sf.ObjectStorage, loadFile.Location)
	schemaIdentifier := sf.schemaIdentifier()
	sqlStatement := fmt.Sprintf(`
		COPY INTO %s.%q(%v)
		FROM '%v'
		%s
		PATTERN = '.*\.csv\.gz'
		FILE_FORMAT = ( TYPE = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE )
		TRUNCATECOLUMNS = TRUE;`,
		schemaIdentifier, identityMergeRulesTable, sortedColumnNames,
		loadLocation,
		sf.authString(),
	)

	sanitisedSQLStmt, regexErr := misc.ReplaceMultiRegex(sqlStatement, map[string]string{
		"AWS_KEY_ID='[^']*'":     "AWS_KEY_ID='***'",
		"AWS_SECRET_KEY='[^']*'": "AWS_SECRET_KEY='***'",
		"AWS_TOKEN='[^']*'":      "AWS_TOKEN='***'",
	})
	if regexErr == nil {
		sf.logger.Infow("Copying identity merge rules for table",
			lf.TableName, identityMergeRulesTable, lf.Query, sanitisedSQLStmt,
		)
	}

	if _, err = dbHandle.ExecContext(ctx, sqlStatement); err != nil {
		sf.logger.Errorw("Error while copying identity merge rules for table",
			lf.TableName, identityMergeRulesTable, lf.Error, err.Error(),
		)
		return fmt.Errorf("cannot copy into table %q: %w", identityMergeRulesTable, err)
	}

	sf.logger.Infow("Completed load for table", lf.TableName, identityMergeRulesTable)

	return nil
}

func (sf *Snowflake) LoadIdentityMappingsTable(ctx context.Context) error {
	sf.logger.Infow("Fetching load file location", lf.TableName, identityMappingsTable)

	loadFile, err := sf.Uploader.GetSingleLoadFile(ctx, identityMappingsTable)
	if err != nil {
		return fmt.Errorf("cannot get load file location for %q: %w", identityMappingsTable, err)
	}

	dbHandle, err := sf.connect(ctx, optionalCreds{schemaName: sf.Namespace})
	if err != nil {
		sf.logger.Errorw("Error establishing connection for copying table",
			lf.TableName, identityMappingsTable, lf.Error, err.Error(),
		)
		return fmt.Errorf("cannot connect to snowflake with namespace %q: %w", sf.Namespace, err)
	}

	schemaIdentifier := sf.schemaIdentifier()
	stagingTableName := whutils.StagingTableName(provider, identityMappingsTable, tableNameLimit)
	sqlStatement := fmt.Sprintf(
		`CREATE TEMPORARY TABLE %[1]s.%[2]q LIKE %[1]s.%[3]q`,
		schemaIdentifier, stagingTableName, identityMappingsTable,
	)

	sf.logger.Infow("Creating temporary table", lf.TableName, identityMappingsTable, lf.Query, sqlStatement)
	_, err = dbHandle.ExecContext(ctx, sqlStatement)
	if err != nil {
		sf.logger.Errorw("Error creating temporary table", lf.TableName, identityMappingsTable, lf.Error, err.Error())
		return fmt.Errorf("cannot create temporary table like %s.%q: %w", schemaIdentifier, identityMappingsTable, err)
	}

	sqlStatement = fmt.Sprintf(
		`ALTER TABLE %s.%q ADD COLUMN "ID" int AUTOINCREMENT start 1 increment 1`,
		schemaIdentifier, stagingTableName,
	)
	sf.logger.Infow("Adding autoincrement column", lf.TableName, stagingTableName, lf.Query, sqlStatement)
	_, err = dbHandle.ExecContext(ctx, sqlStatement)
	if err != nil && !checkAndIgnoreAlreadyExistError(err) {
		sf.logger.Errorw("Error adding autoincrement column", lf.TableName, stagingTableName, lf.Error, err.Error())
		return fmt.Errorf("cannot add autoincrement column to %s.%q: %w", schemaIdentifier, stagingTableName, err)
	}

	loadLocation := whutils.GetObjectLocation(sf.ObjectStorage, loadFile.Location)
	sqlStatement = fmt.Sprintf(
		`COPY INTO %s.%q("MERGE_PROPERTY_TYPE", "MERGE_PROPERTY_VALUE", "RUDDER_ID", "UPDATED_AT")
		FROM '%v' %s PATTERN = '.*\.csv\.gz'
		FILE_FORMAT = ( TYPE = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE )
		TRUNCATECOLUMNS = TRUE`,
		schemaIdentifier, stagingTableName,
		loadLocation,
		sf.authString(),
	)

	sf.logger.Infow("Copying identity mappings for table", lf.TableName, identityMappingsTable, lf.Query, sqlStatement)
	_, err = dbHandle.ExecContext(ctx, sqlStatement)
	if err != nil {
		sf.logger.Errorw("Error running COPY for table",
			lf.TableName, identityMappingsTable, lf.Query, sqlStatement, lf.Error, err.Error(),
		)
		return fmt.Errorf("cannot run copy into %s.%q: %v", schemaIdentifier, stagingTableName, err)
	}

	sqlStatement = fmt.Sprintf(
		`MERGE INTO %[3]s.%[1]q AS original
		USING (
			SELECT * FROM (
				SELECT *, row_number() OVER (
					PARTITION BY "MERGE_PROPERTY_TYPE", "MERGE_PROPERTY_VALUE" ORDER BY "ID" DESC
				) AS _rudder_staging_row_number FROM %[3]s.%[2]q
			) AS q WHERE _rudder_staging_row_number = 1
		) AS staging
		ON (
			original."MERGE_PROPERTY_TYPE" = staging."MERGE_PROPERTY_TYPE" AND
			original."MERGE_PROPERTY_VALUE" = staging."MERGE_PROPERTY_VALUE"
		)
		WHEN MATCHED THEN
		UPDATE SET original."RUDDER_ID" = staging."RUDDER_ID", original."UPDATED_AT" =  staging."UPDATED_AT"
		WHEN NOT MATCHED THEN
		INSERT ("MERGE_PROPERTY_TYPE", "MERGE_PROPERTY_VALUE", "RUDDER_ID", "UPDATED_AT")
		VALUES (
			staging."MERGE_PROPERTY_TYPE", staging."MERGE_PROPERTY_VALUE", staging."RUDDER_ID", staging."UPDATED_AT"
		);`,
		identityMappingsTable, stagingTableName, schemaIdentifier,
	)
	sf.logger.Infow("Merge records for dedup for table", lf.TableName, identityMappingsTable, lf.Query, sqlStatement)
	_, err = dbHandle.ExecContext(ctx, sqlStatement)
	if err != nil {
		sf.logger.Errorw("Error running MERGE for dedup",
			lf.TableName, identityMappingsTable, lf.Query, sqlStatement, lf.Error, err.Error(),
		)
		return fmt.Errorf("cannot run merge for dedup into %s.%q: %w", schemaIdentifier, identityMappingsTable, err)
	}

	sf.logger.Infow("Complete load for table", lf.TableName, identityMappingsTable)

	return nil
}

func (sf *Snowflake) LoadUserTables(ctx context.Context) map[string]error {
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

	logFields := []interface{}{
		lf.SourceID, sf.Warehouse.Source.ID,
		lf.SourceType, sf.Warehouse.Source.SourceDefinition.Name,
		lf.DestinationID, sf.Warehouse.Destination.ID,
		lf.DestinationType, sf.Warehouse.Destination.DestinationDefinition.Name,
		lf.WorkspaceID, sf.Warehouse.WorkspaceID,
		lf.Namespace, sf.Namespace,
	}
	sf.logger.Infow("started loading for identifies and users tables", logFields...)

	resp, err := sf.loadTable(ctx, identifiesTable, identifiesSchema, true)
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
			) AS %[1]q`,
			colName,
		)
		firstValProps = append(firstValProps, firstValPropsQuery)
	}

	schemaIdentifier := sf.schemaIdentifier()
	stagingTableName := whutils.StagingTableName(provider, usersTable, tableNameLimit)
	sqlStatement := fmt.Sprintf(`
		CREATE TEMPORARY TABLE %[1]s.%[2]q AS (
			SELECT DISTINCT *
			FROM (
				SELECT "ID", %[3]s
				FROM (
					(
						SELECT "ID", %[6]s
						FROM %[1]s.%[4]q
						WHERE "ID" IN (
							SELECT "USER_ID"
							FROM %[1]s.%[5]q
							WHERE "USER_ID" IS NOT NULL
						)
					)
					UNION
					(
						SELECT "USER_ID", %[7]s
						FROM %[1]s.%[5]q
						WHERE "USER_ID" IS NOT NULL
					)
				)
			)
		);`,
		schemaIdentifier,
		stagingTableName,
		strings.Join(firstValProps, ","),
		usersTable,
		resp.stagingTable,
		strings.Join(userColNames, ","),
		strings.Join(identifyColNames, ","),
	)

	logFields = append(logFields, lf.TableName, whutils.UsersTable)
	sf.logger.Infow("creating staging table for users",
		append(logFields, lf.StagingTableName, stagingTableName, lf.Query, sqlStatement)...,
	)
	if _, err = resp.db.ExecContext(ctx, sqlStatement); err != nil {
		sf.logger.Warnw("failure creating staging table for users",
			append(logFields, lf.Error, err.Error())...,
		)
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
			SELECT %[3]s
			FROM %[7]s.%[2]q
		) AS staging ON (original.%[4]s = staging.%[4]s)
		WHEN NOT MATCHED THEN INSERT (%[3]s) VALUES (%[6]s)
		WHEN MATCHED THEN UPDATE SET %[5]s;`,
		usersTable,
		stagingTableName,
		columnNamesStr,
		primaryKey,
		columnsWithValues,
		stagingColumnValues,
		schemaIdentifier,
	)

	sf.logger.Infow("deduplication", append(logFields, lf.Query, sqlStatement)...)

	row := resp.db.QueryRowContext(ctx, sqlStatement)
	if row.Err() != nil {
		sf.logger.Warnw("failure running deduplication",
			append(logFields, lf.Query, sqlStatement, lf.Error, row.Err().Error())...,
		)
		return map[string]error{
			identifiesTable: nil,
			usersTable:      fmt.Errorf("running deduplication: %w", row.Err()),
		}
	}
	if err = row.Scan(&inserted, &updated); err != nil {
		sf.logger.Warnw("getting rows affected for dedup",
			append(logFields, lf.Query, sqlStatement, lf.Error, err.Error())...,
		)
		return map[string]error{
			identifiesTable: nil,
			usersTable:      fmt.Errorf("getting rows affected for dedup: %w", err),
		}
	}

	sf.stats.NewTaggedStat("dedup_rows", stats.CountType, stats.Tags{
		"sourceID":       sf.Warehouse.Source.ID,
		"sourceType":     sf.Warehouse.Source.SourceDefinition.Name,
		"sourceCategory": sf.Warehouse.Source.SourceDefinition.Category,
		"destID":         sf.Warehouse.Destination.ID,
		"destType":       sf.Warehouse.Destination.DestinationDefinition.Name,
		"workspaceId":    sf.Warehouse.WorkspaceID,
		"tableName":      whutils.UsersTable,
	}).Count(int(updated))

	sf.logger.Infow("Completed loading for users and identifies tables", logFields...)

	return map[string]error{
		identifiesTable: nil,
		usersTable:      nil,
	}
}

func (sf *Snowflake) connect(ctx context.Context, opts optionalCreds) (*sqlmw.DB, error) {
	cred := sf.getConnectionCredentials(opts)
	urlConfig := snowflake.Config{
		Account:     cred.account,
		User:        cred.user,
		Role:        cred.role,
		Password:    cred.password,
		Database:    cred.database,
		Schema:      cred.schemaName,
		Warehouse:   cred.warehouse,
		Application: application,
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
	_, err = db.ExecContext(ctx, alterStatement)
	if err != nil {
		return nil, fmt.Errorf("SF: snowflake alter session error : (%v)", err)
	}
	middleware := sqlmw.New(
		db,
		sqlmw.WithStats(sf.stats),
		sqlmw.WithLogger(sf.logger),
		sqlmw.WithKeyAndValues(
			lf.SourceID, sf.Warehouse.Source.ID,
			lf.SourceType, sf.Warehouse.Source.SourceDefinition.Name,
			lf.DestinationID, sf.Warehouse.Destination.ID,
			lf.DestinationType, sf.Warehouse.Destination.DestinationDefinition.Name,
			lf.WorkspaceID, sf.Warehouse.WorkspaceID,
			lf.Schema, sf.Namespace,
		),
		sqlmw.WithSlowQueryThreshold(sf.config.slowQueryThreshold),
		sqlmw.WithQueryTimeout(sf.connectTimeout),
		sqlmw.WithSecretsRegex(map[string]string{
			"AWS_KEY_ID='[^']*'":     "AWS_KEY_ID='***'",
			"AWS_SECRET_KEY='[^']*'": "AWS_SECRET_KEY='***'",
			"AWS_TOKEN='[^']*'":      "AWS_TOKEN='***'",
		}),
	)
	return middleware, nil
}

func (sf *Snowflake) CreateSchema(ctx context.Context) (err error) {
	var schemaExists bool
	schemaIdentifier := sf.schemaIdentifier()
	schemaExists, err = sf.schemaExists(ctx)
	if err != nil {
		sf.logger.Errorf("SF: Error checking if schema: %s exists: %v", schemaIdentifier, err)
		return err
	}
	if schemaExists {
		sf.logger.Infof("SF: Skipping creating schema: %s since it already exists", schemaIdentifier)
		return
	}
	return sf.createSchema(ctx)
}

func (sf *Snowflake) CreateTable(ctx context.Context, tableName string, columnMap model.TableSchema) (err error) {
	return sf.createTable(ctx, tableName, columnMap)
}

func (sf *Snowflake) DropTable(ctx context.Context, tableName string) (err error) {
	schemaIdentifier := sf.schemaIdentifier()
	sqlStatement := fmt.Sprintf(`DROP TABLE %[1]s.%[2]q`, schemaIdentifier, tableName)
	sf.logger.Infof("SF: Dropping table in snowflake for SF:%s : %v", sf.Warehouse.Destination.ID, sqlStatement)
	_, err = sf.DB.ExecContext(ctx, sqlStatement)
	return
}

func (sf *Snowflake) AddColumns(ctx context.Context, tableName string, columnsInfo []whutils.ColumnInfo) (err error) {
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

	sf.logger.Infof("SF: Adding columns for destinationID: %s, tableName: %s with query: %v", sf.Warehouse.Destination.ID, tableName, query)
	_, err = sf.DB.ExecContext(ctx, query)

	// Handle error in case of single column
	if len(columnsInfo) == 1 {
		if err != nil {
			if checkAndIgnoreAlreadyExistError(err) {
				sf.logger.Infof("SF: Column %s already exists on %s.%s \nResponse: %v", columnsInfo[0].Name, schemaIdentifier, tableName, err)
				err = nil
			}
		}
	}
	return
}

func (*Snowflake) AlterColumn(context.Context, string, string, string) (model.AlterTableResponse, error) {
	return model.AlterTableResponse{}, nil
}

// DownloadIdentityRules gets distinct combinations of anonymous_id, user_id from tables in warehouse
func (sf *Snowflake) DownloadIdentityRules(ctx context.Context, gzWriter *misc.GZipWriter) (err error) {
	getFromTable := func(tableName string) (err error) {
		var exists bool
		exists, err = sf.tableExists(ctx, tableName)
		if err != nil || !exists {
			return
		}

		schemaIdentifier := sf.schemaIdentifier()
		sqlStatement := fmt.Sprintf(`SELECT count(*) FROM %s.%q`, schemaIdentifier, tableName)
		var totalRows int64
		err = sf.DB.QueryRowContext(ctx, sqlStatement).Scan(&totalRows)
		if err != nil {
			return
		}

		// check if table in warehouse has anonymous_id and user_id and construct accordingly
		hasAnonymousID, err := sf.columnExists(ctx, "ANONYMOUS_ID", tableName)
		if err != nil {
			return
		}
		hasUserID, err := sf.columnExists(ctx, "USER_ID", tableName)
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
			sf.logger.Infof("SF: ANONYMOUS_ID, USER_ID columns not present in table: %s", tableName)
			return nil
		}

		batchSize := int64(10000)
		var offset int64
		for {
			// TODO: Handle case for missing anonymous_id, user_id columns
			sqlStatement = fmt.Sprintf(`SELECT DISTINCT %s FROM %s.%q LIMIT %d OFFSET %d`, toSelectFields, schemaIdentifier, tableName, batchSize, offset)
			sf.logger.Infof("SF: Downloading distinct combinations of anonymous_id, user_id: %s, totalRows: %d", sqlStatement, totalRows)
			var rows *sqlmw.Rows
			rows, err = sf.DB.QueryContext(ctx, sqlStatement)
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
			if err = rows.Err(); err != nil {
				return
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

func (*Snowflake) CrashRecover(context.Context) {}

func (sf *Snowflake) IsEmpty(ctx context.Context, warehouse model.Warehouse) (empty bool, err error) {
	empty = true

	sf.Warehouse = warehouse
	sf.Namespace = warehouse.Namespace
	sf.DB, err = sf.connect(ctx, optionalCreds{})
	if err != nil {
		return
	}
	defer func() { _ = sf.DB.Close() }()

	tables := []string{"TRACKS", "PAGES", "SCREENS", "IDENTIFIES", "ALIASES"}
	for _, tableName := range tables {
		var exists bool
		exists, err = sf.tableExists(ctx, tableName)
		if err != nil {
			return
		}
		if !exists {
			continue
		}
		schemaIdentifier := sf.schemaIdentifier()
		sqlStatement := fmt.Sprintf(`SELECT COUNT(*) FROM %s.%q`, schemaIdentifier, tableName)
		var count int64
		err = sf.DB.QueryRowContext(ctx, sqlStatement).Scan(&count)
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

func (sf *Snowflake) getConnectionCredentials(opts optionalCreds) credentials {
	return credentials{
		account:    whutils.GetConfigValue(account, sf.Warehouse),
		warehouse:  whutils.GetConfigValue(warehouse, sf.Warehouse),
		database:   whutils.GetConfigValue(database, sf.Warehouse),
		user:       whutils.GetConfigValue(user, sf.Warehouse),
		role:       whutils.GetConfigValue(role, sf.Warehouse),
		password:   whutils.GetConfigValue(password, sf.Warehouse),
		schemaName: opts.schemaName,
		timeout:    sf.connectTimeout,
	}
}

func (sf *Snowflake) Setup(ctx context.Context, warehouse model.Warehouse, uploader whutils.Uploader) (err error) {
	sf.Warehouse = warehouse
	sf.Namespace = warehouse.Namespace
	sf.CloudProvider = whutils.SnowflakeCloudProvider(warehouse.Destination.Config)
	sf.Uploader = uploader
	sf.ObjectStorage = whutils.ObjectStorageType(whutils.SNOWFLAKE, warehouse.Destination.Config, sf.Uploader.UseRudderStorage())

	sf.DB, err = sf.connect(ctx, optionalCreds{})
	return err
}

func (sf *Snowflake) TestConnection(ctx context.Context, _ model.Warehouse) error {
	err := sf.DB.PingContext(ctx)
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("connection timeout: %w", err)
	}
	if err != nil {
		return fmt.Errorf("pinging: %w", err)
	}

	return nil
}

// FetchSchema queries the snowflake database and returns the schema
func (sf *Snowflake) FetchSchema(ctx context.Context) (model.Schema, model.Schema, error) {
	schema := make(model.Schema)
	unrecognizedSchema := make(model.Schema)

	sqlStatement := `
        SELECT
            table_name,
            column_name,
            data_type,
            numeric_scale
        FROM
            INFORMATION_SCHEMA.COLUMNS
        WHERE
            table_schema = ?
	`

	rows, err := sf.DB.QueryContext(ctx, sqlStatement, sf.Namespace)
	if errors.Is(err, sql.ErrNoRows) {
		return schema, unrecognizedSchema, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("fetching schema: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var tableName, columnName, columnType string
		var numericScale sql.NullInt64

		if err := rows.Scan(&tableName, &columnName, &columnType, &numericScale); err != nil {
			return nil, nil, fmt.Errorf("scanning schema: %w", err)
		}

		if _, ok := schema[tableName]; !ok {
			schema[tableName] = make(map[string]string)
		}

		if datatype, ok := calculateDataType(columnType, numericScale); ok {
			schema[tableName][columnName] = datatype
		} else {
			if _, ok := unrecognizedSchema[tableName]; !ok {
				unrecognizedSchema[tableName] = make(map[string]string)
			}
			unrecognizedSchema[tableName][columnName] = whutils.MissingDatatype

			whutils.WHCounterStat(whutils.RudderMissingDatatype, &sf.Warehouse, whutils.Tag{Name: "datatype", Value: columnType}).Count(1)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("fetching schema: %w", err)
	}

	return schema, unrecognizedSchema, nil
}

func (sf *Snowflake) Cleanup(context.Context) {
	if sf.DB != nil {
		_ = sf.DB.Close()
	}
}

func (sf *Snowflake) LoadTable(ctx context.Context, tableName string) error {
	_, err := sf.loadTable(ctx, tableName, sf.Uploader.GetTableSchemaInUpload(tableName), false)
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
	err = sf.DB.QueryRowContext(ctx, sqlStatement).Scan(&total)
	return total, err
}

func (sf *Snowflake) Connect(ctx context.Context, warehouse model.Warehouse) (client.Client, error) {
	sf.Warehouse = warehouse
	sf.Namespace = warehouse.Namespace
	sf.CloudProvider = whutils.SnowflakeCloudProvider(warehouse.Destination.Config)
	sf.ObjectStorage = whutils.ObjectStorageType(
		whutils.SNOWFLAKE,
		warehouse.Destination.Config,
		misc.IsConfiguredToUseRudderObjectStorage(sf.Warehouse.Destination.Config),
	)
	dbHandle, err := sf.connect(ctx, optionalCreds{})
	if err != nil {
		return client.Client{}, err
	}

	return client.Client{Type: client.SQLClient, SQL: dbHandle.DB}, err
}

func (sf *Snowflake) LoadTestTable(ctx context.Context, location, tableName string, _ map[string]interface{}, _ string) (err error) {
	loadFolder := whutils.GetObjectFolder(sf.ObjectStorage, location)
	schemaIdentifier := sf.schemaIdentifier()
	sqlStatement := fmt.Sprintf(`COPY INTO %v(%v) FROM '%v' %s PATTERN = '.*\.csv\.gz'
		FILE_FORMAT = ( TYPE = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE ) TRUNCATECOLUMNS = TRUE`,
		fmt.Sprintf(`%s.%q`, schemaIdentifier, tableName),
		fmt.Sprintf(`%q, %q`, "id", "val"),
		loadFolder,
		sf.authString(),
	)

	_, err = sf.DB.ExecContext(ctx, sqlStatement)
	return
}

func (sf *Snowflake) SetConnectionTimeout(timeout time.Duration) {
	sf.connectTimeout = timeout
}

func (*Snowflake) ErrorMappings() []model.JobError {
	return errorsMappings
}
