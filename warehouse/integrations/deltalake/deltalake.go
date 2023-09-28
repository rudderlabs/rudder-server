package deltalake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	dbsql "github.com/databricks/databricks-sql-go"
	dbsqllog "github.com/databricks/databricks-sql-go/logger"
	"golang.org/x/exp/slices"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseclient "github.com/rudderlabs/rudder-server/warehouse/client"
	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const (
	host         = "host"
	port         = "port"
	path         = "path"
	token        = "token"
	catalog      = "catalog"
	useSTSTokens = "useSTSTokens"
	userAgent    = "Rudderstack"

	provider = warehouseutils.DELTALAKE

	// Corresponds to the max length set for event rudder-transformer
	// https://github.com/rudderlabs/rudder-transformer/blob/fb8b818b2cbd05f784117b9f3040856dab1a7346/src/warehouse/v1/util.js#L34
	tableNameLimit = 127

	schemaNotFound       = "[SCHEMA_NOT_FOUND]"
	partitionNotFound    = "SHOW PARTITIONS is not allowed on a table that is not partitioned"
	columnsAlreadyExists = "already exists in root"

	mergeMode  = "MERGE"
	appendMode = "APPEND"

	rudderStagingTableRegex    = "^rudder_staging_.*$"       // matches rudder_staging_* tables
	nonRudderStagingTableRegex = "^(?!rudder_staging_.*$).*" // matches tables that do not start with rudder_staging_
)

// dataTypesMap maps rudder data types to delta lake data types
var dataTypesMap = map[string]string{
	"boolean":  "BOOLEAN",
	"int":      "BIGINT",
	"float":    "DOUBLE",
	"string":   "STRING",
	"datetime": "TIMESTAMP",
	"date":     "DATE",
}

// dataTypesMapToRudder maps delta lake data types to rudder data types
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
	"DATE":      "date",
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
	"date":      "date",
	"timestamp": "datetime",
}

// excludeColumnsMap Columns you need to exclude
// Since event_date is an auto generated column in order to support partitioning.
// We need to ignore it during query generation.
var excludeColumnsMap = map[string]struct{}{
	"event_date": {},
}

var primaryKeyMap = map[string]string{
	warehouseutils.UsersTable:      "id",
	warehouseutils.IdentifiesTable: "id",
	warehouseutils.DiscardsTable:   "row_id",
}

var errorsMappings = []model.JobError{
	{
		Type:   model.PermissionError,
		Format: regexp.MustCompile(`UnauthorizedAccessException: PERMISSION_DENIED: User does not have READ FILES on External Location`),
	},
	{
		Type:   model.PermissionError,
		Format: regexp.MustCompile(`SecurityException: User does not have permission CREATE on CATALOG`),
	},
}

type Deltalake struct {
	DB             *sqlmiddleware.DB
	Namespace      string
	ObjectStorage  string
	Warehouse      model.Warehouse
	Uploader       warehouseutils.Uploader
	connectTimeout time.Duration
	logger         logger.Logger
	stats          stats.Stats

	config struct {
		loadTableStrategy      string
		enablePartitionPruning bool
		slowQueryThreshold     time.Duration
		maxRetries             int
		retryMinWait           time.Duration
		retryMaxWait           time.Duration
		maxErrorLength         int
	}
}

func New(conf *config.Config, log logger.Logger, stat stats.Stats) *Deltalake {
	dl := &Deltalake{}

	dl.logger = log.Child("integration").Child("deltalake")
	dl.stats = stat

	dl.config.loadTableStrategy = conf.GetString("Warehouse.deltalake.loadTableStrategy", mergeMode)
	dl.config.enablePartitionPruning = conf.GetBool("Warehouse.deltalake.enablePartitionPruning", true)
	dl.config.slowQueryThreshold = conf.GetDuration("Warehouse.deltalake.slowQueryThreshold", 5, time.Minute)
	dl.config.maxRetries = conf.GetInt("Warehouse.deltalake.maxRetries", 10)
	dl.config.retryMinWait = conf.GetDuration("Warehouse.deltalake.retryMinWait", 1, time.Second)
	dl.config.retryMaxWait = conf.GetDuration("Warehouse.deltalake.retryMaxWait", 300, time.Second)
	dl.config.maxErrorLength = conf.GetInt("Warehouse.deltalake.maxErrorLength", 64*1024) // 64 KB

	return dl
}

// Setup sets up the warehouse
func (d *Deltalake) Setup(_ context.Context, warehouse model.Warehouse, uploader warehouseutils.Uploader) error {
	d.Warehouse = warehouse
	d.Namespace = warehouse.Namespace
	d.Uploader = uploader
	d.ObjectStorage = warehouseutils.ObjectStorageType(
		warehouseutils.DELTALAKE,
		warehouse.Destination.Config,
		d.Uploader.UseRudderStorage(),
	)

	db, err := d.connect()
	if err != nil {
		return fmt.Errorf("connecting: %w", err)
	}

	d.DB = db

	return nil
}

// connect connects to the warehouse
func (d *Deltalake) connect() (*sqlmiddleware.DB, error) {
	port, err := strconv.Atoi(warehouseutils.GetConfigValue(port, d.Warehouse))
	if err != nil {
		return nil, fmt.Errorf("port is not a number: %w", err)
	}

	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname(warehouseutils.GetConfigValue(host, d.Warehouse)),
		dbsql.WithPort(port),
		dbsql.WithHTTPPath(warehouseutils.GetConfigValue(path, d.Warehouse)),
		dbsql.WithAccessToken(warehouseutils.GetConfigValue(token, d.Warehouse)),
		dbsql.WithSessionParams(map[string]string{
			"ansi_mode": "false",
		}),
		dbsql.WithUserAgentEntry(userAgent),
		dbsql.WithTimeout(d.connectTimeout),
		dbsql.WithInitialNamespace(
			warehouseutils.GetConfigValue(catalog, d.Warehouse),
			"",
		),
		dbsql.WithRetries(d.config.maxRetries, d.config.retryMinWait, d.config.retryMaxWait),
	)
	if err != nil {
		return nil, fmt.Errorf("creating connector: %w", err)
	}

	if err = dbsqllog.SetLogLevel("disabled"); err != nil {
		return nil, fmt.Errorf("setting log level: %w", err)
	}

	db := sql.OpenDB(connector)
	middleware := sqlmiddleware.New(
		db,
		sqlmiddleware.WithStats(d.stats),
		sqlmiddleware.WithLogger(d.logger),
		sqlmiddleware.WithKeyAndValues(
			logfield.SourceID, d.Warehouse.Source.ID,
			logfield.SourceType, d.Warehouse.Source.SourceDefinition.Name,
			logfield.DestinationID, d.Warehouse.Destination.ID,
			logfield.DestinationType, d.Warehouse.Destination.DestinationDefinition.Name,
			logfield.WorkspaceID, d.Warehouse.WorkspaceID,
			logfield.Schema, d.Namespace,
		),
		sqlmiddleware.WithSlowQueryThreshold(d.config.slowQueryThreshold),
		sqlmiddleware.WithQueryTimeout(d.connectTimeout),
		sqlmiddleware.WithSecretsRegex(map[string]string{
			"'awsKeyId' = '[^']*'":        "'awsKeyId' = '***'",
			"'awsSecretKey' = '[^']*'":    "'awsSecretKey' = '***'",
			"'awsSessionToken' = '[^']*'": "'awsSessionToken' = '***'",
		}),
	)
	return middleware, nil
}

// CrashRecover crash recover scenarios
func (d *Deltalake) CrashRecover(ctx context.Context) {
	d.dropDanglingStagingTables(ctx)
}

// dropDanglingStagingTables drops dangling staging tables
func (d *Deltalake) dropDanglingStagingTables(ctx context.Context) {
	tableNames, err := d.fetchTables(ctx, rudderStagingTableRegex)
	if err != nil {
		d.logger.Warnw("fetching tables for dropping dangling staging tables",
			logfield.SourceID, d.Warehouse.Source.ID,
			logfield.SourceType, d.Warehouse.Source.SourceDefinition.Name,
			logfield.DestinationID, d.Warehouse.Destination.ID,
			logfield.DestinationType, d.Warehouse.Destination.DestinationDefinition.Name,
			logfield.WorkspaceID, d.Warehouse.WorkspaceID,
			logfield.Namespace, d.Namespace,
			logfield.Error, err.Error(),
		)
		return
	}

	d.dropStagingTables(ctx, tableNames)
}

// fetchTables fetches tables from the database
func (d *Deltalake) fetchTables(ctx context.Context, regex string) ([]string, error) {
	query := fmt.Sprintf(`SHOW tables FROM %s LIKE '%s';`, d.Namespace, regex)

	rows, err := d.DB.QueryContext(ctx, query)
	if err != nil {
		if strings.Contains(err.Error(), schemaNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("executing fetching tables: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var tables []string
	for rows.Next() {
		var (
			database    string
			tableName   string
			isTemporary bool
		)

		if err := rows.Scan(&database, &tableName, &isTemporary); err != nil {
			return nil, fmt.Errorf("processing fetched tables: %w", err)
		}

		tables = append(tables, tableName)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("processing fetched tables: %w", err)
	}
	return tables, nil
}

// dropStagingTables drops all the staging tables
func (d *Deltalake) dropStagingTables(ctx context.Context, stagingTables []string) {
	for _, stagingTable := range stagingTables {
		err := d.dropTable(ctx, stagingTable)
		if err != nil {
			d.logger.Warnw("dropping staging table",
				logfield.SourceID, d.Warehouse.Source.ID,
				logfield.SourceType, d.Warehouse.Source.SourceDefinition.Name,
				logfield.DestinationID, d.Warehouse.Destination.ID,
				logfield.DestinationType, d.Warehouse.Destination.DestinationDefinition.Name,
				logfield.WorkspaceID, d.Warehouse.WorkspaceID,
				logfield.Namespace, d.Namespace,
				logfield.StagingTableName, stagingTable,
				logfield.Error, err.Error(),
			)
		}
	}
}

// DropTable drops a table from the warehouse
func (d *Deltalake) dropTable(ctx context.Context, table string) error {
	query := fmt.Sprintf(`DROP TABLE %s.%s;`, d.Namespace, table)

	_, err := d.DB.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("executing drop table: %w", err)
	}

	return nil
}

// FetchSchema fetches the schema from the warehouse
func (d *Deltalake) FetchSchema(ctx context.Context) (model.Schema, model.Schema, error) {
	schema := make(model.Schema)
	unrecognizedSchema := make(model.Schema)
	tableNames, err := d.fetchTables(ctx, nonRudderStagingTableRegex)
	if err != nil {
		return model.Schema{}, model.Schema{}, fmt.Errorf("fetching tables: %w", err)
	}

	// For each table, fetch the attributes
	for _, tableName := range tableNames {
		tableSchema, err := d.fetchTableAttributes(ctx, tableName)
		if err != nil {
			return model.Schema{}, model.Schema{}, fmt.Errorf("fetching table attributes: %w", err)
		}

		for colName, dataType := range tableSchema {
			if _, ok := excludeColumnsMap[colName]; ok {
				continue
			}

			if _, ok := schema[tableName]; !ok {
				schema[tableName] = make(model.TableSchema)
			}
			if datatype, ok := dataTypesMapToRudder[dataType]; ok {
				schema[tableName][colName] = datatype
			} else {
				if _, ok := unrecognizedSchema[tableName]; !ok {
					unrecognizedSchema[tableName] = make(model.TableSchema)
				}
				unrecognizedSchema[tableName][colName] = warehouseutils.MissingDatatype

				warehouseutils.WHCounterStat(warehouseutils.RudderMissingDatatype, &d.Warehouse, warehouseutils.Tag{Name: "datatype", Value: datatype}).Count(1)
			}
		}
	}
	return schema, unrecognizedSchema, nil
}

// fetchTableAttributes fetches the attributes of a table
func (d *Deltalake) fetchTableAttributes(ctx context.Context, tableName string) (model.TableSchema, error) {
	tableSchema := make(model.TableSchema)

	query := fmt.Sprintf(`DESCRIBE QUERY TABLE %s.%s;`, d.Namespace, tableName)

	rows, err := d.DB.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("executing fetching table attributes: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var (
			colName, datatype string
			comment           sql.NullString
		)

		if err = rows.Scan(&colName, &datatype, &comment); err != nil {
			return nil, fmt.Errorf("processing fetched table attributes: %w", err)
		}

		tableSchema[colName] = datatype
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("processing fetched table attributes: %w", err)
	}
	return tableSchema, nil
}

// CreateSchema creates a schema in the warehouse if it does not exist.
func (d *Deltalake) CreateSchema(ctx context.Context) error {
	if exists, err := d.schemaExists(ctx); err != nil {
		return fmt.Errorf("checking if schema exists: %w", err)
	} else if exists {
		return nil
	} else if err := d.createSchema(ctx); err != nil {
		return fmt.Errorf("create schema: %w", err)
	}

	return nil
}

// schemaExists checks if a schema exists in the warehouse.
func (d *Deltalake) schemaExists(ctx context.Context) (bool, error) {
	query := fmt.Sprintf(`SHOW SCHEMAS LIKE '%s';`, d.Namespace)

	var schema string
	err := d.DB.QueryRowContext(ctx, query).Scan(&schema)

	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("schema exists: %w", err)
	}

	return schema == d.Namespace, nil
}

// createSchema creates a schema in the warehouse.
func (d *Deltalake) createSchema(ctx context.Context) error {
	query := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s;`, d.Namespace)

	_, err := d.DB.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("executing create schema: %w", err)
	}

	return nil
}

// CreateTable creates a table in the warehouse.
func (d *Deltalake) CreateTable(ctx context.Context, tableName string, columns model.TableSchema) error {
	var partitionedSql, tableLocationSql string

	tableLocationSql = d.tableLocationQuery(tableName)
	if _, ok := columns["received_at"]; ok {
		partitionedSql = `PARTITIONED BY(event_date)`
	}

	createTableClauseSql := "CREATE TABLE IF NOT EXISTS"
	if tableLocationSql != "" {
		createTableClauseSql = "CREATE OR REPLACE TABLE"
	}

	query := fmt.Sprintf(`
		%s %s.%s ( %s ) USING DELTA %s %s;
`,
		createTableClauseSql,
		d.Namespace,
		tableName,
		columnsWithDataTypes(columns, ""),
		tableLocationSql,
		partitionedSql,
	)

	_, err := d.DB.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("creating table: %w", d.TrimErrorMessage(err))
	}

	return nil
}

func (d *Deltalake) TrimErrorMessage(baseError error) error {
	errorString := baseError.Error()

	if len(errorString) <= d.config.maxErrorLength {
		return baseError
	}
	return errors.New(errorString[:d.config.maxErrorLength])
}

// columnsWithDataTypes returns the columns with their data types.
func columnsWithDataTypes(columns model.TableSchema, prefix string) string {
	keys := warehouseutils.SortColumnKeysFromColumnMap(columns)
	format := func(_ int, name string) string {
		if _, ok := excludeColumnsMap[name]; ok {
			return ""
		}
		if name == "received_at" {
			generatedColumnSQL := "DATE GENERATED ALWAYS AS ( CAST(received_at AS DATE) )"
			return fmt.Sprintf(`%s%s %s, %s%s %s`, prefix, name, dataTypesMap[columns[name]], prefix, "event_date", generatedColumnSQL)
		}
		return fmt.Sprintf(`%s%s %s`, prefix, name, dataTypesMap[columns[name]])
	}
	return warehouseutils.JoinWithFormatting(keys, format, ",")
}

// tableLocationQuery returns the location query for the table.
func (d *Deltalake) tableLocationQuery(tableName string) string {
	enableExternalLocation := warehouseutils.GetConfigValueBoolString("enableExternalLocation", d.Warehouse)
	externalLocation := warehouseutils.GetConfigValue("externalLocation", d.Warehouse)

	if enableExternalLocation != "true" || externalLocation == "" {
		return ""
	}

	return fmt.Sprintf("LOCATION '%s/%s/%s'", externalLocation, d.Namespace, tableName)
}

// AddColumns adds columns to the table.
func (d *Deltalake) AddColumns(ctx context.Context, tableName string, columnsInfo []warehouseutils.ColumnInfo) error {
	var queryBuilder strings.Builder

	queryBuilder.WriteString(fmt.Sprintf(`
		ALTER TABLE
		  %s.%s
		ADD COLUMNS(`,
		d.Namespace,
		tableName,
	))

	for _, columnInfo := range columnsInfo {
		queryBuilder.WriteString(fmt.Sprintf(` %s %s,`, columnInfo.Name, dataTypesMap[columnInfo.Type]))
	}

	query := strings.TrimSuffix(queryBuilder.String(), ",")
	query += ");"

	_, err := d.DB.ExecContext(ctx, query)

	// Handle error in case of single column
	if len(columnsInfo) == 1 {
		if err != nil && strings.Contains(err.Error(), columnsAlreadyExists) {
			d.logger.Infow("column already exists",
				logfield.SourceID, d.Warehouse.Source.ID,
				logfield.SourceType, d.Warehouse.Source.SourceDefinition.Name,
				logfield.DestinationID, d.Warehouse.Destination.ID,
				logfield.DestinationType, d.Warehouse.Destination.DestinationDefinition.Name,
				logfield.WorkspaceID, d.Warehouse.WorkspaceID,
				logfield.Namespace, d.Namespace,
				logfield.TableName, tableName,
				logfield.ColumnName, columnsInfo[0].Name,
				logfield.Error, err.Error(),
			)
			return nil
		}
	}

	if err != nil {
		return fmt.Errorf("adding columns: %w", err)
	}

	return nil
}

// AlterColumn alters a column in the warehouse
func (*Deltalake) AlterColumn(context.Context, string, string, string) (model.AlterTableResponse, error) {
	return model.AlterTableResponse{}, nil
}

// LoadTable loads table for table name
func (d *Deltalake) LoadTable(ctx context.Context, tableName string) error {
	uploadTableSchema := d.Uploader.GetTableSchemaInUpload(tableName)
	warehouseTableSchema := d.Uploader.GetTableSchemaInWarehouse(tableName)

	_, err := d.loadTable(ctx, tableName, uploadTableSchema, warehouseTableSchema, false)
	if err != nil {
		return fmt.Errorf("loading table: %w", err)
	}

	return nil
}

func (d *Deltalake) loadTable(ctx context.Context, tableName string, tableSchemaInUpload, tableSchemaAfterUpload model.TableSchema, skipTempTableDelete bool) (string, error) {
	var (
		sortedColumnKeys = warehouseutils.SortColumnKeysFromColumnMap(tableSchemaInUpload)
		stagingTableName = warehouseutils.StagingTableName(provider, tableName, tableNameLimit)

		err  error
		auth string
		row  *sqlmiddleware.Row
	)

	d.logger.Infow("started loading",
		logfield.SourceID, d.Warehouse.Source.ID,
		logfield.SourceType, d.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, d.Warehouse.Destination.ID,
		logfield.DestinationType, d.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, d.Warehouse.WorkspaceID,
		logfield.Namespace, d.Namespace,
		logfield.TableName, tableName,
	)

	if err = d.CreateTable(ctx, stagingTableName, tableSchemaAfterUpload); err != nil {
		return "", fmt.Errorf("creating staging table: %w", err)
	}

	if !skipTempTableDelete {
		defer d.dropStagingTables(ctx, []string{stagingTableName})
	}

	if auth, err = d.authQuery(); err != nil {
		return "", fmt.Errorf("getting auth query: %w", err)
	}

	objectsLocation, err := d.Uploader.GetSampleLoadFileLocation(ctx, tableName)
	if err != nil {
		return "", fmt.Errorf("getting sample load file location: %w", err)
	}

	var (
		loadFolder        = d.getLoadFolder(objectsLocation)
		tableSchemaDiff   = tableSchemaDiff(tableSchemaInUpload, tableSchemaAfterUpload)
		sortedColumnNames = d.sortedColumnNames(tableSchemaInUpload, sortedColumnKeys, tableSchemaDiff)

		query          string
		partitionQuery string
	)

	if d.Uploader.GetLoadFileType() == warehouseutils.LoadFileTypeParquet {
		query = fmt.Sprintf(`
			COPY INTO %s
			FROM
			  (
				SELECT
				  %s
				FROM
				  '%s'
			  )
			FILEFORMAT = PARQUET
			PATTERN = '*.parquet'
			COPY_OPTIONS ('force' = 'true')
			%s;`,
			fmt.Sprintf(`%s.%s`, d.Namespace, stagingTableName),
			sortedColumnNames,
			loadFolder, auth,
		)
	} else {
		query = fmt.Sprintf(`
			COPY INTO %s
			FROM
			  (
				SELECT
				  %s
				FROM
				  '%s'
			  )
			FILEFORMAT = CSV
			PATTERN = '*.gz'
			FORMAT_OPTIONS (
				'compression' = 'gzip',
				'quote' = '"',
				'escape' = '"',
				'multiLine' = 'true'
			  )
			COPY_OPTIONS ('force' = 'true')
			%s;
`,
			fmt.Sprintf(`%s.%s`, d.Namespace, stagingTableName),
			sortedColumnNames,
			loadFolder,
			auth,
		)
	}

	if _, err = d.DB.ExecContext(ctx, query); err != nil {
		return "", fmt.Errorf("running COPY command: %w", err)
	}

	if d.ShouldAppend() {
		query = fmt.Sprintf(`
			INSERT INTO %[1]s.%[2]s (%[4]s)
			SELECT
			  %[4]s
			FROM
			  (
				SELECT
				  *
				FROM
				  (
					SELECT
					  *,
					  row_number() OVER (
						PARTITION BY %[5]s
						ORDER BY
						  RECEIVED_AT DESC
					  ) AS _rudder_staging_row_number
					FROM
					  %[1]s.%[3]s
				  ) AS q
				WHERE
				  _rudder_staging_row_number = 1
			  );
		`,
			d.Namespace,
			tableName,
			stagingTableName,
			columnNames(warehouseutils.SortColumnKeysFromColumnMap(tableSchemaAfterUpload)),
			primaryKey(tableName),
		)
	} else {
		if partitionQuery, err = d.partitionQuery(ctx, tableName); err != nil {
			return "", fmt.Errorf("getting partition query: %w", err)
		}

		pk := primaryKey(tableName)

		query = fmt.Sprintf(`
			MERGE INTO %[1]s.%[2]s AS MAIN USING (
			  SELECT
				*
			  FROM
				(
				  SELECT
					*,
					row_number() OVER (
					  PARTITION BY %[4]s
					  ORDER BY
						RECEIVED_AT DESC
					) AS _rudder_staging_row_number
				  FROM
					%[1]s.%[3]s
				) AS q
			  WHERE
				_rudder_staging_row_number = 1
			)
			AS STAGING ON %[8]s MAIN.%[4]s = STAGING.%[4]s
			WHEN MATCHED THEN
			UPDATE
			SET
			  %[5]s
			WHEN NOT MATCHED THEN
			INSERT (%[6]s)
			VALUES
			  (%[7]s);
		`,
			d.Namespace,
			tableName,
			stagingTableName,
			pk,
			columnsWithValues(sortedColumnKeys),
			columnNames(sortedColumnKeys),
			stagingColumnNames(sortedColumnKeys),
			partitionQuery,
		)
	}

	row = d.DB.QueryRowContext(ctx, query)

	var (
		affected int64
		updated  int64
		deleted  int64
		inserted int64
	)

	if d.ShouldAppend() {
		err = row.Scan(&affected, &inserted)
	} else {
		err = row.Scan(&affected, &updated, &deleted, &inserted)
	}

	if err != nil {
		return "", fmt.Errorf("scanning deduplication: %w", err)
	}
	if row.Err() != nil {
		return "", fmt.Errorf("running deduplication: %w", row.Err())
	}

	d.stats.NewTaggedStat("dedup_rows", stats.CountType, stats.Tags{
		"sourceID":       d.Warehouse.Source.ID,
		"sourceType":     d.Warehouse.Source.SourceDefinition.Name,
		"sourceCategory": d.Warehouse.Source.SourceDefinition.Category,
		"destID":         d.Warehouse.Destination.ID,
		"destType":       d.Warehouse.Destination.DestinationDefinition.Name,
		"workspaceId":    d.Warehouse.WorkspaceID,
		"tableName":      tableName,
	}).Count(int(updated))

	d.logger.Infow("completed loading",
		logfield.SourceID, d.Warehouse.Source.ID,
		logfield.SourceType, d.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, d.Warehouse.Destination.ID,
		logfield.DestinationType, d.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, d.Warehouse.WorkspaceID,
		logfield.Namespace, d.Namespace,
		logfield.TableName, tableName,
	)
	return stagingTableName, nil
}

func tableSchemaDiff(tableSchemaInUpload, tableSchemaAfterUpload model.TableSchema) warehouseutils.TableSchemaDiff {
	diff := warehouseutils.TableSchemaDiff{
		ColumnMap: make(model.TableSchema),
	}

	for columnName, columnType := range tableSchemaAfterUpload {
		if _, ok := tableSchemaInUpload[columnName]; !ok {
			diff.ColumnMap[columnName] = columnType
		}
	}

	return diff
}

func columnNames(columns []string) string {
	return strings.Join(columns, ",")
}

func stagingColumnNames(columns []string) string {
	format := func(_ int, str string) string {
		return fmt.Sprintf(`STAGING.%s`, str)
	}
	return warehouseutils.JoinWithFormatting(columns, format, ",")
}

func columnsWithValues(columns []string) string {
	format := func(_ int, str string) string {
		return fmt.Sprintf(`MAIN.%[1]s = STAGING.%[1]s`, str)
	}
	return warehouseutils.JoinWithFormatting(columns, format, ",")
}

func primaryKey(tableName string) string {
	key := "id"
	if column, ok := primaryKeyMap[tableName]; ok {
		key = column
	}
	return key
}

// sortedColumnNames returns the column names in the order of sortedColumnKeys
func (d *Deltalake) sortedColumnNames(tableSchemaInUpload model.TableSchema, sortedColumnKeys []string, diff warehouseutils.TableSchemaDiff) string {
	if d.Uploader.GetLoadFileType() == warehouseutils.LoadFileTypeParquet {
		return warehouseutils.JoinWithFormatting(sortedColumnKeys, func(_ int, value string) string {
			columnName := value
			columnType := dataTypesMap[tableSchemaInUpload[columnName]]
			return fmt.Sprintf(`%s::%s`, columnName, columnType)
		}, ",")
	}

	format := func(index int, value string) string {
		csvColumnIndex := fmt.Sprintf(`%s%d`, "_c", index)
		columnName := value
		columnType := dataTypesMap[tableSchemaInUpload[columnName]]
		return fmt.Sprintf(`CAST ( %s AS %s ) AS %s`, csvColumnIndex, columnType, columnName)
	}
	formatString := warehouseutils.JoinWithFormatting(sortedColumnKeys, format, ",")

	if len(diff.ColumnMap) > 0 {
		diffCols := make([]string, 0, len(diff.ColumnMap))
		for key := range diff.ColumnMap {
			diffCols = append(diffCols, key)
		}

		diffFormat := func(_ int, value string) string {
			return fmt.Sprintf(`NULL AS %s`, value)
		}
		diffString := warehouseutils.JoinWithFormatting(diffCols, diffFormat, ",")

		return fmt.Sprintf("%s, %s", formatString, diffString)
	}

	return formatString
}

// authQuery return authentication for AWS STS and SSE-C encryption
// STS authentication is only supported with S3A client.
func (d *Deltalake) authQuery() (string, error) {
	if d.ObjectStorage != warehouseutils.S3 || !d.canUseAuth() {
		return "", nil
	}

	tempAccessKeyId, tempSecretAccessKey, token, err := warehouseutils.GetTemporaryS3Cred(&d.Warehouse.Destination)
	if err != nil {
		return "", fmt.Errorf("getting temporary s3 credentials: %w", err)
	}

	auth := fmt.Sprintf(`CREDENTIALS ( 'awsKeyId' = '%s', 'awsSecretKey' = '%s', 'awsSessionToken' = '%s' )`, tempAccessKeyId, tempSecretAccessKey, token)
	return auth, nil
}

// canUseAuth returns true if the warehouse is configured to use RudderObjectStorage or STS tokens
func (d *Deltalake) canUseAuth() bool {
	canUseRudderStorage := misc.IsConfiguredToUseRudderObjectStorage(d.Warehouse.Destination.Config)
	canUseSTSTokens := warehouseutils.GetConfigValueBoolString(useSTSTokens, d.Warehouse) == "true"

	return canUseRudderStorage || canUseSTSTokens
}

// getLoadFolder returns the load folder for the warehouse load files
func (d *Deltalake) getLoadFolder(location string) string {
	loadFolder := warehouseutils.GetObjectFolderForDeltalake(d.ObjectStorage, location)

	if d.ObjectStorage == warehouseutils.S3 && d.hasAWSCredentials() {
		loadFolder = strings.Replace(loadFolder, "s3://", "s3a://", 1)
	}

	return loadFolder
}

// hasAWSCredentials returns true if the warehouse is configured to use AWS credentials
func (d *Deltalake) hasAWSCredentials() bool {
	awsAccessKey := warehouseutils.GetConfigValue(warehouseutils.AWSAccessKey, d.Warehouse)
	awsSecretKey := warehouseutils.GetConfigValue(warehouseutils.AWSAccessSecret, d.Warehouse)

	return awsAccessKey != "" && awsSecretKey != ""
}

// partitionQuery returns a query to fetch partitions for a table
func (d *Deltalake) partitionQuery(ctx context.Context, tableName string) (string, error) {
	if !d.config.enablePartitionPruning {
		return "", nil
	}

	query := fmt.Sprintf(`SHOW PARTITIONS %s.%s;`, d.Namespace, tableName)
	rows, err := d.DB.QueryContext(ctx, query)
	if err != nil {
		if strings.Contains(err.Error(), partitionNotFound) {
			return "", nil
		}
		return "", fmt.Errorf("executing fetching partitions: %w", err)
	}
	defer func() { _ = rows.Close() }()

	_ = rows.Err() // ignore error

	partitionColumns, err := rows.Columns()
	if err != nil {
		return "", fmt.Errorf("scanning partition columns: %w", err)
	}

	if !partitionedByEventDate(partitionColumns) {
		return "", nil
	}

	firstEvent, lastEvent := d.Uploader.GetFirstLastEvent()
	dateRange := warehouseutils.GetDateRangeList(firstEvent, lastEvent, "2006-01-02")
	if len(dateRange) == 0 {
		return "", nil
	}

	dateRangeString := warehouseutils.JoinWithFormatting(dateRange, func(_ int, str string) string {
		return fmt.Sprintf(`'%s'`, str)
	}, ",")
	partitionQuery := fmt.Sprintf(`CAST ( MAIN.event_date AS string) IN (%s) AND`, dateRangeString)

	return partitionQuery, nil
}

// partitionedByEventDate returns true if the table is partitioned by event_date
func partitionedByEventDate(columns []string) bool {
	return slices.Contains(columns, "event_date")
}

// LoadUserTables loads user tables
func (d *Deltalake) LoadUserTables(ctx context.Context) map[string]error {
	var (
		identifiesSchemaInUpload    = d.Uploader.GetTableSchemaInUpload(warehouseutils.IdentifiesTable)
		identifiesSchemaInWarehouse = d.Uploader.GetTableSchemaInWarehouse(warehouseutils.IdentifiesTable)
		usersSchemaInUpload         = d.Uploader.GetTableSchemaInUpload(warehouseutils.UsersTable)
		usersSchemaInWarehouse      = d.Uploader.GetTableSchemaInWarehouse(warehouseutils.UsersTable)
	)

	d.logger.Infow("started loading for identifies and users tables",
		logfield.SourceID, d.Warehouse.Source.ID,
		logfield.SourceType, d.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, d.Warehouse.Destination.ID,
		logfield.DestinationType, d.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, d.Warehouse.WorkspaceID,
		logfield.Namespace, d.Namespace,
	)

	identifyStagingTable, err := d.loadTable(ctx, warehouseutils.IdentifiesTable, identifiesSchemaInUpload, identifiesSchemaInWarehouse, true)
	if err != nil {
		return map[string]error{
			warehouseutils.IdentifiesTable: fmt.Errorf("loading table %s: %w", warehouseutils.IdentifiesTable, err),
		}
	}

	defer d.dropStagingTables(ctx, []string{identifyStagingTable})

	if len(usersSchemaInUpload) == 0 {
		return map[string]error{
			warehouseutils.IdentifiesTable: nil,
		}
	}

	var (
		partitionQuery string
		row            *sqlmiddleware.Row
	)

	userColNames, firstValProps := getColumnProperties(usersSchemaInWarehouse)
	stagingTableName := warehouseutils.StagingTableName(provider, warehouseutils.UsersTable, tableNameLimit)
	tableLocationSql := d.tableLocationQuery(stagingTableName)

	query := fmt.Sprintf(`
		CREATE TABLE %[1]s.%[2]s USING DELTA %[7]s AS (
		  SELECT
			DISTINCT *
		  FROM
			(
			  SELECT
				id,
				%[3]s
			  FROM
				(
				  (
					SELECT
					  id,
					  %[6]s
					FROM
					  %[1]s.%[4]s
					WHERE
					  id IN (
						SELECT
						  DISTINCT(user_id)
						FROM
						  %[1]s.%[5]s
						WHERE
						  user_id IS NOT NULL
					  )
				  )
				  UNION
					(
					  SELECT
						user_id,
						%[6]s
					  FROM
						%[1]s.%[5]s
					  WHERE
						user_id IS NOT NULL
					)
				)
			)
		);
`,
		d.Namespace,
		stagingTableName,
		strings.Join(firstValProps, ","),
		warehouseutils.UsersTable,
		identifyStagingTable,
		columnNames(userColNames),
		tableLocationSql,
	)

	_, err = d.DB.ExecContext(ctx, query)

	if err != nil {
		return map[string]error{
			warehouseutils.IdentifiesTable: nil,
			warehouseutils.UsersTable:      fmt.Errorf("creating staging table for users: %w", err),
		}
	}

	defer d.dropStagingTables(ctx, []string{stagingTableName})

	columnKeys := append([]string{`id`}, userColNames...)

	if d.ShouldAppend() {
		query = fmt.Sprintf(`
			INSERT INTO %[1]s.%[2]s (%[4]s)
			SELECT
			  %[4]s
			FROM
			  (
				SELECT
				  %[4]s
				FROM
				  %[1]s.%[3]s
			  );
		`,
			d.Namespace,
			warehouseutils.UsersTable,
			stagingTableName,
			columnNames(columnKeys),
		)
	} else {
		if partitionQuery, err = d.partitionQuery(ctx, warehouseutils.UsersTable); err != nil {
			return map[string]error{
				warehouseutils.IdentifiesTable: nil,
				warehouseutils.UsersTable:      fmt.Errorf("getting partition query: %w", err),
			}
		}

		pk := primaryKey(warehouseutils.UsersTable)

		query = fmt.Sprintf(`
			MERGE INTO %[1]s.%[2]s AS MAIN USING (
			  SELECT
				%[6]s
			  FROM
				%[1]s.%[3]s
			) AS STAGING ON %[8]s MAIN.%[4]s = STAGING.%[4]s
			WHEN MATCHED THEN
			UPDATE
			SET
			  %[5]s WHEN NOT MATCHED
			THEN INSERT (%[6]s)
			VALUES
			  (%[7]s);`,
			d.Namespace,
			warehouseutils.UsersTable,
			stagingTableName,
			pk,
			columnsWithValues(columnKeys),
			columnNames(columnKeys),
			stagingColumnNames(columnKeys),
			partitionQuery,
		)
	}

	row = d.DB.QueryRowContext(ctx, query)

	var (
		affected int64
		updated  int64
		deleted  int64
		inserted int64
	)

	if d.config.loadTableStrategy == appendMode {
		err = row.Scan(&affected, &inserted)
	} else {
		err = row.Scan(&affected, &updated, &deleted, &inserted)
	}

	if err != nil {
		return map[string]error{
			warehouseutils.IdentifiesTable: nil,
			warehouseutils.UsersTable:      fmt.Errorf("getting rows affected for dedup: %w", err),
		}
	}

	if row.Err() != nil {
		return map[string]error{
			warehouseutils.IdentifiesTable: nil,
			warehouseutils.UsersTable:      fmt.Errorf("running deduplication: %w", row.Err()),
		}
	}

	d.stats.NewTaggedStat("dedup_rows", stats.CountType, stats.Tags{
		"sourceID":       d.Warehouse.Source.ID,
		"sourceType":     d.Warehouse.Source.SourceDefinition.Name,
		"sourceCategory": d.Warehouse.Source.SourceDefinition.Category,
		"destID":         d.Warehouse.Destination.ID,
		"destType":       d.Warehouse.Destination.DestinationDefinition.Name,
		"workspaceId":    d.Warehouse.WorkspaceID,
		"tableName":      warehouseutils.UsersTable,
	}).Count(int(updated))

	d.logger.Infow("completed loading for users and identifies tables",
		logfield.SourceID, d.Warehouse.Source.ID,
		logfield.SourceType, d.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, d.Warehouse.Destination.ID,
		logfield.DestinationType, d.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, d.Warehouse.WorkspaceID,
		logfield.Namespace, d.Namespace,
	)

	return map[string]error{
		warehouseutils.IdentifiesTable: nil,
		warehouseutils.UsersTable:      nil,
	}
}

// getColumnProperties returns the column names and first value properties for the given table schema
func getColumnProperties(usersSchemaInWarehouse model.TableSchema) ([]string, []string) {
	var (
		userColNames    []string
		firstValProps   []string
		ignoredColNames = map[string]struct{}{
			"id":      {},
			"user_id": {},
			"uuid":    {},
		}
	)

	for colName := range usersSchemaInWarehouse {
		if _, ignore := ignoredColNames[colName]; ignore {
			continue
		}

		userColNames = append(userColNames, colName)
		firstValProps = append(firstValProps, fmt.Sprintf(`FIRST_VALUE(%[1]s, TRUE) OVER (PARTITION BY id ORDER BY received_at DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS %[1]s`, colName))
	}

	return userColNames, firstValProps
}

// LoadIdentityMergeRulesTable loads identifies merge rules tables
func (*Deltalake) LoadIdentityMergeRulesTable(context.Context) error {
	return nil
}

// LoadIdentityMappingsTable loads identifies mappings table
func (*Deltalake) LoadIdentityMappingsTable(context.Context) error {
	return nil
}

// Cleanup cleans up the warehouse
func (d *Deltalake) Cleanup(ctx context.Context) {
	if d.DB != nil {
		d.dropDanglingStagingTables(ctx)
		_ = d.DB.Close()
	}
}

// IsEmpty checks if the warehouse is empty or not
func (*Deltalake) IsEmpty(context.Context, model.Warehouse) (bool, error) {
	return false, nil
}

// TestConnection tests the connection to the warehouse
func (d *Deltalake) TestConnection(ctx context.Context, _ model.Warehouse) error {
	err := d.DB.PingContext(ctx)
	if errors.Is(err, context.DeadlineExceeded) {
		return errors.New("connection timeout: verify the availability of the SQL warehouse/cluster on Databricks (this process may take up to 15 minutes). Once the SQL warehouse/cluster is ready, please attempt your connection again")
	}
	if err != nil {
		return fmt.Errorf("pinging: %w", err)
	}

	return nil
}

// DownloadIdentityRules downloadchecking if schema exists identity rules
func (*Deltalake) DownloadIdentityRules(context.Context, *misc.GZipWriter) error {
	return nil
}

// GetTotalCountInTable returns the total count in the table
func (d *Deltalake) GetTotalCountInTable(ctx context.Context, tableName string) (int64, error) {
	query := fmt.Sprintf(`
		SELECT COUNT(*) FROM %[1]s.%[2]s;
	`,
		d.Namespace,
		tableName,
	)

	var total int64
	err := d.DB.QueryRowContext(ctx, query).Scan(&total)
	if err != nil {
		if strings.Contains(err.Error(), schemaNotFound) {
			return 0, nil
		}
		return 0, fmt.Errorf("total count in table: %w", err)
	}

	return total, nil
}

// Connect returns Client
func (d *Deltalake) Connect(_ context.Context, warehouse model.Warehouse) (warehouseclient.Client, error) {
	d.Warehouse = warehouse
	d.Namespace = warehouse.Namespace
	d.ObjectStorage = warehouseutils.ObjectStorageType(
		warehouseutils.DELTALAKE,
		warehouse.Destination.Config,
		misc.IsConfiguredToUseRudderObjectStorage(d.Warehouse.Destination.Config),
	)

	db, err := d.connect()
	if err != nil {
		return warehouseclient.Client{}, fmt.Errorf("connecting: %w", err)
	}

	return warehouseclient.Client{Type: warehouseclient.SQLClient, SQL: db.DB}, nil
}

// LoadTestTable loads the test table
func (d *Deltalake) LoadTestTable(ctx context.Context, location, tableName string, _ map[string]interface{}, format string) error {
	auth, err := d.authQuery()
	if err != nil {
		return fmt.Errorf("auth query: %w", err)
	}

	loadFolder := d.getLoadFolder(location)

	var query string
	if format == warehouseutils.LoadFileTypeParquet {
		query = fmt.Sprintf(`
			COPY INTO %s
			FROM
			  (
				SELECT
				  %s
				FROM
				  '%s'
			  )
			FILEFORMAT = PARQUET
			PATTERN = '*.parquet'
			COPY_OPTIONS ('force' = 'true')
			%s;
`,
			fmt.Sprintf(`%s.%s`, d.Namespace, tableName),
			fmt.Sprintf(`%s, %s`, "id", "val"),
			loadFolder,
			auth,
		)
	} else {
		query = fmt.Sprintf(`
			COPY INTO %s
			FROM
			  (
				SELECT
				  %s
				FROM
				  '%s'
			  )
			FILEFORMAT = CSV
			PATTERN = '*.gz'
			FORMAT_OPTIONS (
				'compression' = 'gzip',
				'quote' = '"',
				'escape' = '"',
				'multiLine' = 'true'
			)
			COPY_OPTIONS ('force' = 'true')
			%s;
`,
			fmt.Sprintf(`%s.%s`, d.Namespace, tableName),
			"CAST ( '_c0' AS BIGINT ) AS id, CAST ( '_c1' AS STRING ) AS val",
			loadFolder,
			auth,
		)
	}

	_, err = d.DB.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("loading test table: %w", err)
	}

	return nil
}

// SetConnectionTimeout sets the connection timeout
func (d *Deltalake) SetConnectionTimeout(timeout time.Duration) {
	d.connectTimeout = timeout
}

// ErrorMappings returns the error mappings
func (*Deltalake) ErrorMappings() []model.JobError {
	return errorsMappings
}

// DropTable drops a table in the warehouse
func (d *Deltalake) DropTable(ctx context.Context, tableName string) error {
	return d.dropTable(ctx, tableName)
}

func (*Deltalake) DeleteBy(context.Context, []string, warehouseutils.DeleteByParams) error {
	return fmt.Errorf(warehouseutils.NotImplementedErrorCode)
}

// ShouldAppend returns true if:
// * the load table strategy is "append" mode
// * the uploader says we can append
func (d *Deltalake) ShouldAppend() bool {
	return d.config.loadTableStrategy == appendMode && d.Uploader.CanAppend()
}
