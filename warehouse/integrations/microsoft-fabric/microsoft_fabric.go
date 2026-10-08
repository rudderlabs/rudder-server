package microsoftfabric

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"maps"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	sqlconnectconfig "github.com/rudderlabs/sqlconnect-go/sqlconnect/config"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	sqlmw "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/types"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const (
	provider       = warehouseutils.MicrosoftFabric
	tableNameLimit = 128
)

var (
	dataTypesMap = map[string]string{
		model.BooleanDataType:  "bit",
		model.IntDataType:      "bigint",
		model.FloatDataType:    "float",
		model.DateTimeDataType: "datetime2(6)",
		model.StringDataType:   "varchar(max)",
		model.TextDataType:     "varchar(max)",
		model.JSONDataType:     "varchar(max)",
	}
	dataTypesMapToRudder = map[string]string{
		"bit":              model.BooleanDataType,
		"smallint":         model.IntDataType,
		"int":              model.IntDataType,
		"bigint":           model.IntDataType,
		"decimal":          model.FloatDataType,
		"numeric":          model.FloatDataType,
		"float":            model.FloatDataType,
		"real":             model.FloatDataType,
		"date":             model.DateTimeDataType,
		"time":             model.DateTimeDataType,
		"datetime2":        model.DateTimeDataType,
		"char":             model.StringDataType,
		"varchar":          model.StringDataType,
		"varbinary":        model.StringDataType,
		"uniqueidentifier": model.StringDataType,
	}
	primaryKeyMap = map[string]string{
		warehouseutils.UsersTable:      "id",
		warehouseutils.IdentifiesTable: "id",
		warehouseutils.DiscardsTable:   "row_id",
	}
	partitionKeyMap = map[string]string{
		warehouseutils.UsersTable:      "id",
		warehouseutils.IdentifiesTable: "id",
		warehouseutils.DiscardsTable:   "row_id, column_name, table_name",
	}
	errorsMappings = []model.JobError{
		{Type: model.PermissionError, Format: regexp.MustCompile(`(?i)(login failed|authentication failed|AADSTS|principal.*not able|permission.*denied|not authorized)`)},
		{Type: model.ResourceNotFoundError, Format: regexp.MustCompile(`(?i)(cannot open (?:database|server)|invalid object name|could not be found|does not exist)`)},
		{Type: model.ConcurrentQueriesError, Format: regexp.MustCompile(`(?i)(was deadlocked on .* resources|lock request time out period exceeded)`)},
		{Type: model.ColumnCountError, Format: regexp.MustCompile(`(?i)(1024 columns|maximum.*columns)`)},
		{Type: model.ColumnSizeError, Format: regexp.MustCompile(`(?i)(string or binary data would be truncated|16 MB)`)},
		{Type: model.AlterColumnError, Format: regexp.MustCompile(`(?i)(is not compatible with external data type|cannot be converted from parquet)`)},
		{Type: model.PermissionError, Format: regexp.MustCompile(`(?i)spn_token_bootstrap`)},
	}
)

// MicrosoftFabric implements Fabric Warehouse loading through the SQL analytics endpoint.
type MicrosoftFabric struct {
	db             *sqlmw.DB
	namespace      string
	warehouse      model.Warehouse
	uploader       warehouseutils.Uploader
	connectTimeout time.Duration
	conf           *config.Config
	logger         logger.Logger
	stats          stats.Stats

	config struct {
		slowQueryThreshold time.Duration
	}
}

func New(conf *config.Config, log logger.Logger, stat stats.Stats) *MicrosoftFabric {
	fabric := &MicrosoftFabric{
		conf:   conf,
		logger: log.Child("integrations").Child("microsoft-fabric"),
		stats:  stat,
	}
	fabric.config.slowQueryThreshold = conf.GetDurationVar(5, time.Minute, "Warehouse.microsoft_fabric.slowQueryThreshold")
	return fabric
}

func stringConfig(values map[string]any, key string) string {
	value, _ := values[key].(string)
	return value
}

func (f *MicrosoftFabric) connectionConfig() sqlconnectconfig.Fabric {
	config := f.warehouse.Destination.Config
	return sqlconnectconfig.Fabric{
		Host:              f.warehouse.GetStringDestinationConfig(f.conf, model.HostSetting),
		Database:          f.warehouse.GetStringDestinationConfig(f.conf, model.DatabaseSetting),
		TenantID:          stringConfig(config, "tenantId"),
		ClientID:          stringConfig(config, "clientId"),
		ClientSecret:      stringConfig(config, "clientSecret"),
		FabricWorkspaceID: stringConfig(config, "fabricWorkspaceId"),
		Timeout:           f.connectTimeout,
	}
}

func (f *MicrosoftFabric) connect() (*sqlmw.DB, error) {
	credentialsJSON, err := jsonrs.Marshal(f.connectionConfig())
	if err != nil {
		return nil, fmt.Errorf("marshalling credentials: %w", err)
	}
	sqlConnectDB, err := sqlconnect.NewDB("fabric", credentialsJSON)
	if err != nil {
		return nil, fmt.Errorf("creating sqlconnect db: %w", err)
	}
	return sqlmw.New(
		sqlConnectDB.SqlDB(),
		sqlmw.WithStats(f.stats),
		sqlmw.WithLogger(f.logger),
		sqlmw.WithKeyAndValues(
			logfield.SourceID, f.warehouse.Source.ID,
			logfield.SourceType, f.warehouse.Source.SourceDefinition.Name,
			logfield.DestinationID, f.warehouse.Destination.ID,
			logfield.DestinationType, f.warehouse.Destination.DestinationDefinition.Name,
			logfield.WorkspaceID, f.warehouse.WorkspaceID,
			logfield.Namespace, f.namespace,
		),
		sqlmw.WithQueryTimeout(f.connectTimeout),
		sqlmw.WithSlowQueryThreshold(f.config.slowQueryThreshold),
	), nil
}

func columnDataType(rudderType, column string) (string, error) {
	dataType, ok := dataTypesMap[rudderType]
	if !ok {
		return "", fmt.Errorf("schema_evolution: unsupported Rudder type %q for column %q", rudderType, column)
	}
	return dataType, nil
}

func columnsWithDataTypes(columns model.TableSchema) (string, error) {
	keys := warehouseutils.SortColumnKeysFromColumnMap(columns)
	definitions := make([]string, 0, len(keys))
	for _, name := range keys {
		dataType, err := columnDataType(columns[name], name)
		if err != nil {
			return "", err
		}
		definitions = append(definitions, fmt.Sprintf("%s %s NULL", warehouseutils.BracketQuoteIdentifier(name), dataType))
	}
	return strings.Join(definitions, ","), nil
}

func qualified(namespace, table string) string {
	return warehouseutils.QuoteQualifiedIdentifier(warehouseutils.BracketQuoteIdentifier, namespace, table)
}

func (f *MicrosoftFabric) Setup(ctx context.Context, warehouse model.Warehouse, uploader warehouseutils.Uploader) error {
	f.warehouse = warehouse
	f.namespace = warehouse.Namespace
	f.uploader = uploader
	db, err := f.connect()
	if err != nil {
		return fmt.Errorf("connecting to Microsoft Fabric: %w", err)
	}
	f.db = db
	return nil
}

func (f *MicrosoftFabric) Connect(ctx context.Context, warehouse model.Warehouse) (client.Client, error) {
	f.warehouse = warehouse
	f.namespace = warehouse.Namespace
	db, err := f.connect()
	if err != nil {
		return client.Client{}, fmt.Errorf("connecting to Microsoft Fabric: %w", err)
	}
	return client.Client{Type: client.SQLClient, SQL: db.DB}, nil
}

func (f *MicrosoftFabric) TestConnection(ctx context.Context, _ model.Warehouse) error {
	if err := f.db.PingContext(ctx); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("connection timeout: %w", err)
		}
		return fmt.Errorf("pinging Microsoft Fabric: %w", err)
	}
	return nil
}

func (f *MicrosoftFabric) CreateSchema(ctx context.Context) error {
	statement := fmt.Sprintf(`IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = %s) EXEC(%s);`,
		warehouseutils.UnicodeStringLiteral(f.namespace),
		warehouseutils.SQLStringLiteral("CREATE SCHEMA "+warehouseutils.BracketQuoteIdentifier(f.namespace)),
	)
	_, err := f.db.ExecContext(ctx, statement)
	return err
}

func (f *MicrosoftFabric) CreateTable(ctx context.Context, tableName string, columns model.TableSchema) error {
	definitions, err := columnsWithDataTypes(columns)
	if err != nil {
		return err
	}
	table := qualified(f.namespace, tableName)
	statement := fmt.Sprintf(`IF OBJECT_ID(%s, 'U') IS NULL CREATE TABLE %s (%s);`,
		warehouseutils.UnicodeStringLiteral(table), table, definitions,
	)
	_, err = f.db.ExecContext(ctx, statement)
	return err
}

func (f *MicrosoftFabric) DropTable(ctx context.Context, tableName string) error {
	_, err := f.db.ExecContext(ctx, fmt.Sprintf(`DROP TABLE %s;`, qualified(f.namespace, tableName)))
	return err
}

func (f *MicrosoftFabric) AddColumns(ctx context.Context, tableName string, columns []warehouseutils.ColumnInfo) error {
	for _, column := range columns {
		dataType, err := columnDataType(column.Type, column.Name)
		if err != nil {
			return err
		}
		var existingColumns int
		err = f.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table AND COLUMN_NAME = @column;`,
			sql.Named("schema", f.namespace), sql.Named("table", tableName), sql.Named("column", column.Name)).Scan(&existingColumns)
		if err != nil {
			return fmt.Errorf("checking nullable column %q: %w", column.Name, err)
		}
		if existingColumns > 0 {
			continue
		}
		statement := fmt.Sprintf(`ALTER TABLE %s ADD %s %s NULL;`,
			qualified(f.namespace, tableName), warehouseutils.BracketQuoteIdentifier(column.Name), dataType)
		if _, err := f.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("adding nullable column %q: %w", column.Name, err)
		}
	}
	return nil
}

func (*MicrosoftFabric) AlterColumn(_ context.Context, tableName, columnName, columnType string) (model.AlterTableResponse, error) {
	return model.AlterTableResponse{}, fmt.Errorf("schema_evolution: automatic ALTER COLUMN is unsupported in Microsoft Fabric for %s.%s to %s; add a compatible nullable column instead", tableName, columnName, columnType)
}

func (f *MicrosoftFabric) FetchSchema(ctx context.Context) (model.Schema, error) {
	rows, err := f.db.QueryContext(ctx, `SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = @schema AND table_name NOT LIKE @prefix;`,
		sql.Named("schema", f.namespace), sql.Named("prefix", warehouseutils.StagingTablePrefix(provider)+"%"))
	if errors.Is(err, io.EOF) {
		return model.Schema{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("fetching schema: %w", err)
	}
	defer func() { _ = rows.Close() }()

	schema := make(model.Schema)
	for rows.Next() {
		var tableName, columnName, columnType string
		if err := rows.Scan(&tableName, &columnName, &columnType); err != nil {
			return nil, fmt.Errorf("scanning schema: %w", err)
		}
		logicalType, ok := dataTypesMapToRudder[strings.ToLower(columnType)]
		if !ok {
			warehouseutils.WHCounterStat(f.stats, warehouseutils.RudderMissingDatatype, &f.warehouse, warehouseutils.Tag{Name: "datatype", Value: columnType}).Count(1)
			continue
		}
		if schema[tableName] == nil {
			schema[tableName] = make(model.TableSchema)
		}
		schema[tableName][columnName] = logicalType
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating schema: %w", err)
	}
	return schema, nil
}

func (f *MicrosoftFabric) ShouldMerge(tableName string) bool {
	if tableName == warehouseutils.UsersTable {
		return true
	}
	return !f.warehouse.GetPreferAppendSetting() || !f.uploader.CanAppend()
}

func (f *MicrosoftFabric) copyInto(ctx context.Context, tableName, location string, columns []string) error {
	statement := fmt.Sprintf(`COPY INTO %s (%s) FROM %s WITH (FILE_TYPE = 'PARQUET');`,
		qualified(f.namespace, tableName),
		warehouseutils.JoinQuotedIdentifiers(columns, warehouseutils.BracketQuoteIdentifier, ","),
		warehouseutils.SQLStringLiteral(location))
	_, err := f.db.ExecContext(ctx, statement)
	if err != nil {
		return fmt.Errorf("copy_into: loading Parquet into %q: %w", tableName, err)
	}
	return nil
}

func (f *MicrosoftFabric) createStagingTable(ctx context.Context, tableName string) (string, error) {
	stagingTableName := warehouseutils.StagingTableName(provider, tableName, tableNameLimit)
	statement := fmt.Sprintf(`SELECT TOP 0 * INTO %s FROM %s;`, qualified(f.namespace, stagingTableName), qualified(f.namespace, tableName))
	if _, err := f.db.ExecContext(ctx, statement); err != nil {
		return "", fmt.Errorf("creating staging table: %w", err)
	}
	return stagingTableName, nil
}

func (f *MicrosoftFabric) dropStagingTable(ctx context.Context, tableName string) error {
	if tableName == "" {
		return nil
	}
	_, err := f.db.ExecContext(ctx, fmt.Sprintf(`IF OBJECT_ID(%s, 'U') IS NOT NULL DROP TABLE %s;`,
		warehouseutils.UnicodeStringLiteral(qualified(f.namespace, tableName)), qualified(f.namespace, tableName)))
	if err != nil {
		f.logger.Warnn("dropping Microsoft Fabric staging table", logger.NewStringField(logfield.TableName, tableName), logger.NewStringField(logfield.Error, err.Error()))
		return err
	}
	return nil
}

func primaryKey(tableName string) string {
	if key, ok := primaryKeyMap[tableName]; ok {
		return key
	}
	return "id"
}

func partitionKey(tableName string) string {
	if key, ok := partitionKeyMap[tableName]; ok {
		return key
	}
	return "id"
}

func discardsJoinCondition(target string) string {
	if target != warehouseutils.DiscardsTable {
		return ""
	}
	return fmt.Sprintf(" AND target.%s = source.%s AND target.%s = source.%s",
		warehouseutils.BracketQuoteIdentifier("table_name"), warehouseutils.BracketQuoteIdentifier("table_name"),
		warehouseutils.BracketQuoteIdentifier("column_name"), warehouseutils.BracketQuoteIdentifier("column_name"))
}

func mergeStatement(namespace, target, staging string, columns []string, useNewRecord bool) string {
	quotedColumns := warehouseutils.JoinQuotedIdentifiers(columns, warehouseutils.BracketQuoteIdentifier, ",")
	sourceColumns := warehouseutils.JoinWithFormatting(columns, func(_ int, name string) string {
		return "source." + warehouseutils.BracketQuoteIdentifier(name)
	}, ",")
	updates := warehouseutils.JoinWithFormatting(columns, func(_ int, name string) string {
		quoted := warehouseutils.BracketQuoteIdentifier(name)
		if !useNewRecord {
			return "target." + quoted + " = target." + quoted
		}
		return "target." + quoted + " = source." + quoted
	}, ",")
	pk := warehouseutils.BracketQuoteIdentifier(primaryKey(target))
	return fmt.Sprintf(`MERGE INTO %[1]s AS target USING (
SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY %[3]s ORDER BY %[4]s DESC) AS _rudder_staging_row_number FROM %[2]s) AS ranked
WHERE _rudder_staging_row_number = 1
) AS source ON target.%[5]s = source.%[5]s%[6]s
WHEN MATCHED THEN UPDATE SET %[7]s
WHEN NOT MATCHED THEN INSERT (%[8]s) VALUES (%[9]s);`,
		qualified(namespace, target), qualified(namespace, staging),
		warehouseutils.QuoteCommaSeparatedIdentifiers(partitionKey(target), warehouseutils.BracketQuoteIdentifier),
		warehouseutils.BracketQuoteIdentifier("received_at"), pk, discardsJoinCondition(target), updates, quotedColumns, sourceColumns)
}

func matchingRowsStatement(namespace, target, staging string) string {
	pk := warehouseutils.BracketQuoteIdentifier(primaryKey(target))
	return fmt.Sprintf(`SELECT COUNT(*) FROM %s AS target WHERE EXISTS (
SELECT 1 FROM %s AS source WHERE target.%s = source.%s%s
);`, qualified(namespace, target), qualified(namespace, staging), pk, pk, discardsJoinCondition(target))
}

// loadTable loads tableName through a staging table and returns load statistics plus that
// staging table's name, even on error, so the caller can drop it. The staging name is empty
// when there are no load files.
func (f *MicrosoftFabric) loadTable(ctx context.Context, tableName string) (*types.LoadTableStats, string, error) {
	metadata, err := f.uploader.GetLoadFilesMetadata(ctx, warehouseutils.GetLoadFilesOptions{Table: tableName})
	if err != nil {
		return nil, "", fmt.Errorf("getting load files: %w", err)
	}
	if len(metadata) == 0 {
		return &types.LoadTableStats{}, "", nil
	}
	columns := warehouseutils.SortColumnKeysFromColumnMap(f.uploader.GetTableSchemaInUpload(tableName))

	stagingTableName, err := f.createStagingTable(ctx, tableName)
	if err != nil {
		return nil, "", err
	}
	for _, loadFile := range metadata {
		if err := f.copyInto(ctx, stagingTableName, loadFile.Location, columns); err != nil {
			return nil, stagingTableName, err
		}
	}

	if f.ShouldMerge(tableName) {
		var rowsUpdated int64
		if err := f.db.QueryRowContext(ctx, matchingRowsStatement(f.namespace, tableName, stagingTableName)).Scan(&rowsUpdated); err != nil {
			return nil, stagingTableName, fmt.Errorf("counting rows matched by merge for %q: %w", tableName, err)
		}
		statement := mergeStatement(f.namespace, tableName, stagingTableName, columns, f.uploader.ShouldOnDedupUseNewRecord())
		result, err := f.db.ExecContext(ctx, statement)
		if err != nil {
			return nil, stagingTableName, fmt.Errorf("merge: loading %q from staging: %w", tableName, err)
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return nil, stagingTableName, fmt.Errorf("merge: counting affected rows for %q: %w", tableName, err)
		}
		return &types.LoadTableStats{RowsInserted: rowsAffected - rowsUpdated, RowsUpdated: rowsUpdated}, stagingTableName, nil
	}

	quotedColumns := warehouseutils.JoinQuotedIdentifiers(columns, warehouseutils.BracketQuoteIdentifier, ",")
	statement := fmt.Sprintf(`INSERT INTO %s (%s) SELECT %s FROM %s;`, qualified(f.namespace, tableName), quotedColumns, quotedColumns, qualified(f.namespace, stagingTableName))
	result, err := f.db.ExecContext(ctx, statement)
	if err != nil {
		return nil, stagingTableName, fmt.Errorf("inserting append rows from staging: %w", err)
	}
	rowsInserted, err := result.RowsAffected()
	if err != nil {
		return nil, stagingTableName, fmt.Errorf("counting appended rows for %q: %w", tableName, err)
	}
	return &types.LoadTableStats{RowsInserted: rowsInserted}, stagingTableName, nil
}

func (f *MicrosoftFabric) LoadTable(ctx context.Context, tableName string) (*types.LoadTableStats, error) {
	loadTableStats, stagingTableName, err := f.loadTable(ctx, tableName)
	_ = f.dropStagingTable(context.WithoutCancel(ctx), stagingTableName)
	if err != nil {
		return nil, err
	}
	return loadTableStats, nil
}

func (f *MicrosoftFabric) LoadUserTables(ctx context.Context) map[string]error {
	_, identifiesStaging, err := f.loadTable(ctx, warehouseutils.IdentifiesTable)
	defer func() { _ = f.dropStagingTable(context.WithoutCancel(ctx), identifiesStaging) }()
	if err != nil {
		return map[string]error{warehouseutils.IdentifiesTable: fmt.Errorf("loading identifies table: %w", err)}
	}
	if len(f.uploader.GetTableSchemaInUpload(warehouseutils.UsersTable)) == 0 {
		return map[string]error{warehouseutils.IdentifiesTable: nil}
	}
	usersResult := func(err error) map[string]error {
		return map[string]error{warehouseutils.IdentifiesTable: nil, warehouseutils.UsersTable: err}
	}
	if identifiesStaging == "" {
		return usersResult(errors.New("loading users: no identifies load files"))
	}

	userSchema := f.uploader.GetTableSchemaInWarehouse(warehouseutils.UsersTable)
	identifySchema := f.uploader.GetTableSchemaInUpload(warehouseutils.IdentifiesTable)
	columns := slices.DeleteFunc(slices.Sorted(maps.Keys(userSchema)), func(column string) bool { return column == "id" })

	unionTable := warehouseutils.StagingTableName(provider, "users_identifies_union", tableNameLimit)
	latestTable := warehouseutils.StagingTableName(provider, warehouseutils.UsersTable, tableNameLimit)
	defer func() { _ = f.dropStagingTable(context.WithoutCancel(ctx), latestTable) }()
	defer func() { _ = f.dropStagingTable(context.WithoutCancel(ctx), unionTable) }()

	userColumns := warehouseutils.JoinQuotedIdentifiers(columns, warehouseutils.BracketQuoteIdentifier, ",")
	identifyColumns := make([]string, 0, len(columns))
	for _, column := range columns {
		if _, ok := identifySchema[column]; ok {
			identifyColumns = append(identifyColumns, warehouseutils.BracketQuoteIdentifier(column))
		} else {
			identifyColumns = append(identifyColumns, "NULL AS "+warehouseutils.BracketQuoteIdentifier(column))
		}
	}
	unionStatement := fmt.Sprintf(`SELECT * INTO %[1]s FROM (
SELECT %[2]s AS %[2]s, %[3]s FROM %[4]s WHERE %[2]s IN (SELECT %[5]s FROM %[6]s WHERE %[5]s IS NOT NULL)
UNION ALL
SELECT %[5]s AS %[2]s, %[7]s FROM %[6]s WHERE %[5]s IS NOT NULL
) AS users_identifies;`,
		qualified(f.namespace, unionTable), warehouseutils.BracketQuoteIdentifier("id"), userColumns,
		qualified(f.namespace, warehouseutils.UsersTable), warehouseutils.BracketQuoteIdentifier("user_id"), qualified(f.namespace, identifiesStaging), strings.Join(identifyColumns, ","))
	if _, err := f.db.ExecContext(ctx, unionStatement); err != nil {
		return usersResult(fmt.Errorf("creating users union staging table: %w", err))
	}

	latestColumns := make([]string, 0, len(columns)+1)
	latestColumns = append(latestColumns, warehouseutils.BracketQuoteIdentifier("id"))
	for _, column := range columns {
		latestColumns = append(latestColumns, fmt.Sprintf(
			`FIRST_VALUE(%[1]s) IGNORE NULLS OVER (PARTITION BY %[2]s ORDER BY %[3]s DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS %[1]s`,
			warehouseutils.BracketQuoteIdentifier(column),
			warehouseutils.BracketQuoteIdentifier("id"),
			warehouseutils.BracketQuoteIdentifier("received_at")))
	}
	latestStatement := fmt.Sprintf(`SELECT DISTINCT %s INTO %s FROM %s;`,
		strings.Join(latestColumns, ","), qualified(f.namespace, latestTable), qualified(f.namespace, unionTable))
	if _, err := f.db.ExecContext(ctx, latestStatement); err != nil {
		return usersResult(fmt.Errorf("creating latest users staging table: %w", err))
	}

	allColumns := append([]string{"id"}, columns...)
	if _, err := f.db.ExecContext(ctx, mergeStatement(f.namespace, warehouseutils.UsersTable, latestTable, allColumns, true)); err != nil {
		return usersResult(fmt.Errorf("merge: loading users latest traits: %w", err))
	}
	return usersResult(nil)
}

func (f *MicrosoftFabric) TestLoadTable(ctx context.Context, location, tableName string, payload map[string]any, loadFileFormat string) error {
	if loadFileFormat != warehouseutils.LoadFileTypeParquet {
		return fmt.Errorf("copy_into: Microsoft Fabric supports only Parquet load files")
	}
	return f.copyInto(ctx, tableName, location, slices.Sorted(maps.Keys(payload)))
}

func (f *MicrosoftFabric) TestFetchSchema(ctx context.Context) error {
	_, err := f.FetchSchema(ctx)
	return err
}

func (f *MicrosoftFabric) dropDanglingStagingTables(ctx context.Context) error {
	rows, err := f.db.QueryContext(ctx, `SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = @schema AND table_name LIKE @prefix;`,
		sql.Named("schema", f.namespace), sql.Named("prefix", warehouseutils.StagingTablePrefix(provider)+"%"))
	if err != nil {
		return fmt.Errorf("querying for dangling staging tables: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var stagingTableNames []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return fmt.Errorf("scanning dangling staging tables: %w", err)
		}
		stagingTableNames = append(stagingTableNames, tableName)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating dangling staging tables: %w", err)
	}
	for _, tableName := range stagingTableNames {
		if err := f.dropStagingTable(ctx, tableName); err != nil {
			return fmt.Errorf("dropping dangling staging table %q.%q: %w", f.namespace, tableName, err)
		}
	}
	return nil
}

func (f *MicrosoftFabric) Cleanup(ctx context.Context) {
	if f.db != nil {
		if err := f.dropDanglingStagingTables(context.WithoutCancel(ctx)); err != nil {
			f.logger.Warnn("dropping dangling Microsoft Fabric staging tables", logger.NewStringField(logfield.Error, err.Error()))
		}
		_ = f.db.Close()
	}
}

func (*MicrosoftFabric) IsEmpty(context.Context, model.Warehouse) (bool, error) { return false, nil }
func (*MicrosoftFabric) LoadIdentityMergeRulesTable(context.Context) error      { return nil }
func (*MicrosoftFabric) LoadIdentityMappingsTable(context.Context) error        { return nil }
func (*MicrosoftFabric) DownloadIdentityRules(context.Context, *misc.GZipWriter) error {
	return nil
}

func (*MicrosoftFabric) DeleteBy(context.Context, []string, warehouseutils.DeleteByParams) error {
	return fmt.Errorf(warehouseutils.NotImplementedErrorCode)
}
func (f *MicrosoftFabric) SetConnectionTimeout(timeout time.Duration) { f.connectTimeout = timeout }
func (*MicrosoftFabric) ErrorMappings() []model.JobError              { return errorsMappings }
