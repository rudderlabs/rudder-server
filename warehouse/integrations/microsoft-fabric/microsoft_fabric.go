package microsoftfabric

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/microsoft/go-mssqldb/azuread"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/microsoft-fabric/auth"
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
	rudderDataTypes = map[string]string{
		"boolean":  "bit",
		"int":      "bigint",
		"float":    "decimal(28,10)",
		"datetime": "datetime2(6)",
		"string":   "varchar(max)",
		"json":     "varchar(max)",
	}
	fabricDataTypes = map[string]string{
		"bit":              "boolean",
		"smallint":         "int",
		"int":              "int",
		"integer":          "int",
		"bigint":           "int",
		"tinyint":          "int",
		"decimal":          "float",
		"numeric":          "float",
		"float":            "float",
		"real":             "float",
		"date":             "datetime",
		"time":             "datetime",
		"datetime2":        "datetime",
		"datetimeoffset":   "datetime",
		"char":             "string",
		"nchar":            "string",
		"varchar":          "string",
		"nvarchar":         "string",
		"text":             "string",
		"ntext":            "string",
		"varbinary":        "string",
		"image":            "string",
		"uniqueidentifier": "string",
		"money":            "float",
	}
	primaryKeys = map[string][]string{
		warehouseutils.UsersTable:      {"id"},
		warehouseutils.IdentifiesTable: {"id"},
		warehouseutils.DiscardsTable:   {"row_id", "column_name", "table_name"},
	}
	errorMappings = []model.JobError{
		{Type: model.PermissionError, Format: regexp.MustCompile(`(?i)(login failed|not authorized|permission|principal|forbidden|denied|HTTP 401|HTTP 403)`)},
		{Type: model.ResourceNotFoundError, Format: regexp.MustCompile(`(?i)(not found|does not exist|cannot find|HTTP 404)`)},
		{Type: model.AlterColumnError, Format: regexp.MustCompile(`(?i)(automatic ALTER COLUMN is not supported|unsupported Microsoft Fabric data type)`)},
		{Type: model.ConcurrentQueriesError, Format: regexp.MustCompile(`(?i)(deadlock|lock request time out|concurrent)`)},
		{Type: model.ColumnCountError, Format: regexp.MustCompile(`(?i)(maximum.*columns|too many columns|1024 columns)`)},
		{Type: model.ColumnSizeError, Format: regexp.MustCompile(`(?i)(truncated|too long|exceed.*16.?MB)`)},
	}
)

type Fabric struct {
	db             *sqlmw.DB
	warehouse      model.Warehouse
	uploader       warehouseutils.Uploader
	namespace      string
	connectTimeout time.Duration
	conf           *config.Config
	logger         logger.Logger
	stats          stats.Stats
	bootstrap      func(context.Context, map[string]any) error
}

func New(conf *config.Config, log logger.Logger, statsFactory stats.Stats) *Fabric {
	f := &Fabric{
		conf:   conf,
		logger: log.Child("integrations").Child("microsoft-fabric"),
		stats:  statsFactory,
	}
	f.bootstrap = f.bootstrapCredential
	return f
}

func (f *Fabric) bootstrapCredential(ctx context.Context, destinationConfig map[string]any) error {
	credential, err := auth.New(configString(destinationConfig, "tenantId"), configString(destinationConfig, "clientId"), configString(destinationConfig, "clientSecret"), nil)
	if err != nil {
		return err
	}
	return credential.Bootstrap(ctx)
}

func configString(destinationConfig map[string]any, key string) string {
	value, _ := destinationConfig[key].(string)
	return value
}

func (f *Fabric) connect() (*sqlmw.DB, error) {
	destinationConfig := f.warehouse.Destination.Config
	port, err := strconv.Atoi(configString(destinationConfig, "port"))
	if err != nil {
		return nil, fmt.Errorf("invalid port: %w", err)
	}
	query := url.Values{}
	query.Set("database", configString(destinationConfig, "database"))
	query.Set("encrypt", "true")
	query.Set("TrustServerCertificate", "false")
	query.Set("fedauth", "ActiveDirectoryServicePrincipal")
	if f.connectTimeout > 0 {
		query.Set("dial timeout", strconv.FormatInt(int64(f.connectTimeout/time.Second), 10))
	}
	connectionURL := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(configString(destinationConfig, "clientId")+"@"+configString(destinationConfig, "tenantId"), configString(destinationConfig, "clientSecret")),
		Host:     net.JoinHostPort(configString(destinationConfig, "host"), strconv.Itoa(port)),
		RawQuery: query.Encode(),
	}
	connector, err := azuread.NewConnector(connectionURL.String())
	if err != nil {
		// Connector parsing errors can include the input DSN, whose user info
		// contains the client secret. Keep that value out of propagated errors.
		return nil, errors.New("creating Microsoft Fabric SQL connector")
	}
	db := sql.OpenDB(connector)
	return sqlmw.New(
		db,
		sqlmw.WithStats(f.stats),
		sqlmw.WithLogger(f.logger),
		sqlmw.WithKeyAndValues([]any{
			logfield.SourceID, f.warehouse.Source.ID,
			logfield.DestinationID, f.warehouse.Destination.ID,
			logfield.DestinationType, provider,
			logfield.WorkspaceID, f.warehouse.WorkspaceID,
			logfield.Namespace, f.namespace,
		}),
		sqlmw.WithQueryTimeout(f.connectTimeout),
		sqlmw.WithSlowQueryThreshold(f.conf.GetDurationVar(5, time.Minute, "Warehouse.microsoft_fabric.slowQueryThreshold")),
		sqlmw.WithSecretsRegex(map[string]string{`https://[^']+`: "https://***"}),
	), nil
}

func (f *Fabric) Setup(ctx context.Context, warehouse model.Warehouse, uploader warehouseutils.Uploader) error {
	f.warehouse = warehouse
	f.namespace = warehouse.Namespace
	f.uploader = uploader
	if err := f.bootstrap(ctx, warehouse.Destination.Config); err != nil {
		return fmt.Errorf("bootstrapping Fabric service principal: %w", err)
	}
	db, err := f.connect()
	if err != nil {
		return err
	}
	f.db = db
	return nil
}

func (f *Fabric) Connect(ctx context.Context, warehouse model.Warehouse) (client.Client, error) {
	f.warehouse = warehouse
	f.namespace = warehouse.Namespace
	if err := f.bootstrap(ctx, warehouse.Destination.Config); err != nil {
		return client.Client{}, fmt.Errorf("bootstrapping Fabric service principal: %w", err)
	}
	db, err := f.connect()
	if err != nil {
		return client.Client{}, err
	}
	return client.Client{Type: client.SQLClient, SQL: db.DB}, nil
}

func (f *Fabric) TestConnection(ctx context.Context, _ model.Warehouse) error {
	if err := f.db.PingContext(ctx); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("connection timeout: %w", err)
		}
		return fmt.Errorf("pinging Microsoft Fabric: %w", err)
	}
	return nil
}

func (*Fabric) IsEmpty(context.Context, model.Warehouse) (bool, error) { return false, nil }

func (f *Fabric) CreateSchema(ctx context.Context) error {
	statement := fmt.Sprintf(`IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = %s) EXEC(%s);`,
		warehouseutils.UnicodeStringLiteral(f.namespace),
		warehouseutils.SQLStringLiteral("CREATE SCHEMA "+warehouseutils.BracketQuoteIdentifier(f.namespace)),
	)
	_, err := f.db.ExecContext(ctx, statement)
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

func columnsWithTypes(columns model.TableSchema) (string, error) {
	names := warehouseutils.SortColumnKeysFromColumnMap(columns)
	definitions := make([]string, 0, len(names))
	for _, name := range names {
		dataType, ok := rudderDataTypes[columns[name]]
		if !ok {
			return "", fmt.Errorf("unsupported Microsoft Fabric data type %q", columns[name])
		}
		definitions = append(definitions, fmt.Sprintf("%s %s", warehouseutils.BracketQuoteIdentifier(name), dataType))
	}
	return strings.Join(definitions, ","), nil
}

func (f *Fabric) CreateTable(ctx context.Context, tableName string, columns model.TableSchema) error {
	definitions, err := columnsWithTypes(columns)
	if err != nil {
		return err
	}
	qualified := f.qualified(tableName)
	statement := fmt.Sprintf(`IF OBJECT_ID(%s, 'U') IS NULL CREATE TABLE %s (%s);`, warehouseutils.UnicodeStringLiteral(qualified), qualified, definitions)
	_, err = f.db.ExecContext(ctx, statement)
	return err
}

func (f *Fabric) AddColumns(ctx context.Context, tableName string, columns []warehouseutils.ColumnInfo) error {
	for _, column := range columns {
		dataType, ok := rudderDataTypes[column.Type]
		if !ok {
			return fmt.Errorf("unsupported Microsoft Fabric data type %q", column.Type)
		}
		statement := fmt.Sprintf(`IF COL_LENGTH(%s, %s) IS NULL ALTER TABLE %s ADD %s %s NULL;`,
			warehouseutils.UnicodeStringLiteral(f.namespace+"."+tableName),
			warehouseutils.UnicodeStringLiteral(column.Name),
			f.qualified(tableName),
			warehouseutils.BracketQuoteIdentifier(column.Name),
			dataType,
		)
		if _, err := f.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("adding nullable column %q: %w", column.Name, err)
		}
	}
	return nil
}

func (*Fabric) AlterColumn(context.Context, string, string, string) (model.AlterTableResponse, error) {
	return model.AlterTableResponse{}, errors.New("automatic ALTER COLUMN is not supported by Microsoft Fabric; add a compatible nullable column or resolve the source schema conflict")
}

func (f *Fabric) DropTable(ctx context.Context, tableName string) error {
	_, err := f.db.ExecContext(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s;`, f.qualified(tableName)))
	return err
}

func (f *Fabric) DeleteBy(ctx context.Context, tableNames []string, params warehouseutils.DeleteByParams) error {
	if !f.conf.GetBoolVar(false, "Warehouse.microsoft_fabric.enableDeleteByJobs") {
		return nil
	}
	for _, tableName := range tableNames {
		statement := fmt.Sprintf(`DELETE FROM %s WHERE %s <> @jobrunid AND %s <> @taskrunid AND %s = @sourceid AND %s < @starttime`,
			f.qualified(tableName),
			warehouseutils.BracketQuoteIdentifier("context_sources_job_run_id"),
			warehouseutils.BracketQuoteIdentifier("context_sources_task_run_id"),
			warehouseutils.BracketQuoteIdentifier("context_source_id"),
			warehouseutils.BracketQuoteIdentifier("received_at"),
		)
		if _, err := f.db.ExecContext(ctx, statement,
			sql.Named("jobrunid", params.JobRunId), sql.Named("taskrunid", params.TaskRunId),
			sql.Named("sourceid", params.SourceId), sql.Named("starttime", params.StartTime),
		); err != nil {
			return err
		}
	}
	return nil
}

func (f *Fabric) FetchSchema(ctx context.Context) (model.Schema, error) {
	rows, err := f.db.QueryContext(ctx, `SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = @schema AND table_name NOT LIKE @prefix`,
		sql.Named("schema", f.namespace), sql.Named("prefix", warehouseutils.StagingTablePrefix(provider)+"%"))
	if errors.Is(err, io.EOF) {
		return model.Schema{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("fetching Microsoft Fabric schema: %w", err)
	}
	defer func() { _ = rows.Close() }()
	schema := model.Schema{}
	for rows.Next() {
		var tableName, columnName, dataType string
		if err := rows.Scan(&tableName, &columnName, &dataType); err != nil {
			return nil, err
		}
		rudderType, ok := fabricDataTypes[strings.ToLower(dataType)]
		if !ok {
			warehouseutils.WHCounterStat(f.stats, warehouseutils.RudderMissingDatatype, &f.warehouse, warehouseutils.Tag{Name: "datatype", Value: dataType}).Count(1)
			continue
		}
		if schema[tableName] == nil {
			schema[tableName] = model.TableSchema{}
		}
		schema[tableName][columnName] = rudderType
	}
	return schema, rows.Err()
}

func (f *Fabric) ShouldMerge(tableName string) bool {
	if tableName == warehouseutils.UsersTable {
		return true
	}
	return !f.warehouse.GetPreferAppendSetting() || !f.uploader.CanAppend()
}

func (f *Fabric) LoadTable(ctx context.Context, tableName string) (*types.LoadTableStats, error) {
	stats, _, err := f.loadTable(ctx, tableName, false)
	return stats, err
}

func (f *Fabric) loadTable(ctx context.Context, tableName string, keepStaging bool) (*types.LoadTableStats, string, error) {
	columns := warehouseutils.SortColumnKeysFromColumnMap(f.uploader.GetTableSchemaInUpload(tableName))
	loadFiles, err := f.uploader.GetLoadFilesMetadata(ctx, warehouseutils.GetLoadFilesOptions{Table: tableName})
	if err != nil {
		return nil, "", fmt.Errorf("getting Parquet load files: %w", err)
	}
	if !f.ShouldMerge(tableName) && !keepStaging {
		rows, err := f.copyInto(ctx, f.qualified(tableName), columns, loadFiles)
		return &types.LoadTableStats{RowsInserted: rows}, "", err
	}
	stagingTable := warehouseutils.StagingTableName(provider, tableName, tableNameLimit)
	if _, err := f.db.ExecContext(ctx, fmt.Sprintf(`SELECT TOP 0 * INTO %s FROM %s;`, f.qualified(stagingTable), f.qualified(tableName))); err != nil {
		return nil, "", fmt.Errorf("creating staging table: %w", err)
	}
	if !keepStaging {
		defer f.dropStagingTable(ctx, stagingTable)
	}
	if _, err := f.copyInto(ctx, f.qualified(stagingTable), columns, loadFiles); err != nil {
		f.dropStagingTable(ctx, stagingTable)
		return nil, stagingTable, err
	}
	result, err := f.merge(ctx, tableName, stagingTable, columns, primaryKeys[tableName])
	if err != nil {
		f.dropStagingTable(ctx, stagingTable)
		return nil, stagingTable, err
	}
	rows, _ := result.RowsAffected()
	return &types.LoadTableStats{RowsUpdated: rows}, stagingTable, nil
}

func (f *Fabric) copyInto(ctx context.Context, qualifiedTable string, columns []string, loadFiles []warehouseutils.LoadFile) (int64, error) {
	quotedColumns := quoteColumns(columns)
	var rows int64
	for _, loadFile := range loadFiles {
		statement := copyIntoStatement(qualifiedTable, quotedColumns, loadFile.Location)
		result, err := f.db.ExecContext(ctx, statement)
		if err != nil {
			return rows, fmt.Errorf("COPY INTO table: %w", err)
		}
		count, _ := result.RowsAffected()
		rows += count
	}
	return rows, nil
}

func copyIntoStatement(qualifiedTable string, quotedColumns []string, location string) string {
	return fmt.Sprintf(`COPY INTO %s (%s) FROM %s WITH (FILE_TYPE = 'PARQUET');`, qualifiedTable, strings.Join(quotedColumns, ","), warehouseutils.SQLStringLiteral(location))
}

func (f *Fabric) merge(ctx context.Context, tableName, stagingTable string, columns, keys []string) (sql.Result, error) {
	if len(keys) == 0 {
		keys = []string{"id"}
	}
	statement := mergeStatement(f.qualified(tableName), f.qualified(stagingTable), columns, keys)
	result, err := f.db.ExecContext(ctx, statement)
	if err != nil {
		return nil, fmt.Errorf("merging staging table into target: %w", err)
	}
	return result, nil
}

func mergeStatement(target, staging string, columns, keys []string) string {
	quotedColumns := quoteColumns(columns)
	partitionColumns := quoteColumns(keys)
	orderColumn := "uuid_ts"
	if !contains(columns, orderColumn) {
		orderColumn = "received_at"
	}
	if !contains(columns, orderColumn) {
		orderColumn = keys[0]
	}
	conditions := make([]string, 0, len(keys))
	keySet := map[string]struct{}{}
	for _, key := range keys {
		quoted := warehouseutils.BracketQuoteIdentifier(key)
		conditions = append(conditions, "target."+quoted+" = source."+quoted)
		keySet[key] = struct{}{}
	}
	updates := make([]string, 0, len(columns))
	values := make([]string, 0, len(columns))
	for _, column := range columns {
		quoted := warehouseutils.BracketQuoteIdentifier(column)
		values = append(values, "source."+quoted)
		if _, isKey := keySet[column]; !isKey {
			updates = append(updates, "target."+quoted+" = source."+quoted)
		}
	}
	matched := ""
	if len(updates) > 0 {
		matched = " WHEN MATCHED THEN UPDATE SET " + strings.Join(updates, ",")
	}
	return fmt.Sprintf(`MERGE %s AS target USING (SELECT %s FROM (SELECT %s, ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s DESC) AS [rudder_row_number] FROM %s) AS ranked WHERE [rudder_row_number] = 1) AS source ON %s%s WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s);`,
		target, strings.Join(quotedColumns, ","), strings.Join(quotedColumns, ","), strings.Join(partitionColumns, ","), warehouseutils.BracketQuoteIdentifier(orderColumn), staging, strings.Join(conditions, " AND "), matched, strings.Join(quotedColumns, ","), strings.Join(values, ","))
}

func quoteColumns(columns []string) []string {
	quoted := make([]string, len(columns))
	for index, column := range columns {
		quoted[index] = warehouseutils.BracketQuoteIdentifier(column)
	}
	return quoted
}

func contains(values []string, expected string) bool {
	return slices.Contains(values, expected)
}

func (f *Fabric) LoadUserTables(ctx context.Context) map[string]error {
	errorsByTable := map[string]error{warehouseutils.IdentifiesTable: nil}
	_, identifiesStaging, err := f.loadTable(ctx, warehouseutils.IdentifiesTable, true)
	if err != nil {
		errorsByTable[warehouseutils.IdentifiesTable] = err
		return errorsByTable
	}
	defer f.dropStagingTable(ctx, identifiesStaging)
	usersSchema := f.uploader.GetTableSchemaInUpload(warehouseutils.UsersTable)
	if len(usersSchema) == 0 {
		return errorsByTable
	}
	errorsByTable[warehouseutils.UsersTable] = f.mergeUsers(ctx, identifiesStaging)
	return errorsByTable
}

func (f *Fabric) mergeUsers(ctx context.Context, identifiesStaging string) error {
	usersSchema := f.uploader.GetTableSchemaInWarehouse(warehouseutils.UsersTable)
	identifiesSchema := f.uploader.GetTableSchemaInUpload(warehouseutils.IdentifiesTable)
	columns := warehouseutils.SortColumnKeysFromColumnMap(usersSchema)
	traitColumns := make([]string, 0, len(columns))
	identifyColumns := make([]string, 0, len(columns))
	for _, column := range columns {
		if column == "id" {
			continue
		}
		quoted := warehouseutils.BracketQuoteIdentifier(column)
		traitColumns = append(traitColumns, quoted)
		if _, ok := identifiesSchema[column]; ok {
			identifyColumns = append(identifyColumns, quoted)
		} else {
			identifyColumns = append(identifyColumns, "NULL AS "+quoted)
		}
	}
	unionTable := warehouseutils.StagingTableName(provider, "users_identifies_union", tableNameLimit)
	usersStaging := warehouseutils.StagingTableName(provider, warehouseutils.UsersTable, tableNameLimit)
	defer f.dropStagingTable(ctx, unionTable)
	defer f.dropStagingTable(ctx, usersStaging)
	statement := fmt.Sprintf(`SELECT * INTO %s FROM (SELECT %s,%s FROM %s WHERE %s IN (SELECT %s FROM %s WHERE %s IS NOT NULL) UNION ALL SELECT %s,%s FROM %s WHERE %s IS NOT NULL) AS combined;`,
		f.qualified(unionTable), warehouseutils.BracketQuoteIdentifier("id"), strings.Join(traitColumns, ","), f.qualified(warehouseutils.UsersTable), warehouseutils.BracketQuoteIdentifier("id"), warehouseutils.BracketQuoteIdentifier("user_id"), f.qualified(identifiesStaging), warehouseutils.BracketQuoteIdentifier("user_id"), warehouseutils.BracketQuoteIdentifier("user_id"), strings.Join(identifyColumns, ","), f.qualified(identifiesStaging), warehouseutils.BracketQuoteIdentifier("user_id"))
	if _, err := f.db.ExecContext(ctx, statement); err != nil {
		return fmt.Errorf("creating users union staging table: %w", err)
	}
	latestTraits := make([]string, 0, len(traitColumns))
	for _, column := range traitColumns {
		latestTraits = append(latestTraits, fmt.Sprintf(`(SELECT TOP 1 candidate.%[1]s FROM %[2]s AS candidate WHERE candidate.%[3]s = source.%[3]s AND candidate.%[1]s IS NOT NULL ORDER BY candidate.%[4]s DESC) AS %[1]s`,
			column, f.qualified(unionTable), warehouseutils.BracketQuoteIdentifier("id"), warehouseutils.BracketQuoteIdentifier("received_at")))
	}
	statement = fmt.Sprintf(`SELECT DISTINCT source.%s,%s INTO %s FROM %s AS source;`, warehouseutils.BracketQuoteIdentifier("id"), strings.Join(latestTraits, ","), f.qualified(usersStaging), f.qualified(unionTable))
	if _, err := f.db.ExecContext(ctx, statement); err != nil {
		return fmt.Errorf("creating latest-traits staging table: %w", err)
	}
	_, err := f.merge(ctx, warehouseutils.UsersTable, usersStaging, columns, []string{"id"})
	return err
}

func (f *Fabric) dropStagingTable(ctx context.Context, tableName string) {
	if tableName == "" {
		return
	}
	if _, err := f.db.ExecContext(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s;`, f.qualified(tableName))); err != nil {
		f.logger.Warnn("dropping Microsoft Fabric staging table", logger.NewStringField(logfield.TableName, tableName), obskit.Error(err))
	}
}

func (f *Fabric) qualified(tableName string) string {
	return warehouseutils.QuoteQualifiedIdentifier(warehouseutils.BracketQuoteIdentifier, f.namespace, tableName)
}

func (f *Fabric) Cleanup(_ context.Context) {
	if f.db == nil {
		return
	}
	_ = f.db.Close()
}

func (f *Fabric) TestLoadTable(ctx context.Context, location, tableName string, payload map[string]any, loadFileFormat string) error {
	if loadFileFormat != warehouseutils.GetLoadFileFormat(warehouseutils.LoadFileTypeParquet) {
		return fmt.Errorf("validation for Microsoft Fabric requires Parquet, got %q", loadFileFormat)
	}
	columns := make([]string, 0, len(payload))
	for column := range payload {
		columns = append(columns, column)
	}
	sort.Strings(columns)
	_, err := f.copyInto(ctx, f.qualified(tableName), columns, []warehouseutils.LoadFile{{Location: location}})
	return err
}

func (f *Fabric) TestFetchSchema(ctx context.Context) error {
	_, err := f.FetchSchema(ctx)
	return err
}

func (f *Fabric) SetConnectionTimeout(timeout time.Duration)                  { f.connectTimeout = timeout }
func (*Fabric) ErrorMappings() []model.JobError                               { return errorMappings }
func (*Fabric) LoadIdentityMergeRulesTable(context.Context) error             { return nil }
func (*Fabric) LoadIdentityMappingsTable(context.Context) error               { return nil }
func (*Fabric) DownloadIdentityRules(context.Context, *misc.GZipWriter) error { return nil }
