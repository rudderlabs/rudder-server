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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/microsoft/go-mssqldb/azuread"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/microsoft-fabric/onelake"
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
		"integer":          model.IntDataType,
		"bigint":           model.IntDataType,
		"tinyint":          model.IntDataType,
		"decimal":          model.FloatDataType,
		"numeric":          model.FloatDataType,
		"float":            model.FloatDataType,
		"real":             model.FloatDataType,
		"money":            model.FloatDataType,
		"date":             model.DateTimeDataType,
		"time":             model.DateTimeDataType,
		"datetime":         model.DateTimeDataType,
		"datetime2":        model.DateTimeDataType,
		"datetimeoffset":   model.DateTimeDataType,
		"char":             model.StringDataType,
		"nchar":            model.StringDataType,
		"varchar":          model.StringDataType,
		"nvarchar":         model.StringDataType,
		"text":             model.StringDataType,
		"ntext":            model.StringDataType,
		"varbinary":        model.StringDataType,
		"image":            model.StringDataType,
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
		{Type: model.PermissionError, Format: regexp.MustCompile(`(?i)(login failed|principal.*not able|permission.*denied|not authorized)`)},
		{Type: model.ResourceNotFoundError, Format: regexp.MustCompile(`(?i)(cannot open server|could not be found|does not exist)`)},
		{Type: model.ConcurrentQueriesError, Format: regexp.MustCompile(`(?i)(deadlock|lock request time out|blocked)`)},
		{Type: model.ColumnCountError, Format: regexp.MustCompile(`(?i)(1024 columns|maximum.*columns)`)},
		{Type: model.ColumnSizeError, Format: regexp.MustCompile(`(?i)(string or binary data would be truncated|16 MB)`)},
		{Type: model.PermissionError, Format: regexp.MustCompile(`(?i)spn_token_bootstrap`)},
		{Type: model.PermissionError, Format: regexp.MustCompile(`(?i)lakehouse_access`)},
		{Type: model.ResourceNotFoundError, Format: regexp.MustCompile(`(?i)lakehouse_not_found`)},
		{Type: model.ResourceNotFoundError, Format: regexp.MustCompile(`(?i)copy_into`)},
		{Type: model.AlterColumnError, Format: regexp.MustCompile(`(?i)schema_evolution`)},
		{Type: model.ConcurrentQueriesError, Format: regexp.MustCompile(`(?i)merge`)},
	}
	defaultBootstrapper = newBootstrapper(nil)
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
	bootstrapper   *bootstrapper

	config struct {
		slowQueryThreshold time.Duration
	}
}

func New(conf *config.Config, log logger.Logger, stat stats.Stats) *MicrosoftFabric {
	fabric := &MicrosoftFabric{
		conf:         conf,
		logger:       log.Child("integrations").Child("microsoft-fabric"),
		stats:        stat,
		bootstrapper: defaultBootstrapper,
	}
	fabric.config.slowQueryThreshold = conf.GetDurationVar(5, time.Minute, "Warehouse.microsoft_fabric.slowQueryThreshold")
	return fabric
}

func stringConfig(values map[string]any, key string) string {
	value, _ := values[key].(string)
	return value
}

func (f *MicrosoftFabric) credentials() (host, port, database, tenantID, clientID, clientSecret, workspaceID string) {
	config := f.warehouse.Destination.Config
	return f.warehouse.GetStringDestinationConfig(f.conf, model.HostSetting),
		f.warehouse.GetStringDestinationConfig(f.conf, model.PortSetting),
		f.warehouse.GetStringDestinationConfig(f.conf, model.DatabaseSetting),
		stringConfig(config, "tenantId"), stringConfig(config, "clientId"), stringConfig(config, "clientSecret"),
		stringConfig(config, "fabricWorkspaceId")
}

func (f *MicrosoftFabric) bootstrap(ctx context.Context) error {
	_, _, _, tenantID, clientID, clientSecret, workspaceID := f.credentials()
	for name, value := range map[string]string{
		"tenantId": tenantID, "clientId": clientID, "clientSecret": clientSecret, "fabricWorkspaceId": workspaceID,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("spn_token_bootstrap: %s is required", name)
		}
	}
	return f.bootstrapper.Bootstrap(ctx, tenantID, clientID, clientSecret, workspaceID)
}

func (f *MicrosoftFabric) connectionDSN() (string, error) {
	host, port, database, tenantID, clientID, clientSecret, _ := f.credentials()
	portNumber, err := strconv.Atoi(port)
	if err != nil {
		return "", fmt.Errorf("invalid port %q: %w", port, err)
	}
	query := url.Values{}
	query.Set("database", database)
	query.Set("fedauth", azuread.ActiveDirectoryServicePrincipal)
	query.Set("encrypt", "true")
	if f.connectTimeout > 0 {
		query.Set("dial timeout", strconv.FormatInt(int64(f.connectTimeout/time.Second), 10))
	}
	return (&url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(clientID+"@"+tenantID, clientSecret),
		Host:     net.JoinHostPort(host, strconv.Itoa(portNumber)),
		RawQuery: query.Encode(),
	}).String(), nil
}

func (f *MicrosoftFabric) connect() (*sqlmw.DB, error) {
	dsn, err := f.connectionDSN()
	if err != nil {
		return nil, err
	}
	connector, err := azuread.NewConnector(dsn)
	if err != nil {
		return nil, fmt.Errorf("creating Entra SQL connector: %w", err)
	}
	db := sql.OpenDB(connector)
	return sqlmw.New(
		db,
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

func columnsWithDataTypes(columns model.TableSchema) (string, error) {
	keys := warehouseutils.SortColumnKeysFromColumnMap(columns)
	definitions := make([]string, 0, len(keys))
	for _, name := range keys {
		dataType, ok := dataTypesMap[columns[name]]
		if !ok {
			return "", fmt.Errorf("schema_evolution: unsupported Rudder type %q for column %q", columns[name], name)
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
	if err := f.bootstrap(ctx); err != nil {
		return err
	}
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
	if err := f.bootstrap(ctx); err != nil {
		return client.Client{}, err
	}
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
		dataType, ok := dataTypesMap[column.Type]
		if !ok {
			return fmt.Errorf("schema_evolution: unsupported Rudder type %q for column %q", column.Type, column.Name)
		}
		statement := fmt.Sprintf(`IF COL_LENGTH(%s, %s) IS NULL ALTER TABLE %s ADD %s %s NULL;`,
			warehouseutils.UnicodeStringLiteral(f.namespace+"."+tableName),
			warehouseutils.UnicodeStringLiteral(column.Name),
			qualified(f.namespace, tableName), warehouseutils.BracketQuoteIdentifier(column.Name), dataType,
		)
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
	if errors.Is(err, io.EOF) || errors.Is(err, sql.ErrNoRows) {
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
	if err := f.validateOneLakeLocation(location); err != nil {
		return err
	}
	if len(columns) == 0 {
		return errors.New("copy_into: target column list is empty")
	}
	statement := fmt.Sprintf(`COPY INTO %s (%s) FROM %s WITH (FILE_TYPE = 'PARQUET');`,
		qualified(f.namespace, tableName),
		warehouseutils.JoinQuotedIdentifiers(columns, warehouseutils.BracketQuoteIdentifier, ","),
		warehouseutils.SQLStringLiteral(location))
	if _, err := f.db.ExecContext(ctx, statement); err != nil {
		return fmt.Errorf("copy_into: loading Parquet into %q: %w", tableName, err)
	}
	return nil
}

func (f *MicrosoftFabric) validateOneLakeLocation(location string) error {
	u, err := url.Parse(location)
	if err != nil {
		return fmt.Errorf("copy_into: invalid OneLake location")
	}
	host, err := onelake.HostFromConfig(f.warehouse.Destination.Config)
	if err != nil {
		return fmt.Errorf("copy_into: invalid OneLake host: %w", err)
	}
	workspaceID := stringConfig(f.warehouse.Destination.Config, "fabricWorkspaceId")
	lakehouseID := stringConfig(f.warehouse.Destination.Config, "lakehouseId")
	expectedPrefix := "/" + workspaceID + "/" + lakehouseID + "/Files/"
	if u.Scheme != "https" || !strings.EqualFold(u.Host, host) || !strings.HasPrefix(u.EscapedPath(), expectedPrefix) {
		return errors.New("copy_into: load file is outside the configured OneLake Lakehouse")
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

func (f *MicrosoftFabric) dropStagingTable(ctx context.Context, tableName string) {
	if tableName == "" || f.db == nil {
		return
	}
	_, err := f.db.ExecContext(ctx, fmt.Sprintf(`IF OBJECT_ID(%s, 'U') IS NOT NULL DROP TABLE %s;`,
		warehouseutils.UnicodeStringLiteral(qualified(f.namespace, tableName)), qualified(f.namespace, tableName)))
	if err != nil {
		f.logger.Warnn("dropping Microsoft Fabric staging table", logger.NewStringField(logfield.TableName, tableName), logger.NewStringField(logfield.Error, err.Error()))
	}
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
	additionalJoin := ""
	if target == warehouseutils.DiscardsTable {
		additionalJoin = fmt.Sprintf(" AND target.%s = source.%s AND target.%s = source.%s",
			warehouseutils.BracketQuoteIdentifier("table_name"), warehouseutils.BracketQuoteIdentifier("table_name"),
			warehouseutils.BracketQuoteIdentifier("column_name"), warehouseutils.BracketQuoteIdentifier("column_name"))
	}
	return fmt.Sprintf(`MERGE INTO %[1]s AS target USING (
SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY %[3]s ORDER BY %[4]s DESC) AS _rudder_staging_row_number FROM %[2]s) AS ranked
WHERE _rudder_staging_row_number = 1
) AS source ON target.%[5]s = source.%[5]s%[6]s
WHEN MATCHED THEN UPDATE SET %[7]s
WHEN NOT MATCHED THEN INSERT (%[8]s) VALUES (%[9]s);`,
		qualified(namespace, target), qualified(namespace, staging),
		warehouseutils.QuoteCommaSeparatedIdentifiers(partitionKey(target), warehouseutils.BracketQuoteIdentifier),
		warehouseutils.BracketQuoteIdentifier("received_at"), pk, additionalJoin, updates, quotedColumns, sourceColumns)
}

// loadTable loads tableName through a staging table and returns that staging table's name,
// even on error, so the caller can drop it. It returns "" when there are no load files.
func (f *MicrosoftFabric) loadTable(ctx context.Context, tableName string) (string, error) {
	metadata, err := f.uploader.GetLoadFilesMetadata(ctx, warehouseutils.GetLoadFilesOptions{Table: tableName})
	if err != nil {
		return "", fmt.Errorf("getting load files: %w", err)
	}
	if len(metadata) == 0 {
		return "", nil
	}
	columns := warehouseutils.SortColumnKeysFromColumnMap(f.uploader.GetTableSchemaInUpload(tableName))

	stagingTableName, err := f.createStagingTable(ctx, tableName)
	if err != nil {
		return "", err
	}
	for _, loadFile := range metadata {
		if err := f.copyInto(ctx, stagingTableName, loadFile.Location, columns); err != nil {
			return stagingTableName, err
		}
	}

	if f.ShouldMerge(tableName) {
		statement := mergeStatement(f.namespace, tableName, stagingTableName, columns, f.uploader.ShouldOnDedupUseNewRecord())
		if _, err := f.db.ExecContext(ctx, statement); err != nil {
			return stagingTableName, fmt.Errorf("merge: loading %q from staging: %w", tableName, err)
		}
	} else {
		quotedColumns := warehouseutils.JoinQuotedIdentifiers(columns, warehouseutils.BracketQuoteIdentifier, ",")
		statement := fmt.Sprintf(`INSERT INTO %s (%s) SELECT %s FROM %s;`, qualified(f.namespace, tableName), quotedColumns, quotedColumns, qualified(f.namespace, stagingTableName))
		if _, err := f.db.ExecContext(ctx, statement); err != nil {
			return stagingTableName, fmt.Errorf("inserting append rows from staging: %w", err)
		}
	}
	return stagingTableName, nil
}

func (f *MicrosoftFabric) LoadTable(ctx context.Context, tableName string) (*types.LoadTableStats, error) {
	stagingTableName, err := f.loadTable(ctx, tableName)
	f.dropStagingTable(ctx, stagingTableName)
	if err != nil {
		return nil, err
	}
	return &types.LoadTableStats{}, nil
}

func (f *MicrosoftFabric) LoadUserTables(ctx context.Context) map[string]error {
	identifiesStaging, err := f.loadTable(ctx, warehouseutils.IdentifiesTable)
	defer f.dropStagingTable(ctx, identifiesStaging)
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
	columns := make([]string, 0, len(userSchema))
	for column := range userSchema {
		if column != "id" {
			columns = append(columns, column)
		}
	}
	sort.Strings(columns)

	unionTable := warehouseutils.StagingTableName(provider, "users_identifies_union", tableNameLimit)
	latestTable := warehouseutils.StagingTableName(provider, warehouseutils.UsersTable, tableNameLimit)
	defer f.dropStagingTable(ctx, latestTable)
	defer f.dropStagingTable(ctx, unionTable)

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
SELECT %[2]s AS %[3]s, %[4]s FROM %[5]s WHERE %[2]s IN (SELECT %[6]s FROM %[7]s WHERE %[6]s IS NOT NULL)
UNION ALL
SELECT %[6]s AS %[3]s, %[8]s FROM %[7]s WHERE %[6]s IS NOT NULL
) AS users_identifies;`,
		qualified(f.namespace, unionTable), warehouseutils.BracketQuoteIdentifier("id"), warehouseutils.BracketQuoteIdentifier("id"), userColumns,
		qualified(f.namespace, warehouseutils.UsersTable), warehouseutils.BracketQuoteIdentifier("user_id"), qualified(f.namespace, identifiesStaging), strings.Join(identifyColumns, ","))
	if _, err := f.db.ExecContext(ctx, unionStatement); err != nil {
		return usersResult(fmt.Errorf("creating users union staging table: %w", err))
	}

	latestColumns := make([]string, 0, len(columns)+1)
	latestColumns = append(latestColumns, "x."+warehouseutils.BracketQuoteIdentifier("id"))
	for _, column := range columns {
		quoted := warehouseutils.BracketQuoteIdentifier(column)
		latestColumns = append(latestColumns, fmt.Sprintf(`(SELECT TOP 1 s.%[1]s FROM %[2]s AS s WHERE s.%[3]s = x.%[3]s AND s.%[1]s IS NOT NULL ORDER BY s.%[4]s DESC) AS %[1]s`,
			quoted, qualified(f.namespace, unionTable), warehouseutils.BracketQuoteIdentifier("id"), warehouseutils.BracketQuoteIdentifier("received_at")))
	}
	latestStatement := fmt.Sprintf(`SELECT DISTINCT %s INTO %s FROM %s AS x;`,
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
	columns := make([]string, 0, len(payload))
	for column := range payload {
		columns = append(columns, column)
	}
	sort.Strings(columns)
	return f.copyInto(ctx, tableName, location, columns)
}

func (f *MicrosoftFabric) TestFetchSchema(ctx context.Context) error {
	_, err := f.FetchSchema(ctx)
	return err
}

func (f *MicrosoftFabric) Cleanup(context.Context) {
	if f.db != nil {
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
