package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	sqlmw "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/service/loadfiles/downloader"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	errNotImplemented         = errors.New(warehouseutils.NotImplementedErrorCode)
	errInvalidPartitionType   = errors.New("invalid partition type")
	errAppendingCACertificate = errors.New("appending ca certificate to pool")
)

type Clickhouse struct {
	DB *sqlmw.DB

	Namespace          string
	ObjectStorage      string
	Warehouse          model.Warehouse
	Uploader           warehouseutils.Uploader
	connectTimeout     time.Duration
	LoadFileDownloader downloader.Downloader

	// TemporaryS3Cred mints the short-lived credentials the copy engine hands
	// to the s3 table function. New points it at the shared helper, which
	// reaches AWS; a test can point it somewhere closer.
	TemporaryS3Cred func(*backendconfig.DestinationT) (string, string, string, error)

	conf   *config.Config
	logger logger.Logger
	stats  stats.Stats

	config struct {
		queryDebugLogs              bool
		commitEvery                 int
		maxRetriesPerBlock          int
		poolSize                    int
		connMaxIdleTime             time.Duration
		connMaxLifetime             time.Duration
		readTimeout                 time.Duration
		compress                    bool
		disableNullable             bool
		numWorkersDownloadLoadFiles int
		s3EngineEnabledWorkspaceIDs []string
		s3CopySettings              func(string) []string
		slowQueryThreshold          time.Duration
		randomLoadDelay             func(string) time.Duration
		disableLoadTableStats       func(string) bool
	}
}

// Settings are read from Warehouse.clickhouse.v2.* first and Warehouse.clickhouse.*
// second, so config deployed against the v2 namespace keeps winning while the
// unprefixed name becomes canonical. Once deployments have moved, the v2 keys
// can be dropped.
func New(conf *config.Config, log logger.Logger, stat stats.Stats) *Clickhouse {
	ch := &Clickhouse{}

	ch.conf = conf
	ch.logger = log.Child("integrations").Child("clickhouse")
	ch.stats = stat
	ch.TemporaryS3Cred = warehouseutils.GetTemporaryS3Cred

	ch.config.queryDebugLogs = conf.GetBoolVar(false, "Warehouse.clickhouse.v2.queryDebugLogs", "Warehouse.clickhouse.queryDebugLogs")
	// commitEvery is the number of rows between commits, which is what bounds
	// how much of an upload is held client-side at once.
	// Floored at 1: at zero or below, the block loop never runs a single row, so
	// insertBlock returns nothing, the caller never sees a short block, and the
	// load spins forever committing empty batches. One is enough to rule that
	// out, and keeping the floor there leaves small values usable in tests.
	ch.config.commitEvery = max(conf.GetIntVar(1000000, 1, "Warehouse.clickhouse.v2.commitEvery", "Warehouse.clickhouse.commitEvery"), 1)
	// The number of times one block may be sent again, not a count of blocked
	// retries: a block that failed on a connection the pool handed over dead is
	// worth repeating, anything the server rejected is not. Zero disables them.
	ch.config.maxRetriesPerBlock = conf.GetIntVar(3, 1, "Warehouse.clickhouse.v2.maxRetriesPerBlock", "Warehouse.clickhouse.maxRetriesPerBlock")
	ch.config.poolSize = conf.GetIntVar(100, 1, "Warehouse.clickhouse.v2.poolSize", "Warehouse.clickhouse.poolSize")
	// Every block takes a connection out of the pool, so a connection the
	// server closed while it sat idle has to be retired before a block picks
	// it up.
	ch.config.connMaxIdleTime = conf.GetDurationVar(5, time.Minute, "Warehouse.clickhouse.v2.connMaxIdleTime", "Warehouse.clickhouse.connMaxIdleTime")
	ch.config.connMaxLifetime = conf.GetDurationVar(30, time.Minute, "Warehouse.clickhouse.v2.connMaxLifetime", "Warehouse.clickhouse.connMaxLifetime")
	ch.config.readTimeout = conf.GetDurationVar(300, time.Second, "Warehouse.clickhouse.v2.readTimeout", "Warehouse.clickhouse.readTimeout")
	ch.config.compress = conf.GetBoolVar(false, "Warehouse.clickhouse.v2.compress", "Warehouse.clickhouse.compress")
	ch.config.disableNullable = conf.GetBoolVar(false, "Warehouse.clickhouse.v2.disableNullable", "Warehouse.clickhouse.disableNullable")
	ch.config.numWorkersDownloadLoadFiles = conf.GetIntVar(8, 1, "Warehouse.clickhouse.v2.numWorkersDownloadLoadFiles", "Warehouse.clickhouse.numWorkersDownloadLoadFiles")
	ch.config.s3EngineEnabledWorkspaceIDs = conf.GetStringSliceVar(nil, "Warehouse.clickhouse.v2.s3EngineEnabledWorkspaceIDs", "Warehouse.clickhouse.s3EngineEnabledWorkspaceIDs")
	// s3CopySettings are the SETTINGS the copy statement carries on top of the
	// two it always needs. The copy reads a whole folder of gzipped CSV in one
	// INSERT ... SELECT, so its peak memory follows the parse and insert
	// parallelism rather than anything the server batches client-side, and a
	// large enough folder can exhaust the ClickHouse instance. Every one is
	// unset by default, so a deployment that configures none of them sends the
	// statement it sent before; -1 rather than 0 marks unset because 0 is a
	// value min_insert_block_size_rows is meant to take.
	ch.config.s3CopySettings = func(workspaceID string) []string {
		keys := func(name string) []string {
			return []string{
				fmt.Sprintf("Warehouse.clickhouse.v2.%s.s3Copy.%s", workspaceID, name),
				fmt.Sprintf("Warehouse.clickhouse.%s.s3Copy.%s", workspaceID, name),
				"Warehouse.clickhouse.v2.s3Copy." + name,
				"Warehouse.clickhouse.s3Copy." + name,
			}
		}
		var settings []string
		// What each one does, in ClickHouse's own words:
		//   max_threads                   https://clickhouse.com/docs/operations/settings/settings#max_threads
		//   max_parsing_threads           https://clickhouse.com/docs/operations/settings/settings#max_parsing_threads
		//   max_insert_threads            https://clickhouse.com/docs/operations/settings/settings#max_insert_threads
		//   max_memory_usage              https://clickhouse.com/docs/operations/settings/query-complexity#max_memory_usage
		//   min_insert_block_size_bytes   https://clickhouse.com/docs/operations/settings/settings#min_insert_block_size_bytes
		//   min_insert_block_size_rows    https://clickhouse.com/docs/operations/settings/settings#min_insert_block_size_rows
		// Sizing the last two against the thread count is ClickHouse's own
		// guidance for S3 inserts, min_insert_block_size_bytes being roughly
		// peak memory over three times max_insert_threads:
		// https://clickhouse.com/docs/integrations/s3/performance
		for _, s := range []struct{ conf, clickhouse string }{
			{"maxThreads", "max_threads"},
			{"maxParsingThreads", "max_parsing_threads"},
			{"maxInsertThreads", "max_insert_threads"},
			{"maxMemoryUsage", "max_memory_usage"},
			{"minInsertBlockSizeBytes", "min_insert_block_size_bytes"},
			{"minInsertBlockSizeRows", "min_insert_block_size_rows"},
		} {
			if v := conf.GetInt64Var(-1, 1, keys(s.conf)...); v >= 0 {
				settings = append(settings, fmt.Sprintf("%s = %d", s.clickhouse, v))
			}
		}
		// Parallel CSV parsing keeps a buffer per parsing thread, which is what
		// tips a folder of many files over the instance limit. Turning it off
		// is the blunt form of maxParsingThreads above, which bounds the
		// threads instead of giving the parallelism up altogether:
		// https://clickhouse.com/docs/operations/settings/formats#input_format_parallel_parsing
		if conf.GetBoolVar(false, keys("disableParallelParsing")...) {
			settings = append(settings, "input_format_parallel_parsing = 0")
		}
		return settings
	}
	ch.config.slowQueryThreshold = conf.GetDurationVar(5, time.Minute, "Warehouse.clickhouse.v2.slowQueryThreshold", "Warehouse.clickhouse.slowQueryThreshold")
	ch.config.disableLoadTableStats = func(workspaceID string) bool {
		return conf.GetBoolVar(
			false,
			fmt.Sprintf("Warehouse.clickhouse.v2.%s.disableLoadTableStats", workspaceID),
			fmt.Sprintf("Warehouse.clickhouse.%s.disableLoadTableStats", workspaceID),
			"Warehouse.clickhouse.v2.disableLoadTableStats", "Warehouse.clickhouse.disableLoadTableStats",
		)
	}
	ch.config.randomLoadDelay = func(workspaceID string) time.Duration {
		maxDelay := conf.GetDurationVar(
			0,
			time.Second,
			fmt.Sprintf("Warehouse.clickhouse.v2.%s.maxLoadDelay", workspaceID),
			fmt.Sprintf("Warehouse.clickhouse.%s.maxLoadDelay", workspaceID),
			"Warehouse.clickhouse.v2.maxLoadDelay", "Warehouse.clickhouse.maxLoadDelay",
		)
		return time.Duration(float64(maxDelay) * (1 - rand.Float64()))
	}

	return ch
}

func (ch *Clickhouse) Setup(_ context.Context, warehouse model.Warehouse, uploader warehouseutils.Uploader) (err error) {
	ch.Warehouse = warehouse
	ch.Namespace = warehouse.Namespace
	ch.Uploader = uploader
	ch.ObjectStorage = warehouseutils.ObjectStorageType(warehouseutils.CLICKHOUSE, warehouse.Destination.Config, ch.Uploader.UseRudderStorage())
	ch.LoadFileDownloader = downloader.NewDownloader(&warehouse, uploader, ch.config.numWorkersDownloadLoadFiles)

	if ch.DB, err = ch.connect(true); err != nil {
		return fmt.Errorf("connecting: %w", err)
	}
	return nil
}

func (ch *Clickhouse) Cleanup(_ context.Context) {
	if ch.DB != nil {
		_ = ch.DB.Close()
	}
}

func (ch *Clickhouse) Connect(_ context.Context, warehouse model.Warehouse) (client.Client, error) {
	ch.Warehouse = warehouse
	ch.Namespace = warehouse.Namespace
	ch.ObjectStorage = warehouseutils.ObjectStorageType(
		warehouseutils.CLICKHOUSE,
		warehouse.Destination.Config,
		misc.IsConfiguredToUseRudderObjectStorage(ch.Warehouse.Destination.Config),
	)

	db, err := ch.connect(true)
	if err != nil {
		return client.Client{}, fmt.Errorf("connecting: %w", err)
	}

	return client.Client{Type: client.SQLClient, SQL: db.DB}, nil
}

// TestConnection is used destination connection tester to test the clickhouse connection
func (ch *Clickhouse) TestConnection(ctx context.Context, _ model.Warehouse) error {
	err := ch.DB.PingContext(ctx)
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("connection timeout: %w", err)
	}
	if err != nil {
		return fmt.Errorf("pinging: %w", err)
	}

	return nil
}

func (ch *Clickhouse) CreateSchema(ctx context.Context) error {
	if !ch.Uploader.IsWarehouseSchemaEmpty() {
		return nil
	}

	if schemaExists, err := ch.schemaExists(ctx, ch.Namespace); err != nil {
		return fmt.Errorf("checking if database %s exists: %w", ch.Namespace, err)
	} else if schemaExists {
		return nil
	}

	db, err := ch.connect(false)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	ch.logger.Infon("Creating schema",
		logger.NewStringField("clusterClause", ch.clusterClause()),
	)

	query := fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %s %s`, warehouseutils.ClickHouseQuoteIdentifier(ch.Namespace), ch.clusterClause())
	if _, err = db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("creating database: %w", err)
	}
	return nil
}

func (ch *Clickhouse) schemaExists(ctx context.Context, schemaName string) (exists bool, err error) {
	var count int64
	sqlStatement := "SELECT count(*) FROM system.databases WHERE name = ?"
	err = ch.DB.QueryRowContext(ctx, sqlStatement, schemaName).Scan(&count)
	// ignore err if no results for query
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	exists = count > 0
	return exists, err
}

func (ch *Clickhouse) clusterClause() string {
	if cluster := ch.Warehouse.GetStringDestinationConfig(ch.conf, model.ClusterSetting); len(strings.TrimSpace(cluster)) > 0 {
		return fmt.Sprintf(`ON CLUSTER %s`, warehouseutils.ClickHouseQuoteIdentifier(cluster))
	}
	return ""
}

func (ch *Clickhouse) CreateTable(ctx context.Context, tableName string, columns model.TableSchema) (err error) {
	sortKeyFields := []string{"received_at", "id"}
	if tableName == warehouseutils.DiscardsTable {
		sortKeyFields = []string{"received_at"}
	}
	if strings.HasPrefix(tableName, warehouseutils.CTStagingTablePrefix) {
		sortKeyFields = []string{"id"}
	}
	var sqlStatement string
	if tableName == warehouseutils.UsersTable {
		return ch.createUsersTable(ctx, tableName, columns)
	}
	clusterClause := ""
	engine := "ReplacingMergeTree"
	engineOptions := ""
	cluster := ch.Warehouse.GetStringDestinationConfig(ch.conf, model.ClusterSetting)
	if len(strings.TrimSpace(cluster)) > 0 {
		clusterClause = fmt.Sprintf(`ON CLUSTER %s`, warehouseutils.ClickHouseQuoteIdentifier(cluster))
		engine = fmt.Sprintf(`%s%s`, "Replicated", engine)
		engineOptions = fmt.Sprintf(`'/clickhouse/{cluster}/tables/%s/{database}/{table}', '{replica}'`, uuid.New().String())
	}
	var orderByClause string
	if len(sortKeyFields) > 0 {
		orderByClause = fmt.Sprintf(`ORDER BY %s`, getSortKeyTuple(sortKeyFields))
	}

	var partitionByClause string
	if _, ok := columns[partitionField]; ok {
		partitionByClause, err = ch.partitionByClause()
		if err != nil {
			return fmt.Errorf("getting partition by clause: %w", err)
		}
	}

	sqlStatement = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s %s ( %v ) ENGINE = %s(%s) %s %s`, warehouseutils.QuoteQualifiedIdentifier(warehouseutils.ClickHouseQuoteIdentifier, ch.Namespace, tableName), clusterClause, ch.ColumnsWithDataTypes(tableName, columns, sortKeyFields), engine, engineOptions, orderByClause, partitionByClause)

	ch.logger.Infon("CH: Creating table in clickhouse for ch",
		logger.NewStringField(logfield.DestinationID, ch.Warehouse.Destination.ID),
		logger.NewStringField(logfield.Query, sqlStatement),
	)
	_, err = ch.DB.ExecContext(ctx, sqlStatement)
	return err
}

/*
createUsersTable creates a user's table with engine AggregatingMergeTree,
this lets us choose aggregation logic before merging records with same user id.
current behaviour is to replace user  properties with the latest non-null values
*/
func (ch *Clickhouse) createUsersTable(ctx context.Context, name string, columns model.TableSchema) (err error) {
	sortKeyFields := []string{"id"}
	notNullableColumns := []string{"received_at", "id"}
	clusterClause := ""
	engine := "AggregatingMergeTree"
	engineOptions := ""
	cluster := ch.Warehouse.GetStringDestinationConfig(ch.conf, model.ClusterSetting)
	if len(strings.TrimSpace(cluster)) > 0 {
		clusterClause = fmt.Sprintf(`ON CLUSTER %s`, warehouseutils.ClickHouseQuoteIdentifier(cluster))
		engine = fmt.Sprintf(`%s%s`, "Replicated", engine)
		engineOptions = fmt.Sprintf(`'/clickhouse/{cluster}/tables/%s/{database}/{table}', '{replica}'`, uuid.New().String())
	}
	partitionByClause, err := ch.partitionByClause()
	if err != nil {
		return fmt.Errorf("getting partition by clause: %w", err)
	}

	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s %s ( %v )  ENGINE = %s(%s) ORDER BY %s %s`, warehouseutils.QuoteQualifiedIdentifier(warehouseutils.ClickHouseQuoteIdentifier, ch.Namespace, name), clusterClause, ch.ColumnsWithDataTypes(name, columns, notNullableColumns), engine, engineOptions, getSortKeyTuple(sortKeyFields), partitionByClause)
	ch.logger.Infon("CH: Creating table in clickhouse for ch",
		logger.NewStringField(logfield.DestinationID, ch.Warehouse.Destination.ID),
		logger.NewStringField(logfield.Query, sqlStatement),
	)
	_, err = ch.DB.ExecContext(ctx, sqlStatement)
	return err
}

// ColumnsWithDataTypes creates columns and its datatype into sql format for creating table
func (ch *Clickhouse) ColumnsWithDataTypes(tableName string, columns model.TableSchema, notNullableColumns []string) string {
	columnsWithDataTypes := lo.Map(lo.Keys(columns), func(columnName string, _ int) string {
		dataType := columns[columnName]
		codec := ch.getClickHouseCodecForColumnType(dataType, tableName)
		columnType := ch.getClickHouseColumnTypeForSpecificTable(tableName, columnName, rudderDataTypesMapToClickHouse[dataType], slices.Contains(notNullableColumns, columnName))
		return fmt.Sprintf(`%s %s %s`, warehouseutils.ClickHouseQuoteIdentifier(columnName), columnType, codec)
	})
	return strings.Join(columnsWithDataTypes, ",")
}

func (ch *Clickhouse) getClickHouseCodecForColumnType(columnType, tableName string) string {
	if columnType == model.DateTimeDataType {
		if ch.config.disableNullable && (tableName != warehouseutils.IdentifiesTable && tableName != warehouseutils.UsersTable) {
			return "Codec(DoubleDelta, LZ4)"
		}
	}
	return ""
}

func (ch *Clickhouse) getClickHouseColumnTypeForSpecificTable(tableName, columnName, columnType string, notNullableKey bool) string {
	if notNullableKey || (tableName != warehouseutils.IdentifiesTable && ch.config.disableNullable) {
		return getClickhouseColumnTypeForSpecificColumn(columnName, columnType, false)
	}
	// Nullable is not disabled for users and identity table
	if tableName == warehouseutils.UsersTable {
		return fmt.Sprintf(`SimpleAggregateFunction(anyLast, %s)`, getClickhouseColumnTypeForSpecificColumn(columnName, columnType, true))
	}
	return getClickhouseColumnTypeForSpecificColumn(columnName, columnType, true)
}

func (ch *Clickhouse) partitionByClause() (string, error) {
	partitionExpr, err := ch.partitionExpr()
	if err != nil {
		return "", fmt.Errorf("getting partition expr: %w", err)
	}
	return fmt.Sprintf(`PARTITION BY %s`, partitionExpr), nil
}

func (ch *Clickhouse) partitionExpr() (string, error) {
	partitionType := ch.Warehouse.GetStringDestinationConfig(ch.conf, model.PartitionTypeSetting)
	switch partitionType {
	case "", "day":
		return fmt.Sprintf(`toDate(%s)`, partitionField), nil
	case "week":
		return fmt.Sprintf(`toStartOfWeek(%s)`, partitionField), nil
	case "month":
		return fmt.Sprintf(`toStartOfMonth(%s)`, partitionField), nil
	case "quarter":
		return fmt.Sprintf(`toStartOfQuarter(%s)`, partitionField), nil
	default:
		ch.logger.Warnn("CH: Invalid partition type for clickhouse destination",
			logger.NewStringField("partitionType", partitionType),
			logger.NewStringField(logfield.DestinationID, ch.Warehouse.Destination.ID),
		)
		return "", fmt.Errorf("%w: %s", errInvalidPartitionType, partitionType)
	}
}

func (ch *Clickhouse) DropTable(ctx context.Context, tableName string) (err error) {
	sqlStatement := fmt.Sprintf(`DROP TABLE %s %s `, warehouseutils.QuoteQualifiedIdentifier(warehouseutils.ClickHouseQuoteIdentifier, ch.Warehouse.Namespace, tableName), ch.clusterClause())
	_, err = ch.DB.ExecContext(ctx, sqlStatement)
	return err
}

func (ch *Clickhouse) AddColumns(ctx context.Context, tableName string, columnsInfo []warehouseutils.ColumnInfo) (err error) {
	var (
		query        string
		queryBuilder strings.Builder
	)

	fmt.Fprintf(&queryBuilder, `
		ALTER TABLE
		  %s %s`,
		warehouseutils.QuoteQualifiedIdentifier(warehouseutils.ClickHouseQuoteIdentifier, ch.Namespace, tableName),
		ch.clusterClause())

	for _, columnInfo := range columnsInfo {
		columnType := ch.getClickHouseColumnTypeForSpecificTable(
			tableName,
			columnInfo.Name,
			rudderDataTypesMapToClickHouse[columnInfo.Type],
			false,
		)
		fmt.Fprintf(&queryBuilder, ` ADD COLUMN IF NOT EXISTS %s %s,`, warehouseutils.ClickHouseQuoteIdentifier(columnInfo.Name), columnType)
	}

	query = strings.TrimSuffix(queryBuilder.String(), ",")
	query += ";"

	ch.logger.Infon("CH: Adding columns for destinationID with query",
		logger.NewStringField(logfield.DestinationID, ch.Warehouse.Destination.ID),
		logger.NewStringField(logfield.TableName, tableName),
		logger.NewStringField(logfield.Query, query),
	)
	_, err = ch.DB.ExecContext(ctx, query)
	return err
}

func (*Clickhouse) AlterColumn(_ context.Context, _, _, _ string) (model.AlterTableResponse, error) {
	return model.AlterTableResponse{}, nil
}

// FetchSchema queries clickhouse and returns the schema associated with provided namespace
func (ch *Clickhouse) FetchSchema(ctx context.Context) (model.Schema, error) {
	schema := make(model.Schema)

	sqlStatement := `
		SELECT
		  table,
		  name,
		  type
		FROM
		  system.columns
		WHERE
		  database = ?
	`

	rows, err := ch.DB.QueryContext(ctx, sqlStatement, ch.Namespace)
	if errors.Is(err, sql.ErrNoRows) {
		return schema, nil
	}
	if err != nil {
		if isUnknownDatabase(err) {
			return schema, nil
		}
		return nil, fmt.Errorf("fetching schema: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var tableName, columnName, columnType string

		if err := rows.Scan(&tableName, &columnName, &columnType); err != nil {
			return nil, fmt.Errorf("scanning schema row: %w", err)
		}

		if _, ok := schema[tableName]; !ok {
			schema[tableName] = make(model.TableSchema)
		}
		if datatype, ok := clickhouseDataTypesMapToRudder[columnType]; ok {
			schema[tableName][columnName] = datatype
		} else {
			warehouseutils.WHCounterStat(ch.stats, warehouseutils.RudderMissingDatatype, &ch.Warehouse, warehouseutils.Tag{Name: "datatype", Value: columnType}).Count(1)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating schema rows: %w", err)
	}

	return schema, nil
}

func (ch *Clickhouse) TestFetchSchema(ctx context.Context) error {
	_, err := ch.FetchSchema(ctx)
	return err
}

func (*Clickhouse) DeleteBy(context.Context, []string, warehouseutils.DeleteByParams) error {
	return errNotImplemented
}

func (*Clickhouse) LoadIdentityMergeRulesTable(_ context.Context) error {
	return nil
}

func (*Clickhouse) LoadIdentityMappingsTable(_ context.Context) error {
	return nil
}

func (*Clickhouse) DownloadIdentityRules(context.Context, *misc.GZipWriter) error {
	return nil
}

func (*Clickhouse) IsEmpty(_ context.Context, _ model.Warehouse) (bool, error) {
	return false, nil
}

func (ch *Clickhouse) SetConnectionTimeout(timeout time.Duration) {
	ch.connectTimeout = timeout
}

func (*Clickhouse) ErrorMappings() []model.JobError {
	return errorsMappings
}
