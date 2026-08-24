package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"path"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	sqlmw "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/types"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/service/loadfiles/downloader"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	errNotImplemented            = errors.New(warehouseutils.NotImplementedErrorCode)
	errInvalidPartitionType      = errors.New("invalid partition type")
	errObjectStorageNotSupported = errors.New("objectStorage not supported for loading using S3 engine")
	errAppendingCACertificate    = errors.New("appending ca certificate to pool")
	errCSVColumnsMismatch        = errors.New("csv columns mismatch")
)

type ClickhouseV2 struct {
	DB *sqlmw.DB

	Namespace          string
	ObjectStorage      string
	Warehouse          model.Warehouse
	Uploader           warehouseutils.Uploader
	connectTimeout     time.Duration
	LoadFileDownloader downloader.Downloader

	conf   *config.Config
	logger logger.Logger
	stats  stats.Stats

	config struct {
		queryDebugLogs              bool
		commitEvery                 int
		poolSize                    int
		readTimeout                 time.Duration
		compress                    bool
		disableNullable             bool
		numWorkersDownloadLoadFiles int
		s3EngineEnabledWorkspaceIDs []string
		slowQueryThreshold          time.Duration
		randomLoadDelay             func(string) time.Duration
		disableLoadTableStats       func(string) bool
	}
}

func NewV2(conf *config.Config, log logger.Logger, stat stats.Stats) *ClickhouseV2 {
	ch := &ClickhouseV2{}

	ch.conf = conf
	ch.logger = log.Child("integrations").Child("clickhouse").Child("v2")
	ch.stats = stat

	ch.config.queryDebugLogs = conf.GetBoolVar(false, "Warehouse.clickhouse.queryDebugLogs")
	// commitEvery is the number of rows between commits, which is what bounds
	// how much of an upload is held client-side at once.
	// Floored at 1: at zero or below, the block loop never runs a single row, so
	// insertBlock returns nothing, the caller never sees a short block, and the
	// load spins forever committing empty batches. One is enough to rule that
	// out, and keeping the floor there leaves small values usable in tests.
	ch.config.commitEvery = max(conf.GetIntVar(1000000, 1, "Warehouse.clickhouse.commitEvery"), 1)
	ch.config.poolSize = conf.GetIntVar(100, 1, "Warehouse.clickhouse.poolSize")
	ch.config.readTimeout = conf.GetDurationVar(300, time.Second, "Warehouse.clickhouse.readTimeout")
	ch.config.compress = conf.GetBoolVar(false, "Warehouse.clickhouse.compress")
	ch.config.disableNullable = conf.GetBoolVar(false, "Warehouse.clickhouse.disableNullable")
	ch.config.numWorkersDownloadLoadFiles = conf.GetIntVar(8, 1, "Warehouse.clickhouse.numWorkersDownloadLoadFiles")
	ch.config.s3EngineEnabledWorkspaceIDs = conf.GetStringSliceVar(nil, "Warehouse.clickhouse.s3EngineEnabledWorkspaceIDs")
	ch.config.slowQueryThreshold = conf.GetDurationVar(5, time.Minute, "Warehouse.clickhouse.slowQueryThreshold")
	ch.config.disableLoadTableStats = func(workspaceID string) bool {
		return conf.GetBoolVar(
			false,
			fmt.Sprintf("Warehouse.clickhouse.%s.disableLoadTableStats", workspaceID),
			"Warehouse.clickhouse.disableLoadTableStats",
		)
	}
	ch.config.randomLoadDelay = func(workspaceID string) time.Duration {
		maxDelay := conf.GetDurationVar(
			0,
			time.Second,
			fmt.Sprintf("Warehouse.clickhouse.%s.maxLoadDelay", workspaceID),
			"Warehouse.clickhouse.maxLoadDelay",
		)
		return time.Duration(float64(maxDelay) * (1 - rand.Float64()))
	}

	return ch
}

func (ch *ClickhouseV2) Setup(_ context.Context, warehouse model.Warehouse, uploader warehouseutils.Uploader) (err error) {
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

func (ch *ClickhouseV2) Cleanup(_ context.Context) {
	if ch.DB != nil {
		_ = ch.DB.Close()
	}
}

func (ch *ClickhouseV2) Connect(_ context.Context, warehouse model.Warehouse) (client.Client, error) {
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
func (ch *ClickhouseV2) TestConnection(ctx context.Context, _ model.Warehouse) error {
	err := ch.DB.PingContext(ctx)
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("connection timeout: %w", err)
	}
	if err != nil {
		return fmt.Errorf("pinging: %w", err)
	}

	return nil
}

func (ch *ClickhouseV2) CreateSchema(ctx context.Context) error {
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

	query := fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %q %s`, ch.Namespace, ch.clusterClause())
	if _, err = db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("creating database: %w", err)
	}
	return nil
}

func (ch *ClickhouseV2) schemaExists(ctx context.Context, schemaName string) (exists bool, err error) {
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

func (ch *ClickhouseV2) clusterClause() string {
	if cluster := ch.Warehouse.GetStringDestinationConfig(ch.conf, model.ClusterSetting); len(strings.TrimSpace(cluster)) > 0 {
		return fmt.Sprintf(`ON CLUSTER %q`, cluster)
	}
	return ""
}

func (ch *ClickhouseV2) CreateTable(ctx context.Context, tableName string, columns model.TableSchema) (err error) {
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
		clusterClause = fmt.Sprintf(`ON CLUSTER %q`, cluster)
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

	sqlStatement = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %q.%q %s ( %v ) ENGINE = %s(%s) %s %s`, ch.Namespace, tableName, clusterClause, ch.ColumnsWithDataTypes(tableName, columns, sortKeyFields), engine, engineOptions, orderByClause, partitionByClause)

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
func (ch *ClickhouseV2) createUsersTable(ctx context.Context, name string, columns model.TableSchema) (err error) {
	sortKeyFields := []string{"id"}
	notNullableColumns := []string{"received_at", "id"}
	clusterClause := ""
	engine := "AggregatingMergeTree"
	engineOptions := ""
	cluster := ch.Warehouse.GetStringDestinationConfig(ch.conf, model.ClusterSetting)
	if len(strings.TrimSpace(cluster)) > 0 {
		clusterClause = fmt.Sprintf(`ON CLUSTER %q`, cluster)
		engine = fmt.Sprintf(`%s%s`, "Replicated", engine)
		engineOptions = fmt.Sprintf(`'/clickhouse/{cluster}/tables/%s/{database}/{table}', '{replica}'`, uuid.New().String())
	}
	partitionByClause, err := ch.partitionByClause()
	if err != nil {
		return fmt.Errorf("getting partition by clause: %w", err)
	}

	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %q.%q %s ( %v )  ENGINE = %s(%s) ORDER BY %s %s`, ch.Namespace, name, clusterClause, ch.ColumnsWithDataTypes(name, columns, notNullableColumns), engine, engineOptions, getSortKeyTuple(sortKeyFields), partitionByClause)
	ch.logger.Infon("CH: Creating table in clickhouse for ch",
		logger.NewStringField(logfield.DestinationID, ch.Warehouse.Destination.ID),
		logger.NewStringField(logfield.Query, sqlStatement),
	)
	_, err = ch.DB.ExecContext(ctx, sqlStatement)
	return err
}

// ColumnsWithDataTypes creates columns and its datatype into sql format for creating table
func (ch *ClickhouseV2) ColumnsWithDataTypes(tableName string, columns model.TableSchema, notNullableColumns []string) string {
	columnsWithDataTypes := lo.Map(lo.Keys(columns), func(columnName string, _ int) string {
		dataType := columns[columnName]
		codec := ch.getClickHouseCodecForColumnType(dataType, tableName)
		columnType := ch.getClickHouseColumnTypeForSpecificTable(tableName, columnName, rudderDataTypesMapToClickHouse[dataType], slices.Contains(notNullableColumns, columnName))
		return fmt.Sprintf(`%q %s %s`, columnName, columnType, codec)
	})
	return strings.Join(columnsWithDataTypes, ",")
}

func (ch *ClickhouseV2) getClickHouseCodecForColumnType(columnType, tableName string) string {
	if columnType == model.DateTimeDataType {
		if ch.config.disableNullable && (tableName != warehouseutils.IdentifiesTable && tableName != warehouseutils.UsersTable) {
			return "Codec(DoubleDelta, LZ4)"
		}
	}
	return ""
}

func (ch *ClickhouseV2) getClickHouseColumnTypeForSpecificTable(tableName, columnName, columnType string, notNullableKey bool) string {
	if notNullableKey || (tableName != warehouseutils.IdentifiesTable && ch.config.disableNullable) {
		return getClickhouseColumnTypeForSpecificColumn(columnName, columnType, false)
	}
	// Nullable is not disabled for users and identity table
	if tableName == warehouseutils.UsersTable {
		return fmt.Sprintf(`SimpleAggregateFunction(anyLast, %s)`, getClickhouseColumnTypeForSpecificColumn(columnName, columnType, true))
	}
	return getClickhouseColumnTypeForSpecificColumn(columnName, columnType, true)
}

func (ch *ClickhouseV2) partitionByClause() (string, error) {
	partitionExpr, err := ch.partitionExpr()
	if err != nil {
		return "", fmt.Errorf("getting partition expr: %w", err)
	}
	return fmt.Sprintf(`PARTITION BY %s`, partitionExpr), nil
}

func (ch *ClickhouseV2) partitionExpr() (string, error) {
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

func (ch *ClickhouseV2) DropTable(ctx context.Context, tableName string) (err error) {
	sqlStatement := fmt.Sprintf(`DROP TABLE %q.%q %s `, ch.Warehouse.Namespace, tableName, ch.clusterClause())
	_, err = ch.DB.ExecContext(ctx, sqlStatement)
	return err
}

func (ch *ClickhouseV2) AddColumns(ctx context.Context, tableName string, columnsInfo []warehouseutils.ColumnInfo) (err error) {
	var (
		query        string
		queryBuilder strings.Builder
	)

	queryBuilder.WriteString(fmt.Sprintf(`
		ALTER TABLE
		  %q.%q %s`,
		ch.Namespace,
		tableName,
		ch.clusterClause(),
	))

	for _, columnInfo := range columnsInfo {
		columnType := ch.getClickHouseColumnTypeForSpecificTable(
			tableName,
			columnInfo.Name,
			rudderDataTypesMapToClickHouse[columnInfo.Type],
			false,
		)
		queryBuilder.WriteString(fmt.Sprintf(` ADD COLUMN IF NOT EXISTS %q %s,`, columnInfo.Name, columnType))
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

func (*ClickhouseV2) AlterColumn(_ context.Context, _, _, _ string) (model.AlterTableResponse, error) {
	return model.AlterTableResponse{}, nil
}

// FetchSchema queries clickhouse and returns the schema associated with provided namespace
func (ch *ClickhouseV2) FetchSchema(ctx context.Context) (model.Schema, error) {
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

func (ch *ClickhouseV2) LoadTable(ctx context.Context, tableName string) (*types.LoadTableStats, error) {
	var (
		preLoadTableCount int64
		err               error
	)
	if !ch.config.disableLoadTableStats(ch.Warehouse.WorkspaceID) {
		preLoadTableCount, err = ch.totalCountIntable(ctx, tableName)
		if err != nil {
			return nil, fmt.Errorf("counting rows before load: %w", err)
		}
	}

	err = ch.loadTable(ctx, tableName, ch.Uploader.GetTableSchemaInUpload(tableName))
	if err != nil {
		return nil, fmt.Errorf("loading table: %w", err)
	}

	var postLoadTableCount int64
	if !ch.config.disableLoadTableStats(ch.Warehouse.WorkspaceID) {
		postLoadTableCount, err = ch.totalCountIntable(ctx, tableName)
		if err != nil {
			return nil, fmt.Errorf("counting rows after load: %w", err)
		}
	}

	return &types.LoadTableStats{
		RowsInserted: postLoadTableCount - preLoadTableCount,
	}, nil
}

func (ch *ClickhouseV2) totalCountIntable(ctx context.Context, tableName string) (int64, error) {
	var (
		total        int64
		err          error
		sqlStatement string
	)
	sqlStatement = fmt.Sprintf(`
		SELECT count(*) FROM "%[1]s"."%[2]s";
	`,
		ch.Namespace,
		tableName,
	)
	err = ch.DB.QueryRowContext(ctx, sqlStatement).Scan(&total)
	return total, err
}

func (ch *ClickhouseV2) LoadUserTables(ctx context.Context) (errorMap map[string]error) {
	errorMap = map[string]error{warehouseutils.IdentifiesTable: nil}
	err := ch.loadTable(ctx, warehouseutils.IdentifiesTable, ch.Uploader.GetTableSchemaInUpload(warehouseutils.IdentifiesTable))
	if err != nil {
		errorMap[warehouseutils.IdentifiesTable] = err
		return errorMap
	}

	if len(ch.Uploader.GetTableSchemaInUpload(warehouseutils.UsersTable)) == 0 {
		return errorMap
	}
	errorMap[warehouseutils.UsersTable] = nil
	err = ch.loadTable(ctx, warehouseutils.UsersTable, ch.Uploader.GetTableSchemaInUpload(warehouseutils.UsersTable))
	if err != nil {
		errorMap[warehouseutils.UsersTable] = err
		return errorMap
	}
	return errorMap
}

func (ch *ClickhouseV2) loadTable(ctx context.Context, tableName string, tableSchemaInUpload model.TableSchema) (err error) {
	if delay := ch.config.randomLoadDelay(ch.Warehouse.WorkspaceID); delay > 0 {
		if err = misc.SleepCtx(ctx, delay); err != nil {
			return err
		}
	}
	if ch.UseS3CopyEngineForLoading() {
		return ch.loadByCopyCommand(ctx, tableName, tableSchemaInUpload)
	}
	return ch.loadByDownloadingLoadFiles(ctx, tableName, tableSchemaInUpload)
}

func (ch *ClickhouseV2) UseS3CopyEngineForLoading() bool {
	if !slices.Contains(ch.config.s3EngineEnabledWorkspaceIDs, ch.Warehouse.WorkspaceID) {
		return false
	}
	return ch.ObjectStorage == warehouseutils.S3 || ch.ObjectStorage == warehouseutils.MINIO
}

func (ch *ClickhouseV2) loadByCopyCommand(ctx context.Context, tableName string, tableSchemaInUpload model.TableSchema) error {
	log := ch.logger.Withn(
		logger.NewStringField(logfield.SourceID, ch.Warehouse.Source.ID),
		logger.NewStringField(logfield.SourceType, ch.Warehouse.Source.SourceDefinition.Name),
		logger.NewStringField(logfield.DestinationID, ch.Warehouse.Destination.ID),
		logger.NewStringField(logfield.DestinationType, ch.Warehouse.Destination.DestinationDefinition.Name),
		logger.NewStringField(logfield.WorkspaceID, ch.Warehouse.WorkspaceID),
		logger.NewStringField(logfield.Namespace, ch.Namespace),
		logger.NewStringField(logfield.TableName, tableName),
	)
	log.Infon("Starting load by copy command")

	strKeys := warehouseutils.GetColumnsFromTableSchema(tableSchemaInUpload)
	sort.Strings(strKeys)
	sortedColumnNames := strings.Join(strKeys, ",")
	sortedColumnNamesWithDataTypes := warehouseutils.JoinWithFormatting(strKeys, func(idx int, name string) string {
		return fmt.Sprintf(`%s %s`, name, rudderDataTypesMapToClickHouse[tableSchemaInUpload[name]])
	}, ",")

	csvObjectLocation, err := ch.Uploader.GetSampleLoadFileLocation(ctx, tableName)
	if err != nil {
		return fmt.Errorf("getting sample load file location: %w", err)
	}
	loadFolderDir, _ := path.Split(csvObjectLocation)
	loadFolder := loadFolderDir + "*.csv.gz"

	accessKeyID, secretAccessKey, err := ch.credentials()
	if err != nil {
		return fmt.Errorf("getting auth credentials: %w", err)
	}

	sqlStatement := fmt.Sprintf(`
		INSERT INTO %[1]q.%[2]q (
			%[3]s
		)
		SELECT
		  *
		FROM
		  s3(
			'%[4]s',
		  	'%[5]s',
		  	'%[6]s',
			'CSV',
			'%[7]s',
			'gz'
		  )
			settings
				date_time_input_format = 'best_effort',
				input_format_csv_arrays_as_nested_csv = 1;
		`,
		ch.Namespace,                   // 1
		tableName,                      // 2
		sortedColumnNames,              // 3
		loadFolder,                     // 4
		accessKeyID,                    // 5
		secretAccessKey,                // 6
		sortedColumnNamesWithDataTypes, // 7
	)
	_, err = ch.DB.ExecContext(ctx, sqlStatement)
	if err != nil {
		return fmt.Errorf("executing insert query: %w", err)
	}

	log.Infon("Completed load by copy command")
	return nil
}

func (ch *ClickhouseV2) loadByDownloadingLoadFiles(ctx context.Context, tableName string, tableSchemaInUpload model.TableSchema) error {
	log := ch.logger.Withn(
		logger.NewStringField(logfield.SourceID, ch.Warehouse.Source.ID),
		logger.NewStringField(logfield.SourceType, ch.Warehouse.Source.SourceDefinition.Name),
		logger.NewStringField(logfield.DestinationID, ch.Warehouse.Destination.ID),
		logger.NewStringField(logfield.DestinationType, ch.Warehouse.Destination.DestinationDefinition.Name),
		logger.NewStringField(logfield.WorkspaceID, ch.Warehouse.WorkspaceID),
		logger.NewStringField(logfield.Namespace, ch.Namespace),
		logger.NewStringField(logfield.TableName, tableName),
	)
	log.Infon("Starting load by downloading load files")

	fileNames, err := ch.LoadFileDownloader.Download(ctx, tableName)
	if err != nil {
		return fmt.Errorf("downloading load files: %w", err)
	}
	defer func() {
		misc.RemoveFilePaths(fileNames...)
	}()

	if err := ch.loadTableFromFiles(ctx, log, tableName, tableSchemaInUpload, fileNames); err != nil {
		return fmt.Errorf("loading table from files: %w", err)
	}
	log.Infon("Completed load by downloading load files")
	return nil
}

func (ch *ClickhouseV2) credentials() (accessKeyID, secretAccessKey string, err error) {
	if ch.ObjectStorage == warehouseutils.S3 {
		return ch.Warehouse.GetStringDestinationConfig(ch.conf, model.AWSAccessSecretSetting), ch.Warehouse.GetStringDestinationConfig(ch.conf, model.AWSAccessKeySetting), nil
	}
	if ch.ObjectStorage == warehouseutils.MINIO {
		return ch.Warehouse.GetStringDestinationConfig(ch.conf, model.MinioAccessKeyIDSetting), ch.Warehouse.GetStringDestinationConfig(ch.conf, model.MinioSecretAccessKeySetting), nil
	}
	return "", "", errObjectStorageNotSupported
}

func (ch *ClickhouseV2) TestLoadTable(ctx context.Context, _, tableName string, payloadMap map[string]any, _ string) error {
	columns := lo.Keys(payloadMap)
	values := lo.Values(payloadMap)

	sqlStatement := fmt.Sprintf(`INSERT INTO %q.%q (%v) VALUES (%s)`,
		ch.Namespace,
		tableName,
		strings.Join(columns, ","),
		generateArgumentString(len(columns)),
	)
	txn, err := ch.DB.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = txn.Rollback()
		}
	}()

	stmt, err := txn.PrepareContext(ctx, sqlStatement)
	if err != nil {
		return fmt.Errorf("preparing statement: %w", err)
	}
	defer func() {
		_ = stmt.Close()
	}()

	if _, err = stmt.ExecContext(ctx, values...); err != nil {
		return fmt.Errorf("executing statement: %w", err)
	}
	if err = txn.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}
	return nil
}

func (ch *ClickhouseV2) TestFetchSchema(ctx context.Context) error {
	_, err := ch.FetchSchema(ctx)
	return err
}

func (*ClickhouseV2) DeleteBy(context.Context, []string, warehouseutils.DeleteByParams) error {
	return errNotImplemented
}

func (*ClickhouseV2) LoadIdentityMergeRulesTable(_ context.Context) error {
	return nil
}

func (*ClickhouseV2) LoadIdentityMappingsTable(_ context.Context) error {
	return nil
}

func (*ClickhouseV2) DownloadIdentityRules(context.Context, *misc.GZipWriter) error {
	return nil
}

func (*ClickhouseV2) IsEmpty(_ context.Context, _ model.Warehouse) (bool, error) {
	return false, nil
}

func (ch *ClickhouseV2) SetConnectionTimeout(timeout time.Duration) {
	ch.connectTimeout = timeout
}

func (*ClickhouseV2) ErrorMappings() []model.JobError {
	return errorsMappings
}
