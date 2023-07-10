package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"

	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/service/loadfiles/downloader"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/tunnelling"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const (
	host     = "host"
	dbName   = "database"
	user     = "user"
	password = "password"
	port     = "port"
	sslMode  = "sslMode"
	verifyCA = "verify-ca"
)

const (
	provider       = warehouseutils.POSTGRES
	tableNameLimit = 127
)

var errorsMappings = []model.JobError{
	{
		Type:   model.ResourceNotFoundError,
		Format: regexp.MustCompile(`dial tcp: lookup .*: no such host`),
	},
	{
		Type:   model.PermissionError,
		Format: regexp.MustCompile(`dial tcp .* connect: connection refused`),
	},
	{
		Type:   model.ResourceNotFoundError,
		Format: regexp.MustCompile(`pq: database .* does not exist`),
	},
	{
		Type:   model.ResourceNotFoundError,
		Format: regexp.MustCompile(`pq: the database system is starting up`),
	},
	{
		Type:   model.ResourceNotFoundError,
		Format: regexp.MustCompile(`pq: the database system is shutting down`),
	},
	{
		Type:   model.ResourceNotFoundError,
		Format: regexp.MustCompile(`pq: relation .* does not exist`),
	},
	{
		Type:   model.ResourceNotFoundError,
		Format: regexp.MustCompile(`pq: cannot set transaction read-write mode during recovery`),
	},
	{
		Type:   model.ColumnCountError,
		Format: regexp.MustCompile(`pq: tables can have at most 1600 columns`),
	},
	{
		Type:   model.PermissionError,
		Format: regexp.MustCompile(`pq: password authentication failed for user`),
	},
	{
		Type:   model.PermissionError,
		Format: regexp.MustCompile(`pq: permission denied`),
	},
}

var rudderDataTypesMapToPostgres = map[string]string{
	"int":      "bigint",
	"float":    "numeric",
	"string":   "text",
	"datetime": "timestamptz",
	"boolean":  "boolean",
	"json":     "jsonb",
}

var postgresDataTypesMapToRudder = map[string]string{
	"integer":                  "int",
	"smallint":                 "int",
	"bigint":                   "int",
	"double precision":         "float",
	"numeric":                  "float",
	"real":                     "float",
	"text":                     "string",
	"varchar":                  "string",
	"char":                     "string",
	"timestamptz":              "datetime",
	"timestamp with time zone": "datetime",
	"timestamp":                "datetime",
	"boolean":                  "boolean",
	"jsonb":                    "json",
}

type Postgres struct {
	db                 *sqlmiddleware.DB
	namespace          string
	objectStorage      string
	warehouse          model.Warehouse
	uploader           warehouseutils.Uploader
	connectTimeout     time.Duration
	logger             logger.Logger
	stats              stats.Stats
	loadFileDownloader downloader.Downloader

	config struct {
		enableDeleteByJobs                        bool
		numWorkersDownloadLoadFiles               int
		slowQueryThreshold                        time.Duration
		txnRollbackTimeout                        time.Duration
		skipDedupDestinationIDs                   []string
		skipComputingUserLatestTraits             bool
		skipComputingUserLatestTraitsWorkspaceIDs []string
	}
}

type credentials struct {
	host       string
	database   string
	user       string
	password   string
	port       string
	sslMode    string
	sslDir     string
	tunnelInfo *tunnelling.TunnelInfo
	timeout    time.Duration
}

var primaryKeyMap = map[string]string{
	warehouseutils.UsersTable:      "id",
	warehouseutils.IdentifiesTable: "id",
	warehouseutils.DiscardsTable:   "row_id",
}

var partitionKeyMap = map[string]string{
	warehouseutils.UsersTable:      "id",
	warehouseutils.IdentifiesTable: "id",
	warehouseutils.DiscardsTable:   "row_id, column_name, table_name",
}

func New(conf *config.Config, log logger.Logger, stat stats.Stats) *Postgres {
	pg := &Postgres{}

	pg.logger = log.Child("integrations").Child("postgres")
	pg.stats = stat

	pg.config.enableDeleteByJobs = conf.GetBool("Warehouse.postgres.enableDeleteByJobs", false)
	pg.config.numWorkersDownloadLoadFiles = conf.GetInt("Warehouse.postgres.numWorkersDownloadLoadFiles", 1)
	pg.config.slowQueryThreshold = conf.GetDuration("Warehouse.postgres.slowQueryThreshold", 5, time.Minute)
	pg.config.txnRollbackTimeout = conf.GetDuration("Warehouse.postgres.txnRollbackTimeout", 30, time.Second)
	pg.config.skipDedupDestinationIDs = conf.GetStringSlice("Warehouse.postgres.skipDedupDestinationIDs", nil)
	pg.config.skipComputingUserLatestTraits = conf.GetBool("Warehouse.postgres.skipComputingUserLatestTraits", false)
	pg.config.skipComputingUserLatestTraitsWorkspaceIDs = conf.GetStringSlice("Warehouse.postgres.skipComputingUserLatestTraitsWorkspaceIDs", nil)

	return pg
}

func (pg *Postgres) getNewMiddleWare(db *sql.DB) *sqlmiddleware.DB {
	middleware := sqlmiddleware.New(
		db,
		sqlmiddleware.WithLogger(pg.logger),
		sqlmiddleware.WithKeyAndValues(
			logfield.SourceID, pg.warehouse.Source.ID,
			logfield.SourceType, pg.warehouse.Source.SourceDefinition.Name,
			logfield.DestinationID, pg.warehouse.Destination.ID,
			logfield.DestinationType, pg.warehouse.Destination.DestinationDefinition.Name,
			logfield.WorkspaceID, pg.warehouse.WorkspaceID,
			logfield.Schema, pg.namespace,
		),
		sqlmiddleware.WithSlowQueryThreshold(pg.config.slowQueryThreshold),
		sqlmiddleware.WithQueryTimeout(pg.connectTimeout),
	)
	return middleware
}

func (pg *Postgres) connect() (*sqlmiddleware.DB, error) {
	cred := pg.getConnectionCredentials()
	dsn := url.URL{
		Scheme: "postgres",
		Host:   fmt.Sprintf("%s:%s", cred.host, cred.port),
		User:   url.UserPassword(cred.user, cred.password),
		Path:   cred.database,
	}

	values := url.Values{}
	values.Add("sslmode", cred.sslMode)

	if cred.timeout > 0 {
		values.Add("connect_timeout", fmt.Sprintf("%d", cred.timeout/time.Second))
	}

	if cred.sslMode == verifyCA {
		values.Add("sslrootcert", fmt.Sprintf("%s/server-ca.pem", cred.sslDir))
		values.Add("sslcert", fmt.Sprintf("%s/client-cert.pem", cred.sslDir))
		values.Add("sslkey", fmt.Sprintf("%s/client-key.pem", cred.sslDir))
	}

	dsn.RawQuery = values.Encode()

	var (
		err error
		db  *sql.DB
	)

	if cred.tunnelInfo != nil {

		db, err = tunnelling.SQLConnectThroughTunnel(dsn.String(), cred.tunnelInfo.Config)
		if err != nil {
			return nil, fmt.Errorf("opening connection to postgres through tunnelling: %w", err)
		}
		return pg.getNewMiddleWare(db), nil
	}

	if db, err = sql.Open("postgres", dsn.String()); err != nil {
		return nil, fmt.Errorf("opening connection to postgres: %w", err)
	}

	return pg.getNewMiddleWare(db), nil
}

func (pg *Postgres) getConnectionCredentials() credentials {
	sslMode := warehouseutils.GetConfigValue(sslMode, pg.warehouse)
	creds := credentials{
		host:     warehouseutils.GetConfigValue(host, pg.warehouse),
		database: warehouseutils.GetConfigValue(dbName, pg.warehouse),
		user:     warehouseutils.GetConfigValue(user, pg.warehouse),
		password: warehouseutils.GetConfigValue(password, pg.warehouse),
		port:     warehouseutils.GetConfigValue(port, pg.warehouse),
		sslMode:  sslMode,
		sslDir:   warehouseutils.GetSSLKeyDirPath(pg.warehouse.Destination.ID),
		timeout:  pg.connectTimeout,
		tunnelInfo: warehouseutils.ExtractTunnelInfoFromDestinationConfig(
			pg.warehouse.Destination.Config,
		),
	}

	return creds
}

func ColumnsWithDataTypes(columns model.TableSchema, prefix string) string {
	var arr []string
	for name, dataType := range columns {
		arr = append(arr, fmt.Sprintf(`"%s%s" %s`, prefix, name, rudderDataTypesMapToPostgres[dataType]))
	}
	return strings.Join(arr, ",")
}

func (*Postgres) IsEmpty(context.Context, model.Warehouse) (empty bool, err error) {
	return
}

// DeleteBy Need to create a structure with delete parameters instead of simply adding a long list of params
func (pg *Postgres) DeleteBy(ctx context.Context, tableNames []string, params warehouseutils.DeleteByParams) (err error) {
	pg.logger.Infof("PG: Cleaning up the following tables in postgres for PG:%s : %+v", tableNames, params)
	for _, tb := range tableNames {
		sqlStatement := fmt.Sprintf(`DELETE FROM "%[1]s"."%[2]s" WHERE
		context_sources_job_run_id <> $1 AND
		context_sources_task_run_id <> $2 AND
		context_source_id = $3 AND
		received_at < $4`,
			pg.namespace,
			tb,
		)
		pg.logger.Infof("PG: Deleting rows in table in postgres for PG:%s", pg.warehouse.Destination.ID)
		pg.logger.Debugf("PG: Executing the statement  %v", sqlStatement)
		if pg.config.enableDeleteByJobs {
			_, err = pg.db.ExecContext(ctx, sqlStatement,
				params.JobRunId,
				params.TaskRunId,
				params.SourceId,
				params.StartTime)
			if err != nil {
				pg.logger.Errorf("Error %s", err)
				return err
			}
		}

	}
	return nil
}

func (pg *Postgres) schemaExists(ctx context.Context, _ string) (exists bool, err error) {
	sqlStatement := fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = '%s');`, pg.namespace)
	err = pg.db.QueryRowContext(ctx, sqlStatement).Scan(&exists)
	return
}

func (pg *Postgres) CreateSchema(ctx context.Context) (err error) {
	var schemaExists bool
	schemaExists, err = pg.schemaExists(ctx, pg.namespace)
	if err != nil {
		pg.logger.Errorf("PG: Error checking if schema: %s exists: %v", pg.namespace, err)
		return err
	}
	if schemaExists {
		pg.logger.Infof("PG: Skipping creating schema: %s since it already exists", pg.namespace)
		return
	}
	sqlStatement := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %q`, pg.namespace)
	pg.logger.Infof("PG: Creating schema name in postgres for PG:%s : %v", pg.warehouse.Destination.ID, sqlStatement)
	_, err = pg.db.ExecContext(ctx, sqlStatement)
	return
}

func (pg *Postgres) createTable(ctx context.Context, name string, columns model.TableSchema) (err error) {
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%[1]s"."%[2]s" ( %v )`, pg.namespace, name, ColumnsWithDataTypes(columns, ""))
	pg.logger.Infof("PG: Creating table in postgres for PG:%s : %v", pg.warehouse.Destination.ID, sqlStatement)
	_, err = pg.db.ExecContext(ctx, sqlStatement)
	return
}

func (pg *Postgres) CreateTable(ctx context.Context, tableName string, columnMap model.TableSchema) (err error) {
	// set the schema in search path. so that we can query table with unqualified name which is just the table name rather than using schema.table in queries
	sqlStatement := fmt.Sprintf(`SET search_path to %q`, pg.namespace)
	_, err = pg.db.ExecContext(ctx, sqlStatement)
	if err != nil {
		return err
	}
	pg.logger.Infof("PG: Updated search_path to %s in postgres for PG:%s : %v", pg.namespace, pg.warehouse.Destination.ID, sqlStatement)
	err = pg.createTable(ctx, tableName, columnMap)
	return err
}

func (pg *Postgres) DropTable(ctx context.Context, tableName string) (err error) {
	sqlStatement := `DROP TABLE "%[1]s"."%[2]s"`
	pg.logger.Infof("PG: Dropping table in postgres for PG:%s : %v", pg.warehouse.Destination.ID, sqlStatement)
	_, err = pg.db.ExecContext(ctx, fmt.Sprintf(sqlStatement, pg.namespace, tableName))
	return
}

func (pg *Postgres) AddColumns(ctx context.Context, tableName string, columnsInfo []warehouseutils.ColumnInfo) (err error) {
	var (
		query        string
		queryBuilder strings.Builder
	)

	// set the schema in search path. so that we can query table with unqualified name which is just the table name rather than using schema.table in queries
	query = fmt.Sprintf(`SET search_path to %q`, pg.namespace)
	if _, err = pg.db.ExecContext(ctx, query); err != nil {
		return
	}
	pg.logger.Infof("PG: Updated search_path to %s in postgres for PG:%s : %v", pg.namespace, pg.warehouse.Destination.ID, query)

	queryBuilder.WriteString(fmt.Sprintf(`
		ALTER TABLE
		  %s.%s`,
		pg.namespace,
		tableName,
	))

	for _, columnInfo := range columnsInfo {
		queryBuilder.WriteString(fmt.Sprintf(` ADD COLUMN IF NOT EXISTS %q %s,`, columnInfo.Name, rudderDataTypesMapToPostgres[columnInfo.Type]))
	}

	query = strings.TrimSuffix(queryBuilder.String(), ",")
	query += ";"

	pg.logger.Infof("PG: Adding columns for destinationID: %s, tableName: %s with query: %v", pg.warehouse.Destination.ID, tableName, query)
	_, err = pg.db.ExecContext(ctx, query)
	return
}

func (*Postgres) AlterColumn(context.Context, string, string, string) (model.AlterTableResponse, error) {
	return model.AlterTableResponse{}, nil
}

func (pg *Postgres) TestConnection(ctx context.Context, warehouse model.Warehouse) error {
	if warehouse.Destination.Config["sslMode"] == verifyCA {
		if sslKeyError := warehouseutils.WriteSSLKeys(warehouse.Destination); sslKeyError.IsError() {
			return fmt.Errorf("writing ssl keys: %s", sslKeyError.Error())
		}
	}

	err := pg.db.PingContext(ctx)
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("connection timeout: %w", err)
	}
	if err != nil {
		return fmt.Errorf("pinging: %w", err)
	}

	return nil
}

func (pg *Postgres) Setup(_ context.Context, warehouse model.Warehouse, uploader warehouseutils.Uploader) (err error) {
	pg.warehouse = warehouse
	pg.namespace = warehouse.Namespace
	pg.uploader = uploader
	pg.objectStorage = warehouseutils.ObjectStorageType(warehouseutils.POSTGRES, warehouse.Destination.Config, pg.uploader.UseRudderStorage())
	pg.loadFileDownloader = downloader.NewDownloader(&warehouse, uploader, pg.config.numWorkersDownloadLoadFiles)

	pg.db, err = pg.connect()
	return err
}

func (*Postgres) CrashRecover(context.Context) {}

// FetchSchema queries postgres and returns the schema associated with provided namespace
func (pg *Postgres) FetchSchema(ctx context.Context) (model.Schema, model.Schema, error) {
	schema := make(model.Schema)
	unrecognizedSchema := make(model.Schema)

	sqlStatement := `
		SELECT
		  table_name,
		  column_name,
		  data_type
		FROM
		  INFORMATION_SCHEMA.COLUMNS
		WHERE
		  table_schema = $1
		  AND table_name NOT LIKE $2;
	`
	rows, err := pg.db.QueryContext(
		ctx,
		sqlStatement,
		pg.namespace,
		fmt.Sprintf(`%s%%`, warehouseutils.StagingTablePrefix(provider)),
	)
	if errors.Is(err, sql.ErrNoRows) {
		return schema, unrecognizedSchema, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("fetching schema: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var tableName, columnName, columnType string

		if err := rows.Scan(&tableName, &columnName, &columnType); err != nil {
			return nil, nil, fmt.Errorf("scanning schema: %w", err)
		}

		if _, ok := schema[tableName]; !ok {
			schema[tableName] = make(model.TableSchema)
		}
		if datatype, ok := postgresDataTypesMapToRudder[columnType]; ok {
			schema[tableName][columnName] = datatype
		} else {
			if _, ok := unrecognizedSchema[tableName]; !ok {
				unrecognizedSchema[tableName] = make(model.TableSchema)
			}
			unrecognizedSchema[tableName][columnName] = warehouseutils.MissingDatatype

			warehouseutils.WHCounterStat(warehouseutils.RudderMissingDatatype, &pg.warehouse, warehouseutils.Tag{Name: "datatype", Value: columnType}).Count(1)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("fetching schema: %w", err)
	}

	return schema, unrecognizedSchema, nil
}

func (pg *Postgres) Cleanup(context.Context) {
	if pg.db != nil {
		_ = pg.db.Close()
	}
}

func (*Postgres) LoadIdentityMergeRulesTable(context.Context) (err error) {
	return
}

func (*Postgres) LoadIdentityMappingsTable(context.Context) (err error) {
	return
}

func (*Postgres) DownloadIdentityRules(context.Context, *misc.GZipWriter) (err error) {
	return
}

func (pg *Postgres) GetTotalCountInTable(ctx context.Context, tableName string) (int64, error) {
	var (
		total        int64
		err          error
		sqlStatement string
	)
	sqlStatement = fmt.Sprintf(`
		SELECT count(*) FROM "%[1]s"."%[2]s";
	`,
		pg.namespace,
		tableName,
	)
	err = pg.db.QueryRowContext(ctx, sqlStatement).Scan(&total)
	return total, err
}

func (pg *Postgres) Connect(_ context.Context, warehouse model.Warehouse) (client.Client, error) {
	if warehouse.Destination.Config["sslMode"] == "verify-ca" {
		if err := warehouseutils.WriteSSLKeys(warehouse.Destination); err.IsError() {
			pg.logger.Error(err.Error())
			return client.Client{}, fmt.Errorf(err.Error())
		}
	}
	pg.warehouse = warehouse
	pg.namespace = warehouse.Namespace
	pg.objectStorage = warehouseutils.ObjectStorageType(
		warehouseutils.POSTGRES,
		warehouse.Destination.Config,
		misc.IsConfiguredToUseRudderObjectStorage(pg.warehouse.Destination.Config),
	)
	dbHandle, err := pg.connect()
	if err != nil {
		return client.Client{}, err
	}

	return client.Client{Type: client.SQLClient, SQL: dbHandle.DB}, err
}

func (pg *Postgres) LoadTestTable(ctx context.Context, _, tableName string, payloadMap map[string]interface{}, _ string) (err error) {
	sqlStatement := fmt.Sprintf(`INSERT INTO %q.%q (%v) VALUES (%s)`,
		pg.namespace,
		tableName,
		fmt.Sprintf(`%q, %q`, "id", "val"),
		fmt.Sprintf(`'%d', '%s'`, payloadMap["id"], payloadMap["val"]),
	)
	_, err = pg.db.ExecContext(ctx, sqlStatement)
	return
}

func (pg *Postgres) SetConnectionTimeout(timeout time.Duration) {
	pg.connectTimeout = timeout
}

func (*Postgres) ErrorMappings() []model.JobError {
	return errorsMappings
}
