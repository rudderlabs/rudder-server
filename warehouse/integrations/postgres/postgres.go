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

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/tunnelling"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/service/loadfiles/downloader"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const (
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
	DB                 *sqlmiddleware.DB
	Namespace          string
	ObjectStorage      string
	Warehouse          model.Warehouse
	Uploader           warehouseutils.Uploader
	connectTimeout     time.Duration
	conf               *config.Config
	logger             logger.Logger
	stats              stats.Stats
	LoadFileDownloader downloader.Downloader

	config struct {
		allowMerge                                bool
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

	pg.conf = conf
	pg.logger = log.Child("integrations").Child("postgres")
	pg.stats = stat

	pg.config.allowMerge = conf.GetBool("Warehouse.postgres.allowMerge", true)
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
		sqlmiddleware.WithStats(pg.stats),
		sqlmiddleware.WithLogger(pg.logger),
		sqlmiddleware.WithKeyAndValues(
			logfield.SourceID, pg.Warehouse.Source.ID,
			logfield.SourceType, pg.Warehouse.Source.SourceDefinition.Name,
			logfield.DestinationID, pg.Warehouse.Destination.ID,
			logfield.DestinationType, pg.Warehouse.Destination.DestinationDefinition.Name,
			logfield.WorkspaceID, pg.Warehouse.WorkspaceID,
			logfield.Schema, pg.Namespace,
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

		db, err = tunnelling.Connect(dsn.String(), cred.tunnelInfo.Config)
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
	sslMode := pg.Warehouse.GetStringDestinationConfig(pg.conf, model.SSLModeSetting)
	creds := credentials{
		host:     pg.Warehouse.GetStringDestinationConfig(pg.conf, model.HostSetting),
		database: pg.Warehouse.GetStringDestinationConfig(pg.conf, model.DatabaseSetting),
		user:     pg.Warehouse.GetStringDestinationConfig(pg.conf, model.UserSetting),
		password: pg.Warehouse.GetStringDestinationConfig(pg.conf, model.PasswordSetting),
		port:     pg.Warehouse.GetStringDestinationConfig(pg.conf, model.PortSetting),
		sslMode:  sslMode,
		sslDir:   warehouseutils.GetSSLKeyDirPath(pg.Warehouse.Destination.ID),
		timeout:  pg.connectTimeout,
		tunnelInfo: tunnelling.ExtractTunnelInfoFromDestinationConfig(
			pg.Warehouse.Destination.Config,
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
			pg.Namespace,
			tb,
		)
		pg.logger.Infof("PG: Deleting rows in table in postgres for PG:%s", pg.Warehouse.Destination.ID)
		pg.logger.Debugf("PG: Executing the statement  %v", sqlStatement)
		if pg.config.enableDeleteByJobs {
			_, err = pg.DB.ExecContext(ctx, sqlStatement,
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
	sqlStatement := fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = '%s');`, pg.Namespace)
	err = pg.DB.QueryRowContext(ctx, sqlStatement).Scan(&exists)
	return
}

func (pg *Postgres) CreateSchema(ctx context.Context) (err error) {
	var schemaExists bool
	schemaExists, err = pg.schemaExists(ctx, pg.Namespace)
	if err != nil {
		pg.logger.Errorf("PG: Error checking if schema: %s exists: %v", pg.Namespace, err)
		return err
	}
	if schemaExists {
		pg.logger.Infof("PG: Skipping creating schema: %s since it already exists", pg.Namespace)
		return
	}
	sqlStatement := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %q`, pg.Namespace)
	pg.logger.Infof("PG: Creating schema name in postgres for PG:%s : %v", pg.Warehouse.Destination.ID, sqlStatement)
	_, err = pg.DB.ExecContext(ctx, sqlStatement)
	return
}

func (pg *Postgres) createTable(ctx context.Context, name string, columns model.TableSchema) (err error) {
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%[1]s"."%[2]s" ( %v )`, pg.Namespace, name, ColumnsWithDataTypes(columns, ""))
	pg.logger.Infof("PG: Creating table in postgres for PG:%s : %v", pg.Warehouse.Destination.ID, sqlStatement)
	_, err = pg.DB.ExecContext(ctx, sqlStatement)
	return
}

func (pg *Postgres) CreateTable(ctx context.Context, tableName string, columnMap model.TableSchema) (err error) {
	// set the schema in search path. so that we can query table with unqualified name which is just the table name rather than using schema.table in queries
	sqlStatement := fmt.Sprintf(`SET search_path to %q`, pg.Namespace)
	_, err = pg.DB.ExecContext(ctx, sqlStatement)
	if err != nil {
		return err
	}
	pg.logger.Infof("PG: Updated search_path to %s in postgres for PG:%s : %v", pg.Namespace, pg.Warehouse.Destination.ID, sqlStatement)
	err = pg.createTable(ctx, tableName, columnMap)
	return err
}

func (pg *Postgres) DropTable(ctx context.Context, tableName string) (err error) {
	sqlStatement := `DROP TABLE "%[1]s"."%[2]s"`
	pg.logger.Infof("PG: Dropping table in postgres for PG:%s : %v", pg.Warehouse.Destination.ID, sqlStatement)
	_, err = pg.DB.ExecContext(ctx, fmt.Sprintf(sqlStatement, pg.Namespace, tableName))
	return
}

func (pg *Postgres) AddColumns(ctx context.Context, tableName string, columnsInfo []warehouseutils.ColumnInfo) (err error) {
	var (
		query        string
		queryBuilder strings.Builder
	)

	// set the schema in search path. so that we can query table with unqualified name which is just the table name rather than using schema.table in queries
	query = fmt.Sprintf(`SET search_path to %q`, pg.Namespace)
	if _, err = pg.DB.ExecContext(ctx, query); err != nil {
		return
	}
	pg.logger.Infof("PG: Updated search_path to %s in postgres for PG:%s : %v", pg.Namespace, pg.Warehouse.Destination.ID, query)

	queryBuilder.WriteString(fmt.Sprintf(`
		ALTER TABLE
		  %s.%s`,
		pg.Namespace,
		tableName,
	))

	for _, columnInfo := range columnsInfo {
		queryBuilder.WriteString(fmt.Sprintf(` ADD COLUMN IF NOT EXISTS %q %s,`, columnInfo.Name, rudderDataTypesMapToPostgres[columnInfo.Type]))
	}

	query = strings.TrimSuffix(queryBuilder.String(), ",")
	query += ";"

	pg.logger.Infof("PG: Adding columns for destinationID: %s, tableName: %s with query: %v", pg.Warehouse.Destination.ID, tableName, query)
	_, err = pg.DB.ExecContext(ctx, query)
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

	err := pg.DB.PingContext(ctx)
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("connection timeout: %w", err)
	}
	if err != nil {
		return fmt.Errorf("pinging: %w", err)
	}

	return nil
}

func (pg *Postgres) Setup(_ context.Context, warehouse model.Warehouse, uploader warehouseutils.Uploader) (err error) {
	pg.Warehouse = warehouse
	pg.Namespace = warehouse.Namespace
	pg.Uploader = uploader
	pg.ObjectStorage = warehouseutils.ObjectStorageType(warehouseutils.POSTGRES, warehouse.Destination.Config, pg.Uploader.UseRudderStorage())
	pg.LoadFileDownloader = downloader.NewDownloader(&warehouse, uploader, pg.config.numWorkersDownloadLoadFiles)

	pg.DB, err = pg.connect()
	return err
}

// FetchSchema queries postgres and returns the schema associated with provided namespace
func (pg *Postgres) FetchSchema(ctx context.Context) (model.Schema, error) {
	schema := make(model.Schema)

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
	rows, err := pg.DB.QueryContext(
		ctx,
		sqlStatement,
		pg.Namespace,
		fmt.Sprintf(`%s%%`, warehouseutils.StagingTablePrefix(provider)),
	)
	if errors.Is(err, sql.ErrNoRows) {
		return schema, nil
	}
	if err != nil {
		return nil, fmt.Errorf("fetching schema: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var tableName, columnName, columnType string

		if err := rows.Scan(&tableName, &columnName, &columnType); err != nil {
			return nil, fmt.Errorf("scanning schema: %w", err)
		}

		if _, ok := schema[tableName]; !ok {
			schema[tableName] = make(model.TableSchema)
		}
		if datatype, ok := postgresDataTypesMapToRudder[columnType]; ok {
			schema[tableName][columnName] = datatype
		} else {
			warehouseutils.WHCounterStat(pg.stats, warehouseutils.RudderMissingDatatype, &pg.Warehouse, warehouseutils.Tag{Name: "datatype", Value: columnType}).Count(1)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("fetching schema: %w", err)
	}

	return schema, nil
}

func (pg *Postgres) Cleanup(context.Context) {
	if pg.DB != nil {
		_ = pg.DB.Close()
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

func (pg *Postgres) Connect(_ context.Context, warehouse model.Warehouse) (client.Client, error) {
	if warehouse.Destination.Config["sslMode"] == "verify-ca" {
		if err := warehouseutils.WriteSSLKeys(warehouse.Destination); err.IsError() {
			pg.logger.Error(err.Error())
			return client.Client{}, errors.New(err.Error())
		}
	}
	pg.Warehouse = warehouse
	pg.Namespace = warehouse.Namespace
	pg.ObjectStorage = warehouseutils.ObjectStorageType(
		warehouseutils.POSTGRES,
		warehouse.Destination.Config,
		misc.IsConfiguredToUseRudderObjectStorage(pg.Warehouse.Destination.Config),
	)
	db, err := pg.connect()
	if err != nil {
		return client.Client{}, err
	}

	return client.Client{Type: client.SQLClient, SQL: db.DB}, err
}

func (pg *Postgres) LoadTestTable(ctx context.Context, _, tableName string, payloadMap map[string]interface{}, _ string) (err error) {
	sqlStatement := fmt.Sprintf(`INSERT INTO %q.%q (%v) VALUES (%s)`,
		pg.Namespace,
		tableName,
		fmt.Sprintf(`%q, %q`, "id", "val"),
		fmt.Sprintf(`'%d', '%s'`, payloadMap["id"], payloadMap["val"]),
	)
	_, err = pg.DB.ExecContext(ctx, sqlStatement)
	return
}

func (pg *Postgres) SetConnectionTimeout(timeout time.Duration) {
	pg.connectTimeout = timeout
}

func (*Postgres) ErrorMappings() []model.JobError {
	return errorsMappings
}
